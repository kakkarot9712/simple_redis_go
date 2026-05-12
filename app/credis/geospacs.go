package credis

import (
	"math"
)

const LAT = 0
const LNG = 1
const MIN_LATITUDE = -85.05112878
const MAX_LATITUDE = 85.05112878
const MIN_LONGITUDE = -180
const MAX_LONGITUDE = 180
const LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
const LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE

const EARTH_RADIUS = 6372797.560856 // in meters

type Location struct {
	Lat float64
	Lng float64
}

func ValidateCoords(loc Location) bool {
	isValid := false
	if loc.Lat >= MIN_LATITUDE && loc.Lat <= MAX_LATITUDE &&
		loc.Lng >= MIN_LONGITUDE && loc.Lng <= MAX_LONGITUDE {
		isValid = true
	}
	return isValid
}

func normalizeCoords(loc Location) Location {
	normLoc := Location{
		Lng: math.Pow(2, 26) * (loc.Lng - MIN_LONGITUDE) / LONGITUDE_RANGE,
		Lat: math.Pow(2, 26) * (loc.Lat - MIN_LATITUDE) / LATITUDE_RANGE,
	}
	return normLoc
}

func interleave(x int32, y int32) int64 {
	// # First, the values are spread from 32-bit to 64-bit integers.
	// # This is done by inserting 32 zero bits in-between.
	// # Before spread: x1  x2  ...  x31  x32
	// # After spread:  0   x1  ...   0   x16  ... 0  x31  0  x32
	x64 := spreadInt32toInt64(x)
	y64 := spreadInt32toInt64(y)
	y_shifted := y64 << 1
	return x64 | y_shifted
}

func spreadInt32toInt64(v32 int32) int64 {
	v := int64(v32) & 0xFFFFFFFF

	// Bitwise operations to spread 32 bits into 64 bits with zeros in-between
	v = (v | (v << 16)) & 0x0000FFFF0000FFFF
	v = (v | (v << 8)) & 0x00FF00FF00FF00FF
	v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
	v = (v | (v << 2)) & 0x3333333333333333
	v = (v | (v << 1)) & 0x5555555555555555
	return v
}

func Score(loc Location) int64 {
	normLoc := normalizeCoords(loc)
	return interleave(int32(normLoc.Lat), int32(normLoc.Lng))
}

func LatLng(scr uint64) Location {
	// Extract longitude bits (they were shifted left by 1 during encoding)
	y := scr >> 1

	// Extract latitude bits (they were in the original positions)
	x := scr
	// Compact both latitude and longitude back to 32-bit integers
	grdLat := compactInt64ToInt32(x)
	grdLng := compactInt64ToInt32(y)
	return convertGridNumbersToCoordinates(grdLat, grdLng)
}

func compactInt64ToInt32(v uint64) uint32 {
	result := v & 0x5555555555555555
	result = (result | (result >> 1)) & 0x3333333333333333
	result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F
	result = (result | (result >> 4)) & 0x00FF00FF00FF00FF
	result = (result | (result >> 8)) & 0x0000FFFF0000FFFF
	result = (result | (result >> 16)) & 0x00000000FFFFFFFF
	return uint32(result)
}

func convertGridNumbersToCoordinates(gridLatitudeNumber, gridLongitudeNumber uint32) Location {
	// Calculate the grid boundaries
	gridLatitudeMin := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber)/math.Pow(2, 26))
	gridLatitudeMax := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber+1)/math.Pow(2, 26))
	gridLongitudeMin := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber)/math.Pow(2, 26))
	gridLongitudeMax := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber+1)/math.Pow(2, 26))

	// Calculate the center point of the grid cell
	lat := (gridLatitudeMin + gridLatitudeMax) / 2
	lng := (gridLongitudeMin + gridLongitudeMax) / 2

	return Location{
		Lat: lat,
		Lng: lng,
	}
}

func haversine(θ float64) float64 {
	return .5 * (1 - math.Cos(θ))
}

type pos struct {
	φ float64 // latitude, radians
	ψ float64 // longitude, radians
}

func degPos(loc Location) pos {
	return pos{loc.Lat * math.Pi / 180, loc.Lng * math.Pi / 180}
}

// Haversine dist in meters
func Dist(loc1 Location, loc2 Location) float64 {
	p1 := degPos(loc1)
	p2 := degPos(loc2)
	return 2 * EARTH_RADIUS * math.Asin(math.Sqrt(haversine(p2.φ-p1.φ)+
		math.Cos(p1.φ)*math.Cos(p2.φ)*haversine(p2.ψ-p1.ψ)))
}
