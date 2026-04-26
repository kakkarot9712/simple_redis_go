package credis

import "math"

const LAT = 0
const LNG = 1
const MIN_LATITUDE = -85.05112878
const MAX_LATITUDE = 85.05112878
const MIN_LONGITUDE = -180
const MAX_LONGITUDE = 180
const LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
const LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE

func ValidateCoords(val float64, typ uint8) bool {
	isValid := false
	switch typ {
	case LNG:
		isValid = val >= MIN_LONGITUDE && val <= MAX_LONGITUDE
	case LAT:
		isValid = val >= MIN_LATITUDE && val <= MAX_LATITUDE
	}
	return isValid
}

func normalizeCoords(val float64, typ uint8) int {
	norms := 0.0
	switch typ {
	case LNG:
		norms = math.Pow(2, 26) * (val - MIN_LONGITUDE) / LONGITUDE_RANGE
	case LAT:
		norms = math.Pow(2, 26) * (val - MIN_LATITUDE) / LATITUDE_RANGE
	}
	return int(norms)
}

func interleave(x int, y int) int {
	// # First, the values are spread from 32-bit to 64-bit integers.
	// # This is done by inserting 32 zero bits in-between.
	// # Before spread: x1  x2  ...  x31  x32
	// # After spread:  0   x1  ...   0   x16  ... 0  x31  0  x32
	x = spreadInt32toInt64(x)
	y = spreadInt32toInt64(y)
	y_shifted := y << 1
	return x | y_shifted
}

func spreadInt32toInt64(v int) int {
	v = v & 0xFFFFFFFF

	// Bitwise operations to spread 32 bits into 64 bits with zeros in-between
	v = (v | (v << 16)) & 0x0000FFFF0000FFFF
	v = (v | (v << 8)) & 0x00FF00FF00FF00FF
	v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
	v = (v | (v << 2)) & 0x3333333333333333
	v = (v | (v << 1)) & 0x5555555555555555
	return v
}

func Score(lat float64, lng float64) int {
	return interleave(normalizeCoords(lat, LAT), normalizeCoords(lng, LNG))
}

func LatLng(scr int) (lat float64, lng float64) {
	// Extract longitude bits (they were shifted left by 1 during encoding)
	y := scr >> 1

	// Extract latitude bits (they were in the original positions)
	x := scr
	// Compact both latitude and longitude back to 32-bit integers
	grdLat := compactInt64toInt32(y)
	grdLng := compactInt64toInt32(x)
	grdLatMin := MIN_LATITUDE + LATITUDE_RANGE*(float64(grdLat)/(math.Pow(2, 26)))
	grdLatMax := MIN_LATITUDE + LATITUDE_RANGE*(float64(grdLat+1)/(math.Pow(2, 26)))
	grdLngMIn := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(grdLng)/math.Pow(2, 26))
	grdLngMax := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(grdLng+1)/(math.Pow(2, 26)))
	lat = (grdLatMin + grdLatMax) / 2
	lng = (grdLngMIn + grdLngMax) / 2
	return
}

func compactInt64toInt32(v int) int {
	// Keep only the bits in even positions
	v = v & 0x5555555555555555
	// Before masking: w1   v1  ...   w2   v16  ... w31  v31  w32  v32
	// After masking: 0   v1  ...   0   v16  ... 0  v31  0  v32

	// Where w1, w2,..w31 are the digits from longitude if we're compacting latitude, or digits from latitude if we're compacting longitude
	// So, we mask them out and only keep the relevant bits that we wish to compact

	//  ------
	// Reverse the spreading process by shifting and masking
	v = (v | (v >> 1)) & 0x3333333333333333
	v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
	v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
	v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
	v = (v | (v >> 16)) & 0x00000000FFFFFFFF

	// Before compacting: 0   v1  ...   0   v16  ... 0  v31  0  v32
	// After compacting: v1  v2  ...  v31  v32
	// -----
	return v
}
