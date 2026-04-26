package credis

import (
	"fmt"
	"strconv"
	"strings"
)

func (s *ECHOSpecs) Parse(args ...Token) error {
	var data strings.Builder
	for _, arg := range args {
		switch arg.Type {
		case BULK_STRING, SIMPLE_STRING:
			data.WriteString(arg.Literal.(string))
		default:
			return fmt.Errorf("unsupported type as a string conv: %v", arg.Literal)
			// TODO: Maybe need to convert other data to string?
		}
	}
	s.Data = data.String()
	return nil
}

func (s *REPLCONFSpecs) ParseFlags(args ...Token) error {
	subCmd := args[0].Literal.(string)
	value := args[1].Literal.(string)
	switch subCmd {
	case "listening-port":
		s.ListeningPort = &value
	case "capa":
		s.Capability = &value
	case "GETACK", "getack":
		s.GetAck = &value
	default:
		return fmt.Errorf("unsupported args for REPLCONF")
	}
	return nil
}

func (s *SETSpecs) ParseTail(args ...Token) error {
	index := 0
	for index < len(args)-1 {
		subCmd := strings.ToLower(args[index].Literal.(string))
		switch subCmd {
		case "px":
			if len(args) <= index+1 {
				return fmt.Errorf("no value for px option in SET command")
			}
			subCmdVal := args[index+1].Literal.(string)

			pxVal, err := strconv.ParseUint(subCmdVal, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid value for px option in SET command")
			}
			s.Px = &pxVal
		default:
			return fmt.Errorf("invalid option %v passed for SET", subCmd)
		}
		index += 2
	}
	return nil
}

func (spec *XADDSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for XADD", invalidIndex)
	}
	if len(args[2:])%2 != 0 {
		return fmt.Errorf("value for key %v is not provided", args[len(args)-1])
	}
	// validate stream ID
	// Possible values
	// - number-number
	// - number-*
	// - *
	spec.Key = args[0].Literal.(string)
	streamId := args[1].Literal.(string)
	if streamId == "*" {
		return nil
	}
	ids := strings.Split(streamId, "-")
	if len(ids) == 2 {
		for index, id := range ids {
			if index == 1 && id == "*" {
				continue
			}
			if val, err := strconv.ParseInt(id, 10, 64); err != nil {
				return fmt.Errorf("invalid stream id for command XADD")
			} else {
				if index == 0 {
					spec.Id = &val
				} else {
					spec.Seq = &val
				}
			}
		}
	}
	i := 2
	for i < len(args[2:])-1 {
		key := args[i].Literal.(string)
		value := args[i+1].Literal.(string)
		spec.KVs = append(spec.KVs, KeyValue{
			Key:   key,
			Value: value,
		})
		i += 2
	}
	return nil
}

func (specs *BLPOPSpecs) Parse(args ...Token) error {
	if isAllString, invalidIndex := IsAllString(args); !isAllString {
		return fmt.Errorf("arg at index %v has invalid type for LLEN", invalidIndex)
	}
	length := len(args)
	for _, key := range args[:length-1] {
		specs.Keys = append(specs.Keys, key.Literal.(string))
	}
	if parsed, err := strconv.ParseFloat(args[length-1].Literal.(string), 64); err != nil {
		specs.Keys = append(specs.Keys, args[length-1].Literal.(string))
	} else if parsed != 0 {
		specs.Lifetime = &parsed
	}
	return nil
}
