package parser

import (
	"bufio"
	"fmt"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/tokens"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/types"
)

type Parser struct {
	reader *bufio.Reader
	err    error
}

func New(reader *bufio.Reader) *Parser {
	return &Parser{
		reader: reader,
	}
}

func (p *Parser) TryParse() (tokens.Token, int) {
	b, err := p.reader.ReadByte()
	if err != nil {
		p.err = err
		return tokens.Token{}, 0
	}
	tokenType := string(b)
	switch tokenType {
	case types.BULK_STRING:
		return p.bulkString()
	case types.SIMPLE_STRING:
		return p.simpleString()
	case types.ARRAY:
		return p.array()
	default:
		p.setBufferInvalidError(fmt.Errorf("invalid character in buffer: %v", string(b)))
		p.reader.UnreadByte()
		return tokens.Token{}, 0
	}
}

func (p *Parser) ProcessRDB() {
	b, err := p.reader.ReadByte()
	if err != nil || b != '$' {
		p.err = fmt.Errorf("not RDB file")
		p.reader.UnreadByte()
		return
	}
	lenght, _ := p.length()
	if p.err != nil {
		return
	}
	strBytes := make([]byte, lenght)
	_, err = p.reader.Read(strBytes)
	if err != nil {
		p.err = err
		return
	}
}

func (p *Parser) Error() error {
	return p.err
}

func (p *Parser) bulkString() (tokens.Token, int) {
	bytesProcessed := 1
	lenght, bytesConsumed := p.length()
	if p.err != nil {
		return tokens.New(types.BULK_STRING, ""), 0
	}
	bytesProcessed += bytesConsumed
	strBytes := make([]byte, lenght)
	n, err := p.reader.Read(strBytes)
	bytesProcessed += n
	if err != nil {
		p.err = err
		return tokens.New(types.BULK_STRING, ""), 0
	}

	p.validateEnd()
	bytesProcessed += 2
	if p.err != nil {
		return tokens.New(types.BULK_STRING, ""), 0
	}
	return tokens.New(types.BULK_STRING, string(strBytes)), bytesProcessed
}

func (p *Parser) array() (tokens.Token, int) {
	bytesProcessed := 1
	elementLength, bytesConsumed := p.length()
	if p.err != nil {
		return tokens.New(types.ARRAY, []any{}), 0
	}
	bytesProcessed += bytesConsumed
	elements := make([]tokens.Token, 0)
	if p.err != nil {
		return tokens.New(types.ARRAY, elements), 0
	}
	for range elementLength {
		t, n := p.TryParse()
		if p.err != nil {
			return tokens.New(types.ARRAY, make([]tokens.Token, 0)), 0
		}
		elements = append(elements, t)
		bytesProcessed += n
	}
	return tokens.New(types.ARRAY, elements), bytesProcessed
}

func (p *Parser) simpleString() (tokens.Token, int) {
	bytesProcessed := 1
	str := ""
	isLastByte := false
	for {
		b, err := p.reader.ReadByte()
		if err != nil {
			p.err = err
			return tokens.New(types.SIMPLE_STRING, ""), 0
		}
		bytesProcessed++
		if b == '\r' {
			if isLastByte {
				p.err = fmt.Errorf("forbidden character received")
				return tokens.New(types.SIMPLE_STRING, ""), 0
			}
			isLastByte = true
		} else if b == '\n' {
			if !isLastByte {
				p.err = fmt.Errorf("unexpected end of stream")
				return tokens.New(types.SIMPLE_STRING, ""), 0
			}
			break
		} else {
			str += string(b)
		}
	}
	return tokens.New(types.SIMPLE_STRING, str), bytesProcessed
}

func (p *Parser) length() (int, int) {
	bytesProcessed := 0
	lenBytes := []byte{}
	isSecondLastByte := false
	for {
		b, err := p.reader.ReadByte()
		bytesProcessed++
		if err != nil {
			p.err = err
			return 0, 0
		}
		if b == '\r' && !isSecondLastByte {
			isSecondLastByte = true
		} else if b == '\n' && isSecondLastByte {
			// All Ok
			break
		} else if !p.isNumber(b) {
			p.setBufferInvalidError(fmt.Errorf("invalid character received as a number: %v", b))
			return 0, 0
		} else {
			lenBytes = append(lenBytes, b)
		}
	}
	num, err := strconv.ParseUint(string(lenBytes), 10, 64)
	if err != nil {
		p.setBufferInvalidError(err)
		return 0, 0
	}
	return int(num), bytesProcessed
}

func (p *Parser) isNumber(b byte) bool {
	if b >= '0' && b <= '9' {
		return true
	}
	return false
}

func (p *Parser) setBufferInvalidError(err error) {
	p.err = &BufferSchemaInvalid{
		rawError: err,
	}
}

// *3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"

func (p *Parser) validateEnd() {
	// Check end of string
	b, err := p.reader.ReadByte()
	if err != nil {
		p.err = err
		return
	}

	if b != '\r' {
		p.setBufferInvalidError(fmt.Errorf("unexpected end of buffer: %v", b))
		return
	}

	b, err = p.reader.ReadByte()
	if err != nil {
		p.err = err
		return
	}

	if b != '\n' {
		p.setBufferInvalidError(fmt.Errorf("unexpected end of buffer: %v", b))
		return
	}
}
