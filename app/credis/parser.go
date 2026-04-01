package credis

import (
	"bufio"
	"fmt"
	"strconv"
)

type Token struct {
	Type    string
	Literal any
}

func NewToken(tokenType string, literal any) Token {
	return Token{
		Type:    tokenType,
		Literal: literal,
	}
}

func (t *Token) IsPong() bool {
	return t.IsString() && t.Literal.(string) == "PONG"
}

func (t *Token) IsOk() bool {
	return t.IsString() && t.Literal.(string) == "OK"
}

func (t *Token) IsString() bool {
	switch t.Type {
	case SIMPLE_STRING, BULK_STRING:
		if _, ok := t.Literal.(string); ok {
			return true
		}
	}
	return false
}

func IsAllString(args []Token) (bool, int) {
	for index, arg := range args {
		if !arg.IsString() {
			return false, index
		}
	}
	return true, -1
}

type Parser interface {
	TryParse() (Token, int)
	Error() error
	ProcessRDB()
}

type parser struct {
	reader *bufio.Reader
	err    error
}

func NewParser(reader *bufio.Reader) Parser {
	return &parser{
		reader: reader,
	}
}

func (p *parser) TryParse() (Token, int) {
	b, err := p.reader.ReadByte()
	if err != nil {
		p.err = err
		return Token{}, 0
	}
	tokenType := string(b)
	switch tokenType {
	case BULK_STRING:
		return p.bulkString()
	case SIMPLE_STRING:
		return p.simpleString()
	case ARRAY:
		return p.array()
	default:
		p.setBufferInvalidError(fmt.Errorf("invalid character in buffer: %v", string(b)))
		p.reader.UnreadByte()
		return Token{}, 0
	}
}

func (p *parser) ProcessRDB() {
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

func (p *parser) Error() error {
	return p.err
}

func (p *parser) bulkString() (Token, int) {
	bytesProcessed := 1
	lenght, bytesConsumed := p.length()
	if p.err != nil {
		return NewToken(BULK_STRING, ""), 0
	}
	bytesProcessed += bytesConsumed
	strBytes := make([]byte, lenght)
	n, err := p.reader.Read(strBytes)
	bytesProcessed += n
	if err != nil {
		p.err = err
		return NewToken(BULK_STRING, ""), 0
	}

	p.validateEnd()
	bytesProcessed += 2
	if p.err != nil {
		return NewToken(BULK_STRING, ""), 0
	}
	return NewToken(BULK_STRING, string(strBytes)), bytesProcessed
}

func (p *parser) array() (Token, int) {
	bytesProcessed := 1
	elementLength, bytesConsumed := p.length()
	if p.err != nil {
		return NewToken(ARRAY, []any{}), 0
	}
	bytesProcessed += bytesConsumed
	elements := make([]Token, 0)
	if p.err != nil {
		return NewToken(ARRAY, elements), 0
	}
	for range elementLength {
		t, n := p.TryParse()
		if p.err != nil {
			return NewToken(ARRAY, make([]Token, 0)), 0
		}
		elements = append(elements, t)
		bytesProcessed += n
	}
	return NewToken(ARRAY, elements), bytesProcessed
}

func (p *parser) simpleString() (Token, int) {
	bytesProcessed := 1
	str := ""
	isLastByte := false
	for {
		b, err := p.reader.ReadByte()
		if err != nil {
			p.err = err
			return NewToken(SIMPLE_STRING, ""), 0
		}
		bytesProcessed++
		if b == '\r' {
			if isLastByte {
				p.err = fmt.Errorf("forbidden character received")
				return NewToken(SIMPLE_STRING, ""), 0
			}
			isLastByte = true
		} else if b == '\n' {
			if !isLastByte {
				p.err = fmt.Errorf("unexpected end of stream")
				return NewToken(SIMPLE_STRING, ""), 0
			}
			break
		} else {
			str += string(b)
		}
	}
	return NewToken(SIMPLE_STRING, str), bytesProcessed
}

func (p *parser) length() (int, int) {
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

func (p *parser) isNumber(b byte) bool {
	if b >= '0' && b <= '9' {
		return true
	}
	return false
}

func (p *parser) setBufferInvalidError(err error) {
	p.err = &BufferSchemaInvalid{
		rawError: err,
	}
}

// *3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"

func (p *parser) validateEnd() {
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
