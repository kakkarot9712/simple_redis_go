package encoder

import (
	"bufio"
	"bytes"
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/tokens"
	"github.com/codecrafters-io/redis-starter-go/app/credis/resp/types"
)

type encoder struct {
	isDesposed bool
	writer     *bufio.Writer
	buff       *bytes.Buffer
	err        error
}

type Encoder interface {
	EncodeToken(token tokens.Token)
	Error() error
	Bytes() []byte
	Array(args ...tokens.Token)
	BulkString(data string)
	SimpleString(data string)
	Integer(data int)
	SimpleError(err string)
	ArrayRaw(rawData [][]byte)
	RawBytes(data []byte)
	Commit()
}

func New() Encoder {
	buff := bytes.NewBuffer(nil)
	writer := bufio.NewWriter(buff)
	return &encoder{
		writer: writer,
		buff:   buff,
	}
}

func (e *encoder) Error() error {
	return e.err
}

func (e *encoder) Bytes() []byte {
	if !e.isDesposed {
		e.err = fmt.Errorf("encoding process has not been commited yet")
		return []byte{}
	}
	return e.buff.Bytes()
}

func (e *encoder) Commit() {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return
	}
	e.isDesposed = true
	e.err = e.writer.Flush()
}

func (e *encoder) EncodeToken(t tokens.Token) {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return
	}
	switch t.Type {
	case types.BULK_STRING:
		e.BulkString(t.Literal.(string))
	case types.SIMPLE_STRING:
		e.SimpleString(t.Literal.(string))
	case types.INTEGER:
		e.Integer(t.Literal.(int))
	case types.SIMPLE_ERROR:
		e.SimpleError(t.Literal.(string))
	case types.ARRAY:
		e.Array(t.Literal.([]tokens.Token)...)
	default:
		// TODO: Support other types
		e.err = &UnsupportedTypeForEncoding{
			typ: t.Type,
		}
	}
}

func (e *encoder) Array(args ...tokens.Token) {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return
	}
	// Write Length
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", types.ARRAY, len(args))
	if e.err != nil {
		return
	}
	for _, t := range args {
		e.EncodeToken(t)
	}
}

func (e *encoder) BulkString(data string) {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return
	}
	if data == "" {
		// Null bulk string
		_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", types.BULK_STRING, -1)
		return
	}
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n%v\r\n", types.BULK_STRING, len(data), data)
}

func (e *encoder) SimpleString(data string) {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return
	}
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", types.SIMPLE_STRING, data)
}

func (e *encoder) Integer(data int) {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return
	}
	// :[<+|->]<value>\r\n
	sign := "+"
	if data < 0 {
		sign = "-"
	}
	_, e.err = fmt.Fprintf(e.writer, "%v%v%v\r\n", types.INTEGER, sign, data)
}

func (e *encoder) SimpleError(err string) {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return
	}
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", types.SIMPLE_ERROR, err)
}

func (e *encoder) ArrayRaw(rawData [][]byte) {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return
	}
	// Write Length
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", types.ARRAY, len(rawData))
	if e.err != nil {
		return
	}
	for _, rawResponse := range rawData {
		e.writer.Write(rawResponse)
	}
}

func (e *encoder) RawBytes(data []byte) {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return
	}
	_, e.err = e.writer.Write(data)
}
