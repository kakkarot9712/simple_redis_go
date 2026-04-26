package credis

import (
	"bufio"
	"bytes"
	"fmt"
)

type encoder struct {
	isDesposed bool
	writer     *bufio.Writer
	buff       *bytes.Buffer
	err        error
}

type Encoder interface {
	Error() error
	Commit() CommitedEncoder
	EncodeToken(t Token)
	Array(args ...Token) []byte
	BulkString(data *string) []byte
	SimpleString(data string) []byte
	Integer(data int) []byte
	SimpleError(err string) []byte
	ArrayRaw(rawData [][]byte) []byte
	Ok() []byte
	NullArray() []byte
}

type CommitedEncoder interface {
	Bytes() []byte
	Error() error
}

func NewEncoder() Encoder {
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

func (e *encoder) Commit() CommitedEncoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return e
	}
	e.isDesposed = true
	e.err = e.writer.Flush()
	return e
}

func (e *encoder) EncodeToken(t Token) {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return
	}
	switch t.Type {
	case BULK_STRING:
		t, _ := t.Literal.(string)
		e.bulkString(&t)
	case SIMPLE_STRING:
		e.simpleString(t.Literal.(string))
	case INTEGER:
		e.integer(t.Literal.(int))
	case SIMPLE_ERROR:
		e.simpleError(t.Literal.(string))
	case ARRAY:
		e.array(t.Literal.([]Token)...)
	default:
		// TODO: Support other types
		e.err = &UnsupportedTypeForEncoding{
			typ: t.Type,
		}
	}
}

func (e *encoder) Array(args ...Token) []byte {
	return e.array(args...).Commit().Bytes()
}

func (e *encoder) array(args ...Token) *encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return nil
	}
	// Write Length
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", ARRAY, len(args))
	if e.err != nil {
		return nil
	}
	for _, t := range args {
		e.EncodeToken(t)
	}
	return e
}

func (e *encoder) BulkString(data *string) []byte {
	return e.bulkString(data).Commit().Bytes()
}

func (e *encoder) bulkString(data *string) *encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return e
	}
	if data == nil {
		// Null bulk string
		_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", BULK_STRING, -1)
	} else {
		_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n%v\r\n", BULK_STRING, len(*data), *data)
	}
	return e
}

func (e *encoder) SimpleString(data string) []byte {
	return e.simpleString(data).Commit().Bytes()
}

func (e *encoder) simpleString(data string) *encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return nil
	}
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", SIMPLE_STRING, data)
	return e
}

func (e *encoder) Integer(data int) []byte {
	return e.integer(data).Commit().Bytes()
}

func (e *encoder) integer(data int) *encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return nil
	}
	// :[<+|->]<value>\r\n
	sign := ""
	if data < 0 {
		sign = "-"
	}
	_, e.err = fmt.Fprintf(e.writer, "%v%v%v\r\n", INTEGER, sign, data)
	return e
}

func (e *encoder) SimpleError(err string) []byte {
	return e.simpleError(err).Commit().Bytes()
}

func (e *encoder) simpleError(err string) *encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return nil
	}
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", SIMPLE_ERROR, err)
	return e
}

func (e *encoder) ArrayRaw(rawData [][]byte) []byte {
	return e.arrayRaw(rawData).Commit().Bytes()
}

func (e *encoder) arrayRaw(rawData [][]byte) *encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return nil
	}
	// Write Length
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", ARRAY, len(rawData))
	if e.err != nil {
		return nil
	}
	for _, rawResponse := range rawData {
		e.writer.Write(rawResponse)
	}
	return e
}

func (e *encoder) RawBytes(data []byte) []byte {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return nil
	}
	_, e.err = e.writer.Write(data)
	return e.Commit().Bytes()
}

func (e *encoder) Ok() []byte {
	return e.SimpleString("OK")
}

func (e *encoder) NullArray() []byte {
	return e.RawBytes([]byte("*-1\r\n"))
}
