package credis

import (
	"bufio"
	"bytes"
	"fmt"
)

type Encoder struct {
	isDesposed bool
	writer     *bufio.Writer
	buff       *bytes.Buffer
	err        error
}

func NewEncoder() *Encoder {
	buff := bytes.NewBuffer(nil)
	writer := bufio.NewWriter(buff)
	return &Encoder{
		writer: writer,
		buff:   buff,
	}
}

func (e *Encoder) Error() error {
	return e.err
}

func (e *Encoder) Bytes() []byte {
	if !e.isDesposed {
		e.err = fmt.Errorf("encoding process has not been commited yet")
		return []byte{}
	}
	return e.buff.Bytes()
}

func (e *Encoder) Commit() *Encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return e
	}
	e.isDesposed = true
	e.err = e.writer.Flush()
	return e
}

func (e *Encoder) EncodeToken(t Token) {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return
	}
	switch t.Type {
	case BULK_STRING:
		t, _ := t.Literal.(string)
		e.BulkString(&t)
	case SIMPLE_STRING:
		e.SimpleString(t.Literal.(string))
	case INTEGER:
		e.Integer(t.Literal.(int))
	case SIMPLE_ERROR:
		e.SimpleError(t.Literal.(string))
	case ARRAY:
		e.Array(t.Literal.([]Token)...)
	default:
		// TODO: Support other types
		e.err = &UnsupportedTypeForEncoding{
			typ: t.Type,
		}
	}
}

func (e *Encoder) Array(args ...Token) *Encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return e
	}
	// Write Length
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", ARRAY, len(args))
	if e.err != nil {
		return e
	}
	for _, t := range args {
		e.EncodeToken(t)
	}
	return e
}

func (e *Encoder) BulkString(data *string) *Encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return e
	}
	if data == nil {
		// Null bulk string
		_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", BULK_STRING, -1)
		return e
	}
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n%v\r\n", BULK_STRING, len(*data), *data)
	return e
}

func (e *Encoder) SimpleString(data string) *Encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return e
	}
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", SIMPLE_STRING, data)
	return e
}

func (e *Encoder) Integer(data int) *Encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return e
	}
	// :[<+|->]<value>\r\n
	sign := ""
	if data < 0 {
		sign = "-"
	}
	_, e.err = fmt.Fprintf(e.writer, "%v%v%v\r\n", INTEGER, sign, data)
	return e
}

func (e *Encoder) SimpleError(err string) *Encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return e
	}
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", SIMPLE_ERROR, err)
	return e
}

func (e *Encoder) ArrayRaw(rawData [][]byte) *Encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return e
	}
	// Write Length
	_, e.err = fmt.Fprintf(e.writer, "%v%v\r\n", ARRAY, len(rawData))
	if e.err != nil {
		return e
	}
	for _, rawResponse := range rawData {
		e.writer.Write(rawResponse)
	}
	return e
}

func (e *Encoder) RawBytes(data []byte) *Encoder {
	if e.isDesposed {
		e.err = fmt.Errorf("encoding process is already commited")
		return e
	}
	_, e.err = e.writer.Write(data)
	return e
}

func (e *Encoder) Ok() []byte {
	return e.SimpleString("OK").Commit().Bytes()
}

func (e *Encoder) NullArray() []byte {
	return e.RawBytes([]byte("*-1\r\n")).Commit().Bytes()
}
