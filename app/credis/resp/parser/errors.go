package parser

import "fmt"

type BufferIncomplete struct{}

func (e *BufferIncomplete) Error() string {
	return "Buffer is incomplete to parse"
}

type BufferSchemaInvalid struct {
	rawError error
}

func (e *BufferSchemaInvalid) Error() string {
	return fmt.Sprintf("Invalis RESP schema: %v", e.rawError)
}
