package credis

import "fmt"

type UnsupportedCommandForExecution struct {
	cmd string
}

type ErrNotInteger struct {
	data any
}

type ErrExecWithoutMulti struct{}

type ErrDiscardWithoutMulti struct{}

type ErrInvalidStreamId struct{}

func (e *ErrInvalidStreamId) Error() string {
	return "ERR The ID specified in XADD must be greater than 0-0"
}

type ErrIdLessThenStreamTop struct{}

func (e *ErrIdLessThenStreamTop) Error() string {
	return "ERR The ID specified in XADD is equal or smaller than the target stream top item"
}

func (e *UnsupportedCommandForExecution) Error() string {
	return fmt.Sprintf("ERR unsupported command: %v", e.cmd)
}

func (e *ErrNotInteger) Error() string {
	return "ERR value is not an integer or out of range"
}

func (e *ErrExecWithoutMulti) Error() string {
	return "ERR EXEC without MULTI"
}

func (e *ErrDiscardWithoutMulti) Error() string {
	return "ERR DISCARD without MULTI"
}

type UnsupportedTypeForEncoding struct {
	typ string
}

func (e *UnsupportedTypeForEncoding) Error() string {
	return fmt.Sprintf("unsupported type: %v", e.typ)
}

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

type NoOtherCommandsInSubscribeContext struct {
	cmd string
}

func (e *NoOtherCommandsInSubscribeContext) Error() string {
	return fmt.Sprintf("ERR Can't execute '%v': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", e.cmd)
}
