package executor

import "fmt"

type UnsupportedCommandForExecution struct {
	cmd string
}

type ErrNotInteger struct {
	data any
}

type ErrExecWithoutMulti struct{}

type ErrDiscardWithoutMulti struct{}

func (e *UnsupportedCommandForExecution) Error() string {
	return fmt.Sprintf("unsupported command: %v", e.cmd)
}

func (e *ErrNotInteger) Error() string {
	return fmt.Sprintf("value is not an integer: %v", e.data)
}

func (e *ErrExecWithoutMulti) Error() string {
	return fmt.Sprintln("EXEC without MULTI")
}

func (e *ErrDiscardWithoutMulti) Error() string {
	return fmt.Sprintln("DISCARD without MULTI")
}
