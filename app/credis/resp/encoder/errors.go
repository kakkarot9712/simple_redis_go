package encoder

import "fmt"

type UnsupportedTypeForEncoding struct {
	typ string
}

func (e *UnsupportedTypeForEncoding) Error() string {
	return fmt.Sprintf("unsupported type: %v", e.typ)
}
