package tokens

import "github.com/codecrafters-io/redis-starter-go/app/credis/resp/types"

type Token struct {
	Type    string
	Literal any
}

func New(tokenType string, literal any) Token {
	return Token{
		Type:    tokenType,
		Literal: literal,
	}
}

func (t *Token) IsPong() bool {
	return t.Type == types.SIMPLE_STRING && t.Literal.(string) == "PONG"
}

func (t *Token) IsOk() bool {
	return t.Type == types.SIMPLE_STRING && t.Literal.(string) == "OK"
}
