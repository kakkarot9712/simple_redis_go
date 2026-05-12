package credis

import (
	"time"
)

type Middleware func(e *executor, req Request, res Response) error

type BLPOPHold struct {
	req     Request
	resp    []string
	keys    []string
	timeout *time.Time
}

type Executor interface {
	Use(m Middleware)
	Exec(req Request) Response
	processHold(hold *BLPOPHold) (concluded bool, resData []byte)
}

type Exec interface {
	Execute(e *executor, req Request) Response
}

// Executor must remain stateless to allow concurrent usage
type executor struct {
	deps        *deps
	middlewares []Middleware
}

func NewExec(deps *deps) Executor {
	return &executor{
		deps: deps,
	}
}

func (e *executor) Use(m Middleware) {
	e.middlewares = append(e.middlewares, m)
}

func (e *executor) Exec(req Request) Response {
	// e.processed = cfg.processedBytes
	if vp, ok := req.Specs().(Exec); ok {
		for _, m := range e.middlewares {
			var res response
			err := m(e, req, &res)
			if err != nil {
				return &response{
					data: NewEncoder().SimpleError(err.Error()),
				}
			} else if len(res.data) > 0 {
				return &res
			}
		}
		return vp.Execute(e, req)
	} else {
		return notImplemented(req.Specs().String())
	}
}

func (e *executor) processHold(hold *BLPOPHold) (concluded bool, resData []byte) {
	req := hold.req
	select {
	case <-req.Ctx().Done():
		concluded = true
	default:
		for i := 0; i < len(hold.keys); i++ {
			key := hold.keys[i]
			popped := e.deps.ListStore.Pop(key)
			if popped == nil {
				return
			}
			hold.resp = append(hold.resp, key, *popped)
		}
		concluded = true
		tokens := []Token{}
		for _, p := range hold.resp {
			tokens = append(tokens, NewToken(BULK_STRING, p))
		}
		resData = NewEncoder().Array(tokens...)
	}
	return
}
