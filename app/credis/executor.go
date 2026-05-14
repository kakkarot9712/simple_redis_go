package credis

import (
	"strings"
	"time"
)

type Middleware func(e *executor, req Request, res Response, terminate TerminateFunc)
type TerminateFunc func(e ...error)

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
	Execute(e *executor, req Request, res Response) error
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
	var res response
	for _, m := range e.middlewares {
		var terminate bool
		var errors []error
		terminateFunc := func(errs ...error) {
			terminate = true
			errors = append(errors, errs...)
		}
		terminate = false
		m(e, req, &res, terminateFunc)
		if len(errors) > 0 {
			var errStr strings.Builder
			for _, e := range errors {
				errStr.Write([]byte(e.Error()))
			}
			return &response{
				data: NewEncoder().SimpleError(errStr.String()),
			}
		}
		if terminate {
			return &res
		}
	}
	return &res
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

func ExecutorMiddleware(e *executor, req Request, res Response, terminate TerminateFunc) {
	if vp, ok := req.Specs().(Exec); ok {
		err := vp.Execute(e, req, res)
		if err != nil {
			terminate(err)
		}
		return
	}
	terminate(&ErrNotImplimented{})
}
