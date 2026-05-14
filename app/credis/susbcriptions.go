package credis

import (
	llist "container/list"
	"context"
	"errors"
	"slices"
	"sync"
)

type Sub struct {
	C       chan string
	Channel string
	el      *llist.Element
}

type Subscription interface {
	Subscribe(to string, clientId string) (*Sub, int, error)
	Count(clientId string) int
	IsAllowed(cmd string, clientId string) bool
	Publish(to string, msg string) int
	Cancel(clientId, channelId string) int
}

type subscription struct {
	// client id -> channelId -> cancel()
	subMapping map[string]map[string]*Sub
	mu         sync.RWMutex
	list       map[string]*llist.List // list per channel
	count      map[string]int         // counts per client
}

func NewSubscriptionManager() Subscription {
	return &subscription{
		list:       make(map[string]*llist.List),
		count:      make(map[string]int),
		subMapping: make(map[string]map[string]*Sub),
	}
}

func (s *subscription) Subscribe(to string, clientId string) (*Sub, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.list[to]
	if l == nil {
		s.list[to] = llist.New()
		l = s.list[to]
	}
	c := make(chan string)
	el := l.PushBack(c)
	s.count[clientId]++
	if el == nil {
		return nil, 0, errors.New("memory alloc error")
	}
	sub := Sub{
		C:       c,
		Channel: to,
		el:      el,
	}
	if s.subMapping[clientId] == nil {
		s.subMapping[clientId] = map[string]*Sub{}
	}
	s.subMapping[clientId][to] = &sub
	return &sub, s.count[clientId], nil
}

func (s *subscription) Count(clientId string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.count[clientId]
}

func (s *subscription) IsAllowed(cmd string, clientId string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	allowedCmdsInSubMode := []string{
		SUBSCRIBE,
		"PSUBSCRIBE",
		"PUNSUBSCRIBE",
		UNSUBSCRIBE,
		PING,
		QUIT,
	}
	if s.count[clientId] > 0 {
		return slices.Contains(allowedCmdsInSubMode, cmd)
	} else {
		return cmd != QUIT
	}
}

func (s *subscription) Publish(to string, msg string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	subs := s.list[to]
	chans := []chan string{}
	count := subs.Len()
	sub := subs.Front()
	for sub != nil {
		c := sub.Value.(chan string)
		chans = append(chans, c)
		sub = sub.Next()
	}
	go func() {
		for _, c := range chans {
			c <- msg
		}
	}()
	return count
}

func (s *subscription) Cancel(clientId, channelId string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subMapping[clientId] == nil {
		return s.count[clientId]
	}
	if s.subMapping[clientId][channelId] == nil {
		return s.count[clientId]
	}
	sub := s.subMapping[clientId][channelId]
	l := s.list[channelId]
	l.Remove(sub.el)
	s.count[clientId]--
	delete(s.subMapping[clientId], channelId)
	return s.count[clientId]
}

func ListenForMsgs(ctx context.Context, s *Sub, client Client) {
l:
	for {
		select {
		case <-ctx.Done():
			break l
		case msg := <-s.C:
			enc := NewEncoder()
			enc.Array([]Token{
				NewToken(BULK_STRING, "message"),
				NewToken(BULK_STRING, s.Channel),
				NewToken(BULK_STRING, msg),
			}...)
			client.Write(enc.Commit().Bytes())
		}
	}
}

func SubscriptionCommandGuardMiddleware(e *executor, req Request, res Response, terminate TerminateFunc) {
	isAllowed := e.deps.SubManager.IsAllowed(
		req.Specs().String(),
		req.ClientId(),
	)
	if !isAllowed {
		terminate(&NoOtherCommandsInSubscribeContext{cmd: req.Specs().String()})
		return
	}
}

func SubListenerMiddleware(e *executor, req Request, res Response, terminate TerminateFunc) {
	if res.Artifacts() == nil {
		return
	}
	if sub, ok := res.Artifacts().(*Sub); ok {
		go ListenForMsgs(req.Client().Ctx(), sub, req.Client())
	}
}
