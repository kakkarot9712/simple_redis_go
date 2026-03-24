package credis

import "slices"

type SubscriptionHandler interface {
	Subscribe(to string) error
	Unsubscribe(from string) error
	Count() int
	IsAllowed(cmd Cmd) bool
}

// It will be per connection
type subHandler struct {
	id            string
	count         int
	subscriptions map[string]bool
}

func NewSubscriptionProvider(id string) SubscriptionHandler {
	return &subHandler{
		id:            id,
		subscriptions: make(map[string]bool),
	}
}

func (s *subHandler) Subscribe(to string) error {
	s.subscriptions[to] = true
	s.count++
	return nil
}

func (s *subHandler) Unsubscribe(from string) error {
	delete(s.subscriptions, from)
	s.count--
	return nil
}

func (s *subHandler) Count() int {
	return s.count
}

func (s *subHandler) IsAllowed(cmd Cmd) bool {
	allowedCmdsInSubMode := []string{
		SUBSCRIBE,
		PSUBSCRIBE,
		PUNSUBSCRIBE,
		UNSUBSCRIBE,
		PING,
		QUIT,
	}
	if s.count > 0 {
		return slices.Contains(allowedCmdsInSubMode, cmd.String())
	} else {
		return cmd.String() != QUIT
	}
}
