package credis

import (
	"slices"
	"sync"
)

type Auth interface {
	Flags() []string
	User() string
	Passwords() []string
	SetPassword(password string)
	Authenticate(password string) bool
	PassRequired() bool
}

type auth struct {
	mu              sync.RWMutex
	user            string
	isAuthenticated bool
	flags           []string
	passwords       []string
}

func DefaultAuth() *auth {
	return &auth{
		user: "default",
		flags: []string{
			"nopass",
		},
		passwords: []string{},
	}
}

func (c *auth) Flags() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.flags
}

func (c *auth) User() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.user
}

func (c *auth) Passwords() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.passwords
}

func (c *auth) SetPassword(password string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.passwords = append(c.passwords, SHA256Hex(password))
	c.flags = slices.DeleteFunc(c.flags, func(e string) bool {
		return e == "nopass"
	})
	// return c.passwords
}

func (c *auth) Authenticate(password string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if slices.Contains(c.passwords, SHA256Hex(password)) {
		c.isAuthenticated = true
	}
	return c.isAuthenticated
}

func (c *auth) PassRequired() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !slices.Contains(c.flags, "nopass")
}
