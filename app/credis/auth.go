package credis

import (
	"slices"
	"sync"
)

type authContext struct {
	id              string
	user            string
	isAuthenticated bool
}

type User struct {
	id        string
	flags     []string
	passwords []string
}

type Auth struct {
	users *sync.Map
}

func NewAuth() *Auth {
	users := sync.Map{}
	users.Store("default", User{
		id: "default",
		flags: []string{
			"nopass",
		},
		passwords: []string{},
	})
	return &Auth{
		users: &users,
	}
}

func (auth *Auth) Authenticate(ctx *authContext, user, password string) bool {
	userData, _ := auth.users.LoadOrStore(ctx.user, User{})
	passwords := (userData.(User)).passwords
	if slices.Contains(passwords, SHA256Hex(password)) {
		ctx.isAuthenticated = true
	} else {
		ctx.isAuthenticated = false
	}
	ctx.user = user
	return ctx.isAuthenticated
}

func DefaultAuthContext() *authContext {
	return &authContext{
		user:            "default",
		isAuthenticated: true,
	}
}

func NewAuthContext() *authContext {
	ctx := authContext{
		user:            "default",
		isAuthenticated: false,
	}
	return &ctx
}

func (c *Auth) Flags(ctx *authContext) []string {
	userData, _ := c.users.LoadOrStore(ctx.user, User{})
	flags := (userData.(User)).flags
	return flags
}

func (c *Auth) AuthenticatedUser(ctx *authContext) User {
	userData, _ := c.users.LoadOrStore(ctx.user, User{})
	return userData.(User)
}

func (c *Auth) SetPassword(user, password string) {
	userData, _ := c.users.LoadOrStore(user, User{
		flags:     make([]string, 0),
		passwords: make([]string, 0),
	})
	passwords := (userData.(User)).passwords
	flags := (userData.(User)).flags
	passwords = append(passwords, SHA256Hex(password))
	flags = slices.DeleteFunc(flags, func(e string) bool {
		return e == "nopass"
	})
	c.users.Store(user, User{
		flags:     flags,
		passwords: passwords,
	})
	// return c.passwords
}

func (c *Auth) PassRequired(user string) bool {
	userData, _ := c.users.LoadOrStore(user, User{})
	flags := (userData.(User)).flags
	return !slices.Contains(flags, "nopass")
}

func AuthMiddleware(e *executor, req Request, res Response, terminate TerminateFunc) {
	// If nopass flag is set, authenticate that user by default
	flags := e.deps.Auth.Flags(req.AuthCtx())
	if slices.Contains(flags, "nopass") {
		req.AuthCtx().isAuthenticated = true
	}
	if !req.AuthCtx().isAuthenticated && req.Specs().String() != AUTH {
		terminate(&ErrNoAuth{})
		return
	}
}
