package atlassian

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

type AuthProvider interface {
	Apply(req *http.Request) error
}

type BearerAuth struct {
	TokenGetter func() (string, error)
}

func (a BearerAuth) Apply(req *http.Request) error {
	if a.TokenGetter == nil {
		return errors.New("token getter is required")
	}
	token, err := a.TokenGetter()
	if err != nil {
		return fmt.Errorf("get token: %w", err)
	}
	token = strings.TrimSpace(token)
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[len("bearer "):])
	}
	if token == "" {
		return errors.New("empty bearer token")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return nil
}

type BasicAPITokenAuth struct {
	Email string
	Token string
}

func (a BasicAPITokenAuth) Apply(req *http.Request) error {
	if a.Email == "" || a.Token == "" {
		return errors.New("email and token required for basic auth")
	}
	req.SetBasicAuth(a.Email, a.Token)
	return nil
}

type CookieAuth struct {
	Cookies []*http.Cookie
}

func (a CookieAuth) Apply(req *http.Request) error {
	if len(a.Cookies) == 0 {
		return errors.New("cookies required for cookie auth")
	}
	for _, c := range a.Cookies {
		if c != nil {
			req.AddCookie(c)
		}
	}
	return nil
}
