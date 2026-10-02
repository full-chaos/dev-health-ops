// Package fakedefault declares a variable named DefaultClient that is not net/http's.
package fakedefault

import (
	"errors"
	nethttp "net/http"
)

var DefaultClient = &nethttp.Client{CheckRedirect: nil}

// ErrUseLastResponse is named like net/http's and is not it.
var ErrUseLastResponse = errors.New("not net/http's")
