// Package fakedefault declares a variable named DefaultClient that is not net/http's.
package fakedefault

import nethttp "net/http"

var DefaultClient = &nethttp.Client{CheckRedirect: nil}
