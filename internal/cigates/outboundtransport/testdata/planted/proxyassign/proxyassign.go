package proxyassign

import (
	"net/http"
	"net/url"
)

func Assign(transport *http.Transport, proxy func(*http.Request) (*url.URL, error)) {
	transport.Proxy = proxy
}
