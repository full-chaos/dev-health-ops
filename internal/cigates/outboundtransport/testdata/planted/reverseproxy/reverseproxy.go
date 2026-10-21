package reverseproxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"
)

func Bare() *httputil.ReverseProxy { return &httputil.ReverseProxy{Director: func(*http.Request) {}} }

func NilTransport() *httputil.ReverseProxy {
	return &httputil.ReverseProxy{Transport: nil}
}

func Single(target *url.URL) *httputil.ReverseProxy { return httputil.NewSingleHostReverseProxy(target) }
