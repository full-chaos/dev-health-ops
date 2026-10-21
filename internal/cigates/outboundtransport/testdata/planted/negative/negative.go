package negative

import (
	"net/http"
	"net/http/httputil"
	"time"
)

type roundTripper struct{ next http.RoundTripper }

func (r roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return r.next.RoundTrip(req)
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func Own() *http.Client {
	transport := &http.Transport{Proxy: nil, IdleConnTimeout: time.Second}
	transport.Proxy = nil
	return &http.Client{Transport: roundTripper{next: transport}, Timeout: time.Second}
}

func Unkeyed() http.Client { return http.Client{&http.Transport{}, nil, nil, 0} }

func Cloned() *http.Client {
	base := &http.Transport{}
	return &http.Client{Transport: base.Clone()}
}

func Adapter() *http.Client {
	return &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) { return nil, nil })}
}

func Proxied(next http.RoundTripper) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{Transport: next}
}
