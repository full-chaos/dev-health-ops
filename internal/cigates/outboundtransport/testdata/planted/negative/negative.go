package negative

import (
	"net/http"
	"time"
)

type roundTripper struct{ next http.RoundTripper }

func (r roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return r.next.RoundTrip(req)
}

func Own() *http.Client {
	transport := &http.Transport{Proxy: nil, IdleConnTimeout: time.Second}
	transport.Proxy = nil
	return &http.Client{Transport: roundTripper{next: transport}, Timeout: time.Second}
}

func Pointer(client *http.Client) *http.Client { return client }

func Copied(client *http.Client) *http.Client {
	copied := *client
	copied.Timeout = time.Second
	return &copied
}

func Unkeyed(rt http.RoundTripper) http.Client { return http.Client{rt, nil, nil, 0} }
