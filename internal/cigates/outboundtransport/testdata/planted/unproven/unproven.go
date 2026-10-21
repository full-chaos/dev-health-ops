package unproven

import (
	"net/http"
	"time"
)

type wrapper struct{ next http.RoundTripper }

func (w wrapper) RoundTrip(req *http.Request) (*http.Response, error) { return w.next.RoundTrip(req) }

type holder struct{ transport http.RoundTripper }

func NilVariable() *http.Client {
	var transport http.RoundTripper
	return &http.Client{Transport: transport}
}

func Parameter(transport http.RoundTripper) *http.Client {
	return &http.Client{Transport: transport, Timeout: time.Second}
}

func WrapperOverNil() *http.Client { return &http.Client{Transport: wrapper{}} }

func FieldRead(h holder) *http.Client { return &http.Client{Transport: h.transport} }

func Assigned(client *http.Client, transport http.RoundTripper) {
	client.Transport = wrapper{next: transport}
}

func Unkeyed(rt http.RoundTripper) http.Client { return http.Client{rt, nil, nil, 0} }

func ParameterReassigned(transport http.RoundTripper, own bool) *http.Client {
	if own {
		transport = &http.Transport{}
	}
	return &http.Client{Transport: transport}
}
