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

func ParameterWithFallback(transport http.RoundTripper) *http.Client {
	if transport == nil {
		transport = &http.Transport{}
	}
	return &http.Client{Transport: transport}
}

func FallbackInDeadBranch(supplied http.RoundTripper, never bool) *http.Client {
	base := supplied
	if never {
		if base == nil {
			base = &http.Transport{}
		}
	}
	return &http.Client{Transport: base}
}

func Overwritten() *http.Client {
	base := http.RoundTripper(&http.Transport{})
	base = nil
	return &http.Client{Transport: base}
}

func pair() (http.RoundTripper, error) { return nil, nil }

func Tuple(client *http.Client) error {
	var err error
	client.Transport, err = pair()
	return err
}

func Address(client *http.Client) {
	slot := &client.Transport
	*slot = nil
}

func NamedResult() (client http.Client) { return }

func zero[T any]() T {
	var value T
	return value
}

func Generic() *http.Client {
	client := zero[http.Client]()
	return &client
}

func AddressOfLocal() *http.Client {
	base := http.RoundTripper(&http.Transport{})
	slot := &base
	*slot = nil
	return &http.Client{Transport: base}
}
