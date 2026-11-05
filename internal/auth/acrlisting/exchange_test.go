package acrlisting

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// Exchange is the Client's own exchange, exported for the other acr clients (acrrevoke): same form, same answer.
func TestExchangeIsTheClientExchange(t *testing.T) {
	f := newFake(t)
	tok, _ := url.Parse(f.srv.URL + exchangePath)
	token, lifetime, err := Exchange(context.Background(), f.srv.Client(), tok, subjectFile(t, subjectJWT+"\n"), orgTwo)
	if err != nil || token != accessOne || lifetime != time.Hour {
		t.Fatalf("Exchange = %q %v %v", token, lifetime, err)
	}
	if len(f.exchanges) != 1 || f.exchanges[0].Get("resource") != "urn:acr:org:"+orgTwo || f.exchanges[0].Get("subject_token") != subjectJWT {
		t.Fatalf("exchanges = %v", f.exchanges)
	}
}

type countingTransport struct{ requests int }

func (c *countingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	c.requests++
	return nil, errors.New("no network in this test")
}

// A direct caller has no Config check in front of it, so Exchange refuses an unusable call itself, before any
// request: the transport sees none.
func TestExchangeRefusesAnUnusableCallBeforeAnyRequest(t *testing.T) {
	transport := &countingTransport{}
	client := &http.Client{Transport: transport}
	tok, _ := url.Parse("http://127.0.0.1:1/api/v1/oauth/token")
	plain, _ := url.Parse("http://acr.example.test/api/v1/oauth/token")
	subject := subjectFile(t, subjectJWT)
	cases := []struct {
		name     string
		client   *http.Client
		endpoint *url.URL
		org      string
	}{
		{"nil client", nil, tok, orgOne},
		{"nil endpoint", client, nil, orgOne},
		{"plain http endpoint", client, plain, orgOne},
		{"upper-case org", client, tok, strings.ToUpper(orgOne)},
		{"empty org", client, tok, ""},
	}
	for _, tc := range cases {
		_, _, err := Exchange(context.Background(), tc.client, tc.endpoint, subject, tc.org)
		var failure *Error
		if !errors.As(err, &failure) || failure.Kind != KindExchange {
			t.Errorf("%s: err = %v", tc.name, err)
		}
	}
	if transport.requests != 0 {
		t.Fatalf("%d requests sent", transport.requests)
	}
	if _, _, err := Exchange(context.Background(), client, tok, subject, orgOne); err == nil || transport.requests != 1 {
		t.Fatalf("the usable call: err %v, %d requests", err, transport.requests)
	}
}
