package acrrevoke

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"testing"
)

const clusterOrigin = "http://dev-health-acr.dev-health.svc.cluster.local:8080"

func clusterClient(f *fakeACR) *http.Client {
	transport := f.srv.Client().Transport.(*http.Transport).Clone()
	addr := f.srv.Listener.Addr().String()
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
	return &http.Client{Transport: transport}
}

func TestNewTakesAClusterHTTPOriginOnlyWithTheOptIn(t *testing.T) {
	base, _ := url.Parse(clusterOrigin)
	token, _ := url.Parse(clusterOrigin + tokenPath)
	https, _ := url.Parse("https://acr.example.test")
	httpsToken, _ := url.Parse("https://acr.example.test" + tokenPath)
	off, _ := url.Parse("http://acr.example.test")
	query, _ := url.Parse(clusterOrigin + "/?a=b")
	cases := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"cluster http, opt-in", Config{BaseURL: base, TokenEndpoint: token, SubjectTokenFile: "/t", AllowClusterHTTP: true}, true},
		{"cluster http base, no opt-in", Config{BaseURL: base, TokenEndpoint: httpsToken, SubjectTokenFile: "/t"}, false},
		{"cluster http endpoint, no opt-in", Config{BaseURL: https, TokenEndpoint: token, SubjectTokenFile: "/t"}, false},
		{"off-cluster http base, opt-in", Config{BaseURL: off, TokenEndpoint: token, SubjectTokenFile: "/t", AllowClusterHTTP: true}, false},
		{"off-cluster http endpoint, opt-in", Config{BaseURL: base, TokenEndpoint: off, SubjectTokenFile: "/t", AllowClusterHTTP: true}, false},
		{"cluster http base with a query, opt-in", Config{BaseURL: query, TokenEndpoint: token, SubjectTokenFile: "/t", AllowClusterHTTP: true}, false},
		{"https, opt-in", Config{BaseURL: https, TokenEndpoint: httpsToken, SubjectTokenFile: "/t", AllowClusterHTTP: true}, true},
	}
	for _, tc := range cases {
		_, err := New(tc.cfg)
		if (err == nil) != tc.ok || (!tc.ok && !errors.Is(err, ErrInvalidConfig)) {
			t.Errorf("%s: err = %v", tc.name, err)
		}
	}
}

// With the opt-in, both requests go to the cluster-local Service: the exchange and the revoke.
func TestRevokeOverClusterHTTPSendsBothRequests(t *testing.T) {
	f := newFake(t)
	base, _ := url.Parse(clusterOrigin)
	token, _ := url.Parse(clusterOrigin + tokenPath)
	c, err := New(Config{BaseURL: base, TokenEndpoint: token, SubjectTokenFile: subjectFile(t), HTTPClient: clusterClient(f), AllowClusterHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Revoke(context.Background(), orgOne, credOne, ActorUser, actorOne); err != nil {
		t.Fatalf("Revoke = %v", err)
	}
	calls := f.snapshot()
	if len(calls) != 2 || calls[0].path != tokenPath || calls[1].path != "/internal/v1/credentials/"+credOne+"/revoke" || calls[1].auth != "Bearer "+accessOne {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestValidOriginNeedsTheOptInForClusterHTTP(t *testing.T) {
	cluster, _ := url.Parse(clusterOrigin)
	https, _ := url.Parse("https://acr.example.test")
	if ValidOrigin(cluster, false) || !ValidOrigin(cluster, true) || !ValidOrigin(https, false) || !ValidOrigin(https, true) {
		t.Fatal("ValidOrigin truth table")
	}
}
