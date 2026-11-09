package acrlisting

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestValidClusterHTTPOriginTakesOnlyAPlainHTTPClusterServiceHost(t *testing.T) {
	cases := []struct {
		raw string
		ok  bool
	}{
		{"http://dev-health-acr.dev-health.svc.cluster.local:8080", true},
		{"http://acr.ns.svc.cluster.local", true},
		{"http://ACR.NS.SVC.CLUSTER.LOCAL/api/v1/oauth/token", true},
		{"https://acr.ns.svc.cluster.local", false},
		{"ftp://acr.ns.svc.cluster.local", false},
		{"http://acr.internal.test", false},
		{"http://127.0.0.1:8080", false},
		{"http://svc.cluster.local", false},
		{"http://.svc.cluster.local", false},
		{"http://acr.svc.cluster.local.evil.test", false},
		{"http://acr.ns.svc.cluster.local.", false},
		{"http://acr.ns.svc.cluster.localx", false},
		{"http://u@acr.ns.svc.cluster.local", false},
		{"http://acr.ns.svc.cluster.local#f", false},
		{"http://:8080", false},
		{"http://dev..svc.cluster.local", false},
		{"http://dev_health.svc.cluster.local", false},
		{"http://-dev.svc.cluster.local", false},
		{"http://dev-.svc.cluster.local", false},
		{"http://a.b-c.svc.cluster.local", true},
		{"http://" + strings.Repeat("a", 63) + ".svc.cluster.local", true},
		{"http://" + strings.Repeat("a", 64) + ".svc.cluster.local", false},
		{"http://@acr.ns.svc.cluster.local", false},
	}
	for _, tc := range cases {
		u, err := url.Parse(tc.raw)
		if err != nil {
			t.Fatalf("%s: %v", tc.raw, err)
		}
		if got := ValidClusterHTTPOrigin(u); got != tc.ok {
			t.Errorf("ValidClusterHTTPOrigin(%s) = %v, want %v", tc.raw, got, tc.ok)
		}
		if ValidOrigin(u) && u.Scheme == "http" && tc.ok {
			t.Errorf("ValidOrigin(%s) accepts a cluster http host", tc.raw)
		}
	}
	if ValidClusterHTTPOrigin(nil) {
		t.Error("nil accepted")
	}
}

// clusterClient sends every request to the fake, whatever host the URL names, so a .svc.cluster.local endpoint is reachable.
func clusterClient(f *fakeACR) *http.Client {
	transport := f.srv.Client().Transport.(*http.Transport).Clone()
	addr := f.srv.Listener.Addr().String()
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
	return &http.Client{Transport: transport}
}

func TestOnlyTheClusterExchangeAcceptsAClusterHTTPEndpoint(t *testing.T) {
	f := newFake(t)
	tok, _ := url.Parse("http://dev-health-acr.dev-health.svc.cluster.local:8080" + exchangePath)
	subject := subjectFile(t, subjectJWT)
	_, _, err := Exchange(context.Background(), clusterClient(f), tok, subject, orgOne)
	var failure *Error
	if !errors.As(err, &failure) || failure.Kind != KindExchange || len(f.exchanges) != 0 {
		t.Fatalf("Exchange on a cluster http endpoint: err %v, %d exchanges", err, len(f.exchanges))
	}
	token, _, err := ExchangeAllowingClusterHTTP(context.Background(), clusterClient(f), tok, subject, orgOne)
	if err != nil || token != accessOne || len(f.exchanges) != 1 || f.exchanges[0].Get("resource") != "urn:acr:org:"+orgOne {
		t.Fatalf("ExchangeAllowingClusterHTTP = %q %v, exchanges %v", token, err, f.exchanges)
	}
	https, _ := url.Parse(f.srv.URL + exchangePath)
	if _, _, err := ExchangeAllowingClusterHTTP(context.Background(), f.srv.Client(), https, subject, orgOne); err != nil {
		t.Fatalf("the loopback endpoint ValidOrigin takes: %v", err)
	}
	off, _ := url.Parse("http://acr.internal.test" + exchangePath)
	before := len(f.exchanges)
	if _, _, err := ExchangeAllowingClusterHTTP(context.Background(), clusterClient(f), off, subject, orgOne); err == nil || len(f.exchanges) != before {
		t.Fatalf("an off-cluster http endpoint: err %v, %d new exchanges", err, len(f.exchanges)-before)
	}
}
