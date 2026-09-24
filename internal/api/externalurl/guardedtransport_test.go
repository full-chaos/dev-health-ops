package externalurl

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestGuardedTransportRefusesAnInternalDial is the DNS-rebinding case: the
// URL's host name passes no address check of its own here, and the address it
// resolves to (loopback) is refused at dial time, before any request byte --
// a bearer token included -- reaches the server.
func TestGuardedTransportRefusesAnInternalDial(t *testing.T) {
	reached := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached <- r.Header.Get("Authorization")
	}))
	defer server.Close()
	client := &http.Client{Transport: GuardedTransport(), Timeout: 5 * time.Second}
	for _, target := range []string{server.URL, strings.Replace(server.URL, "127.0.0.1", "localhost", 1)} {
		request, err := http.NewRequest(http.MethodGet, target, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer secret-token")
		response, err := client.Do(request)
		if err == nil {
			response.Body.Close()
			t.Fatalf("GET %s reached an internal address (status %d)", target, response.StatusCode)
		}
		if !strings.Contains(err.Error(), "Connection to private/internal networks is not allowed") {
			t.Errorf("GET %s error = %v, want the internal-network refusal", target, err)
		}
	}
	select {
	case token := <-reached:
		t.Fatalf("the server received a request (Authorization %q)", token)
	default:
	}
}

func TestRefuseInternalAddressClassification(t *testing.T) {
	for address, refused := range map[string]bool{
		"127.0.0.1:443": true, "10.1.2.3:443": true, "169.254.169.254:80": true, "[::1]:443": true, "[fd00::1]:443": true,
		"[::ffff:10.0.0.1]:443": true, "93.184.216.34:443": false, "[2606:2800:220:1::1]:443": false, "not-an-address": true,
	} {
		err := refuseInternalAddress("tcp", address, nil)
		if (err != nil) != refused {
			t.Errorf("refuseInternalAddress(%q) = %v, want refused=%v", address, err, refused)
		}
	}
}

// TestGuardedTransportIgnoresEnvironmentProxies: a proxy would make the
// dialled address the proxy's, not the checked host's.
func TestGuardedTransportIgnoresEnvironmentProxies(t *testing.T) {
	if GuardedTransport().Proxy != nil {
		t.Fatal("GuardedTransport consults a proxy; the checked address would no longer be the dialled one")
	}
}
