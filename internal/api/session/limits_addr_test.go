package session

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var addrLimitedRoutes = []string{"/api/v1/auth/resend-verification", "/api/v1/auth/forgot-password"}

// routeAt is the handler of a session route with the local handler replaced by one that answers 202.
func routeAt(t *testing.T, pattern string) http.Handler {
	t.Helper()
	deps := completeDeps(t)
	deps.Inner = func(string, http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) })
	}
	for _, route := range Routes(deps) {
		if route.Pattern == pattern && route.Method == http.MethodPost {
			return route.Handler
		}
	}
	t.Fatalf("no POST route %s", pattern)
	return nil
}

func post(h http.Handler, pattern, peer, body string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, pattern, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.RemoteAddr = peer
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func emailBody(i int) string { return fmt.Sprintf(`{"email":"user%d@example.com"}`, i) }

func TestAddrLimitsAreTwentyAnHourWithTheirOwnIDs(t *testing.T) {
	for _, c := range []struct{ got, want string }{
		{resendAddrLimit.ID, "auth_resend_verification_addr"}, {forgotAddrLimit.ID, "auth_forgot_password_addr"},
	} {
		if c.got != c.want {
			t.Errorf("limit id %q, want %q", c.got, c.want)
		}
	}
	for _, l := range []struct{ count int }{{resendAddrLimit.Count}, {forgotAddrLimit.Count}} {
		if l.count != 20 {
			t.Errorf("address cap %d, want 20", l.count)
		}
	}
	if resendAddrLimit.Window != resendLimit.Window || forgotAddrLimit.Window != forgotLimit.Window {
		t.Error("the address cap window is not the per-e-mail window (one hour)")
	}
}

func TestOneAddressNamingManyEmailsIsRefusedAfterTheCap(t *testing.T) {
	for _, pattern := range addrLimitedRoutes {
		h := routeAt(t, pattern)
		for i := 1; i <= 25; i++ {
			got := post(h, pattern, "198.51.100.7:4000", emailBody(i), nil).Code
			want := http.StatusAccepted
			if i > 20 {
				want = http.StatusTooManyRequests
			}
			if got != want {
				t.Fatalf("%s: e-mail %d from one address = %d, want %d", pattern, i, got, want)
			}
		}
		if got := post(h, pattern, "198.51.100.8:4000", emailBody(99), nil).Code; got != http.StatusAccepted {
			t.Fatalf("%s: a second address was refused (%d)", pattern, got)
		}
	}
}

func TestThePerEmailBudgetStillHoldsBesideTheAggregate(t *testing.T) {
	for _, pattern := range addrLimitedRoutes {
		h := routeAt(t, pattern)
		for i := 1; i <= 4; i++ {
			got := post(h, pattern, "198.51.100.7:4000", emailBody(1), nil).Code
			want := http.StatusAccepted
			if i > 3 {
				want = http.StatusTooManyRequests
			}
			if got != want {
				t.Fatalf("%s: request %d for one e-mail = %d, want %d", pattern, i, got, want)
			}
		}
	}
}

func TestTheAggregateRefusalHasTheSameShapeAsThePerEmailRefusal(t *testing.T) {
	for _, pattern := range addrLimitedRoutes {
		h := routeAt(t, pattern)
		for i := 1; i <= 4; i++ {
			post(h, pattern, "198.51.100.9:1", emailBody(1), nil)
		}
		perEmail := post(h, pattern, "198.51.100.9:1", emailBody(1), nil)
		for i := 2; i <= 20; i++ {
			post(h, pattern, "198.51.100.9:1", emailBody(i), nil)
		}
		aggregate := post(h, pattern, "198.51.100.9:1", emailBody(30), nil)
		if perEmail.Code != http.StatusTooManyRequests || aggregate.Code != http.StatusTooManyRequests {
			t.Fatalf("%s: per-e-mail %d, aggregate %d, want 429 both", pattern, perEmail.Code, aggregate.Code)
		}
		if perEmail.Body.String() != aggregate.Body.String() || perEmail.Header().Get("Content-Type") != aggregate.Header().Get("Content-Type") {
			t.Fatalf("%s: 429 shapes differ: %q vs %q", pattern, perEmail.Body.String(), aggregate.Body.String())
		}
	}
}

func TestSpoofedForwardingHeadersFromAnUntrustedPeerGetNoNewBucket(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "")
	for _, pattern := range addrLimitedRoutes {
		h := routeAt(t, pattern)
		for i := 1; i <= 25; i++ {
			headers := map[string]string{"X-Forwarded-For": fmt.Sprintf("203.0.113.%d", i), "X-Real-IP": fmt.Sprintf("203.0.113.%d", 100+i)}
			got := post(h, pattern, "198.51.100.7:4000", emailBody(i), headers).Code
			want := http.StatusAccepted
			if i > 20 {
				want = http.StatusTooManyRequests
			}
			if got != want {
				t.Fatalf("%s: spoofed header %d = %d, want %d", pattern, i, got, want)
			}
		}
	}
}

func TestATrustedProxyBucketsByTheRightmostUntrustedHop(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "10.0.0.1")
	for _, pattern := range addrLimitedRoutes {
		h := routeAt(t, pattern)
		for i := 1; i <= 21; i++ {
			// The client writes the left entry; the proxy appends the client address it saw.
			headers := map[string]string{"X-Forwarded-For": fmt.Sprintf("203.0.113.%d, 192.0.2.50", i)}
			got := post(h, pattern, "10.0.0.1:4000", emailBody(i), headers).Code
			want := http.StatusAccepted
			if i > 20 {
				want = http.StatusTooManyRequests
			}
			if got != want {
				t.Fatalf("%s: behind a trusted proxy, request %d = %d, want %d", pattern, i, got, want)
			}
		}
		if got := post(h, pattern, "10.0.0.1:4000", emailBody(77), map[string]string{"X-Forwarded-For": "192.0.2.51"}).Code; got != http.StatusAccepted {
			t.Fatalf("%s: another client behind the proxy was refused (%d)", pattern, got)
		}
	}
}

func TestIPv6RotationInsideOneSlash64SharesTheBucket(t *testing.T) {
	for _, pattern := range addrLimitedRoutes {
		h := routeAt(t, pattern)
		for i := 1; i <= 25; i++ {
			peer := fmt.Sprintf("[2001:db8:1:2:%x::%x]:4000", i, i*7)
			got := post(h, pattern, peer, emailBody(i), nil).Code
			want := http.StatusAccepted
			if i > 20 {
				want = http.StatusTooManyRequests
			}
			if got != want {
				t.Fatalf("%s: rotating inside one /64, request %d = %d, want %d", pattern, i, got, want)
			}
		}
		if got := post(h, pattern, "[2001:db8:1:3::1]:4000", emailBody(50), nil).Code; got != http.StatusAccepted {
			t.Fatalf("%s: another /64 was refused (%d)", pattern, got)
		}
	}
}

func TestAnIPv4MappedIPv6PeerSharesTheIPv4Bucket(t *testing.T) {
	pattern := addrLimitedRoutes[0]
	h := routeAt(t, pattern)
	for i := 1; i <= 20; i++ {
		if got := post(h, pattern, "198.51.100.20:1", emailBody(i), nil).Code; got != http.StatusAccepted {
			t.Fatalf("request %d = %d", i, got)
		}
	}
	if got := post(h, pattern, "[::ffff:198.51.100.20]:1", emailBody(21), nil).Code; got != http.StatusTooManyRequests {
		t.Fatalf("the mapped form of a spent address = %d, want 429", got)
	}
}

func TestTheAggregateNeverCountsAValidationFailureOrAPerEmailRefusal(t *testing.T) {
	for _, pattern := range addrLimitedRoutes {
		h := routeAt(t, pattern)
		// 30 malformed bodies: 422 each, no allowance spent on either budget.
		for i := 0; i < 30; i++ {
			if got := post(h, pattern, "198.51.100.30:1", `{"email":"not-an-email"}`, nil).Code; got != http.StatusUnprocessableEntity {
				t.Fatalf("%s: malformed body %d = %d, want 422", pattern, i, got)
			}
		}
		// 3 pass, then 40 refused by the per-e-mail budget: those 40 never reach the aggregate.
		for i := 1; i <= 43; i++ {
			got := post(h, pattern, "198.51.100.30:1", emailBody(1), nil).Code
			want := http.StatusAccepted
			if i > 3 {
				want = http.StatusTooManyRequests
			}
			if got != want {
				t.Fatalf("%s: repeat %d = %d, want %d", pattern, i, got, want)
			}
		}
		// 17 more distinct e-mails still fit in the aggregate: 3 + 17 = 20.
		for i := 2; i <= 18; i++ {
			if got := post(h, pattern, "198.51.100.30:1", emailBody(i), nil).Code; got != http.StatusAccepted {
				t.Fatalf("%s: e-mail %d = %d, want 202 (422s and per-e-mail refusals must not spend the aggregate)", pattern, i, got)
			}
		}
		if got := post(h, pattern, "198.51.100.30:1", emailBody(19), nil).Code; got != http.StatusTooManyRequests {
			t.Fatalf("%s: the 21st served request = %d, want 429", pattern, got)
		}
	}
}
