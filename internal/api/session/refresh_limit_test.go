package session

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const refreshPattern = "/api/v1/auth/refresh"

// refreshAddrCeiling is the per-address refresh ceiling of both planes (CHAOS-8441).
const refreshAddrCeiling = 100

func refreshBody(token string) string { return fmt.Sprintf(`{"refresh_token":%q}`, token) }

func refreshToken(i int) string { return fmt.Sprintf("dhr1_token_%04d", i) }

func refreshRouteWith(t *testing.T, deps Deps) http.Handler {
	t.Helper()
	deps.Inner = func(string, http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) })
	}
	for _, route := range Routes(deps) {
		if route.Pattern == refreshPattern && route.Method == http.MethodPost {
			return route.Handler
		}
	}
	t.Fatal("no refresh route")
	return nil
}

func assertRefreshLimited(t *testing.T, name string, got *httptest.ResponseRecorder) {
	t.Helper()
	if got.Code != http.StatusTooManyRequests {
		t.Fatalf("%s = %d, want 429: %s", name, got.Code, got.Body)
	}
	if retry := got.Header().Get("Retry-After"); retry != "900" {
		t.Fatalf("%s Retry-After = %q, want 900", name, retry)
	}
}

func TestRefreshLimitsArePinned(t *testing.T) {
	for _, c := range []struct{ got, want httpapi.Limit }{
		{refreshLimit, httpapi.Limit{ID: "auth_refresh", Count: 10, Window: 15 * time.Minute}},
		{refreshAddrLimit, httpapi.Limit{ID: "auth_refresh_addr", Count: refreshAddrCeiling, Window: 15 * time.Minute}},
	} {
		if c.got != c.want {
			t.Errorf("limit %+v, want %+v", c.got, c.want)
		}
	}
}

// Ten refreshes of one credential spend only that credential's budget: another credential from the same address
// is admitted, and the spent one stays refused from any address.
func TestRefreshLimitsPerCredential(t *testing.T) {
	h := refreshRouteWith(t, completeDeps(t))
	for i := 1; i <= 10; i++ {
		if got := post(h, refreshPattern, "198.51.100.7:4000", refreshBody(refreshToken(1)), nil).Code; got != http.StatusAccepted {
			t.Fatalf("credential A refresh %d = %d, want 202", i, got)
		}
	}
	if got := post(h, refreshPattern, "198.51.100.7:4000", refreshBody(refreshToken(2)), nil).Code; got != http.StatusAccepted {
		t.Fatalf("credential B from the same address after A's 10 = %d, want 202", got)
	}
	assertRefreshLimited(t, "credential A 11th", post(h, refreshPattern, "198.51.100.7:4000", refreshBody(refreshToken(1)), nil))
	assertRefreshLimited(t, "credential A from another address", post(h, refreshPattern, "198.51.100.8:4000", refreshBody(refreshToken(1)), nil))
}

// Nothing another caller sends selects the victim's bucket: forged forwarding headers from an untrusted peer and
// the victim's address forwarded by the trusted proxy spend only the attacker's own credentials.
func TestRefreshAnotherCallerCannotDrainAVictimsCredential(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "10.0.0.1")
	h := refreshRouteWith(t, completeDeps(t))
	victimAddress := map[string]string{"X-Forwarded-For": "192.0.2.10"}
	for i := 1; i <= 10; i++ {
		post(h, refreshPattern, "198.51.100.66:4000", refreshBody(refreshToken(100+i)), map[string]string{"X-Forwarded-For": "192.0.2.10", "X-Real-IP": "192.0.2.10"})
		post(h, refreshPattern, "10.0.0.1:4000", refreshBody(refreshToken(200+i)), victimAddress)
	}
	if got := post(h, refreshPattern, "10.0.0.1:4000", refreshBody(refreshToken(1)), victimAddress).Code; got != http.StatusAccepted {
		t.Fatalf("the victim's refresh after 20 attacker refreshes = %d, want 202", got)
	}
}

// The ceiling keys by the option (a) address: the forwarded client behind the trusted proxy, the peer otherwise,
// an IPv6 client by its /64.
func TestRefreshAddressCeiling(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "10.0.0.1")
	for _, c := range []struct {
		name, peer, other string
		forwarded         func(i int) map[string]string
	}{
		{"direct peer", "198.51.100.7:4000", "198.51.100.8:4000", func(int) map[string]string { return nil }},
		{"forwarded client", "10.0.0.1:4000", "", func(int) map[string]string { return map[string]string{"X-Forwarded-For": "192.0.2.10"} }},
		{"spoofed forwarding from an untrusted peer", "198.51.100.9:4000", "198.51.100.8:4000", func(i int) map[string]string {
			return map[string]string{"X-Forwarded-For": fmt.Sprintf("203.0.113.%d", i%250+1)}
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := refreshRouteWith(t, completeDeps(t))
			for i := 1; i <= refreshAddrCeiling; i++ {
				if got := post(h, refreshPattern, c.peer, refreshBody(refreshToken(i)), c.forwarded(i)).Code; got != http.StatusAccepted {
					t.Fatalf("refresh %d with its own credential = %d, want 202", i, got)
				}
			}
			assertRefreshLimited(t, "the ceiling's refusal", post(h, refreshPattern, c.peer, refreshBody(refreshToken(9999)), c.forwarded(9999)))
			if c.other != "" {
				if got := post(h, refreshPattern, c.other, refreshBody(refreshToken(9998)), nil).Code; got != http.StatusAccepted {
					t.Fatalf("another address = %d, want 202", got)
				}
			} else if got := post(h, refreshPattern, "10.0.0.1:4000", refreshBody(refreshToken(9998)), map[string]string{"X-Forwarded-For": "192.0.2.11"}).Code; got != http.StatusAccepted {
				t.Fatalf("another client behind the proxy = %d, want 202", got)
			}
		})
	}
	t.Run("IPv6 /64", func(t *testing.T) {
		h := refreshRouteWith(t, completeDeps(t))
		for i := 1; i <= refreshAddrCeiling; i++ {
			post(h, refreshPattern, fmt.Sprintf("[2001:db8:1:2::%x]:4000", i), refreshBody(refreshToken(i)), nil)
		}
		assertRefreshLimited(t, "another address in the same /64", post(h, refreshPattern, "[2001:db8:1:2:ffff::9]:4000", refreshBody(refreshToken(9999)), nil))
		if got := post(h, refreshPattern, "[2001:db8:1:3::1]:4000", refreshBody(refreshToken(9998)), nil).Code; got != http.StatusAccepted {
			t.Fatalf("a different /64 = %d, want 202", got)
		}
	})
}

// Retry-After belongs to the refresh refusals only: login's 429 is unchanged.
func TestRetryAfterOnlyOnTheRefreshRoute(t *testing.T) {
	deps := completeDeps(t)
	deps.Inner = func(string, http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) })
	}
	var login http.Handler
	for _, route := range Routes(deps) {
		if route.Pattern == "/api/v1/auth/login" {
			login = route.Handler
		}
	}
	body := func(i int) string { return fmt.Sprintf(`{"email":"user%d@example.com","password":"pw"}`, i) }
	for i := 1; i <= 20; i++ {
		post(login, "/api/v1/auth/login", "198.51.100.7:4000", body(i), nil)
	}
	got := post(login, "/api/v1/auth/login", "198.51.100.7:4000", body(99), nil)
	if got.Code != http.StatusTooManyRequests || got.Header().Get("Retry-After") != "" {
		t.Fatalf("login 21st = %d Retry-After %q, want 429 without it", got.Code, got.Header().Get("Retry-After"))
	}
}

type keyRecorder struct {
	httpapi.CounterStore
	mu   sync.Mutex
	hits []httpapi.Hit
}

func (k *keyRecorder) Increment(ctx context.Context, hit httpapi.Hit) (int64, error) {
	k.mu.Lock()
	k.hits = append(k.hits, hit)
	k.mu.Unlock()
	return k.CounterStore.Increment(ctx, hit)
}

// The counter keys and every log line at debug level carry neither the credential nor any hash of it.
func TestRefreshKeysAndLogsCarryNoCredential(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	previous := slog.Default()
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(previous) })
	store := &keyRecorder{CounterStore: httpapi.NewMemoryCounters(nil)}
	deps := completeDeps(t)
	deps.Limits, deps.Logger = store, logger
	h := refreshRouteWith(t, deps)
	token, other := refreshToken(1), refreshToken(2)
	for i := 0; i < 11; i++ {
		post(h, refreshPattern, "198.51.100.7:4000", refreshBody(token), nil)
	}
	post(h, refreshPattern, "198.51.100.7:4000", refreshBody(other), nil)
	keys := map[string]bool{}
	for _, hit := range store.hits {
		if hit.Limit.ID == refreshLimit.ID {
			keys[hit.Key] = true
		}
	}
	if len(keys) != 2 {
		t.Fatalf("%d per-credential keys for two credentials, want 2", len(keys))
	}
	sum := sha256.Sum256([]byte(token))
	atRest := hex.EncodeToString(sum[:])
	forbidden := []string{token, other, atRest, atRest[:32]}
	for key := range keys {
		for _, text := range []string{token, other, atRest[:32]} {
			if strings.Contains(key, text) {
				t.Fatalf("counter key %q carries %q", key, text)
			}
		}
		forbidden = append(forbidden, key, strings.TrimPrefix(key, "refresh-token:"))
	}
	for _, text := range forbidden {
		if strings.Contains(logs.String(), text) {
			t.Fatalf("a log line carries %q:\n%s", text, logs.String())
		}
	}
}
