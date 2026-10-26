package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/valkey-io/valkey-go"
)

type failingCounters struct{ err error }

func (f failingCounters) Increment(context.Context, Hit) (int64, error) { return 0, f.err }
func (f failingCounters) Peek(context.Context, Hit) (int64, error)      { return 0, f.err }
func (failingCounters) Backend() string                                 { return "redis" }

// codeSeen is an ErrorWriter that records the code it was handed; it renders
// an unknown code as 500 the way WriteError does.
func codeSeen(got *Code) ErrorWriter {
	return func(w http.ResponseWriter, _ *http.Request, code Code) {
		*got = code
		status := http.StatusInternalServerError
		if code == CodeUnavailable {
			status = http.StatusServiceUnavailable
		}
		w.WriteHeader(status)
	}
}

func TestOnlyTheDependencyAwareLimitersPassCodeUnavailable(t *testing.T) {
	typed := failingCounters{fmt.Errorf("rate limit script: %w", valkey.ErrClosing)}
	key := func(*http.Request) string { return "k" }
	validate := func(w http.ResponseWriter, r *http.Request) (*http.Request, bool) { return r, true }
	for _, c := range []struct {
		name string
		wrap func(*KeyedLimiter, ErrorWriter) func(http.Handler) http.Handler
		want Code
		code int
	}{
		{"LimitWith keeps 500", func(l *KeyedLimiter, w ErrorWriter) func(http.Handler) http.Handler { return LimitWith(l, key, w) }, CodeInternal, 500},
		{"ValidateThenLimit keeps 500", func(l *KeyedLimiter, w ErrorWriter) func(http.Handler) http.Handler {
			return ValidateThenLimit(validate, l, key, w)
		}, CodeInternal, 500},
		{"LimitWithDependency", func(l *KeyedLimiter, w ErrorWriter) func(http.Handler) http.Handler {
			return LimitWithDependency(l, key, w)
		}, CodeUnavailable, 503},
		{"ValidateThenLimitDependency", func(l *KeyedLimiter, w ErrorWriter) func(http.Handler) http.Handler {
			return ValidateThenLimitDependency(validate, l, key, w)
		}, CodeUnavailable, 503},
	} {
		t.Run(c.name, func(t *testing.T) {
			var got Code
			limiter := NewKeyedLimiter(typed, Limit{ID: "dependency_test", Count: 1, Window: 60e9})
			h := c.wrap(limiter, codeSeen(&got))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("the handler ran with the counter store down")
			}))
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
			if got != c.want || w.Code != c.code {
				t.Fatalf("code %q status %d, want %q %d", got, w.Code, c.want, c.code)
			}
		})
	}
}
