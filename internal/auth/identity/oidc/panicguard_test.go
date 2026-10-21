package oidc

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// headerPanicWriter panics when the handler commits a status.
type headerPanicWriter struct{ *httptest.ResponseRecorder }

func (headerPanicWriter) WriteHeader(int) { panic("write failed") }

// serveRaw runs req through the route with no recovery middleware and
// returns what the route panicked with.
func (env *unitEnv) serveRaw(c routeCase, w http.ResponseWriter, req *http.Request) (recovered any) {
	mux := http.NewServeMux()
	mux.Handle(c.pattern, c.handler(env.svc))
	defer func() { recovered = recover() }()
	mux.ServeHTTP(w, req)
	return nil
}

// A panic after the request's outcome was counted is not counted again.
func TestPanicAfterACountedOutcomeIsNotCountedAgain(t *testing.T) {
	for _, c := range routeCases {
		for _, known := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/known-provider=%v", c.route, known), func(t *testing.T) {
				row := c.activeRow()
				stored := row
				if !known {
					stored.ID, stored.OrgID = uuid.New(), uuid.New()
				}
				env := newUnitEnv(t, &fakeProviders{before: stored})
				if got := env.serveRaw(c, headerPanicWriter{httptest.NewRecorder()}, env.request(t, c, row, false)); got != "write failed" {
					t.Fatalf("recovered %v, want the write panic passed on", got)
				}
				counts := outcomeCounts(t, env.reader)
				var total int64
				for key, n := range counts {
					total += n
					if strings.Contains(key, "/error/") {
						t.Errorf("auth.oidc.outcomes counts %s after a counted outcome: %v", key, counts)
					}
				}
				if total != 1 {
					t.Errorf("auth.oidc.outcomes total = %d, want 1: %v", total, counts)
				}
			})
		}
	}
}

// http.ErrAbortHandler is passed on as is and not counted.
func TestAbortHandlerPanicIsNotCounted(t *testing.T) {
	for _, c := range routeCases {
		t.Run(c.route, func(t *testing.T) {
			row := c.activeRow()
			env := newUnitEnv(t, &fakeProviders{before: row})
			if c.callback {
				env.flows.onConsume = http.ErrAbortHandler
			} else {
				env.flows.onBegin = http.ErrAbortHandler
			}
			if got := env.serveRaw(c, httptest.NewRecorder(), env.request(t, c, row, true)); got != http.ErrAbortHandler {
				t.Fatalf("recovered %v, want http.ErrAbortHandler", got)
			}
			if counts := outcomeCounts(t, env.reader); len(counts) != 0 {
				t.Errorf("auth.oidc.outcomes = %v, want none", counts)
			}
			if strings.Contains(env.logged.String(), " outcome=") {
				t.Errorf("an outcome log line was written:\n%s", env.logged.String())
			}
		})
	}
}
