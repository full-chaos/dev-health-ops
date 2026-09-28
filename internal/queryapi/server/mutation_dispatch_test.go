package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// CHAOS-6803: /query serves a registered mutation document, /query/proof
// never does. Both handlers run the real dispatch pipeline; only the
// servesMutations flag differs, which is exactly how newQueryHandler builds
// them.

const (
	mdQuery    = "query MutationDispatchQuery { probe }"
	mdMutation = "mutation MutationDispatchMutation { probe }"
	// Registered, but a subscription: its kind cannot be served.
	mdSubscription = "subscription MutationDispatchSubscription { probe }"
	// Registered, but not a parseable document.
	mdUnparseable = "mutation MutationDispatchBroken {"
)

func mdHandler(t *testing.T, requireKind string) (http.HandlerFunc, *int) {
	t.Helper()
	verifier, _ := iaVerifier(t)
	executed := new(int)
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"q": true, "m": true, "s": true, "u": true})
	for _, operation := range []string{"q", "m", "s", "u"} {
		mux.Register(operation, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			*executed++
			w.WriteHeader(http.StatusOK)
		}))
	}
	byDigest := map[string]string{
		digestHex(mdQuery): "q", digestHex(mdMutation): "m",
		digestHex(mdSubscription): "s", digestHex(mdUnparseable): "u",
	}
	return newDocumentDispatchHandler(os.Getenv, mux, byDigest, verifier, nil, nil, requireKind, nil), executed
}

func mdPost(t *testing.T, handler http.HandlerFunc, document string, authenticated bool) int {
	t.Helper()
	body, err := json.Marshal(map[string]string{"query": document})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if authenticated {
		iaHeaders("org-1", "admin", false, false)(req)
	}
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec.Code
}

func TestDispatchHandlerServesAMutationOnlyWhereItServesMutations(t *testing.T) {
	for _, tc := range []struct {
		name         string
		requireKind  string
		document     string
		wantCode     int
		wantExecuted int
	}{
		{"query route serves a query", "", mdQuery, http.StatusOK, 1},
		{"query route serves a mutation", "", mdMutation, http.StatusOK, 1},
		{"proof route serves a query", digest.KindQuery, mdQuery, http.StatusOK, 1},
		{"proof route refuses a mutation", digest.KindQuery, mdMutation, http.StatusMethodNotAllowed, 0},
		{"proof route refuses a subscription", digest.KindQuery, mdSubscription, http.StatusMethodNotAllowed, 0},
		{"proof route refuses an unparseable registered document", digest.KindQuery, mdUnparseable, http.StatusMethodNotAllowed, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, executed := mdHandler(t, tc.requireKind)
			if code := mdPost(t, handler, tc.document, true); code != tc.wantCode {
				t.Fatalf("got %d, want %d", code, tc.wantCode)
			}
			if *executed != tc.wantExecuted {
				t.Fatalf("resolver ran %d time(s), want %d", *executed, tc.wantExecuted)
			}
		})
	}
}

func TestProofRouteRefusesAMutationOnlyAfterAuthentication(t *testing.T) {
	handler, executed := mdHandler(t, digest.KindQuery)
	if code := mdPost(t, handler, mdMutation, false); code != http.StatusUnauthorized {
		t.Fatalf("an unauthenticated mutation got %d, want 401 (authentication precedes the kind refusal)", code)
	}
	if *executed != 0 {
		t.Fatalf("resolver ran %d time(s)", *executed)
	}
}

// The real handler pair newQueryHandler builds, over the real registered
// documents: an authenticated registered MUTATION document is refused by the
// measurement-only handler before anything runs (405), and is not refused that
// way by the serving handler (its routing switch, which cannot reach the
// registry here, answers 404 instead); a registered QUERY document is not
// refused by either. The flag is the only thing standing between the
// measurement-only route and a real write, so it is measured on the handlers
// that serve, not on a copy of their wiring.
func TestNewQueryHandlerPairTreatsARegisteredMutationDifferently(t *testing.T) {
	t.Parallel()
	verifier, _ := iaVerifier(t)
	// A lazy pool pointed nowhere: the switch reads its registry through it,
	// cannot, and so answers "not enabled", which is a 404.
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:none@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	serve, proof, _, _, _ := newQueryHandler(nil, pool, verifier, "schema-digest", os.Getenv)

	for _, tc := range []struct {
		name         string
		document     string
		serveRefuses bool // 405
		proofRefuses bool
	}{
		{"registered query", registeredSavedReportsDocument, false, false},
		{"registered mutation", registeredCreateSavedReportDocument, false, true},
		{"registered mutation with no orgId in reach", registeredTriggerReportDocument, false, true},
	} {
		for route, handler := range map[string]http.HandlerFunc{"serve": serve, "proof": proof} {
			body, err := json.Marshal(map[string]any{"query": tc.document, "variables": map[string]any{"orgId": "org-1"}})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			iaHeaders("org-1", "admin", false, false)(req)
			rec := httptest.NewRecorder()
			handler(rec, req)
			want := tc.serveRefuses
			if route == "proof" {
				want = tc.proofRefuses
			}
			if got := rec.Code == http.StatusMethodNotAllowed; got != want {
				t.Errorf("%s on the %s handler: status %d, refused-as-405=%v, want %v", tc.name, route, rec.Code, got, want)
			}
		}
	}
}
