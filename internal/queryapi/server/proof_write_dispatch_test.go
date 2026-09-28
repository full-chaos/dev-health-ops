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

// CHAOS-7096, D2976 condition (3): the /query/proof-write dispatch pipeline's
// clauses, planted one defect at a time -- each sub-test isolates exactly ONE
// refusal reason, so a mutation that guts one clause is caught by the ONE
// test whose whole job is that clause, not by a shared happy-path assertion
// that a broken clause could still satisfy by accident.

const (
	pwMutation = "mutation ProofWriteDispatchMutation { probe }"
	pwQuery    = "query ProofWriteDispatchQuery { probe }"
)

// pwHandler builds the real newDocumentDispatchHandler with requireKind
// fixed at digest.KindMutation (the /query/proof-write shape) and the given
// orgAllowed closure, over a Mux whose Switch is fully enabled for "m" --
// so a test targeting the KIND clause or the ORG clause is never confused
// by the SWITCH clause also refusing.
func pwHandler(t *testing.T, orgAllowed func(context.Context, string) bool) (http.HandlerFunc, *int) {
	t.Helper()
	verifier, _ := iaVerifier(t)
	executed := new(int)
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"m": true})
	mux.Register("m", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*executed++
		w.WriteHeader(http.StatusOK)
	}))
	byDigest := map[string]string{digestHex(pwMutation): "m", digestHex(pwQuery): "m"}
	return newDocumentDispatchHandler(os.Getenv, mux, byDigest, verifier, nil, nil, digest.KindMutation, orgAllowed), executed
}

func pwPost(handler http.HandlerFunc, document, orgID string) *httptest.ResponseRecorder {
	body, _ := json.Marshal(map[string]string{"query": document})
	req := httptest.NewRequest(http.MethodPost, "/query/proof-write", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	iaHeaders(orgID, "admin", false, false)(req)
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

// Clause 1: KIND. A query document on the write door is refused before the
// org check ever runs -- orgAllowed being called at all would itself be the
// defect this test exists to catch.
func TestProofWriteRefusesAQueryDocumentBeforeCheckingOrg(t *testing.T) {
	orgAllowedCalled := false
	handler, executed := pwHandler(t, func(context.Context, string) bool {
		orgAllowedCalled = true
		return true
	})
	rec := pwPost(handler, pwQuery, "org-1")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("got %d, want 405", rec.Code)
	}
	if *executed != 0 {
		t.Fatalf("resolver ran %d time(s), want 0", *executed)
	}
	if orgAllowedCalled {
		t.Fatal("orgAllowed was called for a document refused on KIND alone -- the kind check must run first")
	}
}

// Clause 2: ORG NOT ON THE ALLOWLIST. A real mutation, real auth, correct
// kind -- refused solely because orgAllowed says no.
func TestProofWriteRefusesAnOrgNotOnTheAllowlist(t *testing.T) {
	handler, executed := pwHandler(t, func(context.Context, string) bool { return false })
	rec := pwPost(handler, pwMutation, "org-not-allowed")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("got %d, want 403", rec.Code)
	}
	if *executed != 0 {
		t.Fatalf("resolver ran %d time(s), want 0", *executed)
	}
}

// Clause 3: STORE ERROR (fails closed). The real orgAllowed wrapper
// (newQueryHandler) collapses a Postgres error into `false` with a log
// line, never a fall-through -- from the HANDLER's point of view, a store
// error and "not on the list" are and must stay indistinguishable, both
// refusing exactly the same way. This is what makes that collapse safe to
// test at this layer without a real Postgres failure: the contract is "any
// false from orgAllowed refuses", proven once, generically, rather than by
// re-deriving it per failure mode.
func TestProofWriteRefusesWhenOrgAllowedItselfSignalsFalse(t *testing.T) {
	calls := 0
	handler, executed := pwHandler(t, func(context.Context, string) bool {
		calls++
		return false // stands in for BOTH "not on the list" and "store errored"
	})
	rec := pwPost(handler, pwMutation, "org-1")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("got %d, want 403", rec.Code)
	}
	if *executed != 0 {
		t.Fatalf("resolver ran %d time(s), want 0", *executed)
	}
	if calls != 1 {
		t.Fatalf("orgAllowed called %d time(s), want exactly 1", calls)
	}
}

// Clause 4: EMPTY OrgID fails closed WITHOUT ever calling orgAllowed -- an
// empty org must never reach a caller that might (today or in a future
// edit) treat "nothing to check" as "allowed".
func TestProofWriteRefusesAnEmptyOrgIDWithoutCallingOrgAllowed(t *testing.T) {
	orgAllowedCalled := false
	handler, executed := pwHandler(t, func(context.Context, string) bool {
		orgAllowedCalled = true
		return true
	})
	rec := pwPost(handler, pwMutation, "")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("got %d, want 403", rec.Code)
	}
	if *executed != 0 {
		t.Fatalf("resolver ran %d time(s), want 0", *executed)
	}
	if orgAllowedCalled {
		t.Fatal("orgAllowed was called with an empty OrgID -- the empty check must refuse first, structurally")
	}
}

// Clause 5: THE HAPPY PATH. Correct kind, org on the allowlist -- the one
// case that must NOT be refused, so the four refusal tests above are
// pinning real clauses and not a handler that always says no.
func TestProofWriteServesAnAllowedOrgsMutation(t *testing.T) {
	handler, executed := pwHandler(t, func(context.Context, string) bool { return true })
	rec := pwPost(handler, pwMutation, "org-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rec.Code)
	}
	if *executed != 1 {
		t.Fatalf("resolver ran %d time(s), want 1", *executed)
	}
}

// Clause 6: SWITCH OFF. Same request shape as the happy path above --
// correct kind, org allowed, registered operation -- refused solely because
// the Mux's Switch says the operation is not enabled (an empty StaticSwitch,
// the same shape newQueryHandler builds for proofWriteMux when nothing is
// registered). Isolates the SWITCH clause from the kind/org clauses above,
// which all use a Switch that is already enabled for "m".
func TestProofWriteRefusesWhenTheSwitchIsOff(t *testing.T) {
	t.Helper()
	verifier, _ := iaVerifier(t)
	executed := new(int)
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{}) // off for every operation
	mux.Register("m", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*executed++
		w.WriteHeader(http.StatusOK)
	}))
	byDigest := map[string]string{digestHex(pwMutation): "m"}
	handler := newDocumentDispatchHandler(os.Getenv, mux, byDigest, verifier, nil, nil, digest.KindMutation, func(context.Context, string) bool { return true })

	rec := pwPost(handler, pwMutation, "org-1")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404 (routeswitch.Mux's own off-switch answer)", rec.Code)
	}
	if *executed != 0 {
		t.Fatalf("resolver ran %d time(s), want 0", *executed)
	}
}

// Scribe VET finding (this session): every test above proves the DISPATCH
// HANDLER fails closed on a false orgAllowed -- none of them exercise the
// REAL closure newQueryHandler builds (newProofOrgAllowed), which is the
// only place the actual fail-closed decision on a genuine store error is
// made. Mutating that closure's own `if err != nil { ...; return false }`
// to `return true` survived the whole server package suite, plain and
// -tags=integration, before this test existed -- a real gap on a security-
// sensitive path. This calls newProofOrgAllowed directly against a pool
// that can never answer (an unreachable DSN, same pattern
// TestNewQueryHandlerPairTreatsARegisteredMutationDifferently already uses
// in this package), forcing ProofOrgAllowed's own query to fail, and
// asserts the closure itself -- not a stub standing in for its contract --
// returns false.
func TestNewProofOrgAllowedFailsClosedOnARealStoreError(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:none@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	orgAllowed := newProofOrgAllowed(pool)
	if orgAllowed(context.Background(), "org-1") {
		t.Fatal("newProofOrgAllowed's real closure returned true against a pool that cannot answer -- a store error must fail closed, never open")
	}
}
