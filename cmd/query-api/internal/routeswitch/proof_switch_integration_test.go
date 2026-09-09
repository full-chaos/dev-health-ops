//go:build integration

package routeswitch

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// The proof switch differs from the production switch in exactly one
// mode, and both directions matter: shadow must become reachable HERE,
// and "python"/"disabled" must stay unreachable. Table-driven across the
// whole plan §5 vocabulary rather than a single shadow happy path -- a
// measurement route that quietly executed an operation somebody turned
// OFF would be a worse surprise than the one it exists to fix.
func TestProofSwitch_AdmitsShadowAndNothingElseExtra(t *testing.T) {
	pool := startRoutingStatePostgres(t)

	cases := []struct {
		mode                string
		wantProductionReach bool
		wantProofReach      bool
	}{
		{mode: "python", wantProductionReach: false, wantProofReach: false},
		{mode: "shadow", wantProductionReach: false, wantProofReach: true},
		{mode: "canary", wantProductionReach: true, wantProofReach: true},
		{mode: "primary", wantProductionReach: true, wantProofReach: true},
		{mode: "disabled", wantProductionReach: false, wantProofReach: false},
	}

	for _, tc := range cases {
		t.Run("mode_"+tc.mode, func(t *testing.T) {
			operation := "proof_op_" + tc.mode
			documentDigest := "proof-doc-" + tc.mode
			insertRoutingState(t, pool, documentDigest, operation, tc.mode)
			digests := map[string]string{operation: documentDigest}

			reachable := func(sw Switch) bool {
				mux := NewMux(sw)
				mux.Register(operation, handlerNamed(operation))
				rec := httptest.NewRecorder()
				mux.Dispatch(operation, rec, httptest.NewRequest(http.MethodGet, "/query", nil))
				return rec.Code == http.StatusOK
			}

			// Production reachability must be UNCHANGED by this change.
			// Asserted here, beside the widened switch, so a future edit
			// that widens the wrong mode set fails in the same test.
			if got := reachable(NewPostgresSwitch(pool, testSchemaDigest, digests)); got != tc.wantProductionReach {
				t.Errorf("production switch: mode=%q reachable=%v, want %v", tc.mode, got, tc.wantProductionReach)
			}
			if got := reachable(NewProofSwitch(pool, testSchemaDigest, digests)); got != tc.wantProofReach {
				t.Errorf("proof switch: mode=%q reachable=%v, want %v", tc.mode, got, tc.wantProofReach)
			}
		})
	}
}

// The proof switch must not fail open either: an operation with no row at
// all stays unreachable, exactly as it does in production.
func TestProofSwitch_UnregisteredOperationIsStillUnreachable(t *testing.T) {
	pool := startRoutingStatePostgres(t)

	sw := NewProofSwitch(pool, testSchemaDigest, map[string]string{"neverRegistered": "doc-x"})
	if sw.Enabled("neverRegistered") {
		t.Fatal("an operation with no routing row must stay unreachable on the proof switch too")
	}
}

// A row at a DIFFERENT schema digest is dead for the proof switch as
// well: the measurement route reads the same key the production one does,
// which is the point of sharing the query rather than copying it.
func TestProofSwitch_StaleSchemaDigestIsUnreachable(t *testing.T) {
	pool := startRoutingStatePostgres(t)
	insertRoutingState(t, pool, "doc-stale", "staleDigestOp", "shadow")

	sw := NewProofSwitch(pool, "sha256:some-other-schema-digest", map[string]string{"staleDigestOp": "doc-stale"})
	if sw.Enabled("staleDigestOp") {
		t.Fatal("a row keyed to another schema digest must be unreachable")
	}
}
