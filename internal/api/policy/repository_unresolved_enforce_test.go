package policy

import (
	"context"
	"net/http"
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"
)

// A well-formed PDP deny with a reason the contract names is a PDP deny (403, counted deny), whichever reason it
// carries; a client that does not know the reason reads it as pdp_malformed and answers 503 (counted error_deny).
func TestEnforcedDenyOfRepositoryUnresolvedIsA403DenyLikeAnyWireReason(t *testing.T) {
	for _, reason := range []string{"role_lacks_action", "repository_unresolved"} {
		t.Run(reason, func(t *testing.T) {
			rec := recordEnforce(t)
			pdp := newTestPDP(t, denyWith(reason))
			client := pdp.client(t)
			var got pep.Decision
			spy := funcDecider{func(ctx context.Context, req pep.Request) pep.Decision {
				got = client.Decide(ctx, req)
				return got
			}}
			resp := enforceRequest(enforceHandler(t, personaStore(personaSuperuser), userClaims(nil), newEnforcerFor(t, spy, onFor("teams"))), "GET", "/api/v1/admin/teams")
			if got.Allowed() || got.Local() || string(got.Reason()) != reason {
				t.Fatalf("decision allowed=%v local=%v reason=%q, want a PDP deny %q", got.Allowed(), got.Local(), got.Reason(), reason)
			}
			if resp.status != http.StatusForbidden || resp.body != enforceDenyBodies["teams"] || resp.header.Get("Retry-After") != "" {
				t.Fatalf("answer %d %q retry-after %q, want 403 %q with no retry hint", resp.status, resp.body, resp.header.Get("Retry-After"), enforceDenyBodies["teams"])
			}
			if !slices.Equal(rec.snapshot(), []string{"teams/deny"}) {
				t.Fatalf("counted %v, want [teams/deny]", rec.snapshot())
			}
		})
	}
}
