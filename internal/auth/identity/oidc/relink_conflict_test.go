package oidc

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// r2 review P1: the owned re-link refuses an account that already holds another subject at the provider row with
// identity.ErrProviderAlreadyLinked. That is an identity conflict (409 identity_conflict), never a store outage, and
// nothing is provisioned or issued.
func TestResolveProviderAlreadyLinkedIsAnIdentityConflict(t *testing.T) {
	for _, protocol := range []string{"oidc", "oauth"} {
		for name, err := range map[string]error{
			"the store sentinel": identity.ErrProviderAlreadyLinked,
			"wrapped":            fmt.Errorf("relink: %w", identity.ErrProviderAlreadyLinked),
		} {
			t.Run(protocol+"/"+name, func(t *testing.T) {
				ids := &resolveIdentities{err: err}
				_, failure, sessions := resolveRun(protocol, ids, resolveMembers{found: true, role: "member"}, nil)
				wantRefused(t, failure, http.StatusConflict, ReasonIdentityConflict)
				if ids.provision != 0 || len(sessions.issued) != 0 {
					t.Fatalf("provisioned %d, sessions issued %d; want none", ids.provision, len(sessions.issued))
				}
			})
		}
	}
}
