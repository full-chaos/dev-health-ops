package customerpush

import (
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushevent"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
)

// ExpectedState is the state the credential projection must hold for a stored
// token (CHAOS-10897). It uses the producer's own mapping (pushevent.WireIDs, pushevent.ScopeNames,
// pushevent.Scopes), so the reconcile never holds a second copy of a rule. It
// reads no name, prefix, hash or last-use column. A row the lifecycle wire
// cannot carry (an organization id that is not a UUID, a scopes column that is
// not a list of strings) is Unwireable: the backfill refuses the same rows.
// Revocation is the stored revoked_at; expiry is judged by the diff, which
// holds the clock.
func ExpectedState(t tokenRow) pushreconcile.Token {
	out := pushreconcile.Token{ID: t.ID.String(), Revoked: t.RevokedAt != nil}
	organization, _, source, err := pushevent.WireIDs(t.OrgID, t.SourceID)
	if err != nil {
		out.Unwireable = true
		return out
	}
	org, err := uuid.Parse(strings.TrimPrefix(organization, "org_"))
	if err != nil || org == uuid.Nil || !strings.HasPrefix(organization, "org_") {
		out.Unwireable = true
		return out
	}
	names, err := pushevent.ScopeNames(t.Scopes)
	if err != nil {
		out.Unwireable = true
		return out
	}
	out.Organization = org.String()
	out.SourceID = source
	out.Scopes = slices.Clone(pushevent.Scopes(names))
	out.ExpiresAt = pushevent.Expiry(t.ExpiresAt)
	return out
}
