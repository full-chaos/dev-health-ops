//go:build integration

package authruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// authorizeCorpusMarker is logged once the corpus was replayed through the router.
const authorizeCorpusMarker = "AUTH_AUTHORIZE_CORPUS_REPLAYED"

// azClockSlack bounds the gap between the test's clock and the database's
// around one call: the decision time is the transaction's now().
const azClockSlack = 2 * time.Second

// azGrantAheadSeconds is where a grant the corpus expires after its clock is
// seeded: ahead of the database clock by far more than one call takes.
const azGrantAheadSeconds = 300

type azSeeded struct {
	user      uuid.UUID
	orgs      map[string]string
	reference string
}

// seedCorpusRow writes one row's state as the writers leave it: go-api's
// users, organizations and memberships rows, the auth projections, an auth
// membership row for a status go-api has no row for, and the grant rows with
// their times moved onto the database clock. A principal the row says is
// suspended is deactivated after its session starts.
func (a *azRouterEnv) seedCorpusRow(t *testing.T, ctx context.Context, v azVector) azSeeded {
	t.Helper()
	s := v.State
	out := azSeeded{orgs: map[string]string{}}
	for _, label := range azOrgLabels(s) {
		status, projected := s.Organizations[label]
		org := a.azOrg(t, ctx, projected)
		if status == "suspended" {
			a.armed(t, ctx, `UPDATE auth.organizations SET disabled_at = now() WHERE id = $1`, org)
		}
		out.orgs[label] = org.String()
	}
	out.user = a.azUser(t, ctx, s.PlatformRole == "platform_admin")
	for _, label := range azSortedKeys(s.Memberships) {
		m := s.Memberships[label]
		if m.Status == "active" {
			a.exec(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, $3, now(), now())`,
				out.user, out.orgs[label], m.Role)
			continue
		}
		a.armed(t, ctx, `INSERT INTO auth.memberships (organization_id, principal_id, status, role) VALUES ($1, $2, $3, $4)`,
			out.orgs[label], out.user, m.Status, m.Role)
	}
	clock, err := time.Parse(time.RFC3339, *s.Clock)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range s.Grants {
		var ahead *float64
		if g.ExpiresAt != nil {
			at, err := time.Parse(time.RFC3339, *g.ExpiresAt)
			if err != nil {
				t.Fatal(err)
			}
			seconds := at.Sub(clock).Seconds()
			if seconds > 0 {
				seconds = azGrantAheadSeconds
			}
			ahead = &seconds
		}
		a.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id, expires_at, revoked_at)
VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6::double precision), CASE WHEN $7::boolean THEN now() END)`,
			out.user, out.orgs[g.OrgID], g.Role, g.ResourceKind, g.ResourceKey, ahead, g.Revoked)
	}
	out.reference = a.azSession(t, ctx, out.user)
	if s.PrincipalStatus == "suspended" {
		a.exec(t, ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, out.user)
	}
	return out
}

// azCheck holds a decision to the row's verdict: allowed, reason code, policy
// revision, the principal and organization, and the expiry (the decision time,
// or 30 s after it for a repository allow).
func azCheck(t *testing.T, v azVector, d contracts.AuthorizationDecision, user uuid.UUID, org string, before, after time.Time) {
	t.Helper()
	if d.Allowed != v.Expect.Allowed || d.ReasonCode != v.Expect.wireReason() || int(d.PolicyRevision) != v.Expect.PolicyRevision {
		t.Errorf("%s: allowed %v reason %q revision %d, want %v %q %d", v.ID, d.Allowed, d.ReasonCode, d.PolicyRevision,
			v.Expect.Allowed, v.Expect.wireReason(), v.Expect.PolicyRevision)
	}
	if got, want := azObligationsWire(t, d.Obligations), azExpectedObligations(t, v, org); got != want {
		t.Errorf("%s: obligations %s, want %s", v.ID, got, want)
	}
	if d.PrincipalID != session.WirePrincipalID(user) {
		t.Errorf("%s: principal_id %q, want %q", v.ID, d.PrincipalID, session.WirePrincipalID(user))
	}
	switch {
	case org == "" && d.OrganizationID != nil:
		t.Errorf("%s: organization_id %q, want null", v.ID, *d.OrganizationID)
	case org != "" && (d.OrganizationID == nil || *d.OrganizationID != org):
		t.Errorf("%s: organization_id %v, want %q", v.ID, d.OrganizationID, org)
	}
	var lifetime time.Duration
	if v.Expect.ExpiresIn != nil {
		lifetime = time.Duration(*v.Expect.ExpiresIn) * time.Second
	}
	decided := d.ExpiresAt.Time.Add(-lifetime)
	if decided.Before(before.Add(-azClockSlack)) || decided.After(after.Add(azClockSlack)) {
		t.Errorf("%s: expires_at %s less %s is outside the call [%s, %s]", v.ID, d.ExpiresAt.Time.Format(time.RFC3339Nano), lifetime,
			before.Format(time.RFC3339Nano), after.Format(time.RFC3339Nano))
	}
}

// TestAuthorizeCorpusReplaysThroughTheProductionRouter replays every pdp row
// of the golden decision corpus that the exclusion table does not name through
// POST /v1/authorize as configure builds it, over its state seeded into
// PostgreSQL, then all rows the decoder accepts as one batch (each item its
// own principal, so one item's state reaching another's is a red cell).
func TestAuthorizeCorpusReplaysThroughTheProductionRouter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rows := azReplayedRows(t)
	a := newAuthorizeRouterEnv(t, ctx, nil)
	a.exec(t, ctx, `INSERT INTO auth.roles (role_key, name, scope) VALUES ('platform_admin', 'platform_admin', 'platform') ON CONFLICT (role_key) DO NOTHING`)

	type item struct {
		v    azVector
		seed azSeeded
		org  string
		body []byte
	}
	var batch []item
	malformed := 0
	for _, v := range rows {
		seed := a.seedCorpusRow(t, ctx, v)
		body, org, err := azRenderBody(v, seed.orgs, seed.reference)
		if err != nil {
			t.Fatalf("%s: render: %v", v.ID, err)
		}
		if slices.Contains(azMalformedAtTheRouter, v.ID) {
			malformed++
			t.Run(v.ID, func(t *testing.T) { a.refused(t, a.authorize(t, body), http.StatusBadRequest, "malformed") })
			continue
		}
		batch = append(batch, item{v: v, seed: seed, org: org, body: body})
		t.Run(v.ID, func(t *testing.T) {
			before := time.Now()
			got := a.authorize(t, body)
			after := time.Now()
			if got.status != http.StatusOK {
				t.Fatalf("status %d, want 200; body %s", got.status, got.body)
			}
			d, err := contracts.ParseAuthorizationDecision(a.root, got.body, after)
			if err != nil {
				t.Fatalf("not authorization_decision.v1: %v; body %s", err, got.body)
			}
			azCheck(t, v, *d, seed.user, org, before, after)
		})
	}

	t.Run("one batch of every decoded row", func(t *testing.T) {
		bodies := make([][]byte, len(batch))
		for i, it := range batch {
			bodies[i] = it.body
		}
		request, err := azBatchBody(bodies)
		if err != nil {
			t.Fatal(err)
		}
		before := time.Now()
		got := a.authorizeBatch(t, request)
		after := time.Now()
		if got.status != http.StatusOK {
			t.Fatalf("status %d, want 200; body %s", got.status, got.body)
		}
		decisions, err := contracts.ParseAuthorizationBatchResponse(a.root, request, got.body, after)
		if err != nil {
			t.Fatalf("not authorization_batch_response.v1 for the request: %v", err)
		}
		for i, it := range batch {
			azCheck(t, it.v, decisions[i], it.seed.user, it.org, before, after)
		}
	})
	t.Logf("%s replayed=%d decided=%d malformed=%d batch=%d excluded=%d", authorizeCorpusMarker, len(rows), len(batch), malformed,
		len(batch), len(azCorpusExclusions))
}

// azObligationsWire is the obligations of a parsed decision as the canonical JSON the wire carried.
func azObligationsWire(t *testing.T, o *contracts.AuthorizationObligations) string {
	t.Helper()
	raw, err := json.Marshal(o)
	if err != nil {
		t.Fatal(err)
	}
	return azCanonical(t, raw)
}

// azExpectedObligations is the row's expect.obligations with its organization label replaced by the
// organization id the replay seeded.
func azExpectedObligations(t *testing.T, v azVector, org string) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(v.Expect.Obligations, &m); err != nil {
		t.Fatalf("%s: expect.obligations: %v", v.ID, err)
	}
	if _, ok := m["tenant_scope"]; ok {
		m["tenant_scope"] = org
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return azCanonical(t, raw)
}

func azCanonical(t *testing.T, raw []byte) string {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}
