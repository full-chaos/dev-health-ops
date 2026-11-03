//go:build integration

package outboxprojection

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
)

const acrdDecidedAudit = `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.credential_projection.decided'`

// acrdSeed runs one statement inside audit.Commit as the migration role: the guarded tables take no other write.
func (e *env) acrdSeed(t *testing.T, sql string, args ...any) {
	t.Helper()
	err := audit.Commit(context.Background(), e.admin, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, sql, args...)
			return err
		},
		Audit:     audit.AuditEvent{EventType: "credential.test", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamWorkloadRegistry,
	})
	if err != nil {
		t.Fatalf("seed %q: %v", sql, err)
	}
}

// acrdSnapshot reads the registry row and the projection row of one credential as one text, updated_at included, so any write shows.
func (e *env) acrdSnapshot(t *testing.T, id string) string {
	t.Helper()
	var reg, proj string
	if err := e.admin.QueryRow(context.Background(), `SELECT coalesce(string_agg(coalesce(revoked_at::text,'-')||'|'||coalesce(revoked_reason::text,'-')||'|'||scopes::text||'|'||coalesce(expires_at::text,'-')||'|'||updated_at::text, ';'), 'none') FROM auth.credential_registry WHERE external_reference = $1`, id).Scan(&reg); err != nil {
		t.Fatal(err)
	}
	if err := e.admin.QueryRow(context.Background(), `SELECT coalesce(string_agg(revision::text||'|'||status||'|'||scopes::text||'|'||coalesce(terminal_at::text,'-')||'|'||updated_at::text, ';'), 'none') FROM auth.credential_projection WHERE external_reference = $1`, id).Scan(&proj); err != nil {
		t.Fatal(err)
	}
	return reg + " / " + proj
}

func (e *env) acrdStatus(t *testing.T, id string) string {
	t.Helper()
	var status string
	if err := e.admin.QueryRow(context.Background(), `SELECT coalesce(max(status), 'none') FROM auth.credential_projection WHERE external_reference = $1`, id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func (e *env) acrdRegistryRevoked(t *testing.T, id string) bool {
	t.Helper()
	return e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND revoked_at IS NOT NULL`, id) == 1
}

// acrdSpec is one acr event of a credential; body byte b names the posted body, so another kind at one revision is another body.
type acrdEvent struct {
	name string
	spec spec
	body byte
}

func (e *env) acrdIn(t *testing.T, ev acrdEvent) Input {
	t.Helper()
	return e.acr(t, ev.spec, "sub-1", ev.body)
}

func acrdTerminal(c spec, kind string, revision int, body byte, at time.Time) acrdEvent {
	s := c
	s.revision, s.kind, s.at = revision, kind, at
	if kind == contracts.ExternalPushRevoked {
		s.change = contracts.ExternalPushLifecycleChange{Status: "revoked"}
	} else {
		exp := contracts.Timestamp{Time: at.Add(24 * time.Hour)}
		s.change = contracts.ExternalPushLifecycleChange{Status: "expired", ExpiresAt: &exp}
	}
	return acrdEvent{name: kind, spec: s, body: body}
}

func acrdRotation(parent spec, scopes ...string) spec {
	id := "cred_" + uuid.NewString()[:12]
	return spec{org: parent.org, source: parent.source, id: id, revision: 1, kind: contracts.ExternalPushRotated, at: parent.at.Add(time.Second),
		change: contracts.ExternalPushLifecycleChange{Status: "active", Fingerprint: acrFingerprint(id), Scopes: scopes, RotationParentID: parent.id}}
}

// Cell I1: replays after a revoke decide as stale, duplicate or conflict and write nothing; the credential stays revoked.
func TestAcrDisorderReplayAfterRevokeWritesNothingAndNeverReactivates(t *testing.T) {
	e := getEnv(t, true)
	c := acrCreated(e.org(t))
	mustOutcome(t, e.project(t, e.acr(t, c, "sub-1", 1)), pushlifecycle.Applied)
	revoke := acrdTerminal(c, contracts.ExternalPushRevoked, 2, 2, c.at.Add(time.Second))
	mustOutcome(t, e.project(t, e.acrdIn(t, revoke)), pushlifecycle.Applied)
	before, audits := e.acrdSnapshot(t, c.id), e.count(t, acrdDecidedAudit)

	wide := c
	wide.change.Scopes = []string{"context:read", "admin:all"}
	otherKind := acrdTerminal(c, contracts.ExternalPushExpired, 2, 3, c.at.Add(time.Second))
	later := acrdTerminal(c, contracts.ExternalPushExpired, 3, 4, c.at.Add(2*time.Second))
	laterRevoke := acrdTerminal(c, contracts.ExternalPushRevoked, 3, 5, c.at.Add(2*time.Second))
	for _, tc := range []struct {
		name string
		in   Input
		want Outcome
	}{
		{"issued r1 again", e.acr(t, c, "sub-1", 1), pushlifecycle.Stale},
		{"issued r1, wider body", e.acr(t, wide, "sub-1", 9), OutcomeConflict},
		{"revoked r2 again", e.acrdIn(t, revoke), pushlifecycle.Duplicate},
		{"expired r2, another body", e.acrdIn(t, otherKind), OutcomeConflict},
		{"expired r3", e.acrdIn(t, later), pushlifecycle.RefusedTerminal},
		// expired r3 recorded its digest, so revoked r3 is another body at that revision
		{"revoked r3, another body", e.acrdIn(t, laterRevoke), OutcomeConflict},
	} {
		res := e.project(t, tc.in)
		if res.Outcome != tc.want {
			t.Errorf("%s: outcome %s, want %s", tc.name, res.Outcome, tc.want)
		}
		if res.Outcome == pushlifecycle.Applied {
			t.Fatalf("%s: applied after a revoke", tc.name)
		}
	}
	if after := e.acrdSnapshot(t, c.id); after != before {
		t.Fatalf("a replay wrote: %q -> %q", before, after)
	}
	if !e.acrdRegistryRevoked(t, c.id) || e.acrdStatus(t, c.id) != "revoked" {
		t.Fatal("the credential is not revoked")
	}
	// Only the refused_terminal replay (expired r3) decides something new and writes its denied audit row; the
	// stale, duplicate and conflict deliveries write none.
	if n := e.count(t, acrdDecidedAudit); n-audits != 1 {
		t.Fatalf("replays wrote %d audit rows, want exactly 1 (the refused_terminal replay)", n-audits)
	}
	if n := e.count(t, acrdDecidedAudit+` AND outcome = 'denied'`); n < 1 {
		t.Fatal("the refused_terminal replay wrote no denied audit row")
	}
}

// Cell I2: every ordered pair of acr events of one credential through the engine, each delivered twice: once a
// credential is terminal its registry and projection state never change and no event applies.
func TestAcrDisorderEveryEventPairThroughTheEngineNeverReactivates(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	pairs := 0
	// The end state of each pair after both deliveries: a terminal event before the issue waits and then applies
	// at revision 2; two terminal events with no issue never project anything.
	type end struct {
		status   string
		revision int
	}
	want := map[string]end{
		"issued then issued": {"active", 1}, "issued then " + contracts.ExternalPushRevoked: {"revoked", 2}, "issued then " + contracts.ExternalPushExpired: {"expired", 2},
		contracts.ExternalPushRevoked + " then issued": {"revoked", 2}, contracts.ExternalPushExpired + " then issued": {"expired", 2},
		contracts.ExternalPushRevoked + " then " + contracts.ExternalPushRevoked: {"none", 0}, contracts.ExternalPushRevoked + " then " + contracts.ExternalPushExpired: {"none", 0},
		contracts.ExternalPushExpired + " then " + contracts.ExternalPushRevoked: {"none", 0}, contracts.ExternalPushExpired + " then " + contracts.ExternalPushExpired: {"none", 0},
	}
	for _, firstKind := range []string{"issued", contracts.ExternalPushRevoked, contracts.ExternalPushExpired} {
		for _, secondKind := range []string{"issued", contracts.ExternalPushRevoked, contracts.ExternalPushExpired} {
			pairs++
			c := acrCreated(org)
			events := map[string]acrdEvent{
				"issued":                      {name: "issued", spec: c, body: 1},
				contracts.ExternalPushRevoked: acrdTerminal(c, contracts.ExternalPushRevoked, 2, 2, c.at.Add(time.Second)),
				contracts.ExternalPushExpired: acrdTerminal(c, contracts.ExternalPushExpired, 2, 3, c.at.Add(time.Second)),
			}
			var frozen string
			for step, kind := range []string{firstKind, secondKind, firstKind, secondKind} {
				ev := events[kind]
				in := e.acr(t, ev.spec, "sub-1", ev.body)
				label := firstKind + " then " + secondKind
				res := e.project(t, in)
				status := e.acrdStatus(t, c.id)
				if frozen != "" {
					if res.Outcome == pushlifecycle.Applied || e.acrdSnapshot(t, c.id) != frozen {
						t.Fatalf("%s, step %d: a terminal credential changed (outcome %s)", label, step, res.Outcome)
					}
					continue
				}
				if status == "revoked" || status == "expired" {
					frozen = e.acrdSnapshot(t, c.id)
					if status == "revoked" != e.acrdRegistryRevoked(t, c.id) && status == "revoked" {
						t.Fatalf("%s: projection revoked but the registry row is live", label)
					}
				}
			}
			label := firstKind + " then " + secondKind
			got := end{status: e.acrdStatus(t, c.id), revision: e.count(t, `SELECT coalesce(max(revision), 0) FROM auth.credential_projection WHERE external_reference = $1`, c.id)}
			if got != want[label] {
				t.Errorf("%s: ended %+v, want %+v", label, got, want[label])
			}
			if got.status == "revoked" && !e.acrdRegistryRevoked(t, c.id) {
				t.Errorf("%s: projection revoked but the registry row is live", label)
			}
			if got.status == "none" && e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1`, c.id) != 0 {
				t.Errorf("%s: a credential never issued has a registry row", label)
			}
		}
	}
	if pairs != 9 {
		t.Fatalf("%d pairs ran, want 9", pairs)
	}
}

// Cell I3: with the digest rows lost, the core still decides and the registry stays revoked.
func TestAcrDisorderReplayAfterRevokeWithLostDigestsWritesNoRegistryChange(t *testing.T) {
	e := getEnv(t, true)
	c := acrCreated(e.org(t))
	mustOutcome(t, e.project(t, e.acr(t, c, "sub-1", 1)), pushlifecycle.Applied)
	revoke := acrdTerminal(c, contracts.ExternalPushRevoked, 2, 2, c.at.Add(time.Second))
	mustOutcome(t, e.project(t, e.acrdIn(t, revoke)), pushlifecycle.Applied)
	before := e.acrdSnapshot(t, c.id)
	e.acrdSeed(t, `DELETE FROM auth.credential_projection_digest WHERE external_reference = $1`, c.id)
	wide := c
	wide.change.Scopes = []string{"context:read", "admin:all"}
	for _, tc := range []struct {
		name string
		in   Input
		want Outcome
	}{
		// the core decides the lost digest at once and records it
		{"issued r1, wider", e.acr(t, wide, "sub-1", 9), pushlifecycle.Stale},
		{"revoked r2", e.acrdIn(t, revoke), pushlifecycle.Duplicate},
	} {
		if res := e.project(t, tc.in); res.Outcome != tc.want {
			t.Errorf("%s: outcome %s, want %s", tc.name, res.Outcome, tc.want)
		}
	}
	if after := e.acrdSnapshot(t, c.id); after != before {
		t.Fatalf("a replay with lost digests wrote: %q -> %q", before, after)
	}
}

// Cell I4: a rotation before its parent waits and writes nothing; a wider child is refused and writes no registry
// row, however often it is delivered; the parent retired in either order never revives.
func TestAcrDisorderRotationThroughTheEngine(t *testing.T) {
	e := getEnv(t, true)
	org := e.org(t)
	parent := acrCreated(org)
	child := acrdRotation(parent, "context:read")
	res := e.project(t, e.acr(t, child, "sub-1", 5))
	mustOutcome(t, res, pushlifecycle.ParentPending)
	if res.Class != ClassRetry || e.count(t, `SELECT count(*) FROM auth.credential_projection WHERE external_reference = $1`, child.id) != 0 {
		t.Fatalf("a rotation before its parent: class %s, or wrote a row", res.Class)
	}
	mustOutcome(t, e.project(t, e.acr(t, parent, "sub-1", 1)), pushlifecycle.Applied)

	wide := acrdRotation(parent, "context:read", "admin:all")
	for try := 1; try <= 2; try++ {
		res := e.project(t, e.acr(t, wide, "sub-1", 6))
		if res.Class == ClassDone && res.Outcome == pushlifecycle.Applied {
			t.Fatalf("try %d: a wider child applied", try)
		}
		if try == 1 {
			mustOutcome(t, res, pushlifecycle.RefusedWiden)
		}
		if e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1`, wide.id) != 0 {
			t.Fatalf("try %d: a refused rotation wrote a registry row", try)
		}
	}

	retire := acrdTerminal(parent, contracts.ExternalPushRevoked, 2, 2, child.at)
	mustOutcome(t, e.project(t, e.acrdIn(t, retire)), pushlifecycle.Applied)
	mustOutcome(t, e.project(t, e.acr(t, child, "sub-1", 5)), pushlifecycle.Applied)
	second := acrdRotation(parent, "context:read")
	second.at = child.at.Add(time.Minute)
	mustOutcome(t, e.project(t, e.acr(t, second, "sub-1", 7)), pushlifecycle.RefusedParent)
	if e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1`, second.id) != 0 {
		t.Fatal("a second rotation of a retired parent wrote a registry row")
	}
	if !e.acrdRegistryRevoked(t, parent.id) || e.acrdStatus(t, parent.id) != "revoked" {
		t.Fatal("the parent is not revoked")
	}
	if e.acrdRegistryRevoked(t, child.id) || e.acrdStatus(t, child.id) != "active" {
		t.Fatal("the child is not active")
	}
}

// Cell I5: a delayed expired event never moves a revoked credential, and an expired event never extends a stored expiry.
func TestAcrDisorderDelayedExpiredNeverRevivesOrExtends(t *testing.T) {
	e := getEnv(t, true)
	c := acrCreated(e.org(t))
	c.change.ExpiresAt = &contracts.Timestamp{Time: c.at.Add(time.Hour)}
	mustOutcome(t, e.project(t, e.acr(t, c, "sub-1", 1)), pushlifecycle.Applied)
	expiredLate := acrdTerminal(c, contracts.ExternalPushExpired, 2, 3, c.at.Add(48*time.Hour))
	expiredLate.spec.change.ExpiresAt = &contracts.Timestamp{Time: c.at.Add(72 * time.Hour)}
	mustOutcome(t, e.project(t, e.acrdIn(t, expiredLate)), pushlifecycle.Applied)
	if n := e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1 AND expires_at <= $2`, c.id, c.at.Add(time.Hour+time.Second)); n != 1 {
		t.Fatal("a late expired event extended the stored expiry")
	}
	r := acrCreated(e.org(t))
	mustOutcome(t, e.project(t, e.acr(t, r, "sub-1", 1)), pushlifecycle.Applied)
	mustOutcome(t, e.project(t, e.acrdIn(t, acrdTerminal(r, contracts.ExternalPushRevoked, 2, 2, r.at.Add(time.Second)))), pushlifecycle.Applied)
	before := e.acrdSnapshot(t, r.id)
	res := e.project(t, e.acrdIn(t, acrdTerminal(r, contracts.ExternalPushExpired, 3, 4, r.at.Add(time.Hour))))
	mustOutcome(t, res, pushlifecycle.RefusedTerminal)
	if e.acrdSnapshot(t, r.id) != before {
		t.Fatal("a delayed expired event changed a revoked credential")
	}
}

// Cell I6: the refused rotation decided revision 1, so the child's later revoke is refused for good (one denied
// audit row), never a retry. The child never becomes live and has no registry row.
func TestAcrDisorderRefusedRotationRefusesTheChildsRevoke(t *testing.T) {
	e := getEnv(t, true)
	parent := acrCreated(e.org(t))
	mustOutcome(t, e.project(t, e.acr(t, parent, "sub-1", 1)), pushlifecycle.Applied)
	wide := acrdRotation(parent, "context:read", "admin:all")
	mustOutcome(t, e.project(t, e.acr(t, wide, "sub-1", 6)), pushlifecycle.RefusedWiden)
	audits := e.count(t, acrdDecidedAudit)
	res := e.project(t, e.acrdIn(t, acrdTerminal(wide, contracts.ExternalPushRevoked, 2, 2, wide.at.Add(time.Second))))
	mustOutcome(t, res, pushlifecycle.RefusedTransition)
	if res.Class != ClassRefused || e.count(t, acrdDecidedAudit) != audits+1 {
		t.Fatalf("class %s, or the refusal did not write exactly one audit row", res.Class)
	}
	if e.count(t, `SELECT count(*) FROM auth.credential_registry WHERE external_reference = $1`, wide.id) != 0 {
		t.Fatal("the refused child has a registry row")
	}
}
