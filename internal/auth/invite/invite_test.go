package invite

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
)

// Unknown, accepted, revoked and expired invites give one refusal; a pending
// invite is consumable up to and at its expires_at instant.
func TestCheckConsumableHasOneRefusalShape(t *testing.T) {
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	pending := row{found: true, status: StatusPending, expiresAt: now.Add(time.Hour)}
	cases := []struct {
		name string
		r    row
		want error
	}{
		{"pending", pending, nil},
		{"pending at its expiry instant", row{found: true, status: StatusPending, expiresAt: now}, nil},
		{"unknown", row{}, ErrInviteInvalid},
		{"expired", row{found: true, status: StatusPending, expiresAt: now.Add(-time.Nanosecond)}, ErrInviteInvalid},
		{"accepted", row{found: true, status: StatusAccepted, expiresAt: now.Add(time.Hour)}, ErrInviteInvalid},
		{"revoked", row{found: true, status: StatusRevoked, expiresAt: now.Add(time.Hour)}, ErrInviteInvalid},
		{"unknown status", row{found: true, status: "expired", expiresAt: now.Add(time.Hour)}, ErrInviteInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkConsumable(c.r, now)
			if c.want == nil && err != nil || c.want != nil && err != c.want {
				t.Fatalf("checkConsumable = %v, want exactly %v", err, c.want)
			}
		})
	}
}

func TestEmailRefusalIsGoAPIParity(t *testing.T) {
	cases := []struct {
		name            string
		invite, account string
		verified        bool
		want            error
	}{
		{"same address", "a@example.test", "a@example.test", true, nil},
		{"case and space", "a@example.test", "  A@Example.TEST ", true, nil},
		{"unverified", "a@example.test", "a@example.test", false, ErrEmailUnverified},
		{"unverified and different: unverified first", "a@example.test", "b@example.test", false, ErrEmailUnverified},
		{"different", "a@example.test", "b@example.test", true, ErrEmailMismatch},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := emailRefusal(c.invite, c.account, c.verified); !errors.Is(err, c.want) || (c.want == nil && err != nil) {
				t.Fatalf("emailRefusal = %v, want %v", err, c.want)
			}
		})
	}
}

func TestNormalizeEmailAcceptsOnlyAFixedPoint(t *testing.T) {
	if got, err := normalizeEmail("member@example.com"); err != nil || got != "member@example.com" {
		t.Fatalf("normalizeEmail(member@example.com) = %q, %v", got, err)
	}
	for _, raw := range []string{"Member@example.com", " member@example.com", "member@example", "not an address", "", "a..b@example.com", "member@example.test"} {
		if _, err := normalizeEmail(raw); !errors.Is(err, ErrEmailInvalid) {
			t.Errorf("normalizeEmail(%q) = %v, want ErrEmailInvalid", raw, err)
		}
	}
}

func TestLifetimeBounds(t *testing.T) {
	d := func(v time.Duration) *time.Duration { return &v }
	if got, err := lifetime(nil); err != nil || got != 72*time.Hour {
		t.Fatalf("lifetime(nil) = %v, %v, want 72h", got, err)
	}
	for _, ok := range []time.Duration{time.Hour, 30 * 24 * time.Hour, 48 * time.Hour} {
		if got, err := lifetime(d(ok)); err != nil || got != ok {
			t.Errorf("lifetime(%v) = %v, %v", ok, got, err)
		}
	}
	for _, bad := range []time.Duration{time.Hour - time.Second, 30*24*time.Hour + time.Second, 0, -time.Hour} {
		if _, err := lifetime(d(bad)); !errors.Is(err, ErrTTLInvalid) {
			t.Errorf("lifetime(%v) = %v, want ErrTTLInvalid", bad, err)
		}
	}
}

func TestDecideRevokeIsMonotonic(t *testing.T) {
	if noop, err := decideRevoke(StatusPending); noop || err != nil {
		t.Fatalf("revoke pending = %v, %v", noop, err)
	}
	if noop, err := decideRevoke(StatusRevoked); !noop || err != nil {
		t.Fatalf("revoke revoked = %v, %v, want a no-op", noop, err)
	}
	if _, err := decideRevoke(StatusAccepted); !errors.Is(err, ErrNotPending) {
		t.Fatalf("revoke accepted = %v, want ErrNotPending", err)
	}
}

func TestEventsAreProvisioningEvents(t *testing.T) {
	id := uuid.New()
	for v, want := range map[verb]struct{ event, status string }{
		verbCreate: {"invite.created", "pending"}, verbAccept: {"invite.accepted", "accepted"}, verbRevoke: {"invite.revoked", "revoked"},
	} {
		e := eventFor(v, id)
		if e.AggregateType != "invite" || e.AggregateID != provisioning.WireID("invite", id.String()) || e.EventType != want.event || e.Change["status"] != want.status {
			t.Fatalf("%s event = %+v", v, e)
		}
		if err := provisioning.CheckEvent(e.AggregateType, e.AggregateID, e.EventType, e.Change); err != nil {
			t.Fatalf("%s CheckEvent: %v", v, err)
		}
	}
}

func TestAuditRowsPassTheAllowlist(t *testing.T) {
	org, id, subject, actor := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	for v, want := range map[verb]struct{ event, reason string }{
		verbCreate: {"auth.invite.created", "invite_created"}, verbAccept: {"auth.invite.accepted", "invite_accepted"},
		verbRevoke: {"auth.invite.revoked", "invite_revoked"},
	} {
		ev := auditFor(v, org, id, &subject, Actor{PrincipalID: &actor})
		if ev.EventType != want.event || ev.Outcome != audit.OutcomeAllowed || ev.Attributes["reason_code"] != want.reason ||
			ev.Attributes["target_kind"] != "invite" || ev.Attributes["target_id"] != id.String() {
			t.Fatalf("%s audit = %+v", v, ev)
		}
		if ev.OrganizationID == nil || *ev.OrganizationID != org.String() || ev.ActorPrincipalID == nil || *ev.ActorPrincipalID != actor.String() {
			t.Fatalf("%s audit ids = %+v", v, ev)
		}
		if err := audit.ValidateAttributes(ev.Attributes); err != nil {
			t.Fatalf("%s ValidateAttributes: %v", v, err)
		}
	}
}

func TestOutcomeLabelsAreClosed(t *testing.T) {
	cases := map[error]string{
		nil: "ok", audit.ErrNoStateChange: "noop", ErrInviteInvalid: "invalid_invite", ErrEmailUnverified: "email_unverified",
		ErrEmailMismatch: "email_mismatch", ErrEmailInvalid: "invalid", ErrRoleInvalid: "invalid", ErrTTLInvalid: "invalid",
		ErrPendingExists: "pending_exists", ErrNotFound: "not_found", ErrNotPending: "not_pending", ErrAlreadyMember: "already_member",
		ErrOrgNotFound: "not_found", ErrOrgSuspended: "suspended", ErrOrgConflict: "org_conflict", ErrInviterNotFound: "not_found", ErrDenied: "denied",
		ErrNotOwned: "not_owned", ErrAccountInactive: "inactive", errors.New("other"): "error",
	}
	for err, want := range cases {
		if got := outcomeLabel(err); got != want {
			t.Errorf("outcomeLabel(%v) = %q, want %q", err, got, want)
		}
	}
}
