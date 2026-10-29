package orgdeprovision

import (
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
)

// The one audit row carries ids and closed values only: the organization and,
// when the deprovision suspends it, its status change.
func TestAuditRowIsInTheAllowlist(t *testing.T) {
	actor, request := uuid.New(), uuid.NewString()
	in := Input{OrgID: uuid.New(), Actor: Actor{PrincipalID: &actor, RequestID: &request}}
	for _, wasActive := range []bool{true, false} {
		ev := auditFor(in, wasActive)
		if err := audit.ValidateAttributes(ev.Attributes); err != nil {
			t.Fatalf("active=%v ValidateAttributes: %v", wasActive, err)
		}
		if ev.EventType != EventDeprovisioned || *ev.OrganizationID != in.OrgID.String() || *ev.ActorPrincipalID != actor.String() ||
			ev.Attributes["target_id"] != in.OrgID.String() || ev.Attributes["reason_code"] != "organization_updated" {
			t.Fatalf("active=%v audit row = %+v", wasActive, ev)
		}
		_, changed := ev.Attributes["new_status"]
		if changed != wasActive {
			t.Fatalf("active=%v: status change named = %v", wasActive, changed)
		}
	}
	e := org.SuspendedEvent(in.OrgID)
	if err := provisioning.CheckEvent(e.AggregateType, e.AggregateID, e.EventType, e.Change); err != nil || e.EventType != "organization.suspended" {
		t.Fatalf("suspended event %+v: %v", e, err)
	}
}

func TestOutcomeLabels(t *testing.T) {
	for err, want := range map[error]string{
		nil: "ok", ErrNotFound: "not_found", ErrDenied: "denied", ErrNotOwned: "not_owned", ErrStateMoved: "state_moved",
		fmt.Errorf("%w: x", ErrStore): "error", errors.New("other"): "error",
	} {
		if got := outcomeLabel(err); got != want {
			t.Errorf("outcomeLabel(%v) = %q, want %q", err, got, want)
		}
	}
}

func TestNewNeedsEveryCollaborator(t *testing.T) {
	for name, cfg := range map[string]Config{"empty": {}, "no stores": {Schema: mustSchema(t)}} {
		if _, err := New(cfg); err == nil {
			t.Fatalf("%s: New accepted", name)
		}
	}
}
