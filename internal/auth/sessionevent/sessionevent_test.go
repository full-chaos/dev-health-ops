package sessionevent

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

func check(t *testing.T, e audit.OutboxEvent) {
	t.Helper()
	if err := provisioning.CheckEvent(e.AggregateType, e.AggregateID, e.EventType, e.Change); err != nil {
		t.Fatalf("%s is not a provisioning-event.v1 event: %v", e.EventType, err)
	}
}

func TestEveryBuilderMakesAnEventTheContractAccepts(t *testing.T) {
	id := uuid.New()
	at := time.Date(2026, 11, 2, 9, 0, 0, 123456789, time.UTC)
	session, err := Session(id.String(), ReasonCaller)
	if err != nil {
		t.Fatal(err)
	}
	check(t, session)
	if session.AggregateID != sessionid.Wire(id) || session.Change["status"] != "revoked" {
		t.Fatalf("session event %+v", session)
	}
	for _, e := range []audit.OutboxEvent{
		Principal(id.String(), ReasonPasswordChange, at),
		Organization(id.String(), ReasonOrgEnded, at),
		SigningKey("example.key-2026-09"),
	} {
		check(t, e)
	}
	if got := Principal(id.String(), ReasonCaller, at).Change["revoked_before"]; got != "2026-11-02T09:00:00.123456Z" {
		t.Fatalf("revoked_before = %v, want the six-digit UTC form", got)
	}
}

func TestSessionRefusesAnIdThatIsNotAUUID(t *testing.T) {
	if _, err := Session("not-a-uuid", ReasonCaller); err == nil || strings.Contains(err.Error(), "not-a-uuid") {
		t.Fatalf("err = %v, want a refusal that does not quote the value", err)
	}
}

func TestEveryReasonIsInTheClosedContractVocabulary(t *testing.T) {
	for _, reason := range []string{ReasonCaller, ReasonRefreshReuse, ReasonPasswordChange, ReasonOrgEnded, ReasonDelegationEnd, ReasonCompromised} {
		if !provisioning.ChangeAllows("reason", reason) {
			t.Errorf("reason %q is outside the contract vocabulary", reason)
		}
	}
}
