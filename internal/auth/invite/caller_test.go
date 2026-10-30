package invite

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// An invite audit row names the calling credential beside the human actor.
func TestAuditRowNamesTheCallingCredential(t *testing.T) {
	org, id, subject, actor, workload := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	for name, caller := range map[string]audit.Caller{
		"workload": {Workload: &workload, JTI: "AAAAAAAAAAAAAAAAAAAAAA"},
		"service":  {CredentialID: uuid.NewString()},
	} {
		for _, v := range []verb{verbCreate, verbAccept, verbRevoke} {
			ev := auditFor(v, org, id, &subject, Actor{PrincipalID: &actor, Caller: caller})
			want := map[string]any{}
			caller.Annotate(want)
			for key, value := range want {
				if ev.Attributes[key] != value {
					t.Fatalf("%s %s: %s = %v, want %v", name, v, key, ev.Attributes[key], value)
				}
			}
			if err := audit.ValidateAttributes(ev.Attributes); err != nil {
				t.Fatalf("%s %s: %v", name, v, err)
			}
		}
	}
}

// The membership an accept adds is written with the accept's caller.
func TestAcceptHandsItsCallerToTheMembership(t *testing.T) {
	actor, workload, request := uuid.New(), uuid.New(), uuid.NewString()
	caller := audit.Caller{Workload: &workload, JTI: "AAAAAAAAAAAAAAAAAAAAAA"}
	got := membershipActor(Actor{PrincipalID: &actor, RequestID: &request, Caller: caller})
	if got.PrincipalID == nil || *got.PrincipalID != actor || got.RequestID == nil || *got.RequestID != request || got.Caller.Workload == nil || *got.Caller.Workload != workload ||
		got.Caller.JTI != caller.JTI || got.Caller.CredentialID != "" {
		t.Fatalf("membership actor = %+v", got)
	}
}
