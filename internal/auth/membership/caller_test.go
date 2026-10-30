package membership

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// A membership audit row names the calling credential beside the human actor.
func TestAuditRowNamesTheCallingCredential(t *testing.T) {
	org, principal, id, actor, workload := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	callers := map[string]audit.Caller{
		"workload": {Workload: &workload, JTI: "AAAAAAAAAAAAAAAAAAAAAA"},
		"service":  {CredentialID: uuid.NewString()},
	}
	for name, caller := range callers {
		ev := auditFor(verbAdd, org, principal, id, Actor{PrincipalID: &actor, Caller: caller}, state{}, state{status: StatusActive, role: "member"})
		want := map[string]any{}
		caller.Annotate(want)
		for key, value := range want {
			if ev.Attributes[key] != value {
				t.Fatalf("%s: %s = %v, want %v", name, key, ev.Attributes[key], value)
			}
		}
		if ev.ActorPrincipalID == nil || *ev.ActorPrincipalID != actor.String() {
			t.Fatalf("%s: actor = %v, want the human actor", name, ev.ActorPrincipalID)
		}
		if err := audit.ValidateAttributes(ev.Attributes); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}
