package orgdeprovision

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// The deprovision audit row names the calling credential beside the human actor.
func TestAuditRowNamesTheCallingCredential(t *testing.T) {
	actor, workload := uuid.New(), uuid.New()
	for name, caller := range map[string]audit.Caller{
		"workload": {Workload: &workload, JTI: "AAAAAAAAAAAAAAAAAAAAAA"},
		"service":  {CredentialID: uuid.NewString()},
	} {
		ev := auditFor(Input{OrgID: uuid.New(), Actor: Actor{PrincipalID: &actor, Caller: caller}}, true)
		want := map[string]any{}
		caller.Annotate(want)
		for key, value := range want {
			if ev.Attributes[key] != value {
				t.Fatalf("%s: %s = %v, want %v", name, key, ev.Attributes[key], value)
			}
		}
		if err := audit.ValidateAttributes(ev.Attributes); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}
