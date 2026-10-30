package org

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// An organization audit row names the calling credential beside the human actor.
func TestAuditRowNamesTheCallingCredential(t *testing.T) {
	id, actor, workload := uuid.New(), uuid.New(), uuid.New()
	for name, caller := range map[string]audit.Caller{
		"workload": {Workload: &workload, JTI: "AAAAAAAAAAAAAAAAAAAAAA"},
		"service":  {CredentialID: uuid.NewString()},
	} {
		ev := auditFor(verbReactivate, id, Actor{PrincipalID: &actor, Caller: caller}, StatusSuspended, StatusActive)
		want := map[string]any{}
		caller.Annotate(want)
		for key, value := range want {
			if ev.Attributes[key] != value {
				t.Fatalf("%s: %s = %v, want %v", name, key, ev.Attributes[key], value)
			}
		}
		if ev.ActorPrincipalID == nil || *ev.ActorPrincipalID != actor.String() {
			t.Fatalf("%s: actor = %v", name, ev.ActorPrincipalID)
		}
		if err := audit.ValidateAttributes(ev.Attributes); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}
