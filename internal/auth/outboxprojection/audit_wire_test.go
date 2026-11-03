package outboxprojection

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/auditwire"
)

// Every attribute the projection sets, as store.go sets them, on both domains,
// with and without an actor, under both outcomes.
func TestDecisionRowIsAValidAuditEventV1(t *testing.T) {
	org := uuid.NewString()
	actor := uuid.NewString()
	reasons := []string{"credential_issued", "credential_rotated", "expired", MapRevokeReason("compromised"), MapRevokeReason("owner_request"), MapRevokeReason("")}
	statuses := []pushlifecycle.Status{pushlifecycle.StatusActive, pushlifecycle.StatusRevoked, pushlifecycle.StatusExpired}
	for _, in := range []Input{pushIn(sampleEvent()), acrIn()} {
		for _, withActor := range []bool{false, true} {
			var a *string
			if withActor {
				a = &actor
			}
			denied := decisionAttributes(in)
			denied["reason_code"] = "denied_policy"
			row := decisionRow(audit.OutcomeDenied, a, &org, denied)
			auditwire.MustValidate(t, row)
			if err := audit.ValidateAttributes(row.Attributes); err != nil {
				t.Fatalf("denied ValidateAttributes: %v", err)
			}
			for i, reason := range reasons {
				attrs := decisionAttributes(in)
				attrs["reason_code"] = reason
				attrs["new_status"] = string(statuses[i%len(statuses)])
				attrs["principal_id"] = uuid.NewString()
				row := decisionRow(audit.OutcomeAllowed, a, &org, attrs)
				auditwire.MustValidate(t, row)
				if err := audit.ValidateAttributes(row.Attributes); err != nil {
					t.Fatalf("%s ValidateAttributes: %v", reason, err)
				}
			}
		}
	}
}
