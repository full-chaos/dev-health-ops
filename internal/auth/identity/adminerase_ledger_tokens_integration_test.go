//go:build integration

package identity

import (
	"strings"
	"testing"
)

const tokenCellBlockers = "platform_role_assignments.granted_by, resource_grants.principal_id, resource_grants.revoked_by and service_accounts.owner_principal_id"

// The ledger reason is read as a set of table.column tokens: punctuation and repeats do not change the answer; any
// other token, a missing blocking key, or no token fails.
func TestLedgerReasonKeyTokenRule(t *testing.T) {
	cases := []struct {
		name, reason string
		ok           bool
	}{
		{"baseline", "The keys are " + tokenCellBlockers + "; they stay RESTRICT.", true},
		{"oxford comma", "The keys are platform_role_assignments.granted_by, resource_grants.principal_id, resource_grants.revoked_by, and service_accounts.owner_principal_id; x.", true},
		{"sentence-ending period", "The keys are " + tokenCellBlockers + ".", true},
		{"other lead-in", "The blocking keys are " + tokenCellBlockers + "; x.", true},
		{"backticks", "The keys are `platform_role_assignments.granted_by`, `resource_grants.principal_id`, `resource_grants.revoked_by` and `service_accounts.owner_principal_id`.", true},
		{"repeated key", "The keys are " + tokenCellBlockers + ", resource_grants.revoked_by; x.", true},
		{"reordered", "The keys are service_accounts.owner_principal_id, resource_grants.revoked_by, resource_grants.principal_id and platform_role_assignments.granted_by; x.", true},
		{"missing key", "The keys are platform_role_assignments.granted_by, resource_grants.principal_id and service_accounts.owner_principal_id.", false},
		{"unrelated table.column in prose", "The keys are " + tokenCellBlockers + "; see audit_events.actor_id for the trail.", false},
		{"second list of five", "The keys are " + tokenCellBlockers + "; it is also blocked by the keys memberships.principal_id, credential_projection.principal_id, push_sources.principal_id, workloads.principal_id and users.principal_id.", false},
		{"empty reason", "", false},
		{"no token", "closed: accepted difference, no keys named", false},
	}
	for _, name := range []string{"credential_projection.principal_id", "memberships.principal_id", "platform_role_assignments.principal_id", "push_sources.principal_id", "service_accounts.principal_id", "users.principal_id", "workloads.principal_id", "resource_grants.granted_by"} {
		cases = append(cases, struct {
			name, reason string
			ok           bool
		}{"fifth key named after the list: " + name, "The keys are " + tokenCellBlockers + "; a fifth blocker is " + name + ".", false})
	}
	for _, c := range cases {
		t.Run(strings.ReplaceAll(c.name, " ", "_"), func(t *testing.T) {
			err := checkLedgerKeys(c.reason)
			if c.ok && err != nil {
				t.Errorf("refused: %v", err)
			}
			if !c.ok && err == nil {
				t.Error("accepted, want refused")
			}
		})
	}
}
