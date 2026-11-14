package policy

import (
	"context"
	"testing"
)

// A skip the decision gate never decides, a high-risk-off skip and an ordinary skip each carry their own reason label.
func TestShadowSkipReasonLabelsDiffer(t *testing.T) {
	rows := map[string]ShadowRow{
		"Query.productTelemetryPlatformDashboard": {Action: "admin.telemetry", Namespace: "admin", ResourceType: "platform", HighRisk: true},
		"Mutation.createTeam":                     {Action: "teams.write", Namespace: "teams", ResourceType: "organization", HighRisk: true},
		"Query.teams":                             {Action: "teams.read", Namespace: "teams", ResourceType: "organization"},
	}
	skip := func(pattern string, highRisk bool, orgID string) string {
		rec := recordShadow(t)
		sh, _ := newShadowFor(t, newTestPDP(t, allowAll).client(t), func(c *ShadowConfig) { c.Rows = rows; c.HighRisk = highRisk })
		sh.Observe(context.Background(), ShadowObservation{Pattern: pattern, Bearer: "aaaa.bbbb.cccc", OrgID: orgID,
			Caller: ShadowCallerMember, UserAccessToken: true, LegacyAllowed: true})
		sh.Wait()
		got := rec.resultReasons()
		if len(got) != 1 {
			t.Fatalf("%s: results %v, want one", pattern, got)
		}
		return got[0]
	}
	const org = "11111111-1111-1111-1111-111111111111"
	never := skip("Query.productTelemetryPlatformDashboard", false, org)
	highRiskOff := skip("Mutation.createTeam", false, org)
	// A token with no organization on an organization row, from a service that names no org-less question (not go-api).
	ordinary := skip("Query.teams", true, "")
	if never != "skipped/never_decided" || highRiskOff != "skipped/high_risk_off" || ordinary != "skipped/none" {
		t.Fatalf("never-decided %q, high-risk-off %q, ordinary %q", never, highRiskOff, ordinary)
	}
	if never == highRiskOff || never == ordinary || highRiskOff == ordinary {
		t.Fatalf("labels not all different: %q %q %q", never, highRiskOff, ordinary)
	}
}
