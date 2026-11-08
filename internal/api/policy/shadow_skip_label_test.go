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
	skip := func(pattern string, highRisk bool, served servedOrg) string {
		rec := recordShadow(t)
		sh, _ := newShadowFor(t, newTestPDP(t, allowAll).client(t), func(c *ShadowConfig) { c.Rows = rows; c.HighRisk = highRisk })
		sh.Observe(context.Background(), ShadowObservation{Pattern: pattern, Bearer: "aaaa.bbbb.cccc", OrgID: "11111111-1111-1111-1111-111111111111",
			Caller: ShadowCallerMember, UserAccessToken: true, LegacyAllowed: true, served: served})
		sh.Wait()
		got := rec.resultReasons()
		if len(got) != 1 {
			t.Fatalf("%s: results %v, want one", pattern, got)
		}
		return got[0]
	}
	never := skip("Query.productTelemetryPlatformDashboard", false, servedOrg{})
	highRiskOff := skip("Mutation.createTeam", false, servedOrg{})
	ordinary := skip("Query.teams", true, servedOrg{named: true, raw: "22222222-2222-2222-2222-222222222222"})
	if never != "skipped/never_decided" || highRiskOff != "skipped/high_risk_off" || ordinary != "skipped/none" {
		t.Fatalf("never-decided %q, high-risk-off %q, ordinary %q", never, highRiskOff, ordinary)
	}
	if never == highRiskOff || never == ordinary || highRiskOff == ordinary {
		t.Fatalf("labels not all different: %q %q %q", never, highRiskOff, ordinary)
	}
}
