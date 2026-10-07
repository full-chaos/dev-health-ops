package providersync

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestGitLabWorkItemFamilyConstructorEmitsOwnershipMetrics,
// TestJiraWorkItemCompositeConstructorEmitsOwnershipMetrics, and
// TestLinearWorkItemFamilyConstructorEmitsOwnershipMetrics are CHAOS-4320's
// red-first pins for codex round 6's second P1 (NOT CLEAN, executed repro):
// the real worker constructors (internal/workerservice/provider_sync.go)
// called these three providers' family/composite constructors WITHOUT a
// Metrics argument at all -- the rejection/ownership plumbing this ticket
// adds was otherwise correct for all four providers, but
// RecordTeamAttributionOwnershipChecked's nil-receiver-safe no-op silently
// swallowed every sample for GitLab, Jira, and Linear regardless: a write
// could succeed and still produce zero ownership_checked telemetry. Round 4
// treated this as an accepted scope limitation ("nil Metrics means this new
// counter is not a provider-wide instrument"); round 6 held the PR body's
// own "all four providers... to the SAME writer" claim to account and found
// it false for three of them.
//
// Each test constructs the REAL family/composite sink via its REAL
// constructor (the only way WriteEffect's completeness check passes) with a
// real providerfoundation.Metrics, writes an effect carrying both a granted
// candidate and a MembershipRejections entry through the real dispatch
// path, and asserts both counter outcomes rendered.
func TestGitLabWorkItemFamilyConstructorEmitsOwnershipMetrics(t *testing.T) {
	claim := nativeTestClaim("gitlab", "work-items")
	metrics := providerfoundation.NewMetrics()
	sink, err := NewGitLabWorkItemFamilyClickHouseEffects(
		&ownershipReasonWriteConn{}, providerOwnershipMetricsLease(), metrics,
	)
	if err != nil {
		t.Fatal(err)
	}
	effect := providerOwnershipMetricsEffect(t, claim)
	if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
		t.Fatal(err)
	}
	assertProviderOwnershipMetricsRendered(t, metrics, "gitlab")
}

func TestJiraWorkItemCompositeConstructorEmitsOwnershipMetrics(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	metrics := providerfoundation.NewMetrics()
	sink, err := NewJiraWorkItemCompositeClickHouseEffects(
		&ownershipReasonWriteConn{}, providerOwnershipMetricsLease(), metrics,
	)
	if err != nil {
		t.Fatal(err)
	}
	effect := providerOwnershipMetricsEffect(t, claim)
	if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
		t.Fatal(err)
	}
	assertProviderOwnershipMetricsRendered(t, metrics, "jira")
}

func TestLinearWorkItemFamilyConstructorEmitsOwnershipMetrics(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	metrics := providerfoundation.NewMetrics()
	sink, err := NewLinearWorkItemFamilyClickHouseEffects(
		&ownershipReasonWriteConn{}, providerOwnershipMetricsLease(), metrics,
	)
	if err != nil {
		t.Fatal(err)
	}
	effect := providerOwnershipMetricsEffect(t, claim)
	if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
		t.Fatal(err)
	}
	assertProviderOwnershipMetricsRendered(t, metrics, "linear")
}

func providerOwnershipMetricsLease() providerfoundation.LeaseGuard {
	return providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
}

// providerOwnershipMetricsEffect builds one work_item_team_attributions
// EffectBatch carrying a granted assignee_membership candidate AND a
// MembershipRejections entry, both org-acme-tenanted to match
// nativeTestClaim's OrgID -- the exact shape needed to exercise both
// ownership_checked outcomes through a single write.
func providerOwnershipMetricsEffect(t *testing.T, claim Claim) EffectBatch {
	t.Helper()
	rows := []githubWorkItemTeamAttributionRow{{
		WorkItemID: "acme/api#1", Provider: claim.Provider, Source: "assignee_membership",
		IsPrimary: 1, Confidence: "high", Evidence: "assignee=dev@example.com",
		OrgID: claim.OrgID, OwnershipReason: "ownership_unknown",
	}}
	effect, err := effectBatchFromValues(githubTeamAttributionsDestination, EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	teamID := "nonowner"
	rejections := []githubWorkItemTeamAttributionRejectionRow{{
		WorkItemID: "acme/api#2", Provider: claim.Provider, Source: "author_membership",
		TeamID: &teamID, Reason: "repo_not_owned",
	}}
	marshaled, err := marshalGitHubWorkItemDerivedRows(rejections)
	if err != nil {
		t.Fatal(err)
	}
	effect.MembershipRejections = marshaled
	return effect
}

func assertProviderOwnershipMetricsRendered(t *testing.T, metrics *providerfoundation.Metrics, provider string) {
	t.Helper()
	var output strings.Builder
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	if !strings.Contains(rendered, `dev_health_team_attribution_ownership_checked_total{reason="ownership_unknown"} 1`) {
		t.Fatalf("%s: granted candidate produced no ownership_unknown sample -- Metrics is likely nil in the real constructor:\n%s", provider, rendered)
	}
	if !strings.Contains(rendered, `dev_health_team_attribution_ownership_checked_total{reason="repo_not_owned"} 1`) {
		t.Fatalf("%s: rejection produced no repo_not_owned sample -- Metrics is likely nil in the real constructor:\n%s", provider, rendered)
	}
}
