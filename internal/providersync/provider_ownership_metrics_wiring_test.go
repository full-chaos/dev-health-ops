package providersync

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The work-items sync sinks of GitLab, Jira and Linear refuse a
// work_item_team_attributions effect: the daily job is the one writer of that
// table. Each case constructs the REAL family/composite sink via its REAL
// constructor with a real providerfoundation.Metrics and hands it an effect
// that carries a granted candidate and a MembershipRejections entry. The
// refusal happens before any store call, so no row is appended and no
// ownership_checked sample is rendered for a write that did not happen.
func TestWorkItemSyncSinksRefuseTheTeamAttributionsEffect(t *testing.T) {
	type sinkUnderTest interface {
		EffectSink
		EffectReadback
	}
	for _, testCase := range []struct {
		provider string
		build    func(*ownershipReasonWriteConn, *providerfoundation.Metrics) (sinkUnderTest, error)
	}{
		{"gitlab", func(conn *ownershipReasonWriteConn, metrics *providerfoundation.Metrics) (sinkUnderTest, error) {
			return NewGitLabWorkItemFamilyClickHouseEffects(conn, providerOwnershipMetricsLease(), metrics)
		}},
		{"jira", func(conn *ownershipReasonWriteConn, metrics *providerfoundation.Metrics) (sinkUnderTest, error) {
			return NewJiraWorkItemCompositeClickHouseEffects(conn, providerOwnershipMetricsLease(), metrics)
		}},
		{"linear", func(conn *ownershipReasonWriteConn, metrics *providerfoundation.Metrics) (sinkUnderTest, error) {
			return NewLinearWorkItemFamilyClickHouseEffects(conn, providerOwnershipMetricsLease(), metrics)
		}},
	} {
		t.Run(testCase.provider, func(t *testing.T) {
			claim := nativeTestClaim(testCase.provider, "work-items")
			metrics := providerfoundation.NewMetrics()
			conn := &ownershipReasonWriteConn{}
			sink, err := testCase.build(conn, metrics)
			if err != nil {
				t.Fatal(err)
			}
			effect := providerOwnershipMetricsEffect(t, claim)
			if err := sink.WriteEffect(context.Background(), claim, effect); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("write error=%v want ErrInvalidConfiguration", err)
			}
			inspection, err := sink.InspectEffect(context.Background(), claim, effect)
			if !errors.Is(err, ErrInvalidConfiguration) || inspection != EffectConflict {
				t.Fatalf("readback=%s error=%v want a refused conflict", inspection, err)
			}
			if conn.batch != nil {
				t.Fatalf("the refused effect reached the store: appended=%v", conn.batch.Appended)
			}
			var output strings.Builder
			if err := metrics.WritePrometheus(&output); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(output.String(), "dev_health_team_attribution_ownership_checked_total{") {
				t.Fatalf("a refused write rendered an ownership sample:\n%s", output.String())
			}
		})
	}
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
