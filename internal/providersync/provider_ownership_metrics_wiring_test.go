package providersync

import (
	"context"
	"encoding/json"
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

// The nine tables computed from stored work-item rows have one writer, the
// daily job. The work-items sync sink of each of the four providers refuses a
// write and a readback for every one of them, and nothing reaches the store.
// The effect is the evaluated-empty one, which an adapter that was still
// dispatched would accept without a row to decode.
func TestWorkItemSyncSinksRefuseEveryDailyJobTable(t *testing.T) {
	dailyJobTables := []string{
		"estimate_coverage_metrics_daily", "investment_classifications_daily", "investment_metrics_daily",
		"issue_type_metrics_daily", "work_item_cycle_times", "work_item_metrics_daily",
		"work_item_state_durations_daily", "work_item_team_attributions", "work_item_user_metrics_daily",
	}
	type sinkUnderTest interface {
		EffectSink
		EffectReadback
	}
	for _, testCase := range []struct {
		provider string
		build    func(*ownershipReasonWriteConn) (sinkUnderTest, error)
	}{
		{"github", func(conn *ownershipReasonWriteConn) (sinkUnderTest, error) {
			return NewGitHubWorkItemClickHouseEffects(conn, providerOwnershipMetricsLease(), nil)
		}},
		{"gitlab", func(conn *ownershipReasonWriteConn) (sinkUnderTest, error) {
			return NewGitLabWorkItemFamilyClickHouseEffects(conn, providerOwnershipMetricsLease(), nil)
		}},
		{"jira", func(conn *ownershipReasonWriteConn) (sinkUnderTest, error) {
			return NewJiraWorkItemCompositeClickHouseEffects(conn, providerOwnershipMetricsLease(), nil)
		}},
		{"linear", func(conn *ownershipReasonWriteConn) (sinkUnderTest, error) {
			return NewLinearWorkItemFamilyClickHouseEffects(conn, providerOwnershipMetricsLease(), nil)
		}},
	} {
		for _, table := range dailyJobTables {
			t.Run(testCase.provider+"/"+table, func(t *testing.T) {
				claim := nativeTestClaim(testCase.provider, "work-items")
				conn := &ownershipReasonWriteConn{}
				sink, err := testCase.build(conn)
				if err != nil {
					t.Fatal(err)
				}
				effect, err := BuildEffectBatch(table, EffectReadbackRequired, []json.RawMessage{})
				if err != nil {
					t.Fatal(err)
				}
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
			})
		}
	}
}

// The jira and linear sync sinks hold a second sink behind their destination
// list, with a dispatch of its own. That dispatch maps ai_attribution only:
// none of the nine tables of the daily job has an adapter there, so the list
// is not the one guard of the refusal.
func TestJiraAndLinearDerivedSinksDispatchOnlyAIAttribution(t *testing.T) {
	dailyJobTables := []string{
		"estimate_coverage_metrics_daily", "investment_classifications_daily", "investment_metrics_daily",
		"issue_type_metrics_daily", "work_item_cycle_times", "work_item_metrics_daily",
		"work_item_state_durations_daily", "work_item_team_attributions", "work_item_user_metrics_daily",
	}
	jira, err := NewJiraWorkItemDerivedClickHouseEffects(&ownershipReasonWriteConn{}, providerOwnershipMetricsLease(), nil)
	if err != nil {
		t.Fatal(err)
	}
	linear, err := NewLinearWorkItemDerivedClickHouseEffects(&ownershipReasonWriteConn{}, providerOwnershipMetricsLease(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if adapter, known := jira.adapterForDestination("ai_attribution"); !known || adapter == nil {
		t.Fatal("jira: ai_attribution is not dispatched")
	}
	if adapter, known := linear.adapterForDestination("ai_attribution"); !known || adapter == nil {
		t.Fatal("linear: ai_attribution is not dispatched")
	}
	for _, table := range dailyJobTables {
		if _, known := jira.adapterForDestination(table); known {
			t.Errorf("jira: the sync sink dispatches the daily-job table %q", table)
		}
		if _, known := linear.adapterForDestination(table); known {
			t.Errorf("linear: the sync sink dispatches the daily-job table %q", table)
		}
	}
}
