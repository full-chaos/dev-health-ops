package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func providerOwnershipMetricsLease() providerfoundation.LeaseGuard {
	return providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
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
