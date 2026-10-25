package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

func linearDerivedTestClaim() Claim {
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	day := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	before := day.AddDate(0, 0, 1)
	claim.SinceAt = &day
	claim.BeforeAt = &before
	return claim
}

type linearDerivedStubConn struct{ driver.Conn }

// linearDailyJobAdapterIdentity is the identity a unit-held adapter of a
// daily-job table would receive. The sync sink identity refuses such a
// destination, so the provider and tenant fence of that adapter is reached
// only by a direct call.
func linearDailyJobAdapterIdentity(t *testing.T, claim Claim, effect EffectBatch) LinearWorkItemEffectIdentity {
	t.Helper()
	if _, err := newLinearWorkItemDerivedEffectIdentity(claim, effect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("the sync sink identity accepted the daily-job table %q: %v", effect.Destination, err)
	}
	return LinearWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset,
		Generation: claim.GenerationKey(), Destination: effect.Destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
}

func TestLinearDerivedAdapterRequiresProviderAndTenantColumns(t *testing.T) {
	claim := linearDerivedTestClaim()
	adapter := linearDerivedGitHubAdapter{
		destination: "estimate_coverage_metrics_daily", delegate: &linearDerivedRecordingAdapter{},
	}
	for _, raw := range []json.RawMessage{
		json.RawMessage(`{"day":"2026-07-10","org_id":"org-acme"}`),
		json.RawMessage(`{"day":"2026-07-10","provider":"linear"}`),
	} {
		effect, err := BuildEffectBatch(
			"estimate_coverage_metrics_daily", EffectReadbackRequired, []json.RawMessage{raw},
		)
		if err != nil {
			t.Fatal(err)
		}
		identity := linearDailyJobAdapterIdentity(t, claim, effect)
		if err := adapter.WriteLinearWorkItemEffect(context.Background(), identity, effect); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("raw=%s error=%v want ErrInvalidConfiguration", raw, err)
		}
	}
}

func TestLinearDerivedAdapterRejectsCrossProviderAIEffect(t *testing.T) {
	claim := linearDerivedTestClaim()
	base := githubAIAttributionRow{
		RecordID: uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		OrgID:    uuid.MustParse(claim.OrgID), Provider: "linear", SubjectType: "issue",
		SubjectID: "LIN-1", Kind: "ai_assisted", Source: "issue_label", Confidence: 0.95,
		Evidence: map[string]any{"label": "codex"}, ObservedAt: *claim.SinceAt,
		IngestedAt: claim.SinceAt.Add(time.Hour),
	}
	for _, testCase := range []struct {
		name   string
		mutate func(*githubAIAttributionRow)
	}{
		{"provider", func(row *githubAIAttributionRow) { row.Provider = "github" }},
		{"source", func(row *githubAIAttributionRow) { row.Source = "pr_label" }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			row := base
			testCase.mutate(&row)
			raw, err := effectRowsFromValues([]githubAIAttributionRow{row})
			if err != nil {
				t.Fatal(err)
			}
			effect, err := BuildEffectBatch("ai_attribution", EffectReadbackRequired, raw)
			if err != nil {
				t.Fatal(err)
			}
			identity, err := newLinearWorkItemDerivedEffectIdentity(claim, effect)
			if err != nil {
				t.Fatal(err)
			}
			adapter := linearDerivedGitHubAdapter{
				destination: "ai_attribution", delegate: &linearDerivedRecordingAdapter{},
			}
			if err := adapter.WriteLinearWorkItemEffect(context.Background(), identity, effect); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("false-provenance AI error=%v want ErrInvalidConfiguration", err)
			}
		})
	}
}

func TestLinearAIAttributionDoesNotInferFromIssueText(t *testing.T) {
	claim := linearDerivedTestClaim()
	rows, err := normalizeLinearWorkItemAIAttributions(claim, linearWorkItemRows{
		WorkItems: []linearWorkItemRow{{
			WorkItemID: "linear:ENG-2", Provider: "linear", Title: "Generated with Codex",
			Description: stringPointer("AI-assisted issue text"), Labels: []string{"bug"},
			CreatedAt: *claim.SinceAt, UpdatedAt: *claim.SinceAt, OrgID: claim.OrgID,
		}},
	}, *claim.BeforeAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("text-only attribution rows=%+v", rows)
	}
}

func TestLinearDerivedSinkRejectsIncompleteBeforeDispatch(t *testing.T) {
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink, err := NewLinearWorkItemDerivedClickHouseEffects(linearDerivedStubConn{}, lease, nil)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch("ai_attribution", EffectReadbackRequired, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(context.Background(), linearDerivedTestClaim(), effect); err != nil {
		t.Fatalf("the sink refused ai_attribution: %v", err)
	}
	sink.AIAttribution = nil
	if missing := sink.MissingDestinations(); !slices.Equal(missing, []string{"ai_attribution"}) {
		t.Fatalf("missing=%v", missing)
	}
	if err := sink.WriteEffect(context.Background(), linearDerivedTestClaim(), effect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("incomplete sink error=%v want ErrInvalidConfiguration", err)
	}
}

func TestLinearDerivedSinkRejectsReplaySafeRecoveryPolicy(t *testing.T) {
	effect, err := BuildEffectBatch("ai_attribution", EffectReplaySafe, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newLinearWorkItemDerivedEffectIdentity(linearDerivedTestClaim(), effect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("replay-safe effect error=%v want ErrInvalidConfiguration", err)
	}
}

type linearDerivedRecordingAdapter struct {
	writes    int
	readbacks int
}

func (adapter *linearDerivedRecordingAdapter) WriteGitHubWorkItemEffect(
	context.Context, GitHubWorkItemEffectIdentity, EffectBatch,
) error {
	adapter.writes++
	return nil
}

func (adapter *linearDerivedRecordingAdapter) InspectGitHubWorkItemEffect(
	context.Context, GitHubWorkItemEffectIdentity, EffectBatch,
) (EffectInspection, error) {
	adapter.readbacks++
	return EffectExact, nil
}
