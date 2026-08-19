package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

type linearDerivedEngineStub struct{}

func (linearDerivedEngineStub) Derive(
	context.Context,
	Claim,
	githubWorkItemRows,
	time.Time,
	time.Time,
	githubWorkItemDerivationContext,
) (map[string][]json.RawMessage, error) {
	return map[string][]json.RawMessage{
		"issue_type_metrics_daily":         {},
		"investment_classifications_daily": {},
		"investment_metrics_daily":         {},
	}, nil
}

func linearDerivedTestClaim() Claim {
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	day := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	before := day.AddDate(0, 0, 1)
	claim.SinceAt = &day
	claim.BeforeAt = &before
	return claim
}

func TestBuildLinearWorkItemDerivedEffectsKeepsAllTenDestinations(t *testing.T) {
	effects, err := BuildLinearWorkItemDerivedEffects(LinearWorkItemDerivedEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	if len(effects) != 10 {
		t.Fatalf("effects=%d want 10", len(effects))
	}
	for index, effect := range effects {
		if effect.Destination != linearWorkItemDerivedEffectDestinations[index] {
			t.Fatalf("effect[%d]=%q want %q", index, effect.Destination, linearWorkItemDerivedEffectDestinations[index])
		}
		if effect.Recovery != EffectReadbackRequired {
			t.Fatalf("%s recovery=%v want readback", effect.Destination, effect.Recovery)
		}
	}
}

func TestLinearWorkItemDeriverUsesLinearRowsAndEvaluatesAllTenDestinations(t *testing.T) {
	claim := linearDerivedTestClaim()
	day := *claim.SinceAt
	row := linearWorkItemRow{
		WorkItemID: "LIN-1", Provider: "linear", Title: "Repair delivery path",
		Type: "issue", Status: "todo", CreatedAt: day.Add(time.Hour),
		UpdatedAt: day.Add(2 * time.Hour), Labels: []string{"codex"}, OrgID: claim.OrgID,
	}
	source := &fakeGitHubWorkItemDerivationContextSource{}
	deriver := LinearWorkItemDeriver{
		Source: source,
		engine: linearDerivedEngineStub{},
	}
	derived, err := deriver.Derive(
		context.Background(), claim, linearWorkItemRows{
			WorkItems: []linearWorkItemRow{row},
			StatusTransitions: []linearWorkItemTransitionRow{{
				WorkItemID: "LIN-1", Provider: "linear", OccurredAt: day.Add(2 * time.Hour),
				FromStatus: "todo", ToStatus: "in_progress", OrgID: claim.OrgID,
			}},
		},
		day.Add(3*time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(derived) != len(linearWorkItemDerivedEffectDestinations) {
		t.Fatalf("derived destinations=%d want %d: %v", len(derived), len(linearWorkItemDerivedEffectDestinations), derived)
	}
	for _, destination := range linearWorkItemDerivedEffectDestinations {
		if _, ok := derived[destination]; !ok {
			t.Fatalf("destination %q was not evaluated", destination)
		}
	}
	if len(derived["ai_attribution"]) != 1 {
		t.Fatalf("Linear explicit issue label attribution rows=%d want 1", len(derived["ai_attribution"]))
	}
	var attribution LinearAIAttributionRow
	if err := json.Unmarshal(derived["ai_attribution"][0], &attribution); err != nil {
		t.Fatal(err)
	}
	if attribution.Provider != "linear" || attribution.SubjectType != "issue" ||
		attribution.SubjectID != "LIN-1" || attribution.RepoID != nil ||
		attribution.Source != "issue_label" || attribution.OrgID.String() != claim.OrgID {
		t.Fatalf("Linear attribution lost semantic fence: %+v", attribution)
	}
	if len(derived["work_item_metrics_daily"]) == 0 ||
		len(derived["estimate_coverage_metrics_daily"]) == 0 ||
		len(derived["work_item_state_durations_daily"]) == 0 {
		t.Fatalf("real derived builders produced no rows: metrics=%d coverage=%d states=%d",
			len(derived["work_item_metrics_daily"]), len(derived["estimate_coverage_metrics_daily"]),
			len(derived["work_item_state_durations_daily"]))
	}
	var metricRow LinearWorkItemMetricsDailyRow
	if err := json.Unmarshal(derived["work_item_metrics_daily"][0], &metricRow); err != nil {
		t.Fatal(err)
	}
	if metricRow.Provider != "linear" || metricRow.OrgID != claim.OrgID {
		t.Fatalf("derived row lost Linear tenant/provider fence: %+v", metricRow)
	}
}

func TestLinearWorkItemDeriverRejectsWrongProviderClaim(t *testing.T) {
	claim := linearDerivedTestClaim()
	claim.Provider = "github"
	day := *claim.SinceAt
	deriver := LinearWorkItemDeriver{
		Source: &fakeGitHubWorkItemDerivationContextSource{},
		engine: linearDerivedEngineStub{},
	}
	_, err := deriver.Derive(
		context.Background(), claim, linearWorkItemRows{}, day.Add(3*time.Hour),
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("error=%v want ErrInvalidConfiguration", err)
	}
}

func TestLinearWorkItemDeriverRejectsMalformedRepositoryID(t *testing.T) {
	claim := linearDerivedTestClaim()
	day := *claim.SinceAt
	badRepo := "not-a-uuid"
	deriver := LinearWorkItemDeriver{
		Source: &fakeGitHubWorkItemDerivationContextSource{},
		engine: linearDerivedEngineStub{},
	}
	_, err := deriver.Derive(context.Background(), claim, linearWorkItemRows{
		WorkItems: []linearWorkItemRow{{
			WorkItemID: "LIN-1", Provider: "linear", CreatedAt: day.Add(time.Hour),
			UpdatedAt: day.Add(2 * time.Hour), OrgID: claim.OrgID, RepoID: &badRepo,
		}},
	}, day.Add(3*time.Hour))
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("error=%v want ErrInvalidConfiguration", err)
	}
}

type linearDerivedStubConn struct{ driver.Conn }

func TestNewLinearWorkItemDerivedClickHouseEffectsWiresAllTen(t *testing.T) {
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink, err := NewLinearWorkItemDerivedClickHouseEffects(linearDerivedStubConn{}, lease)
	if err != nil {
		t.Fatal(err)
	}
	if got := sink.MissingDestinations(); len(got) != 0 {
		t.Fatalf("missing=%v", got)
	}
	for _, destination := range linearWorkItemDerivedEffectDestinations {
		adapter, known := sink.adapterForDestination(destination)
		if !known || adapter == nil {
			t.Fatalf("destination %q not wired", destination)
		}
	}
	effects, err := BuildLinearWorkItemDerivedEffects(LinearWorkItemDerivedEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	claim := linearDerivedTestClaim()
	for _, effect := range effects {
		if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
			t.Fatalf("write %s: %v", effect.Destination, err)
		}
		if inspection, err := sink.InspectEffect(context.Background(), claim, effect); err != nil || inspection != EffectAbsent {
			t.Fatalf("inspect %s: verdict=%v err=%v want absent", effect.Destination, inspection, err)
		}
	}
}

func TestLinearDerivedAdapterRejectsProviderAndTenantCrossingRows(t *testing.T) {
	claim := linearDerivedTestClaim()
	row := githubWorkItemMetricTestGroupRow()
	row.Provider, row.OrgID = "github", claim.OrgID
	raw, err := effectRowsFromValues([]githubWorkItemMetricsDailyRow{row})
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch(
		"work_item_metrics_daily", EffectReadbackRequired, raw,
	)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := newLinearWorkItemDerivedEffectIdentity(claim, effect)
	if err != nil {
		t.Fatal(err)
	}
	adapter := linearDerivedGitHubAdapter{
		destination: "work_item_metrics_daily", delegate: &linearDerivedRecordingAdapter{},
	}
	if err := adapter.WriteLinearWorkItemEffect(context.Background(), identity, effect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("provider crossing error=%v want ErrInvalidConfiguration", err)
	}

	row.Provider, row.OrgID = "linear", "org-other"
	raw, err = effectRowsFromValues([]githubWorkItemMetricsDailyRow{row})
	if err != nil {
		t.Fatal(err)
	}
	effect, err = BuildEffectBatch("work_item_metrics_daily", EffectReadbackRequired, raw)
	if err != nil {
		t.Fatal(err)
	}
	identity, err = newLinearWorkItemDerivedEffectIdentity(claim, effect)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.WriteLinearWorkItemEffect(context.Background(), identity, effect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("tenant crossing error=%v want ErrInvalidConfiguration", err)
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
		identity, err := newLinearWorkItemDerivedEffectIdentity(claim, effect)
		if err != nil {
			t.Fatal(err)
		}
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
	sink, err := NewLinearWorkItemDerivedClickHouseEffects(linearDerivedStubConn{}, lease)
	if err != nil {
		t.Fatal(err)
	}
	sink.WorkItemMetricsDaily = nil
	effect, err := BuildEffectBatch("ai_attribution", EffectReadbackRequired, nil)
	if err != nil {
		t.Fatal(err)
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
