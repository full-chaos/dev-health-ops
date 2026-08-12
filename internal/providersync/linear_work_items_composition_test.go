package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

var linearFamilyDestinations = []string{
	"ai_attribution",
	"estimate_coverage_metrics_daily",
	"investment_classifications_daily",
	"investment_metrics_daily",
	"issue_type_metrics_daily",
	"sprints",
	"work_item_cycle_times",
	"work_item_dependencies",
	"work_item_interactions",
	"work_item_metrics_daily",
	"work_item_reopen_events",
	"work_item_state_durations_daily",
	"work_item_team_attributions",
	"work_item_transitions",
	"work_item_user_metrics_daily",
	"work_items",
}

type linearFamilyDerivationSource struct {
	err error
}

func (source linearFamilyDerivationSource) Load(
	context.Context,
	Claim,
	githubWorkItemDerivationLoadRequest,
) (githubWorkItemDerivationFacts, error) {
	return githubWorkItemDerivationFacts{}, source.err
}

type linearFamilyEngine struct{}

func (linearFamilyEngine) Derive(
	context.Context,
	Claim,
	githubWorkItemRows,
	time.Time,
	time.Time,
	githubWorkItemDerivationContext,
) (map[string][]json.RawMessage, error) {
	return map[string][]json.RawMessage{
		"investment_classifications_daily": {},
		"investment_metrics_daily":         {},
		"issue_type_metrics_daily":         {},
	}, nil
}

func linearFamilyClaim() Claim {
	claim := nativeTestClaim("linear", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	claim.SourceExternalID = "ENG"
	since := time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)
	before := since.AddDate(0, 0, 1)
	claim.SinceAt = &since
	claim.BeforeAt = &before
	claim.ProcessorFlags = map[string]bool{
		"family_dataset_work_items":         true,
		"family_dataset_work_item_labels":   true,
		"family_dataset_work_item_projects": true,
		"family_dataset_work_item_history":  true,
		"family_dataset_work_item_comments": true,
	}
	return claim
}

func linearFamilyDirectHandler() LinearWorkItemsRouteHandler {
	return LinearWorkItemsRouteHandler{
		ReferenceTeams: []LinearReferenceTeam{{
			Provider: "linear", ID: "team-eng", Name: "Engineering",
			ProjectKeys: []string{"ENG"},
		}},
	}
}

func linearFamilyEmptyCyclesResponse() string {
	return `{"data":{"cycles":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
}

func linearFamilyDeriver(source linearFamilyDerivationSource) *LinearWorkItemDeriver {
	return &LinearWorkItemDeriver{
		Source: source,
		engine: linearFamilyEngine{},
	}
}

func TestLinearWorkItemFamilyConstructionExposesOneCompleteBoundary(t *testing.T) {
	var _ CompleteRouteHandler = LinearWorkItemFamilyRouteHandler{}
	var _ EffectSink = LinearWorkItemFamilyClickHouseEffects{}
	var _ EffectReadback = LinearWorkItemFamilyClickHouseEffects{}

	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink, err := NewLinearWorkItemFamilyClickHouseEffects(inertGitHubDerivedConn{}, lease)
	if err != nil {
		t.Fatal(err)
	}
	if missing := sink.MissingDestinations(); len(missing) != 0 {
		t.Fatalf("missing destinations=%v", missing)
	}
	if !slices.Equal(workItemRouteDestinations(), linearFamilyDestinations) {
		t.Fatalf("canonical destinations=%v want=%v", workItemRouteDestinations(), linearFamilyDestinations)
	}
	raw, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	derived, err := BuildLinearWorkItemDerivedEffects(LinearWorkItemDerivedEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	effects := append(raw, derived...)
	sortEffectBatches(effects)
	claim := linearFamilyClaim()
	for index, effect := range effects {
		if effect.Destination != linearFamilyDestinations[index] {
			t.Fatalf("empty effect[%d]=%q", index, effect.Destination)
		}
		inspection, inspectErr := sink.InspectEffect(context.Background(), claim, effect)
		if inspectErr != nil || inspection != EffectAbsent {
			t.Fatalf("empty readback %s=%s error=%v", effect.Destination, inspection, inspectErr)
		}
		if writeErr := sink.WriteEffect(context.Background(), claim, effect); writeErr != nil {
			t.Fatalf("empty write %s: %v", effect.Destination, writeErr)
		}
	}
}

func TestLinearWorkItemFamilyCollectsAndCommitsAllSixteenDestinations(t *testing.T) {
	claim := linearFamilyClaim()
	doer := &linearWorkItemsDoer{responses: []string{
		linearFamilyEmptyCyclesResponse(),
		linearLifecycleIssueResponse("ENG-1", "ENG"),
	}}
	handler := LinearWorkItemFamilyRouteHandler{
		Direct:  linearFamilyDirectHandler(),
		Derived: linearFamilyDeriver(linearFamilyDerivationSource{}),
	}
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 987654321, time.UTC)
	batch, err := handler.Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 2 {
		t.Fatalf("reference prerequisites were not reused: requests=%d", len(doer.requests))
	}
	got := make([]string, 0, len(batch.Effects))
	for _, effect := range batch.Effects {
		got = append(got, effect.Destination)
	}
	if !slices.Equal(got, linearFamilyDestinations) {
		t.Fatalf("effect destinations=%v want=%v", got, linearFamilyDestinations)
	}
	if err := batch.validate(CompleteRouteDescriptor{Destinations: linearFamilyDestinations}); err != nil {
		t.Fatalf("complete route batch: %v", err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v want=%v", batch.Watermark, claim.BeforeAt)
	}
	byDestination := linearFamilyEffectsByDestination(batch.Effects)
	if len(byDestination["work_items"].Rows) != 1 ||
		len(byDestination["work_item_team_attributions"].Rows) == 0 {
		t.Fatalf(
			"raw/derived composition is incomplete: work_items=%d teams=%d",
			len(byDestination["work_items"].Rows),
			len(byDestination["work_item_team_attributions"].Rows),
		)
	}

	backend := newLinearSemanticEffectBackend()
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := linearFamilyEffectsFixture(backend, lease)
	commit, err := (EffectCommitter{
		Ledger: &memoryEffectLedger{}, Sink: sink, Readback: sink,
		Now: func() time.Time { return normalizedAt },
	}).Commit(context.Background(), claim, batch.Effects, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if commit.Written != len(linearFamilyDestinations) ||
		len(backend.writeCounts) != len(linearFamilyDestinations) {
		t.Fatalf("commit=%+v writes=%v", commit, backend.writeCounts)
	}
	for _, effect := range batch.Effects {
		inspection, inspectErr := sink.InspectEffect(context.Background(), claim, effect)
		if inspectErr != nil || inspection != EffectExact {
			t.Fatalf("readback %s=%s error=%v", effect.Destination, inspection, inspectErr)
		}
	}
}

func TestLinearWorkItemFamilyKeepsEveryEmptyDestinationExplicit(t *testing.T) {
	claim := linearFamilyClaim()
	doer := &linearWorkItemsDoer{responses: []string{
		linearFamilyEmptyCyclesResponse(),
		`{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}}
	batch, err := (LinearWorkItemFamilyRouteHandler{
		Direct:  linearFamilyDirectHandler(),
		Derived: linearFamilyDeriver(linearFamilyDerivationSource{}),
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != len(linearFamilyDestinations) {
		t.Fatalf("effects=%d want=%d", len(batch.Effects), len(linearFamilyDestinations))
	}
	for index, effect := range batch.Effects {
		if effect.Destination != linearFamilyDestinations[index] || len(effect.Rows) != 0 ||
			effect.Recovery != EffectReadbackRequired || !validDigest(effect.ContentDigest) {
			t.Fatalf("effect[%d]=%+v", index, effect)
		}
	}
}

func TestLinearWorkItemFamilyFailsBeforeIOAndWithholdsDerivationGap(t *testing.T) {
	claim := linearFamilyClaim()
	credential := providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID}
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)

	t.Run("missing deriver is a construction failure before provider IO", func(t *testing.T) {
		doer := &linearWorkItemsDoer{}
		batch, err := (LinearWorkItemFamilyRouteHandler{
			Direct: linearFamilyDirectHandler(),
		}).Collect(
			context.Background(), claim, credential,
			linearWorkItemsClient(t, doer), normalizedAt,
		)
		if !errors.Is(err, ErrInvalidConfiguration) || len(doer.requests) != 0 ||
			len(batch.Effects) != 0 || batch.Watermark != nil {
			t.Fatalf("batch=%+v requests=%d error=%v", batch, len(doer.requests), err)
		}
	})

	t.Run("derived failure returns no committable batch or watermark", func(t *testing.T) {
		gap := errors.New("derived context unavailable")
		doer := &linearWorkItemsDoer{responses: []string{
			linearFamilyEmptyCyclesResponse(),
			linearLifecycleIssueResponse("ENG-2", "ENG"),
		}}
		batch, err := (LinearWorkItemFamilyRouteHandler{
			Direct:  linearFamilyDirectHandler(),
			Derived: linearFamilyDeriver(linearFamilyDerivationSource{err: gap}),
		}).Collect(
			context.Background(), claim, credential,
			linearWorkItemsClient(t, doer), normalizedAt,
		)
		if !errors.Is(err, gap) || len(doer.requests) != 2 ||
			len(batch.Effects) != 0 || batch.Watermark != nil || batch.Result != nil ||
			batch.Evidence != (FetchEvidence{}) {
			t.Fatalf("batch=%+v requests=%d error=%v", batch, len(doer.requests), err)
		}
	})

	t.Run("disabled family fetches are rejected before provider IO", func(t *testing.T) {
		for _, disable := range []func(*LinearWorkItemsRouteHandler){
			func(direct *LinearWorkItemsRouteHandler) { direct.FetchComments = boolPointer(false) },
			func(direct *LinearWorkItemsRouteHandler) { direct.FetchHistory = boolPointer(false) },
			func(direct *LinearWorkItemsRouteHandler) { direct.FetchCycles = boolPointer(false) },
		} {
			direct := linearFamilyDirectHandler()
			disable(&direct)
			doer := &linearWorkItemsDoer{}
			batch, err := (LinearWorkItemFamilyRouteHandler{
				Direct:  direct,
				Derived: linearFamilyDeriver(linearFamilyDerivationSource{}),
			}).Collect(
				context.Background(), claim, credential,
				linearWorkItemsClient(t, doer), normalizedAt,
			)
			if !errors.Is(err, ErrInvalidConfiguration) || len(doer.requests) != 0 ||
				len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("batch=%+v requests=%d error=%v", batch, len(doer.requests), err)
			}
		}
	})

	t.Run("direct aliases are rejected before provider IO", func(t *testing.T) {
		for _, dataset := range []string{
			"work-item-labels", "work-item-projects",
			"work-item-history", "work-item-comments",
		} {
			alias := claim
			alias.Dataset = dataset
			doer := &linearWorkItemsDoer{}
			batch, err := (LinearWorkItemFamilyRouteHandler{
				Direct:  linearFamilyDirectHandler(),
				Derived: linearFamilyDeriver(linearFamilyDerivationSource{}),
			}).Collect(
				context.Background(), alias, credential,
				linearWorkItemsClient(t, doer), normalizedAt,
			)
			if !errors.Is(err, ErrInvalidConfiguration) || len(doer.requests) != 0 ||
				len(batch.Effects) != 0 || batch.Watermark != nil {
				t.Fatalf("dataset=%s batch=%+v requests=%d error=%v", dataset, batch, len(doer.requests), err)
			}
		}
	})
}

func TestLinearWorkItemFamilyEffectsFailClosedWhenEitherHalfIsIncomplete(t *testing.T) {
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	complete, err := NewLinearWorkItemFamilyClickHouseEffects(inertGitHubDerivedConn{}, lease)
	if err != nil {
		t.Fatal(err)
	}
	derived, err := BuildLinearWorkItemDerivedEffects(LinearWorkItemDerivedEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	claim := linearFamilyClaim()
	for _, testCase := range []struct {
		name    string
		mutate  func(*LinearWorkItemFamilyClickHouseEffects)
		missing []string
		effect  EffectBatch
	}{
		{
			name: "raw adapter", missing: []string{"work_item_transitions"}, effect: derived[0],
			mutate: func(sink *LinearWorkItemFamilyClickHouseEffects) {
				sink.Raw.StatusTransitions = nil
			},
		},
		{
			name: "derived adapter", missing: []string{"investment_metrics_daily"}, effect: raw[0],
			mutate: func(sink *LinearWorkItemFamilyClickHouseEffects) {
				sink.Derived.InvestmentMetricsDaily = nil
			},
		},
		{
			name: "raw lease",
			missing: []string{
				"sprints", "work_item_dependencies", "work_item_interactions",
				"work_item_reopen_events", "work_item_transitions", "work_items",
			},
			effect: derived[0],
			mutate: func(sink *LinearWorkItemFamilyClickHouseEffects) {
				sink.Raw.Lease = nil
			},
		},
		{
			name: "derived lease",
			missing: []string{
				"ai_attribution", "estimate_coverage_metrics_daily",
				"investment_classifications_daily", "investment_metrics_daily",
				"issue_type_metrics_daily", "work_item_cycle_times",
				"work_item_metrics_daily", "work_item_state_durations_daily",
				"work_item_team_attributions", "work_item_user_metrics_daily",
			},
			effect: raw[0],
			mutate: func(sink *LinearWorkItemFamilyClickHouseEffects) {
				sink.Derived.Lease = nil
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			sink := complete
			testCase.mutate(&sink)
			if missing := sink.MissingDestinations(); !slices.Equal(missing, testCase.missing) {
				t.Fatalf("missing=%v", missing)
			}
			if err := sink.WriteEffect(context.Background(), claim, testCase.effect); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("partial sink write error=%v", err)
			}
			if inspection, err := sink.InspectEffect(context.Background(), claim, testCase.effect); !errors.Is(err, ErrInvalidConfiguration) || inspection != EffectConflict {
				t.Fatalf("partial sink readback=%s error=%v", inspection, err)
			}
		})
	}
}

func linearFamilyEffectsFixture(
	backend LinearWorkItemEffectAdapter,
	lease providerfoundation.LeaseGuard,
) LinearWorkItemFamilyClickHouseEffects {
	derived := LinearWorkItemDerivedClickHouseEffects{Lease: lease}
	for _, destination := range linearWorkItemDerivedEffectDestinations {
		adapter := linearDestinationCheckingAdapter{
			destination: destination,
			delegate:    backend,
		}
		switch destination {
		case "ai_attribution":
			derived.AIAttribution = adapter
		case "estimate_coverage_metrics_daily":
			derived.EstimateCoverageMetricsDaily = adapter
		case "investment_classifications_daily":
			derived.InvestmentClassificationsDaily = adapter
		case "investment_metrics_daily":
			derived.InvestmentMetricsDaily = adapter
		case "issue_type_metrics_daily":
			derived.IssueTypeMetricsDaily = adapter
		case "work_item_cycle_times":
			derived.WorkItemCycleTimes = adapter
		case "work_item_metrics_daily":
			derived.WorkItemMetricsDaily = adapter
		case "work_item_state_durations_daily":
			derived.WorkItemStateDurationsDaily = adapter
		case "work_item_team_attributions":
			derived.WorkItemTeamAttributions = adapter
		case "work_item_user_metrics_daily":
			derived.WorkItemUserMetricsDaily = adapter
		}
	}
	return LinearWorkItemFamilyClickHouseEffects{
		Raw:     linearWorkItemEffectsFixture(backend, lease),
		Derived: derived,
	}
}

func linearFamilyEffectsByDestination(effects []EffectBatch) map[string]EffectBatch {
	result := make(map[string]EffectBatch, len(effects))
	for _, effect := range effects {
		result[effect.Destination] = effect
	}
	return result
}
