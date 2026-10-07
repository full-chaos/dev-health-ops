package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// linearFamilyDestinations is the effect set of one Linear work-items unit:
// the eight raw tables and ai_attribution, which is read from the item labels.
// No table the daily job computes from stored rows is in it.
var linearFamilyDestinations = []string{
	"ai_attribution",
	"project_membership_transitions",
	"projects",
	"sprints",
	"work_item_dependencies",
	"work_item_interactions",
	"work_item_reopen_events",
	"work_item_transitions",
	"work_items",
}

type linearFamilyDerivationSource struct {
	err error
}

func (source linearFamilyDerivationSource) Load(
	context.Context,
	Claim,
	teamattribution.GithubWorkItemDerivationLoadRequest,
) (teamattribution.GithubWorkItemDerivationFacts, error) {
	return teamattribution.GithubWorkItemDerivationFacts{}, source.err
}

// Succeeds on purpose: the fail-closed case this double serves is about Load,
// and a stored-edge read that failed too would let the test pass for the wrong
// reason (CHAOS-3978 has its own fail-closed case).
func (source linearFamilyDerivationSource) LoadStoredInheritableEdges(
	context.Context, Claim, []string,
) ([]githubWorkItemDependencyRow, error) {
	return nil, nil
}

type linearFamilyEngine struct{}

func (linearFamilyEngine) Derive(
	context.Context,
	Claim,
	githubWorkItemRows,
	time.Time,
	time.Time,
	teamattribution.GithubWorkItemDerivationContext,
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
	sink, err := NewLinearWorkItemFamilyClickHouseEffects(inertGitHubDerivedConn{}, lease, nil)
	if err != nil {
		t.Fatal(err)
	}
	if missing := sink.MissingDestinations(); len(missing) != 0 {
		t.Fatalf("missing destinations=%v", missing)
	}
	// linearFamilyRawDestinations, not the bare shared workItemRouteDestinations:
	// linear's own raw route writes two more than the shared family manifest
	// as of CHAOS-4193 (project_membership_transitions, projects), the same
	// as github's CHAOS-4194 addition. Sorted for comparison because
	// linearFamilyRawDestinations appends them at the end while
	// linearFamilyDestinations (alphabetized) does not.
	gotDestinations := append([]string(nil), linearFamilyRawDestinations()...)
	slices.Sort(gotDestinations)
	if !slices.Equal(gotDestinations, linearFamilyDestinations) {
		t.Fatalf("canonical destinations=%v want=%v", gotDestinations, linearFamilyDestinations)
	}
	raw, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	derived, err := BuildLinearWorkItemDerivedEffects(LinearWorkItemDerivedEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	claim := linearFamilyClaim()
	// The sink accepts the unit's own effect set and refuses each table of
	// the daily job: a refused effect never reaches a store.
	accepted := make([]EffectBatch, 0, len(linearFamilyDestinations))
	refused := make([]string, 0, len(githubWorkItemDerivedDestinations))
	for _, effect := range append(raw, derived...) {
		if !slices.Contains(githubWorkItemDerivedDestinations, effect.Destination) {
			accepted = append(accepted, effect)
			continue
		}
		if writeErr := sink.WriteEffect(context.Background(), claim, effect); !errors.Is(writeErr, ErrInvalidConfiguration) {
			t.Fatalf("%s: the sync sink wrote a table of the daily job: %v", effect.Destination, writeErr)
		}
		inspection, inspectErr := sink.InspectEffect(context.Background(), claim, effect)
		if !errors.Is(inspectErr, ErrInvalidConfiguration) || inspection != EffectConflict {
			t.Fatalf("%s: readback=%s error=%v want a refused conflict", effect.Destination, inspection, inspectErr)
		}
		refused = append(refused, effect.Destination)
	}
	slices.Sort(refused)
	if !slices.Equal(refused, githubWorkItemDerivedDestinations) {
		t.Fatalf("refused=%v want=%v", refused, githubWorkItemDerivedDestinations)
	}
	sortEffectBatches(accepted)
	if len(accepted) != len(linearFamilyDestinations) {
		t.Fatalf("accepted effects=%d want=%d", len(accepted), len(linearFamilyDestinations))
	}
	for index, effect := range accepted {
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

// One Linear work-items unit collects and commits its raw effect set, counts
// itself once as a unit that left the derived tables to the daily job, and
// builds no effect for one of them.
func TestLinearWorkItemFamilyCollectsAndCommitsRawRowsOnly(t *testing.T) {
	claim := linearFamilyClaim()
	doer := &linearWorkItemsDoer{responses: []string{
		linearFamilyEmptyCyclesResponse(),
		linearLifecycleIssueResponse("ENG-1", "ENG"),
	}}
	handler := LinearWorkItemFamilyRouteHandler{
		Direct: linearFamilyDirectHandler(),
	}
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 987654321, time.UTC)
	client := linearWorkItemsClient(t, fakehttp.Client(doer))
	leftToDaily := providerfoundation.NewMetrics()
	client.Metrics = leftToDaily
	batch, err := handler.Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		client, normalizedAt,
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
	if len(byDestination["work_items"].Rows) != 1 {
		t.Fatalf("work_items rows=%d want=1", len(byDestination["work_items"].Rows))
	}
	for _, destination := range githubWorkItemDerivedDestinations {
		if _, present := byDestination[destination]; present {
			t.Fatalf("the unit built an effect for the derived table %q", destination)
		}
	}
	for _, key := range []string{
		"team_inheritance", "team_attribution_written", "derived_destinations_implemented",
		"derived_destinations_unimplemented", "watermark_held_for_derived_gap",
	} {
		if _, present := batch.Result[key]; present {
			t.Fatalf("result still carries %q: %+v", key, batch.Result)
		}
	}
	assertWorkItemDerivedTablesLeftToDailyJob(t, leftToDaily, "linear", 1)

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
		Direct: linearFamilyDirectHandler(),
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
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

func TestLinearWorkItemFamilyRejectsConstructionDefectsBeforeIO(t *testing.T) {
	claim := linearFamilyClaim()
	credential := providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID}
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)

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
				Direct: direct,
			}).Collect(
				context.Background(), claim, credential,
				linearWorkItemsClient(t, fakehttp.Client(doer)), normalizedAt,
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
				Direct: linearFamilyDirectHandler(),
			}).Collect(
				context.Background(), alias, credential,
				linearWorkItemsClient(t, fakehttp.Client(doer)), normalizedAt,
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
	complete, err := NewLinearWorkItemFamilyClickHouseEffects(inertGitHubDerivedConn{}, lease, nil)
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
			name: "ai attribution adapter", missing: []string{"ai_attribution"}, effect: raw[0],
			mutate: func(sink *LinearWorkItemFamilyClickHouseEffects) {
				sink.Derived.AIAttribution = nil
			},
		},
		{
			// An adapter of a table the daily job owns is no part of the
			// sync sink: a sink without it is complete.
			name: "daily job adapter is not owed", missing: []string{},
			mutate: func(sink *LinearWorkItemFamilyClickHouseEffects) {
				sink.Derived.InvestmentMetricsDaily = nil
			},
		},
		{
			name: "raw lease",
			missing: []string{
				"sprints", "work_item_dependencies", "work_item_interactions",
				"work_item_reopen_events", "work_item_transitions", "work_items",
				"project_membership_transitions", "projects",
			},
			effect: derived[0],
			mutate: func(sink *LinearWorkItemFamilyClickHouseEffects) {
				sink.Raw.Lease = nil
			},
		},
		{
			name:    "derived lease",
			missing: []string{"ai_attribution"},
			effect:  raw[0],
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
			if len(testCase.missing) == 0 {
				for _, effect := range raw {
					if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
						t.Fatalf("complete sink write %s: %v", effect.Destination, err)
					}
				}
				return
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

// LoadStoredBlockingFacts: this double holds no stored blocking relation
// (CHAOS-8493), so no item of its units has an open blocker.
func (source linearFamilyDerivationSource) LoadStoredBlockingFacts(
	context.Context, Claim, []string, []workitemmetrics.BlockingRelation,
) ([]workitemmetrics.BlockingRelation, []workitemmetrics.RelationEnd, error) {
	return nil, nil, nil
}

// ai_attribution is a raw fact of the fetched items: the Linear work-items
// unit still reads it from the item labels and writes it, on the real route
// and through the real sink boundary. A label that is no AI label gives no
// row, and the rows are the unit's only effect beside the raw tables.
func TestLinearWorkItemFamilyStillWritesAIAttributionRowsFromLabels(t *testing.T) {
	claim := linearFamilyClaim()
	issue := strings.Replace(
		linearLifecycleIssueResponse("ENG-1", "ENG"),
		`"labels":{"nodes":[]}`,
		`"labels":{"nodes":[{"name":"bug"},{"name":"Codex"},{"name":"agent-created"}]}`, 1,
	)
	if !strings.Contains(issue, `"name":"Codex"`) {
		t.Fatal("the labels were not planted in the issue payload")
	}
	doer := &linearWorkItemsDoer{responses: []string{linearFamilyEmptyCyclesResponse(), issue}}
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 987654321, time.UTC)
	batch, err := (LinearWorkItemFamilyRouteHandler{Direct: linearFamilyDirectHandler()}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, fakehttp.Client(doer)), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	byDestination := linearFamilyEffectsByDestination(batch.Effects)
	aiEffect, present := byDestination["ai_attribution"]
	if !present || len(aiEffect.Rows) != 2 || aiEffect.Recovery != EffectReadbackRequired {
		t.Fatalf("ai_attribution effect present=%t rows=%d recovery=%s want 2 rows from the two AI labels",
			present, len(aiEffect.Rows), aiEffect.Recovery)
	}
	kinds := map[string]string{}
	for _, raw := range aiEffect.Rows {
		var row githubAIAttributionRow
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		if row.Provider != "linear" || row.SubjectType != "issue" || row.Source != "issue_label" ||
			row.SubjectID == "" || row.OrgID.String() != claim.OrgID || row.RepoID != nil {
			t.Fatalf("ai_attribution row=%+v", row)
		}
		label, _ := row.Evidence["label"].(string)
		kinds[label] = row.Kind
	}
	if len(kinds) != 2 || kinds["Codex"] != "ai_assisted" || kinds["agent-created"] != "agent_created" {
		t.Fatalf("kinds by label=%v", kinds)
	}
	for _, destination := range githubWorkItemDerivedDestinations {
		if _, built := byDestination[destination]; built {
			t.Fatalf("the unit built an effect for the derived table %q", destination)
		}
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
	if commit.Written != len(linearFamilyDestinations) || backend.writeCounts["ai_attribution"] != 1 {
		t.Fatalf("commit=%+v writes=%v", commit, backend.writeCounts)
	}
	if inspection, err := sink.InspectEffect(context.Background(), claim, aiEffect); err != nil || inspection != EffectExact {
		t.Fatalf("ai_attribution readback=%s error=%v want the written rows", inspection, err)
	}
}
