package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

type jiraDerivedStubConn struct{ driver.Conn }

var errJiraDerivedPrepare = errors.New("jira derived prepare reached")

type jiraDerivedProbeConn struct{ driver.Conn }

func (jiraDerivedProbeConn) PrepareBatch(
	context.Context,
	string,
	...driver.PrepareBatchOption,
) (driver.Batch, error) {
	return nil, errJiraDerivedPrepare
}

func (jiraDerivedProbeConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errJiraDerivedPrepare
}

func TestBuildJiraWorkItemDerivedEffectsKeepsTenTypedDestinations(t *testing.T) {
	metric := githubWorkItemMetricTestGroupRow()
	metric.Provider = "jira"
	metric.ItemsCompletedUnassigned = 7
	effects, err := BuildJiraWorkItemDerivedEffects(JiraWorkItemDerivedEffectRows{
		WorkItemMetricsDaily: []JiraWorkItemMetricsDailyRow{metric},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(effects) != len(jiraWorkItemDerivedDestinations) {
		t.Fatalf("effects=%d want=%d", len(effects), len(jiraWorkItemDerivedDestinations))
	}
	for index, effect := range effects {
		if effect.Destination != jiraWorkItemDerivedDestinations[index] ||
			effect.Recovery != EffectReadbackRequired {
			t.Fatalf("effect[%d]=%+v", index, effect)
		}
		wantRows := 0
		if effect.Destination == "work_item_metrics_daily" {
			wantRows = 1
		}
		if len(effect.Rows) != wantRows {
			t.Fatalf("effect[%d] rows=%d want=%d", index, len(effect.Rows), wantRows)
		}
	}
	if effects[0].Destination != "ai_attribution" || len(effects[0].Rows) != 0 {
		t.Fatalf("AI no-producer disposition=%+v", effects[0])
	}
	var persisted JiraWorkItemMetricsDailyRow
	for _, effect := range effects {
		if effect.Destination == "work_item_metrics_daily" {
			if err := json.Unmarshal(effect.Rows[0], &persisted); err != nil {
				t.Fatal(err)
			}
		}
	}
	if persisted.Provider != "jira" || persisted.ItemsCompletedUnassigned != 7 {
		t.Fatalf("typed metric projection=%+v", persisted)
	}
}

type jiraDerivedRecordingAdapter struct {
	writes, inspections int
}

func (adapter *jiraDerivedRecordingAdapter) WriteGitHubWorkItemEffect(
	context.Context,
	GitHubWorkItemEffectIdentity,
	EffectBatch,
) error {
	adapter.writes++
	return nil
}

func (adapter *jiraDerivedRecordingAdapter) InspectGitHubWorkItemEffect(
	context.Context,
	GitHubWorkItemEffectIdentity,
	EffectBatch,
) (EffectInspection, error) {
	adapter.inspections++
	return EffectExact, nil
}

func TestJiraEvaluatedEmptyAIIsNoIOAndReadsAbsent(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	effect, err := BuildEffectBatch(
		"ai_attribution", EffectReadbackRequired, []json.RawMessage{},
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := JiraWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset,
		Generation: claim.GenerationKey(), Destination: effect.Destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	recording := &jiraDerivedRecordingAdapter{}
	adapter := jiraDerivedGitHubAdapter{
		destination: "ai_attribution", delegate: recording,
	}
	if err := adapter.WriteGitHubWorkItemEffect(context.Background(), identity, effect); err != nil {
		t.Fatal(err)
	}
	inspection, err := adapter.InspectGitHubWorkItemEffect(
		context.Background(), identity, effect,
	)
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("empty AI inspection=%s err=%v", inspection, err)
	}
	if recording.writes != 0 || recording.inspections != 0 {
		t.Fatalf("evaluated-empty AI reached ClickHouse delegate: %+v", recording)
	}

	nonEmpty, err := BuildEffectBatch(
		"ai_attribution", EffectReadbackRequired,
		[]json.RawMessage{json.RawMessage(`{"unexpected":true}`)},
	)
	if err != nil {
		t.Fatal(err)
	}
	identity.ContentDigest, identity.RowCount = nonEmpty.ContentDigest, 1
	if err := adapter.WriteGitHubWorkItemEffect(
		context.Background(), identity, nonEmpty,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("non-empty Jira AI error=%v want ErrInvalidConfiguration", err)
	}
	identity.Provider = "github"
	identity.ContentDigest, identity.RowCount = effect.ContentDigest, 0
	if err := adapter.WriteGitHubWorkItemEffect(
		context.Background(), identity, effect,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign-provider AI identity error=%v want ErrInvalidConfiguration", err)
	}
}

func TestJiraEvaluatedEmptyAIRecoversAnInterruptedEffect(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	effect, err := BuildEffectBatch("ai_attribution", EffectReadbackRequired, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 11, 16, 0, 0, 0, time.UTC)
	state, err := NewEffectLedgerState(claim, []EffectBatch{effect}, now)
	if err != nil {
		t.Fatal(err)
	}
	state.Effects[0].Status = GenerationBlockWriting
	state.Effects[0].StartedAt = &now
	ledger := &memoryEffectLedger{state: state}
	sink, err := NewJiraWorkItemDerivedClickHouseEffects(
		jiraDerivedStubConn{},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := (EffectCommitter{
		Ledger: ledger, Sink: sink, Readback: sink,
		Now: func() time.Time { return now.Add(time.Minute) },
	}).Commit(context.Background(), claim, []EffectBatch{effect}, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.ResetForReplay != 1 || result.Written != 1 ||
		ledger.state.Effects[0].Status != GenerationBlockCommitted {
		t.Fatalf("recovery result=%+v state=%+v", result, ledger.state.Effects[0])
	}
}

func TestJiraCompositeSinkOwnsDirectDerivedAndEvaluatedEmptyAI(t *testing.T) {
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink, err := NewJiraWorkItemCompositeClickHouseEffects(jiraDerivedStubConn{}, lease)
	if err != nil {
		t.Fatal(err)
	}
	if missing := sink.MissingDestinations(); len(missing) != 0 {
		t.Fatalf("missing destinations=%v", missing)
	}
	effect, err := BuildEffectBatch("ai_attribution", EffectReadbackRequired, nil)
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("jira", "work-items")
	if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectEffect(
		context.Background(), claim, effect,
	); err != nil || inspection != EffectAbsent {
		t.Fatalf("composite AI readback=%v err=%v", inspection, err)
	}
	unknown, err := BuildEffectBatch("not-a-jira-destination", EffectReadbackRequired, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(context.Background(), claim, unknown); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("unknown composite effect error=%v", err)
	}
}

func TestJiraMetricTripletEffectsReachTheirMigratedStores(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	group := githubWorkItemMetricTestGroupRow()
	group.Provider, group.OrgID = "jira", claim.OrgID
	user := githubWorkItemMetricTestUserRow()
	user.Provider, user.OrgID = "jira", claim.OrgID
	cycle := githubWorkItemMetricTestCycleRow()
	cycle.Provider, cycle.OrgID = "jira", claim.OrgID
	effects, err := BuildJiraWorkItemDerivedEffects(JiraWorkItemDerivedEffectRows{
		WorkItemMetricsDaily:     []JiraWorkItemMetricsDailyRow{group},
		WorkItemUserMetricsDaily: []JiraWorkItemUserMetricsDailyRow{user},
		WorkItemCycleTimes:       []JiraWorkItemCycleTimePersistenceRow{cycle},
	})
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewJiraWorkItemDerivedClickHouseEffects(
		jiraDerivedProbeConn{},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, effect := range effects {
		switch effect.Destination {
		case "work_item_metrics_daily", "work_item_user_metrics_daily", "work_item_cycle_times":
			seen++
			if err := sink.WriteEffect(context.Background(), claim, effect); !errors.Is(err, errJiraDerivedPrepare) {
				t.Fatalf("%s write=%v want migrated store call", effect.Destination, err)
			}
			if inspection, err := sink.InspectEffect(context.Background(), claim, effect); inspection != EffectConflict || !errors.Is(err, errJiraDerivedPrepare) {
				t.Fatalf("%s readback=%v/%v want migrated store call", effect.Destination, inspection, err)
			}
		}
	}
	if seen != 3 {
		t.Fatalf("metric triplet effects exercised=%d want=3", seen)
	}
}

func TestJiraWorkItemDeriverRejectsForeignProviderAndTenant(t *testing.T) {
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	source := &githubMultiDayOracleSource{}
	deriver := JiraWorkItemDeriver{
		Source: source, statusMapping: loadRealStatusMapping(t),
		investmentClassifier: classifier,
	}
	claim := nativeTestClaim("jira", "work-items")
	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	before := day.AddDate(0, 0, 1)
	claim.SinceAt, claim.BeforeAt = &day, &before
	foreignProvider := claim
	foreignProvider.Provider = "github"
	if _, err := deriver.Derive(
		context.Background(), foreignProvider, jiraWorkItemRows{}, day.Add(time.Hour),
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign provider error=%v want ErrInvalidConfiguration", err)
	}
	if source.loads != 0 {
		t.Fatalf("foreign provider reached attribution context: %d load(s)", source.loads)
	}
	foreignTenant := jiraWorkItemRow{
		WorkItemID: "jira:OPS-1", Provider: "jira", Title: "foreign", Type: "task",
		Status: "todo", CreatedAt: day, UpdatedAt: day, OrgID: "org-other",
	}
	if _, err := deriver.Derive(
		context.Background(), claim,
		jiraWorkItemRows{WorkItems: []jiraWorkItemRow{foreignTenant}}, day.Add(time.Hour),
	); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("foreign tenant error=%v want ErrInvalidScope", err)
	}
	if source.loads != 0 {
		t.Fatalf("foreign tenant reached attribution context: %d load(s)", source.loads)
	}
}

type jiraDerivedFailingContextSource struct{ err error }

func (source jiraDerivedFailingContextSource) Load(
	context.Context,
	Claim,
	githubWorkItemDerivationLoadRequest,
) (githubWorkItemDerivationFacts, error) {
	return githubWorkItemDerivationFacts{}, source.err
}

func TestJiraWorkItemDeriverFailsClosedWhenAttributionContextCannotLoad(t *testing.T) {
	wantErr := errors.New("attribution context unavailable")
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	deriver := JiraWorkItemDeriver{
		Source:               jiraDerivedFailingContextSource{err: wantErr},
		statusMapping:        loadRealStatusMapping(t),
		investmentClassifier: classifier,
	}
	claim := nativeTestClaim("jira", "work-items")
	if _, err := deriver.Derive(
		context.Background(), claim, jiraWorkItemRows{},
		time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	); !errors.Is(err, wantErr) {
		t.Fatalf("derive error=%v want=%v", err, wantErr)
	}
}

func TestJiraClickHouseDerivationSourceUsesTenantScopedLoadersAndLease(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	conn := &recordingGitHubWorkItemDerivationConn{}
	leaseChecks := 0
	source := jiraWorkItemClickHouseDerivationContextSource{
		delegate: githubWorkItemClickHouseDerivationContextSource{
			Conn: conn,
			Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error {
				leaseChecks++
				return nil
			}),
		},
	}
	_, err := source.Load(
		context.Background(), claim,
		githubWorkItemDerivationLoadRequest{
			AsOf:             time.Date(2026, 8, 11, 16, 0, 0, 0, time.UTC),
			DonorWorkItemIDs: []string{"jira:OPS-2"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if leaseChecks != 2 {
		t.Fatalf("lease checks=%d want=2", leaseChecks)
	}
	if len(conn.queries) != 6 {
		t.Fatalf("migrated context queries=%d want=6", len(conn.queries))
	}
	for index, args := range conn.args {
		if len(args) == 0 || args[0] != claim.OrgID {
			t.Fatalf("query %d lost tenant fence: %#v", index, args)
		}
	}

	lostConn := &recordingGitHubWorkItemDerivationConn{}
	lost := jiraWorkItemClickHouseDerivationContextSource{
		delegate: githubWorkItemClickHouseDerivationContextSource{
			Conn: lostConn,
			Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error {
				return providerfoundation.ErrLeaseLost
			}),
		},
	}
	if _, err := lost.Load(
		context.Background(), claim,
		githubWorkItemDerivationLoadRequest{AsOf: time.Date(2026, 8, 11, 16, 0, 0, 0, time.UTC)},
	); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("lost lease error=%v", err)
	}
	if len(lostConn.queries) != 0 {
		t.Fatalf("lost lease reached context store: %d queries", len(lostConn.queries))
	}

	lostAfterConn := &recordingGitHubWorkItemDerivationConn{}
	lostAfterChecks := 0
	lostAfter := jiraWorkItemClickHouseDerivationContextSource{
		delegate: githubWorkItemClickHouseDerivationContextSource{
			Conn: lostAfterConn,
			Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error {
				lostAfterChecks++
				if lostAfterChecks == 2 {
					return providerfoundation.ErrLeaseLost
				}
				return nil
			}),
		},
	}
	if _, err := lostAfter.Load(
		context.Background(), claim,
		githubWorkItemDerivationLoadRequest{
			AsOf:             time.Date(2026, 8, 11, 16, 0, 0, 0, time.UTC),
			DonorWorkItemIDs: []string{"jira:OPS-2"},
		},
	); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("post-load lost lease error=%v", err)
	}
	if lostAfterChecks != 2 || len(lostAfterConn.queries) != 6 {
		t.Fatalf("post-load lease checks/queries=%d/%d want=2/6", lostAfterChecks, len(lostAfterConn.queries))
	}
}

func TestJiraAtlassianRawOnlyRouteHoldsWatermarkForDerivedGap(t *testing.T) {
	claim := jiraAtlassianClaim()
	client := jiraWorkItemsTestClient(
		t,
		&jiraAtlassianDoer{t: t},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	batch, err := (JiraAtlassianRouteHandler{
		StatusMapping: loadRealStatusMapping(t), Identity: jiraRouteIdentity,
	}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark != nil || len(batch.Effects) != len(jiraAtlassianRawDestinations) {
		t.Fatalf("raw-only route watermark/effects=%v/%d", batch.Watermark, len(batch.Effects))
	}
	summary, ok := batch.Result["jira_work_items"].(JiraAtlassianWorkItemsResult)
	if !ok || len(summary.DerivedDestinationsImplemented) != 0 ||
		len(summary.DerivedDestinationsUnimplemented) != len(jiraWorkItemDerivedDestinations) ||
		!summary.WatermarkHeldForIncomplete {
		t.Fatalf("raw-only typed result=%#v", batch.Result["jira_work_items"])
	}
}

func TestJiraCanonicalAliasesComposeSixteenDestinationsPlusWorklogs(t *testing.T) {
	for _, dataset := range workitemcontract.FamilyDatasets() {
		dataset := dataset
		t.Run(dataset, func(t *testing.T) {
			claim := jiraAtlassianClaim()
			claim.Dataset = dataset
			capability, ok := Capability("jira", dataset)
			if !ok {
				t.Fatalf("missing Jira capability for %q", dataset)
			}
			claim.CostClass = capability.CostClass
			client := jiraWorkItemsTestClient(
				t,
				&jiraAtlassianDoer{t: t},
				providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
			)
			batch, err := jiraAtlassianCompleteHandler(t).Collect(
				context.Background(), claim, providerfoundation.Credential{}, client,
				time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC),
			)
			if err != nil {
				t.Fatal(err)
			}
			if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
				t.Fatalf("watermark=%v want=%v", batch.Watermark, claim.BeforeAt)
			}
			if len(batch.Effects) != len(workItemRouteDestinations())+1 {
				t.Fatalf("effects=%d want=%d", len(batch.Effects), len(workItemRouteDestinations())+1)
			}
			seen := make(map[string]int, len(batch.Effects))
			for _, effect := range batch.Effects {
				seen[effect.Destination]++
			}
			for _, destination := range workItemRouteDestinations() {
				if seen[destination] != 1 {
					t.Fatalf("canonical destination %q count=%d", destination, seen[destination])
				}
			}
			if seen["worklogs"] != 1 || len(seen) != len(workItemRouteDestinations())+1 {
				t.Fatalf("destination set=%v", seen)
			}
			summary, ok := batch.Result["jira_work_items"].(JiraAtlassianWorkItemsResult)
			if !ok || len(summary.DerivedDestinationsImplemented) != 10 ||
				len(summary.DerivedDestinationsUnimplemented) != 0 ||
				summary.WatermarkHeldForIncomplete {
				t.Fatalf("typed completion result=%#v", batch.Result["jira_work_items"])
			}
		})
	}
}
