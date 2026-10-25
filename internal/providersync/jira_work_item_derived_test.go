package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

type jiraDerivedStubConn struct{ driver.Conn }

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
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }), nil,
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
	sink, err := NewJiraWorkItemCompositeClickHouseEffects(jiraDerivedStubConn{}, lease, nil)
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

// The Jira unit stores raw rows only and takes its watermark from the window
// it stored; no derived table has an effect in its batch.
func TestJiraAtlassianRouteStoresRawRowsAndAdvancesWatermark(t *testing.T) {
	claim := jiraAtlassianClaim()
	client := jiraWorkItemsTestClient(
		t,
		fakehttp.Client(&jiraAtlassianDoer{t: t}),
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
	// The watermark is the end of the window whose raw rows the unit stored;
	// the one effect beside the raw tables is the evaluated-empty ai_attribution.
	if batch.Watermark == nil || !batch.Watermark.Equal(claim.BeforeAt.UTC()) ||
		len(batch.Effects) != len(jiraAtlassianRawDestinations)+1 {
		t.Fatalf("route watermark/effects=%v/%d", batch.Watermark, len(batch.Effects))
	}
	for _, effect := range batch.Effects {
		if slices.Contains(githubWorkItemDerivedDestinations, effect.Destination) {
			t.Fatalf("the unit built an effect for the derived table %q", effect.Destination)
		}
	}
	summary, ok := batch.Result["jira_work_items"].(JiraAtlassianWorkItemsResult)
	if !ok || summary.WatermarkHeldForIncomplete {
		t.Fatalf("typed result=%#v", batch.Result["jira_work_items"])
	}
	for _, key := range []string{
		"derived_destinations_implemented", "derived_destinations_unimplemented", "team_inheritance",
	} {
		if _, present := batch.Result[key]; present {
			t.Fatalf("result still carries %q: %+v", key, batch.Result)
		}
	}
}

func TestJiraCanonicalAliasesComposeTheFamilyDestinationsPlusWorklogs(t *testing.T) {
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
				fakehttp.Client(&jiraAtlassianDoer{t: t}),
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
			// +3: worklogs plus CHAOS-4193's project_membership_transitions
			// and projects, neither of which is in the shared
			// workItemRouteDestinations() list (gitlab must never see them).
			if len(batch.Effects) != len(workItemRouteDestinations())+3 {
				t.Fatalf("effects=%d want=%d", len(batch.Effects), len(workItemRouteDestinations())+3)
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
			if seen["worklogs"] != 1 || seen["project_membership_transitions"] != 1 ||
				seen["projects"] != 1 || len(seen) != len(workItemRouteDestinations())+3 {
				t.Fatalf("destination set=%v", seen)
			}
			summary, ok := batch.Result["jira_work_items"].(JiraAtlassianWorkItemsResult)
			if !ok || summary.WatermarkHeldForIncomplete {
				t.Fatalf("typed completion result=%#v", batch.Result["jira_work_items"])
			}
		})
	}
}
