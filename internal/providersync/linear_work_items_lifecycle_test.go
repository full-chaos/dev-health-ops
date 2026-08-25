package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

func linearLifecycleIssueResponse(identifier, teamKey string) string {
	return `{"data":{"issues":{"nodes":[{"id":"` + identifier + `","identifier":"` + identifier + `","title":"typed Linear issue","createdAt":"2026-07-25T09:00:00Z","updatedAt":"2026-07-28T16:30:00Z","state":{"name":"Todo","type":"unstarted"},"labels":{"nodes":[]},"team":{"id":"team-` + strings.ToLower(teamKey) + `","key":"` + teamKey + `","name":"` + teamKey + `"},"history":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"comments":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"attachments":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"relations":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"inverseRelations":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
}

func TestLinearRouteGlobalDiscoveryCrawlsEveryDiscoveredTeam(t *testing.T) {
	doer := &linearWorkItemsDoer{responses: []string{
		`{"data":{"teams":{"nodes":[{"id":"team-eng","key":"ENG","name":"Engineering"},{"id":"team-ops","key":"OPS","name":"Operations"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		linearLifecycleIssueResponse("ENG-1", "ENG"),
		linearLifecycleIssueResponse("OPS-1", "OPS"),
	}}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "global-discovery"
	noCycles := false
	batch, err := (LinearWorkItemsRouteHandler{GlobalDiscovery: true, FetchCycles: &noCycles}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 3 || batch.Evidence.Pages != 3 || len(batch.Effects[0].Rows) != 2 {
		t.Fatalf("requests=%d evidence=%+v effects=%+v", len(doer.requests), batch.Evidence, batch.Effects)
	}
	for index, wantTeam := range []string{"ENG", "OPS"} {
		var requestBody struct {
			Variables struct {
				Filter struct {
					Team struct {
						Key struct {
							In []string `json:"in"`
						} `json:"key"`
					} `json:"team"`
				} `json:"filter"`
			} `json:"variables"`
		}
		if err := json.NewDecoder(doer.requests[index+1].Body).Decode(&requestBody); err != nil {
			t.Fatal(err)
		}
		if len(requestBody.Variables.Filter.Team.Key.In) != 1 || requestBody.Variables.Filter.Team.Key.In[0] != wantTeam {
			t.Fatalf("team request=%+v", requestBody)
		}
	}
}

func TestLinearRequestPlanBoundsCanonicalRouteFamilies(t *testing.T) {
	want := []RequestEstimate{
		{Dimension: BudgetGraphQLCost, Units: 3, Confidence: "low", RouteFamily: "attachments"},
		{Dimension: BudgetGraphQLCost, Units: 6, Confidence: "low", RouteFamily: "comments"},
		{Dimension: BudgetGraphQLCost, Units: 2, Confidence: "low", RouteFamily: "cycles"},
		{Dimension: BudgetGraphQLCost, Units: 6, Confidence: "low", RouteFamily: "history"},
		{Dimension: BudgetGraphQLCost, Units: 30, Confidence: "low", RouteFamily: "issues"},
		{Dimension: BudgetGraphQLCost, Units: 6, Confidence: "low", RouteFamily: "relations"},
		{Dimension: BudgetGraphQLCost, Units: 1, Confidence: "medium", RouteFamily: "teams"},
	}
	if got := ProviderRequestPlan("linear", "work-items", 3, nil); !reflect.DeepEqual(got, want) {
		t.Fatalf("linear request plan=%+v want=%+v", got, want)
	}
	for _, dataset := range workitemcontract.FamilyDatasets() {
		if plan := ProviderRequestPlan("linear", dataset, 1, nil); len(plan) == 0 {
			t.Fatalf("linear alias %q has no bounded request estimate", dataset)
		}
	}
}

func TestLinearRoutePaginatesInlineHistoryFromTheParentCursor(t *testing.T) {
	doer := &linearWorkItemsDoer{responses: []string{
		linearTeamResponse(),
		`{"data":{"issues":{"nodes":[{"id":"lin-history","identifier":"ENG-1","title":"history","createdAt":"2026-07-25T09:00:00Z","updatedAt":"2026-07-28T16:30:00Z","state":{"name":"Todo","type":"unstarted"},"labels":{"nodes":[]},"team":{"id":"team-eng","key":"ENG","name":"Engineering"},"history":{"nodes":[{"createdAt":"2026-07-26T10:00:00Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"In Progress","type":"started"},"actor":null}],"pageInfo":{"hasNextPage":true,"endCursor":"history-cursor-1"}},"comments":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"attachments":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"relations":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"inverseRelations":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"issue":{"history":{"nodes":[{"createdAt":"2026-07-27T10:00:00Z","fromState":{"name":"In Progress","type":"started"},"toState":{"name":"Done","type":"completed"},"actor":null}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`,
	}}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	noCycles := false
	batch, err := (LinearWorkItemsRouteHandler{FetchCycles: &noCycles}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 3 || len(batch.Effects[1].Rows) != 2 {
		t.Fatalf("requests=%d transitions=%d", len(doer.requests), len(batch.Effects[1].Rows))
	}
	var historyRequest struct {
		Variables map[string]any `json:"variables"`
	}
	if err := json.NewDecoder(doer.requests[2].Body).Decode(&historyRequest); err != nil {
		t.Fatal(err)
	}
	if historyRequest.Variables["after"] != "history-cursor-1" {
		t.Fatalf("history variables=%+v", historyRequest.Variables)
	}
}

func TestLinearRouteRejectsMalformedNestedConnectionBeforeWatermark(t *testing.T) {
	doer := &linearWorkItemsDoer{responses: []string{
		linearTeamResponse(),
		`{"data":{"issues":{"nodes":[{"id":"lin-malformed","identifier":"ENG-1","title":"malformed","createdAt":"2026-07-25T09:00:00Z","updatedAt":"2026-07-28T16:30:00Z","state":{"name":"Todo","type":"unstarted"},"labels":{"nodes":[]},"team":{"id":"team-eng","key":"ENG","name":"Engineering"},"attachments":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	noCycles := false
	batch, err := (LinearWorkItemsRouteHandler{FetchCycles: &noCycles}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, providerfoundation.ErrPaginationInvalid) || batch.Watermark != nil {
		t.Fatalf("batch=%+v error=%v", batch, err)
	}
}

func TestLinearRoutePropagatesCancellationBeforeIssuingAnotherPage(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := providerfoundation.NewHTTPClient(
		"linear", "https://api.linear.app", &linearWorkItemsDoer{},
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(ctx context.Context) error { return ctx.Err() }),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = (LinearWorkItemsRouteHandler{}).Collect(
		ctx, claim, providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		client, time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error=%v", err)
	}
}

func allLinearFamilyFlags(enabled bool) map[string]bool {
	flags := make(map[string]bool, len(workitemcontract.FamilyDatasets()))
	for _, dataset := range workitemcontract.FamilyDatasets() {
		flags[workItemFamilyFlagForDataset(dataset)] = enabled
	}
	return flags
}

func linearTypedLifecycleFixture(t *testing.T) (Claim, LinearWorkItemsRouteBatch) {
	t.Helper()
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	claim.ProcessorFlags = allLinearFamilyFlags(true)
	effects, err := BuildLinearWorkItemEffects(LinearWorkItemEffectRows{
		WorkItems: []json.RawMessage{json.RawMessage(`{"org_id":"org-acme","provider":"linear","work_item_id":"linear:ENG-1"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	watermark := *claim.BeforeAt
	return claim, LinearWorkItemsRouteBatch{
		Effects: effects, Watermark: &watermark,
		Evidence: FetchEvidence{Provider: "linear", Dataset: "work-items", Pages: 1, Requests: 1, Records: 1},
		Result: LinearWorkItemsRouteResult{
			Rows:   LinearWorkItemsRows{WorkItems: []LinearWorkItemRow{{WorkItemID: "linear:ENG-1"}}},
			Counts: LinearWorkItemCounts{WorkItems: 1}, NonEmpty: true,
			Evidence: FetchEvidence{Provider: "linear", Dataset: "work-items", Pages: 1, Requests: 1, Records: 1},
		},
	}
}

func TestLinearTypedLifecycleProducesFiveAliasReadbackAudit(t *testing.T) {
	claim, route := linearTypedLifecycleFixture(t)
	backend := newLinearSemanticEffectBackend()
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := linearWorkItemEffectsFixture(backend, lease)
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	result, commit, err := (LinearWorkItemsLifecycle{
		Committer: EffectCommitter{
			Ledger: &memoryEffectLedger{}, Sink: sink, Readback: sink,
			Now: func() time.Time { return now },
		},
	}).Commit(context.Background(), claim, route, now)
	if err != nil {
		t.Fatal(err)
	}
	if commit.Written != 8 || len(result.Effects) != 8 || len(result.Aliases) != 5 {
		t.Fatalf("commit=%+v result=%+v", commit, result)
	}
	if err := ValidateLinearWorkItemsCompletion(claim, result); err != nil {
		t.Fatalf("typed completion validation: %v", err)
	}
	for index, alias := range result.Aliases {
		if !alias.Complete || alias.Readback != LinearAliasReadbackExact && alias.Readback != LinearAliasReadbackEmpty {
			t.Fatalf("alias[%d]=%+v", index, alias)
		}
	}
}

func TestLinearTypedLifecycleRejectsEmptyProducerBatch(t *testing.T) {
	claim, route := linearTypedLifecycleFixture(t)
	route.Result.NonEmpty = false
	_, _, err := (LinearWorkItemsLifecycle{}).Commit(
		context.Background(), claim, route, time.Now().UTC(),
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("empty lifecycle error=%v", err)
	}
}

func TestLinearTypedLifecycleRejectsMissingWatermark(t *testing.T) {
	claim, route := linearTypedLifecycleFixture(t)
	route.Watermark = nil
	backend := newLinearSemanticEffectBackend()
	sink := linearWorkItemEffectsFixture(backend, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	_, _, err := (LinearWorkItemsLifecycle{
		Committer: EffectCommitter{Ledger: &memoryEffectLedger{}, Sink: sink, Readback: sink, Now: func() time.Time { return now }},
	}).Commit(context.Background(), claim, route, now)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("missing watermark error=%v", err)
	}
}

func TestLinearTypedCompletionRejectsMissingOrUnknownAliasFlags(t *testing.T) {
	claim, route := linearTypedLifecycleFixture(t)
	backend := newLinearSemanticEffectBackend()
	sink := linearWorkItemEffectsFixture(backend, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	result, _, err := (LinearWorkItemsLifecycle{
		Committer: EffectCommitter{Ledger: &memoryEffectLedger{}, Sink: sink, Readback: sink, Now: func() time.Time { return now }},
	}).Commit(context.Background(), claim, route, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*Claim)
	}{
		{name: "missing alias", mutate: func(c *Claim) { delete(c.ProcessorFlags, workItemFamilyFlagForDataset("work-item-comments")) }},
		{name: "unknown alias", mutate: func(c *Claim) { c.ProcessorFlags["family_dataset_future_alias"] = true }},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutated := claim
			mutated.ProcessorFlags = cloneBoolMap(claim.ProcessorFlags)
			test.mutate(&mutated)
			if err := ValidateLinearWorkItemsCompletion(mutated, result); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("validation error=%v", err)
			}
		})
	}
}

func TestLinearTypedCompletionRejectsAliasEffectMismatches(t *testing.T) {
	claim, route := linearTypedLifecycleFixture(t)
	backend := newLinearSemanticEffectBackend()
	sink := linearWorkItemEffectsFixture(backend, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	result, _, err := (LinearWorkItemsLifecycle{
		Committer: EffectCommitter{Ledger: &memoryEffectLedger{}, Sink: sink, Readback: sink, Now: func() time.Time { return now }},
	}).Commit(context.Background(), claim, route, now)
	if err != nil {
		t.Fatal(err)
	}
	mutated := result
	mutated.Aliases[0].EffectDestinations = []string{"work_items", "sprints"}
	if err := ValidateLinearWorkItemsCompletion(claim, mutated); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("mismatched alias destinations error=%v", err)
	}
	mutated = result
	mutated.Aliases[2].EffectDestinations = []string{"sprints", "work_items"}
	if err := ValidateLinearWorkItemsCompletion(claim, mutated); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("reordered alias destinations error=%v", err)
	}
	mutated = result
	mutated.NonEmpty = false
	if err := ValidateLinearWorkItemsCompletion(claim, mutated); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("empty completion result error=%v", err)
	}
	mutated = result
	mutated.Effects = mutated.Effects[:1]
	if err := ValidateLinearWorkItemsCompletion(claim, mutated); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("incomplete effect audit error=%v", err)
	}
}

func cloneBoolMap(input map[string]bool) map[string]bool {
	clone := make(map[string]bool, len(input))
	for key, value := range input {
		clone[key] = value
	}
	return clone
}
