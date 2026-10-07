package providersync

import (
	"context"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// The payload and the ledger below are the bytes the binary BEFORE the
// raw-only cut stored for one GitHub work-items unit: its route, its deriver
// engine, its projection and its manifest encoder ran on that commit, and
// the result was copied here unchanged. Eighteen effects: the nine raw
// destinations and the nine tables the daily job now writes.
const (
	workItemsManifestBeforeRawOnlyPayload = "testdata/prepared_snapshots/github_work_items_before_raw_only_payload.json"
	workItemsManifestBeforeRawOnlyLedger  = "testdata/prepared_snapshots/github_work_items_before_raw_only_ledger.json"
)

// workItemsManifestBeforeRawOnly loads the stored document and builds the
// claim and lease session of the unit it was stored for.
func workItemsManifestBeforeRawOnly(
	t *testing.T, now time.Time,
) (Claim, *LeaseSession, *memoryEffectLedger) {
	t.Helper()
	payload, err := os.ReadFile(workItemsManifestBeforeRawOnlyPayload)
	if err != nil {
		t.Fatal(err)
	}
	ledgerBytes, err := os.ReadFile(workItemsManifestBeforeRawOnlyLedger)
	if err != nil {
		t.Fatal(err)
	}
	state, err := decodeEffectLedgerState(ledgerBytes)
	if err != nil {
		t.Fatalf("the stored ledger no longer decodes: %v", err)
	}
	unit := githubWorkItemsRESTClaim().Unit
	unit.DatasetOptions["fetch_milestones"] = false
	leases := newMemoryLeaseRepository(unit, "dispatching")
	claim, err := leases.Claim(context.Background(), ClaimRequest{
		UnitID: unit.ID, OrgID: unit.OrgID, Owner: githubWorkItemsRESTClaim().Owner, Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	session := &LeaseSession{
		Repository: leases, Claim: claim, LeaseDuration: time.Minute,
		Deadline: now.Add(time.Hour), Now: func() time.Time { return now },
	}
	return claim, session, &memoryEffectLedger{state: state, preparedSnapshot: payload}
}

func preparedRetiredEffectsSkippedCounter(t *testing.T, metrics *providerfoundation.Metrics, provider string) int {
	t.Helper()
	var rendered strings.Builder
	if err := metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	prefix := `dev_health_work_item_prepared_retired_effects_skipped_total{provider="` + provider + `"} `
	for _, line := range strings.Split(rendered.String(), "\n") {
		if value, found := strings.CutPrefix(line, prefix); found {
			count, err := strconv.Atoi(value)
			if err != nil {
				t.Fatalf("counter line %q: %v", line, err)
			}
			return count
		}
	}
	return 0
}

// A unit that was in flight when the worker changed to the raw-only binary
// holds a prepared manifest with effects for the nine tables of the daily
// job. The new binary COMPLETES that manifest: every raw effect is applied
// from the stored rows without a new collection, no row is written for one of
// the nine, and the unit ends with the result a new manifest would give.
func TestCompleteRouteExecutorCompletesWorkItemsManifestOfEarlierBinary(t *testing.T) {
	retired := append([]string(nil), githubWorkItemDerivedDestinations...)
	current := githubWorkItemRouteDestinations()
	for _, testCase := range []struct {
		name string
		// committed and writing name the ledger entries the earlier binary
		// had settled or started when it stopped.
		committed, writing []string
		readback           map[string]EffectInspection
		wantWritten        []string
		wantSkipped        int
		wantMarked         int
		wantRetiredSkipped int
	}{
		{
			name:        "nothing committed yet",
			wantWritten: current, wantRetiredSkipped: len(retired),
		},
		{
			// The state the discard decision refuses: effects of the document
			// already committed, among them tables the daily job now writes,
			// and one such table left in the writing state, which the sync
			// sink can never read back.
			name:      "stopped in the middle of the commit",
			committed: []string{"ai_attribution", "estimate_coverage_metrics_daily", "investment_classifications_daily"},
			writing:   []string{"investment_metrics_daily"},
			wantWritten: slices.DeleteFunc(append([]string(nil), current...), func(destination string) bool {
				return destination == "ai_attribution"
			}),
			wantSkipped: 3, wantRetiredSkipped: len(retired) - 2,
		},
		{
			name:      "a raw effect was being written and had landed",
			committed: []string{"ai_attribution"},
			writing:   []string{"work_items"},
			readback:  map[string]EffectInspection{"work_items": EffectExact},
			wantWritten: slices.DeleteFunc(append([]string(nil), current...), func(destination string) bool {
				return destination == "ai_attribution" || destination == "work_items"
			}),
			wantSkipped: 1, wantMarked: 1, wantRetiredSkipped: len(retired),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			now := time.Date(2026, 8, 4, 12, 30, 0, 0, time.UTC)
			claim, session, ledger := workItemsManifestBeforeRawOnly(t, now)
			if len(ledger.state.Effects) != len(current)+len(retired) {
				t.Fatalf("stored ledger effects=%d want %d raw and %d retired",
					len(ledger.state.Effects), len(current), len(retired))
			}
			startedAt := ledger.state.CreatedAt.Add(time.Second)
			committedAt := startedAt.Add(time.Second)
			for index := range ledger.state.Effects {
				effect := &ledger.state.Effects[index]
				switch {
				case slices.Contains(testCase.committed, effect.Destination):
					effect.Status, effect.StartedAt, effect.CommittedAt = GenerationBlockCommitted, &startedAt, &committedAt
				case slices.Contains(testCase.writing, effect.Destination):
					effect.Status, effect.StartedAt = GenerationBlockWriting, &startedAt
				}
			}
			descriptor, ok := Descriptor("github", "work-items")
			if !ok || !descriptor.PreparedManifestRecovery {
				t.Fatalf("github/work-items descriptor=%+v ok=%v", descriptor, ok)
			}
			if !slices.Equal(descriptor.Destinations, current) {
				t.Fatalf("descriptor destinations=%v want=%v", descriptor.Destinations, current)
			}
			handler := &staticCompleteRouteHandler{
				batch: CompleteRouteBatch{Result: map[string]any{"live_provider": "must not be collected"}},
			}
			sink := &memoryEffectSink{}
			executor := completeRouteExecutor(now, handler, ledger, sink)
			credentials := &forbiddenCredentialRepository{}
			decryptor := &forbiddenCredentialDecryptor{}
			doer := &trackingCompleteRouteDoer{}
			executor.Credentials.Repository = credentials
			executor.Credentials.Decryptor = decryptor
			executor.Doer = fakehttp.Client(doer)
			executor.Metrics = providerfoundation.NewMetrics()
			// A readback that answers for a retired table would be a store
			// call for it: only the named raw destinations may be asked.
			executor.Committer.Readback = refusingRetiredReadback{
				t: t, inner: staticEffectReadback{inspections: testCase.readback},
			}

			result, err := executor.Execute(context.Background(), session, descriptor)
			if err != nil {
				t.Fatalf("the manifest of the earlier binary was not completed: %v", err)
			}
			if !handler.normalizedAt.IsZero() || doer.requests != 0 ||
				credentials.calls != 0 || decryptor.calls != 0 ||
				ledger.preparedLoads != 1 || ledger.preparedPrepares != 0 {
				t.Fatalf("the unit collected again: handler_at=%s requests=%d credential_calls=%d decrypt_calls=%d loads=%d prepares=%d",
					handler.normalizedAt, doer.requests, credentials.calls, decryptor.calls,
					ledger.preparedLoads, ledger.preparedPrepares)
			}
			written := append([]string(nil), sink.destinations...)
			slices.Sort(written)
			wantWritten := append([]string(nil), testCase.wantWritten...)
			slices.Sort(wantWritten)
			if !slices.Equal(written, wantWritten) {
				t.Fatalf("sink writes=%v want=%v", written, wantWritten)
			}
			for _, destination := range written {
				if slices.Contains(retired, destination) {
					t.Fatalf("the sink received the daily-job table %q", destination)
				}
			}
			if result.Effects.Written != len(wantWritten) || result.Effects.Skipped != testCase.wantSkipped ||
				result.Effects.MarkedCommitted != testCase.wantMarked {
				t.Fatalf("effects=%+v want written=%d skipped=%d marked=%d",
					result.Effects, len(wantWritten), testCase.wantSkipped, testCase.wantMarked)
			}
			if got := preparedRetiredEffectsSkippedCounter(t, executor.Metrics, "github"); got != testCase.wantRetiredSkipped {
				t.Fatalf("retired effects skipped counter=%d want=%d", got, testCase.wantRetiredSkipped)
			}
			for index, effect := range ledger.state.Effects {
				if effect.Status != GenerationBlockCommitted {
					t.Fatalf("ledger effect[%d] %s status=%s: the generation is not complete",
						index, effect.Destination, effect.Status)
				}
			}
			// The unit ends as a unit of the new binary does: the result of
			// the stored collection, without the keys of the derivation.
			if result.Watermark == nil || claim.BeforeAt == nil || !result.Watermark.Equal(claim.BeforeAt.UTC()) {
				t.Fatalf("watermark=%v want the claim bound %v", result.Watermark, claim.BeforeAt)
			}
			if result.Fetch.Provider != "github" || result.Fetch.Dataset != "work-items" || result.Fetch.Requests == 0 {
				t.Fatalf("fetch evidence=%+v want the evidence of the stored collection", result.Fetch)
			}
			for _, key := range retiredWorkItemSyncResultKeys {
				if _, present := result.Result[key]; present {
					t.Fatalf("result still carries %q", key)
				}
			}
			if _, present := result.Result["work_items_synced"]; !present {
				t.Fatalf("result lost the stored collection's counts: %v", result.Result)
			}

			// A second attempt finds a completed generation: no write, no
			// second count.
			secondSink := &memoryEffectSink{}
			second := completeRouteExecutor(now.Add(10*time.Minute), handler, ledger, secondSink)
			second.Metrics = executor.Metrics
			second.Committer.Readback = executor.Committer.Readback
			secondResult, err := second.Execute(context.Background(), session, descriptor)
			if err != nil {
				t.Fatal(err)
			}
			if len(secondSink.destinations) != 0 || secondResult.Effects.Written != 0 ||
				secondResult.Effects.Skipped != len(ledger.state.Effects) {
				t.Fatalf("second attempt writes=%v effects=%+v", secondSink.destinations, secondResult.Effects)
			}
			if got := preparedRetiredEffectsSkippedCounter(t, executor.Metrics, "github"); got != testCase.wantRetiredSkipped {
				t.Fatalf("the second attempt counted again: %d want=%d", got, testCase.wantRetiredSkipped)
			}
		})
	}
}

// refusingRetiredReadback fails the test when the executor asks the store
// about a table the daily job owns.
type refusingRetiredReadback struct {
	t     *testing.T
	inner EffectReadback
}

func (readback refusingRetiredReadback) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if _, retired := retiredWorkItemSyncDestinations[effect.Destination]; retired {
		readback.t.Errorf("the store was asked about the daily-job table %q", effect.Destination)
	}
	return readback.inner.InspectEffect(ctx, claim, effect)
}

// The stored document itself is refused by the plain decoder for one reason
// only, its destination set, and it still carries its own rows: that is the
// input the completion works from.
func TestWorkItemsManifestOfEarlierBinaryDecodesAsSupersededWithItsRows(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 30, 0, 0, time.UTC)
	claim, _, ledger := workItemsManifestBeforeRawOnly(t, now)
	manifest, err := decodePreparedRouteManifest(ledger.preparedSnapshot, claim, ledger.state)
	if err != ErrPreparedSnapshotManifestMismatch {
		t.Fatalf("decode error=%v want ErrPreparedSnapshotManifestMismatch", err)
	}
	rows := map[string]int{}
	for _, effect := range manifest.Batch.Effects {
		rows[effect.Destination] = len(effect.Rows)
	}
	if rows["work_items"] != 2 || rows["work_item_team_attributions"] == 0 || len(rows) != 18 {
		t.Fatalf("stored rows by destination=%v", rows)
	}
}

// The completion is for one shape only. Every other difference between a
// stored manifest and the route keeps the discard decision.
func TestRetiredWorkItemEffectsSkipperAcceptsOnlyTheRetiredTables(t *testing.T) {
	effectsFor := func(t *testing.T, destinations []string) PreparedRouteManifest {
		t.Helper()
		effects := make([]EffectBatch, 0, len(destinations))
		for _, destination := range destinations {
			effect, err := BuildEffectBatch(destination, EffectReadbackRequired, nil)
			if err != nil {
				t.Fatal(err)
			}
			effects = append(effects, effect)
		}
		return PreparedRouteManifest{Batch: CompleteRouteBatch{Effects: effects}}
	}
	if len(retiredWorkItemSyncDestinations) != len(githubWorkItemDerivedDestinations) {
		t.Fatalf("retired set=%d names, the derived list=%d", len(retiredWorkItemSyncDestinations), len(githubWorkItemDerivedDestinations))
	}
	for _, destination := range githubWorkItemDerivedDestinations {
		if _, present := retiredWorkItemSyncDestinations[destination]; !present {
			t.Fatalf("%q is not in the retired set", destination)
		}
	}
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		t.Run(provider, func(t *testing.T) {
			claim := nativeTestClaim(provider, "work-items")
			descriptor, ok := Descriptor(provider, "work-items")
			if !ok || len(descriptor.Destinations) == 0 {
				t.Fatalf("descriptor=%+v ok=%v", descriptor, ok)
			}
			for _, destination := range descriptor.Destinations {
				if _, retired := retiredWorkItemSyncDestinations[destination]; retired {
					t.Fatalf("the route still emits the retired table %q", destination)
				}
			}
			current := descriptor.Destinations
			earlier := slices.Concat(current, githubWorkItemDerivedDestinations)
			skipper, ok := newRetiredWorkItemEffectsSkipper(claim, descriptor, effectsFor(t, earlier), EffectCommitter{})
			if !ok || !slices.Equal(skipper.retired, githubWorkItemDerivedDestinations) {
				t.Fatalf("the manifest of the earlier binary was not recognised: ok=%v skipper=%+v", ok, skipper)
			}
			kept := skipper.currentBatch(CompleteRouteBatch{
				Effects: effectsFor(t, earlier).Batch.Effects,
				Result:  map[string]any{"work_items_synced": 2, "team_inheritance": map[string]any{}},
			})
			if len(kept.Effects) != len(current) || kept.Result["work_items_synced"] != 2 {
				t.Fatalf("current batch=%+v", kept)
			}
			if _, present := kept.Result["team_inheritance"]; present {
				t.Fatal("the current batch kept a result key of the derivation")
			}
			for name, destinations := range map[string][]string{
				"the manifest of today":         current,
				"a current destination missing": slices.Concat(current[1:], githubWorkItemDerivedDestinations),
				"an unknown destination":        slices.Concat(earlier, []string{"not_a_work_item_table"}),
				"a retired destination twice":   slices.Concat(earlier, githubWorkItemDerivedDestinations[:1]),
				"a current destination twice":   slices.Concat(earlier, current[:1]),
			} {
				if _, ok := newRetiredWorkItemEffectsSkipper(claim, descriptor, effectsFor(t, destinations), EffectCommitter{}); ok {
					t.Fatalf("%s was taken as a manifest to complete", name)
				}
			}
			other := claim
			other.Dataset = "prs"
			if _, ok := newRetiredWorkItemEffectsSkipper(other, descriptor, effectsFor(t, earlier), EffectCommitter{}); ok {
				t.Fatal("a claim of another dataset was taken as a manifest to complete")
			}
		})
	}
	foreign := nativeTestClaim("github", "work-items")
	foreign.Provider = "pagerduty"
	descriptor, _ := Descriptor("github", "work-items")
	if _, ok := newRetiredWorkItemEffectsSkipper(
		foreign, descriptor, effectsFor(t, slices.Concat(descriptor.Destinations, githubWorkItemDerivedDestinations)), EffectCommitter{},
	); ok {
		t.Fatal("a provider with no work-items unit was taken as a manifest to complete")
	}
}
