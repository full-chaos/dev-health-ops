package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// preparedFixtureColumn is a column the destination's sink INSERTs, other than
// org_id, so a fixture row survives the commit projection.
func preparedFixtureColumn(t *testing.T, destination string) string {
	t.Helper()
	for _, column := range insertStatementColumns(preparedRouteInsertStatements[destination]) {
		if column != "org_id" {
			return column
		}
	}
	t.Fatalf("no INSERT column for %s", destination)
	return ""
}

func preparedDeploymentsBatch(t *testing.T, claim Claim) CompleteRouteBatch {
	t.Helper()
	effect, err := effectBatchFromValues("deployments", EffectReadbackRequired, []deploymentRow{deploymentEffectsUnitRow(claim, "901")})
	if err != nil {
		t.Fatal(err)
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect}, Result: map[string]any{"deployments_synced": 1},
		Watermark: claim.BeforeAt,
		Evidence:  FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Records: 1},
	}
}

type preparedGitHubTokenDecryptor struct{}

func (preparedGitHubTokenDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return []byte(`{"token":"fixture-token"}`), nil
}

// preparedGitHubExecutor is completeRouteExecutor with a github credential, so
// a route that does re-collect can reach its handler.
func preparedGitHubExecutor(now time.Time, handler CompleteRouteHandler, ledger EffectLedger, sink EffectSink) CompleteRouteExecutor {
	executor := completeRouteExecutor(now, handler, ledger, sink)
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = preparedGitHubTokenDecryptor{}
	return executor
}

func preparedDeploymentsDescriptor(t *testing.T) CompleteRouteDescriptor {
	t.Helper()
	descriptor, ok := Descriptor("github", "deployments")
	if !ok || !descriptor.PreparedManifestRecovery {
		t.Fatalf("github/deployments descriptor=%+v ok=%v, want prepared recovery", descriptor, ok)
	}
	return descriptor
}

func TestPreparedManifestRouteListIsExactlyTheEnrolledCanonicalRoutes(t *testing.T) {
	for _, pair := range []struct {
		provider, dataset string
		want              bool
	}{
		{"github", "work-items", true}, {"github", "deployments", true}, {"gitlab", "deployments", true},
		{"github", "prs", true}, {"gitlab", "prs", true},
		{"github", "pr-reviews", false}, {"github", "pr-comments", false},
		{"gitlab", "pr-reviews", false}, {"gitlab", "pr-comments", false},
		{"github", "commits", true}, {"gitlab", "commits", true}, {"github", "commit-stats", true},
		{"gitlab", "commit-stats", true}, {"github", "files", true}, {"gitlab", "files", true},
		{"github", "repo-metadata", true}, {"gitlab", "repo-metadata", true},
		{"github", "security", true}, {"gitlab", "security", true},
		{"github", "blame", false}, {"github", "cicd", false},
		{"gitlab", "feature-flags", true}, {"launchdarkly", "feature-flags", true},
		{"gitlab", "incidents", true}, {"jira", "incidents", true},
		{"gitlab", "work-items", false}, {"linear", "work-items", false},
	} {
		destinations, ok := preparedManifestRouteDestinations(pair.provider, pair.dataset)
		descriptor, _ := Descriptor(pair.provider, pair.dataset)
		if ok != pair.want || descriptor.PreparedManifestRecovery != pair.want {
			t.Fatalf("%s/%s listed=%v descriptor_flag=%v want %v", pair.provider, pair.dataset, ok, descriptor.PreparedManifestRecovery, pair.want)
		}
		if ok && strings.Join(destinations, ",") != strings.Join(descriptor.Destinations, ",") {
			t.Fatalf("%s/%s destinations=%v descriptor=%v", pair.provider, pair.dataset, destinations, descriptor.Destinations)
		}
	}
	if !preparedManifestRouteRequiresSnapshot("github", "work-items") ||
		preparedManifestRouteRequiresSnapshot("github", "deployments") || preparedManifestRouteRequiresSnapshot("gitlab", "prs") {
		t.Fatal("only github/work-items may refuse a ledger written without a snapshot")
	}
}

func TestPreparedRouteRecoversALegacyLedgerByRecollecting(t *testing.T) {
	log := captureSlog(t)
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedWorkItemsSession(t, now, "github", "deployments")
	batch := preparedDeploymentsBatch(t, claim)
	legacy, err := NewEffectLedgerState(claim, batch.Effects, now)
	if err != nil {
		t.Fatal(err)
	}
	ledger := &memoryEffectLedger{state: legacy}
	handler := &staticCompleteRouteHandler{batch: batch}
	sink := &memoryEffectSink{}
	result, err := preparedGitHubExecutor(now.Add(time.Hour), handler, ledger, sink).
		Execute(context.Background(), session, preparedDeploymentsDescriptor(t))
	if err != nil {
		t.Fatalf("legacy-ledger recovery err=%v", err)
	}
	if handler.normalizedAt.IsZero() || ledger.preparedLoads != 0 || ledger.preparedPrepares != 0 ||
		result.Effects.Written != 1 || ledger.state.SchemaVersion == "v2" {
		t.Fatalf("handler_at=%s loads=%d prepares=%d result=%+v schema=%s, want a re-collect committed on the v1 ledger",
			handler.normalizedAt, ledger.preparedLoads, ledger.preparedPrepares, result.Effects, ledger.state.SchemaVersion)
	}
	if strings.Count(log.String(), "provider_sync.prepared_recovery_legacy_ledger") != 1 {
		t.Fatalf("want one prepared_recovery_legacy_ledger line, got: %s", log.String())
	}
	if !strings.Contains(log.String(), "recovery=legacy_ledger") {
		t.Fatalf("completion line lacks recovery=legacy_ledger: %s", log.String())
	}
}

func TestPreparedRouteNeverTakesTheLegacyPathForASnapshotLedger(t *testing.T) {
	log := captureSlog(t)
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedWorkItemsSession(t, now, "github", "deployments")
	batch := preparedDeploymentsBatch(t, claim)
	ledger := &memoryEffectLedger{}
	if _, err := ledger.PrepareRouteSnapshot(context.Background(), claim, batch, ShadowComparison{Match: true}, now); err != nil {
		t.Fatal(err)
	}
	handler := &staticCompleteRouteHandler{batch: CompleteRouteBatch{Result: map[string]any{"live_provider": "drifted"}}}
	sink := &memoryEffectSink{}
	result, err := completeRouteExecutor(now.Add(time.Hour), handler, ledger, sink).
		Execute(context.Background(), session, preparedDeploymentsDescriptor(t))
	if err != nil {
		t.Fatalf("snapshot recovery err=%v", err)
	}
	if !handler.normalizedAt.IsZero() || ledger.preparedLoads != 1 || ledger.preparedPrepares != 1 ||
		result.Effects.Written != 1 || strings.Contains(log.String(), "prepared_recovery_legacy_ledger") {
		t.Fatalf("handler_at=%s loads=%d prepares=%d result=%+v log=%s, want the snapshot replayed without re-collect",
			handler.normalizedAt, ledger.preparedLoads, ledger.preparedPrepares, result.Effects, log.String())
	}
	if !strings.Contains(log.String(), "recovery=snapshot_replay") {
		t.Fatalf("completion line lacks recovery=snapshot_replay: %s", log.String())
	}
}

// preparedPaddedBatch builds a route batch whose encoded snapshot is exactly
// target bytes: every padding byte is one ASCII byte of snapshot JSON, and
// the search re-measures because the payload_bytes digits move with the size.
func preparedPaddedBatch(t *testing.T, claim Claim, destinations []string, target int) (CompleteRouteBatch, int) {
	t.Helper()
	build := func(pad int) (CompleteRouteBatch, int) {
		effects := make([]EffectBatch, 0, len(destinations))
		for index, destination := range destinations {
			size := 0
			if index == 0 {
				size = pad
			}
			effect, err := effectBatchFromValues(destination, EffectReadbackRequired, []map[string]string{{preparedFixtureColumn(t, destination): strings.Repeat("a", size)}})
			if err != nil {
				t.Fatal(err)
			}
			effects = append(effects, effect)
		}
		batch := CompleteRouteBatch{
			Effects: effects, Result: map[string]any{"synced": 1}, Watermark: claim.BeforeAt,
			Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Records: len(effects)},
		}
		encoded, _, err := encodePreparedRouteManifest(claim, batch, ShadowComparison{Match: true}, claim.LeaseExpiresAt)
		if err != nil && !errors.Is(err, ErrPreparedRouteSnapshotOversize) {
			t.Fatal(err)
		}
		if err != nil {
			return batch, oversizeBytes(t, err)
		}
		return batch, len(encoded)
	}
	pad := 0
	for range 8 {
		_, size := build(pad)
		if size == target {
			break
		}
		pad += target - size
	}
	return build(pad)
}

// oversizeBytes reads the encoded size ErrPreparedRouteSnapshotOversize
// reports ("...: <n> bytes").
func oversizeBytes(t *testing.T, err error) int {
	t.Helper()
	message := strings.TrimSuffix(err.Error(), " bytes")
	size, parseErr := strconv.Atoi(message[strings.LastIndex(message, " ")+1:])
	if parseErr != nil {
		t.Fatalf("oversize error without a size: %v", err)
	}
	return size
}

func TestPreparedSnapshotSizeCapBoundary(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, _ := preparedWorkItemsSession(t, now, "github", "deployments")
	claim.LeaseExpiresAt = now
	fits, size := preparedPaddedBatch(t, claim, []string{"deployments"}, maxPreparedRouteSnapshotBytes)
	if size != maxPreparedRouteSnapshotBytes {
		t.Fatalf("fixture size=%d want exactly the cap %d", size, maxPreparedRouteSnapshotBytes)
	}
	if _, _, err := encodePreparedRouteManifest(claim, fits, ShadowComparison{Match: true}, now); err != nil {
		t.Fatalf("a snapshot of exactly the cap was refused: %v", err)
	}
	over, size := preparedPaddedBatch(t, claim, []string{"deployments"}, maxPreparedRouteSnapshotBytes+1)
	if size != maxPreparedRouteSnapshotBytes+1 {
		t.Fatalf("fixture size=%d want cap+1", size)
	}
	_, _, err := encodePreparedRouteManifest(claim, over, ShadowComparison{Match: true}, now)
	if !errors.Is(err, ErrPreparedRouteSnapshotOversize) || !errors.Is(err, ErrEffectRecoveryUnsafe) {
		t.Fatalf("cap+1 err=%v, want ErrPreparedRouteSnapshotOversize (an ErrEffectRecoveryUnsafe)", err)
	}

	// Two effects that each fit the per-effect cap but not one snapshot.
	prsClaim, _ := preparedWorkItemsSession(t, now, "github", "prs")
	half := maxEffectPayloadBytes/2 + 1024
	pulls, err := effectBatchFromValues("git_pull_requests", EffectReadbackRequired, []map[string]string{{preparedFixtureColumn(t, "git_pull_requests"): strings.Repeat("a", half)}})
	if err != nil {
		t.Fatalf("a half-cap effect was refused on its own: %v", err)
	}
	reviews, err := effectBatchFromValues("git_pull_request_reviews", EffectReadbackRequired, []map[string]string{{preparedFixtureColumn(t, "git_pull_request_reviews"): strings.Repeat("b", half)}})
	if err != nil {
		t.Fatalf("a half-cap effect was refused on its own: %v", err)
	}
	pair := CompleteRouteBatch{
		Effects: []EffectBatch{pulls, reviews}, Result: map[string]any{"synced": 2}, Watermark: prsClaim.BeforeAt,
		Evidence: FetchEvidence{Provider: "github", Dataset: "prs", Records: 2},
	}
	if _, _, err := encodePreparedRouteManifest(prsClaim, pair, ShadowComparison{Match: true}, now); !errors.Is(err, ErrPreparedRouteSnapshotOversize) {
		t.Fatalf("two effects over the cap together err=%v, want ErrPreparedRouteSnapshotOversize", err)
	}
}

func TestPreparedRouteCommitsAnOversizeBatchWithoutASnapshot(t *testing.T) {
	log := captureSlog(t)
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedWorkItemsSession(t, now, "github", "deployments")
	claim.LeaseExpiresAt = now
	over, _ := preparedPaddedBatch(t, claim, []string{"deployments"}, maxPreparedRouteSnapshotBytes+1)
	ledger := &memoryEffectLedger{}
	handler := &staticCompleteRouteHandler{batch: over}
	sink := &memoryEffectSink{}
	result, err := preparedGitHubExecutor(now, handler, ledger, sink).
		Execute(context.Background(), session, preparedDeploymentsDescriptor(t))
	if err != nil {
		t.Fatalf("oversize batch err=%v, want a commit without a snapshot", err)
	}
	if result.Effects.Written != 1 || ledger.preparedPrepares != 0 || ledger.state.SchemaVersion == "v2" ||
		ledger.state.PreparedSnapshot != nil {
		t.Fatalf("result=%+v prepares=%d schema=%s, want a v1 ledger commit", result.Effects, ledger.preparedPrepares, ledger.state.SchemaVersion)
	}
	if strings.Count(log.String(), "provider_sync.prepared_snapshot_oversize_fallback") != 1 {
		t.Fatalf("want one prepared_snapshot_oversize_fallback line, got: %s", log.String())
	}
}

// TestSnapshotLedgerRetriedWithoutPreparedRecoveryConflicts is the rollback
// cell: a binary that does not know the route recovers from a snapshot
// re-collects against the snapshot ledger, and the ledger refuses the retry.
// The unit exhausts its attempts; the next scheduled unit starts without a
// ledger.
func TestSnapshotLedgerRetriedWithoutPreparedRecoveryConflicts(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedWorkItemsSession(t, now, "github", "deployments")
	batch := preparedDeploymentsBatch(t, claim)
	ledger := &memoryEffectLedger{}
	if _, err := ledger.PrepareRouteSnapshot(context.Background(), claim, batch, ShadowComparison{Match: true}, now); err != nil {
		t.Fatal(err)
	}
	descriptor := preparedDeploymentsDescriptor(t)
	descriptor.PreparedManifestRecovery = false
	handler := &staticCompleteRouteHandler{batch: batch}
	_, err := preparedGitHubExecutor(now.Add(time.Hour), handler, ledger, &memoryEffectSink{}).
		Execute(context.Background(), session, descriptor)
	if !errors.Is(err, ErrEffectLedgerConflict) || handler.normalizedAt.IsZero() {
		t.Fatalf("err=%v handler_at=%s, want a re-collect refused with ErrEffectLedgerConflict", err, handler.normalizedAt)
	}
}

// TestWorkItemsStillRefusesAnOversizeSnapshot pins that the oversize fallback
// belongs to the routes enrolled after work-items: work-items keeps refusing a
// batch it cannot snapshot.
func TestWorkItemsStillRefusesAnOversizeSnapshot(t *testing.T) {
	log := captureSlog(t)
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, session := preparedGitHubWorkItemsSession(t, now)
	over := preparedGitHubWorkItemsFixture(t, claim)
	// Two effects padded to just over half the cap each: each still fits its
	// own effect cap, together they cannot be snapshotted.
	for index := range 2 {
		effect := over.Effects[index]
		pad, err := json.Marshal(map[string]string{
			preparedFixtureColumn(t, effect.Destination): strings.Repeat("a", maxPreparedRouteSnapshotBytes/2+1024),
		})
		if err != nil {
			t.Fatal(err)
		}
		rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, append(append([]json.RawMessage(nil), effect.Rows...), pad))
		if err != nil {
			t.Fatal(err)
		}
		over.Effects[index] = rebuilt
	}
	descriptor, _ := Descriptor("github", "work-items")
	ledger := &memoryEffectLedger{}
	sink := &memoryEffectSink{}
	_, err := preparedGitHubExecutor(now, &staticCompleteRouteHandler{batch: over}, ledger, sink).
		Execute(context.Background(), session, descriptor)
	if !errors.Is(err, ErrPreparedRouteSnapshotOversize) || len(sink.destinations) != 0 ||
		ledger.state.SchemaVersion != "" || strings.Contains(log.String(), "oversize_fallback") {
		t.Fatalf("err=%v writes=%v ledger=%q, want the oversize refusal with nothing written", err, sink.destinations, ledger.state.SchemaVersion)
	}
}

func TestPreparedManifestDiscardIsSafeOnlyForAnUntouchedEnrolledLedger(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, _ := preparedWorkItemsSession(t, now, "github", "deployments")
	state, err := NewEffectLedgerState(claim, preparedDeploymentsBatch(t, claim).Effects, now)
	if err != nil {
		t.Fatal(err)
	}
	if !isSafePreparedManifestReplanState(claim, state) {
		t.Fatal("an untouched deployments ledger was judged unsafe to discard")
	}
	committed := state
	committed.Effects = append([]EffectLedgerEntry(nil), state.Effects...)
	committed.Effects[0].Status = GenerationBlockCommitted
	committed.Effects[0].StartedAt, committed.Effects[0].CommittedAt = &now, &now
	if isSafePreparedManifestReplanState(claim, committed) {
		t.Fatal("a ledger with a committed effect was judged safe to discard")
	}
	blocked := state
	blocked.Effects = append([]EffectLedgerEntry(nil), state.Effects...)
	blocked.Effects[0].Recovery = EffectRecoveryBlocked
	if isSafePreparedManifestReplanState(claim, blocked) {
		t.Fatal("a ledger with a recovery-blocked effect was judged safe to discard")
	}
	unlisted := claim
	unlisted.Dataset = "cicd"
	unlistedState, err := NewEffectLedgerState(unlisted, preparedDeploymentsBatch(t, unlisted).Effects, now)
	if err != nil {
		t.Fatal(err)
	}
	if isSafePreparedManifestReplanState(unlisted, unlistedState) {
		t.Fatal("a route outside the prepared list was judged safe to discard")
	}
}

// TestEveryPlannableRouteStatesItsRecoveryMode pins how each plannable route
// recovers a crash between its sink write and its ledger commit. A
// "re-collect" route recovers only when the retry rebuilds byte-identical
// rows, so any provider-side change in that window conflicts at
// PrepareEffects; moving a route to "prepared snapshot" or adding a new route
// must change this table on purpose.
func TestEveryPlannableRouteStatesItsRecoveryMode(t *testing.T) {
	want := map[string]string{
		"github/blame":                   "re-collect",
		"github/cicd":                    "chunked checkpoints",
		"github/commit-stats":            "prepared snapshot",
		"github/commits":                 "prepared snapshot",
		"github/deployments":             "prepared snapshot",
		"github/files":                   "prepared snapshot",
		"github/prs":                     "prepared snapshot",
		"github/repo-metadata":           "prepared snapshot",
		"github/security":                "prepared snapshot",
		"github/work-items":              "prepared snapshot",
		"gitlab/blame":                   "re-collect",
		"gitlab/cicd":                    "chunked checkpoints",
		"gitlab/commit-stats":            "prepared snapshot",
		"gitlab/commits":                 "prepared snapshot",
		"gitlab/deployments":             "prepared snapshot",
		"gitlab/feature-flags":           "prepared snapshot",
		"gitlab/files":                   "prepared snapshot",
		"gitlab/incidents":               "prepared snapshot",
		"gitlab/prs":                     "prepared snapshot",
		"gitlab/repo-metadata":           "prepared snapshot",
		"gitlab/security":                "prepared snapshot",
		"gitlab/work-items":              "re-collect",
		"jira/incidents":                 "prepared snapshot",
		"jira/work-items":                "re-collect",
		"launchdarkly/feature-flags":     "prepared snapshot",
		"linear/work-items":              "re-collect",
		"pagerduty/business-services":    "re-collect",
		"pagerduty/escalation-policies":  "re-collect",
		"pagerduty/incident-alerts":      "re-collect",
		"pagerduty/incident-log-entries": "re-collect",
		"pagerduty/incident-notes":       "re-collect",
		"pagerduty/incidents":            "re-collect",
		"pagerduty/on-calls":             "re-collect",
		"pagerduty/schedules":            "re-collect",
		"pagerduty/services":             "re-collect",
		"pagerduty/teams":                "re-collect",
		"pagerduty/users":                "re-collect",
	}
	seen := map[string]bool{}
	for _, provider := range MatrixProviders() {
		for _, capability := range Capabilities(provider) {
			descriptor, ok := Descriptor(provider, capability.Dataset)
			if !ok || !descriptor.RouteReady || !descriptor.Plannable {
				continue
			}
			route := provider + "/" + capability.Dataset
			mode := "re-collect"
			switch {
			case descriptor.PreparedManifestRecovery:
				mode = "prepared snapshot"
			case descriptor.Chunked:
				mode = "chunked checkpoints"
			}
			stated, listed := want[route]
			if !listed {
				t.Errorf("%s recovers by %s and is not in this table", route, mode)
				continue
			}
			if stated != mode {
				t.Errorf("%s recovers by %s, table states %s", route, mode, stated)
			}
			seen[route] = true
		}
	}
	if len(want) == 0 {
		t.Fatal("the recovery-mode table is empty")
	}
	for route := range want {
		if !seen[route] {
			t.Errorf("%s is in this table but is not a plannable route", route)
		}
	}
}

// TestPreparedSnapshotReplayKeepsLargeIntegersExact pins that a replayed
// result keeps an integer past 2^53 (a GitLab project_id) byte for byte.
func TestPreparedSnapshotReplayKeepsLargeIntegersExact(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	claim, _ := preparedWorkItemsSession(t, now, "gitlab", "deployments")
	batch := preparedDeploymentsBatch(t, claim)
	batch.Result = map[string]any{"deployments_synced": 1, "project_id": int64(9007199254740993)}
	payload, reference, err := encodePreparedRouteManifest(claim, batch, ShadowComparison{Match: true}, now)
	if err != nil {
		t.Fatal(err)
	}
	state, err := NewEffectLedgerState(claim, batch.Effects, now)
	if err != nil {
		t.Fatal(err)
	}
	state.SchemaVersion, state.PreparedSnapshot = "v2", &reference
	manifest, err := decodePreparedRouteManifest(payload, claim, state)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(manifest.Batch.Result)
	if err != nil || !strings.Contains(string(encoded), `"project_id":9007199254740993`) {
		t.Fatalf("replayed result=%s err=%v, want project_id 9007199254740993 exactly", encoded, err)
	}
}

func TestPreparedSnapshotDiscardReasonsAreCountedByName(t *testing.T) {
	for _, reason := range []string{
		"manifest_mismatch", "manifest_mismatch_unreplayable", "manifest_mismatch_partially_committed",
		"manifest_mismatch_write_landed", "manifest_mismatch_readback_failed", "manifest_mismatch_readback_unavailable",
	} {
		if got := providerfoundation.MetricSnapshotDiscardReasonLabel(reason); got != reason {
			t.Fatalf("discard reason %q is counted as %q", reason, got)
		}
	}
}

// TestEnrolledRoutesReplayTheirSnapshotWithoutRecollecting runs each route
// enrolled in prepared-snapshot recovery through a crash after prepare: the
// retry replays the stored snapshot, never calls the route's collector, and
// writes every effect.
func TestEnrolledRoutesReplayTheirSnapshotWithoutRecollecting(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	routes := [][2]string{
		{"gitlab", "feature-flags"}, {"launchdarkly", "feature-flags"},
		{"gitlab", "incidents"}, {"jira", "incidents"},
	}
	for _, provider := range []string{"github", "gitlab"} {
		for _, dataset := range []string{"commit-stats", "commits", "files", "repo-metadata", "security"} {
			routes = append(routes, [2]string{provider, dataset})
		}
	}
	for _, route := range routes {
		provider, dataset := route[0], route[1]
		t.Run(provider+"/"+dataset, func(t *testing.T) {
			log := captureSlog(t)
			descriptor, ok := Descriptor(provider, dataset)
			if !ok || !descriptor.PreparedManifestRecovery {
				t.Fatalf("%s/%s is not enrolled", provider, dataset)
			}
			claim, session := preparedWorkItemsSession(t, now, provider, dataset)
			effects := make([]EffectBatch, 0, len(descriptor.Destinations))
			for _, destination := range descriptor.Destinations {
				effect, err := effectBatchFromValues(destination, EffectReadbackRequired,
					[]map[string]string{{"org_id": claim.OrgID, preparedFixtureColumn(t, destination): destination}})
				if err != nil {
					t.Fatal(err)
				}
				effects = append(effects, effect)
			}
			batch := CompleteRouteBatch{
				Effects: effects, Result: map[string]any{"synced": 1}, Watermark: claim.BeforeAt,
				Evidence: FetchEvidence{Provider: provider, Dataset: dataset, Records: len(effects)},
			}
			ledger := &memoryEffectLedger{}
			if _, err := ledger.PrepareRouteSnapshot(context.Background(), claim, batch, ShadowComparison{Match: true}, now); err != nil {
				t.Fatal(err)
			}
			handler := &staticCompleteRouteHandler{batch: CompleteRouteBatch{Result: map[string]any{"live_provider": "drifted"}}}
			sink := &memoryEffectSink{}
			executor := completeRouteExecutor(now.Add(time.Hour), handler, ledger, sink)
			executor.BudgetLimits = map[CostClass]int{claim.CostClass: 1}
			result, err := executor.Execute(context.Background(), session, descriptor)
			if err != nil {
				t.Fatalf("snapshot recovery err=%v", err)
			}
			if !handler.normalizedAt.IsZero() || ledger.preparedLoads != 1 ||
				result.Effects.Written != len(descriptor.Destinations) ||
				!strings.Contains(log.String(), "recovery=snapshot_replay") {
				t.Fatalf("handler_at=%s loads=%d result=%+v log=%s, want the snapshot replayed without re-collect",
					handler.normalizedAt, ledger.preparedLoads, result.Effects, log.String())
			}
		})
	}
}
