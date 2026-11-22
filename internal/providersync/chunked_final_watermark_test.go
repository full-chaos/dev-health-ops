package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// finalWatermarkRuleShape is ChunkFinalWatermarkRule spelled as a method set, so a cell asks a handler for its rule
// at run time.
type finalWatermarkRuleShape interface {
	FinalWatermark(Claim, PreparedProviderChunk) (*time.Time, error)
}

var errFinalWatermarkCrash = errors.New("final watermark cell: the process died here")

// finalWatermarkCrashStore fails one durable step once, leaving the ledger as a process that died there leaves it.
type finalWatermarkCrashStore struct {
	*chunkMemoryStore
	failMark, failInventory bool
}

func (store *finalWatermarkCrashStore) MarkChunkCommitted(ctx context.Context, claim Claim, ordinal int, digest string, now time.Time) error {
	if store.failMark {
		store.failMark = false
		return errFinalWatermarkCrash
	}
	return store.chunkMemoryStore.MarkChunkCommitted(ctx, claim, ordinal, digest, now)
}

func (store *finalWatermarkCrashStore) MarkInventoryComplete(ctx context.Context, claim Claim, now time.Time) error {
	if store.failInventory {
		store.failInventory = false
		return errFinalWatermarkCrash
	}
	return store.chunkMemoryStore.MarkInventoryComplete(ctx, claim, now)
}

func finalWatermarkSession(t *testing.T, now time.Time, provider, dataset string, noBeforeAt bool) (Claim, *LeaseSession) {
	t.Helper()
	unit := nativeTestClaim(provider, dataset).Unit
	if noBeforeAt {
		unit.BeforeAt = nil
	}
	leases := newMemoryLeaseRepository(unit, "dispatching")
	claim, err := leases.Claim(context.Background(), ClaimRequest{
		UnitID: unit.ID, Owner: uuid.NewString(), Now: now, LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return claim, &LeaseSession{
		Repository: leases, Claim: claim, LeaseDuration: time.Minute,
		Deadline: now.Add(time.Hour), Now: func() time.Time { return now },
	}
}

func finalWatermarkExecutor(now time.Time, claim Claim, handler CompleteRouteHandler, store EffectLedger) CompleteRouteExecutor {
	executor := completeRouteExecutor(now, handler, store, &recoveryRowSink{})
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: claim.Provider}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	executor.BudgetLimits = map[CostClass]int{claim.CostClass: 1}
	return executor
}

func finalWatermarkText(value *time.Time) string {
	if value == nil {
		return "nil"
	}
	return value.UTC().Format(time.RFC3339)
}

// finalOnlyRoute emits one final emission: batch, at cursor. It stands for a route of an OLDER binary whose rule
// differs from the running one.
type finalOnlyRoute struct {
	batch  CompleteRouteBatch
	cursor string
}

func (finalOnlyRoute) Collect(context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (route finalOnlyRoute) CollectChunks(
	_ context.Context, _ Claim, _ providerfoundation.Credential, _ *providerfoundation.HTTPClient,
	_ time.Time, _ string, emit func(ChunkRouteEmission) error,
) error {
	return emit(ChunkRouteEmission{Batch: route.batch, CursorBefore: route.cursor, CursorAfter: route.cursor, Final: true})
}

// countingChunkedRoute is a production route that counts its CollectChunks calls; it keeps the production rule.
type countingChunkedRoute struct {
	handler interface {
		CompleteRouteHandler
		ChunkedCompleteRouteHandler
	}
	calls *int
}

func (route countingChunkedRoute) Collect(ctx context.Context, claim Claim, credential providerfoundation.Credential, client *providerfoundation.HTTPClient, now time.Time) (CompleteRouteBatch, error) {
	return route.handler.Collect(ctx, claim, credential, client, now)
}

func (route countingChunkedRoute) CollectChunks(
	ctx context.Context, claim Claim, credential providerfoundation.Credential, client *providerfoundation.HTTPClient,
	normalizedAt time.Time, resume string, emit func(ChunkRouteEmission) error,
) error {
	*route.calls++
	return route.handler.CollectChunks(ctx, claim, credential, client, normalizedAt, resume, emit)
}

func (route countingChunkedRoute) FinalWatermark(claim Claim, final PreparedProviderChunk) (*time.Time, error) {
	rule, ok := any(route.handler).(finalWatermarkRuleShape)
	if !ok {
		return nil, errors.New("final watermark cell: the production route has no rule")
	}
	return rule.FinalWatermark(claim, final)
}

// recoverOldFinal leaves the ledger with the old route's final chunk in the given state (prepared: not marked
// committed; committed: inventory not marked; inventory-marked: the unit completion was lost), then recovers the unit
// with the current route. It returns the published watermark and the number of current-route calls.
func recoverOldFinal(
	t *testing.T, provider, state string, old func(Claim) finalOnlyRoute,
	current interface {
		CompleteRouteHandler
		ChunkedCompleteRouteHandler
	},
) (stored, published *time.Time, calls int) {
	t.Helper()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := finalWatermarkSession(t, now, provider, "cicd", false)
	descriptor, ok := Descriptor(provider, "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatal("descriptor")
	}
	store := &finalWatermarkCrashStore{chunkMemoryStore: newChunkMemoryStore()}
	store.failMark = state == "prepared"
	store.failInventory = state == "committed"
	_, oldErr := finalWatermarkExecutor(now, claim, old(claim), store).Execute(context.Background(), session, descriptor)
	if state == "inventory-marked" {
		if oldErr != nil {
			t.Fatalf("old walk: %v", oldErr)
		}
	} else if !errors.Is(oldErr, errFinalWatermarkCrash) {
		t.Fatalf("old attempt err=%v, want the simulated crash", oldErr)
	}
	checkpoint, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil {
		t.Fatal(err)
	}
	final, err := store.LoadPreparedChunk(context.Background(), claim, checkpoint.PreparedChunks-1, now)
	if err != nil || !final.InventoryComplete {
		t.Fatalf("premise: final=%+v err=%v", final, err)
	}
	wantNext := map[string]int{"prepared": 0, "committed": 1, "inventory-marked": 1}[state]
	if checkpoint.PreparedChunks != 1 || checkpoint.NextOrdinal != wantNext || checkpoint.InventoryComplete != (state == "inventory-marked") {
		t.Fatalf("premise: state %s checkpoint=%+v", state, checkpoint)
	}
	result, err := finalWatermarkExecutor(now, claim, countingChunkedRoute{handler: current, calls: &calls}, store).
		Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	return final.Watermark, result.Watermark, calls
}

func oldGitLabFinal(claim Claim) finalOnlyRoute {
	// The GitLab tests/cicd route before ticket 14383: a done cursor with no window start, watermark = BeforeAt.
	raw, err := encodeGitLabTestsChunkCursor(gitLabTestsChunkCursor{Phase: "done", Repo: "acme/api", ProjectID: 123})
	if err != nil {
		panic(err)
	}
	effects, err := testOpsEffects(nil, nil, nil, nil, nil, nil)
	if err != nil {
		panic(err)
	}
	return finalOnlyRoute{cursor: raw, batch: CompleteRouteBatch{
		Effects: effects, Result: map[string]any{"coverage_complete": true}, Watermark: claim.BeforeAt,
	}}
}

// A GitLab tests/cicd final chunk that an older binary prepared but did not publish carried that binary's watermark
// (BeforeAt for a cursor with no window start) past the current rule, which withholds it: the executor published the
// durable chunk without asking the route (CHAOS-14422). Each recovery state must publish the current rule's verdict and
// still ask the route nothing.
func TestChunkedRecoveredGitLabFinalPublishesTheCurrentRule(t *testing.T) {
	for _, state := range []string{"prepared", "committed", "inventory-marked"} {
		t.Run(state, func(t *testing.T) {
			stored, published, calls := recoverOldFinal(t, "gitlab", state, oldGitLabFinal, GitLabTestsRouteHandler{})
			if stored == nil {
				t.Fatal("premise: the old final stored no watermark")
			}
			if calls != 0 {
				t.Fatalf("recovery asked the route %d time(s)", calls)
			}
			if published != nil {
				t.Fatalf("recovery published the old watermark %s; the current GitLab rule withholds a cursor with no window start",
					finalWatermarkText(published))
			}
		})
	}
}

// The GitHub tests/cicd side of the same class: an older rule that advanced over evidence the current rule
// (githubTestsBlocksWatermark) finds window-blocking. The current rule must withhold on recovery.
func TestChunkedRecoveredGitHubFinalPublishesTheCurrentRule(t *testing.T) {
	old := func(claim Claim) finalOnlyRoute {
		batch, err := githubTestsFinalMetadataBatch(claim, githubTestsChunkCursor{
			Phase: "done", Repo: "acme/api",
			Incomplete: []GitHubTestsIncomplete{{Component: githubTestsRunInventoryComponent, Cause: githubTestsPageBudgetCause, Count: 1}},
		})
		if err != nil {
			panic(err)
		}
		if batch.Watermark != nil {
			panic("premise: the current rule must withhold this evidence")
		}
		batch.Watermark = claim.BeforeAt
		raw, err := encodeGitHubTestsChunkCursor(githubTestsChunkCursor{Phase: "done", Repo: "acme/api"})
		if err != nil {
			panic(err)
		}
		return finalOnlyRoute{batch: batch, cursor: raw}
	}
	for _, state := range []string{"prepared", "committed", "inventory-marked"} {
		t.Run(state, func(t *testing.T) {
			stored, published, calls := recoverOldFinal(t, "github", state, old, GitHubTestsRouteHandler{})
			if stored == nil || calls != 0 || published != nil {
				t.Fatalf("stored=%s calls=%d published=%s, want an old watermark, 0 calls, nil published",
					finalWatermarkText(stored), calls, finalWatermarkText(published))
			}
		})
	}
}

// ruleStubRoute emits one final emission with stored as its watermark and gives rule (or err) as its rule.
type ruleStubRoute struct {
	stored, rule *time.Time
	err          error
}

func (ruleStubRoute) Collect(context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (route ruleStubRoute) CollectChunks(
	_ context.Context, _ Claim, _ providerfoundation.Credential, _ *providerfoundation.HTTPClient,
	_ time.Time, _ string, emit func(ChunkRouteEmission) error,
) error {
	effects, err := testOpsEffects(nil, nil, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	return emit(ChunkRouteEmission{
		Batch:        CompleteRouteBatch{Effects: effects, Result: map[string]any{"complete": true}, Watermark: route.stored},
		CursorBefore: `{"done":true}`, CursorAfter: `{"done":true}`, Final: true,
	})
}

func (route ruleStubRoute) FinalWatermark(Claim, PreparedProviderChunk) (*time.Time, error) {
	return route.rule, route.err
}

// noRuleStubRoute is a chunked route without a rule: its stored watermark is published as is.
type noRuleStubRoute struct{ inner ruleStubRoute }

func (route noRuleStubRoute) Collect(ctx context.Context, claim Claim, credential providerfoundation.Credential, client *providerfoundation.HTTPClient, now time.Time) (CompleteRouteBatch, error) {
	return route.inner.Collect(ctx, claim, credential, client, now)
}

func (route noRuleStubRoute) CollectChunks(
	ctx context.Context, claim Claim, credential providerfoundation.Credential, client *providerfoundation.HTTPClient,
	normalizedAt time.Time, resume string, emit func(ChunkRouteEmission) error,
) error {
	return route.inner.CollectChunks(ctx, claim, credential, client, normalizedAt, resume, emit)
}

// The clamp: the rule can only withhold or lower the stored watermark. It never raises it and never publishes a
// watermark the stored chunk withheld. A rule error withholds with ONE WARN that carries a fixed reason and no error
// text, and the unit still completes; a lowered value logs ONE WARN with both timestamps.
func TestChunkedFinalWatermarkClampOnlyWithholdsOrLowers(t *testing.T) {
	stored := time.Date(2026, 7, 31, 23, 59, 59, 0, time.UTC)
	earlier := stored.Add(-time.Hour)
	later := stored.Add(time.Hour)
	const secret = "rule-failure-detail-7f3a"
	for _, row := range []struct {
		name       string
		route      CompleteRouteHandler
		want       *time.Time
		wantReason string
	}{
		{"rule earlier lowers", ruleStubRoute{stored: &stored, rule: &earlier}, &earlier, "route_rule_lowered"},
		{"rule equal keeps", ruleStubRoute{stored: &stored, rule: &stored}, &stored, ""},
		{"rule later never raises", ruleStubRoute{stored: &stored, rule: &later}, &stored, ""},
		{"rule nil withholds", ruleStubRoute{stored: &stored}, nil, "route_rule_lowered"},
		{"stored nil stays nil", ruleStubRoute{rule: &stored}, nil, ""},
		{"rule error withholds", ruleStubRoute{stored: &stored, rule: &stored, err: errors.New(secret)}, nil, "route_rule_failed"},
		{"no rule keeps stored", noRuleStubRoute{inner: ruleStubRoute{stored: &stored, rule: &earlier}}, &stored, ""},
	} {
		t.Run(row.name, func(t *testing.T) {
			var logs bytes.Buffer
			swapDefaultLogger(t, slog.New(slog.NewJSONHandler(&logs, nil)))
			now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
			claim, session := finalWatermarkSession(t, now, "gitlab", "cicd", false)
			descriptor, _ := Descriptor("gitlab", "cicd")
			result, err := finalWatermarkExecutor(now, claim, row.route, newChunkMemoryStore()).Execute(context.Background(), session, descriptor)
			if err != nil {
				t.Fatalf("the unit must complete: %v", err)
			}
			if finalWatermarkText(result.Watermark) != finalWatermarkText(row.want) {
				t.Fatalf("published %s, want %s", finalWatermarkText(result.Watermark), finalWatermarkText(row.want))
			}
			if strings.Contains(logs.String(), secret) {
				t.Fatalf("a log line carries the rule's error text: %s", logs.String())
			}
			var warns []map[string]any
			for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				var record map[string]any
				if line == "" || json.Unmarshal([]byte(line), &record) != nil {
					continue
				}
				if reason, _ := record["reason"].(string); strings.HasPrefix(reason, "route_rule_") {
					warns = append(warns, record)
				}
			}
			if row.wantReason == "" {
				if len(warns) != 0 {
					t.Fatalf("unexpected rule WARN: %v", warns)
				}
				return
			}
			if len(warns) != 1 || warns[0]["level"] != "WARN" || warns[0]["reason"] != row.wantReason ||
				warns[0]["provider"] != "gitlab" || warns[0]["dataset"] != "cicd" || warns[0]["unit"] != claim.ID {
				t.Fatalf("rule WARNs %v, want one WARN reason=%s with provider, dataset, unit", warns, row.wantReason)
			}
			wantKeys := map[string]bool{"time": true, "level": true, "msg": true, "provider": true, "dataset": true, "unit": true, "reason": true}
			if row.wantReason == "route_rule_lowered" {
				wantKeys["stored_watermark"], wantKeys["published_watermark"] = true, true
				published := "withheld"
				if row.want != nil {
					published = row.want.Format(time.RFC3339Nano)
				}
				if warns[0]["stored_watermark"] != stored.Format(time.RFC3339Nano) || warns[0]["published_watermark"] != published {
					t.Fatalf("lowered WARN timestamps %v", warns[0])
				}
			}
			for key := range warns[0] {
				if !wantKeys[key] {
					t.Fatalf("rule WARN carries key %q: %v", key, warns[0])
				}
			}
			if len(warns[0]) != len(wantKeys) {
				t.Fatalf("rule WARN keys %v, want %v", warns[0], wantKeys)
			}
		})
	}
}

// doneCursorRoute runs a production route from a done cursor, the route's own terminal path (no provider request), so
// a fresh walk emits the final metadata the production emitter builds for that cursor. It keeps the production rule.
type doneCursorRoute struct {
	countingChunkedRoute
	done    string
	emitted *[]*time.Time
}

func (route doneCursorRoute) CollectChunks(
	ctx context.Context, claim Claim, credential providerfoundation.Credential, client *providerfoundation.HTTPClient,
	normalizedAt time.Time, _ string, emit func(ChunkRouteEmission) error,
) error {
	return route.countingChunkedRoute.CollectChunks(ctx, claim, credential, client, normalizedAt, route.done,
		func(emission ChunkRouteEmission) error {
			if emission.Final {
				*route.emitted = append(*route.emitted, emission.Batch.Watermark)
			}
			return emit(emission)
		})
}

// The rule runs on EVERY publish, so a rule that disagrees with the emitter would withhold every fresh walk (the
// CHAOS-4142 stall class). For every watermark input each emitter knows, a fresh walk's published watermark and the
// rule applied to the stored final chunk must both equal the emitted watermark. The rule's own output is checked too:
// the clamp hides a rule that is too lenient, not one that is too strict.
func TestChunkedFinalWatermarkRuleMatchesTheEmitterOnFreshWalks(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	beforeAt := nativeTestClaim("gitlab", "cicd").BeforeAt.UTC()
	wsEarlier := beforeAt.Add(-time.Hour).Truncate(time.Second)
	wsLater := beforeAt.Add(time.Hour)
	reportMember := func(cause string, count int) []GitHubTestsIncomplete {
		return []GitHubTestsIncomplete{{Component: githubTestsReportMemberComponent, Cause: cause, Count: count}}
	}
	markers := func(count int) []GitHubTestsSkippedArtifact {
		out := make([]GitHubTestsSkippedArtifact, 0, count)
		for index := 0; index < count; index++ {
			out = append(out, GitHubTestsSkippedArtifact{
				RunID: "41", ArtifactID: string(rune('a' + index)), Cause: githubTestsArtifactOversizedCause, SizeBytes: 10, CapBytes: 5,
			})
		}
		return out
	}
	type row struct {
		name       string
		provider   string
		noBeforeAt bool
		gitlab     gitLabTestsChunkCursor
		github     githubTestsChunkCursor
		want       *time.Time
	}
	rows := []row{
		{name: "gitlab ws earlier than BeforeAt", provider: "gitlab", gitlab: gitLabTestsChunkCursor{WindowStart: wsEarlier.Unix()}, want: &wsEarlier},
		{name: "gitlab ws later than BeforeAt", provider: "gitlab", gitlab: gitLabTestsChunkCursor{WindowStart: wsLater.Unix()}, want: &beforeAt},
		{name: "gitlab ws unknown", provider: "gitlab", gitlab: gitLabTestsChunkCursor{}, want: nil},
		{name: "gitlab window-blocking truncation", provider: "gitlab",
			gitlab: gitLabTestsChunkCursor{WindowStart: wsEarlier.Unix(), Truncated: []string{gitLabPipelineInventoryComponent}}, want: nil},
		{name: "gitlab advancing truncation", provider: "gitlab",
			gitlab: gitLabTestsChunkCursor{WindowStart: wsEarlier.Unix(), Truncated: []string{gitLabRunJobsComponent}}, want: &wsEarlier},
		{name: "gitlab BeforeAt nil", provider: "gitlab", noBeforeAt: true, gitlab: gitLabTestsChunkCursor{WindowStart: wsEarlier.Unix()}, want: nil},
		{name: "github clean", provider: "github", want: &beforeAt},
		{name: "github incomplete advancing", provider: "github",
			github: githubTestsChunkCursor{Incomplete: []GitHubTestsIncomplete{{Component: githubTestsRunJobsComponent, Cause: githubTestsPerRunCapCause, Count: 1}}}, want: &beforeAt},
		{name: "github incomplete blocking", provider: "github",
			github: githubTestsChunkCursor{Incomplete: []GitHubTestsIncomplete{{Component: githubTestsRunInventoryComponent, Cause: githubTestsPageBudgetCause, Count: 1}}}, want: nil},
		{name: "github saturated listing", provider: "github", github: githubTestsChunkCursor{SaturatedRuns: true}, want: nil},
		{name: "github skipped artifacts cover", provider: "github",
			github: githubTestsChunkCursor{Incomplete: reportMember(githubTestsArtifactOversizedCause, 2), SkippedArtifacts: markers(2)}, want: &beforeAt},
		{name: "github skipped artifacts short", provider: "github",
			github: githubTestsChunkCursor{Incomplete: reportMember(githubTestsArtifactOversizedCause, 2), SkippedArtifacts: markers(1)}, want: nil},
		{name: "github overflow covers (legacy cause overflow)", provider: "github",
			github: githubTestsChunkCursor{Incomplete: reportMember(githubTestsArtifactOversizedCause, 3), SkippedArtifacts: markers(1), SkippedArtifactsOverflow: 2}, want: &beforeAt},
		{name: "github cause count covers", provider: "github",
			github: githubTestsChunkCursor{Incomplete: reportMember(githubTestsArtifactOversizedCause, 3), SkippedArtifactCauseCount: map[string]int{githubTestsArtifactOversizedCause: 3}}, want: &beforeAt},
		{name: "github cause count short", provider: "github",
			github: githubTestsChunkCursor{Incomplete: reportMember(githubTestsArtifactOversizedCause, 3), SkippedArtifactCauseCount: map[string]int{githubTestsArtifactOversizedCause: 2}}, want: nil},
		{name: "github BeforeAt nil", provider: "github", noBeforeAt: true, want: nil},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			claim, session := finalWatermarkSession(t, now, row.provider, "cicd", row.noBeforeAt)
			descriptor, _ := Descriptor(row.provider, "cicd")
			if row.noBeforeAt && row.want != nil {
				t.Fatal("bad row")
			}
			var done string
			var production interface {
				CompleteRouteHandler
				ChunkedCompleteRouteHandler
			}
			var err error
			if row.provider == "gitlab" {
				cursor := row.gitlab
				cursor.Phase, cursor.Repo, cursor.ProjectID = "done", "acme/api", 123
				done, err = encodeGitLabTestsChunkCursor(cursor)
				production = GitLabTestsRouteHandler{}
			} else {
				cursor := row.github
				cursor.Phase, cursor.Repo = "done", "acme/api"
				done, err = encodeGitHubTestsChunkCursor(cursor)
				production = GitHubTestsRouteHandler{}
			}
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			var emitted []*time.Time
			store := newChunkMemoryStore()
			executor := finalWatermarkExecutor(now, claim, doneCursorRoute{
				countingChunkedRoute: countingChunkedRoute{handler: production, calls: &calls}, done: done, emitted: &emitted,
			}, store)
			executor.Comparator = ProductionContractComparator{}
			result, err := executor.Execute(context.Background(), session, descriptor)
			if err != nil {
				t.Fatalf("fresh walk: %v", err)
			}
			if len(emitted) != 1 || finalWatermarkText(emitted[0]) != finalWatermarkText(row.want) {
				t.Fatalf("premise: the emitter gave %v, want %s", emitted, finalWatermarkText(row.want))
			}
			if finalWatermarkText(result.Watermark) != finalWatermarkText(emitted[0]) {
				t.Fatalf("published %s, emitted %s", finalWatermarkText(result.Watermark), finalWatermarkText(emitted[0]))
			}
			rule, ok := any(production).(finalWatermarkRuleShape)
			if !ok {
				t.Fatalf("%T has no final watermark rule", production)
			}
			checkpoint, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
			if err != nil {
				t.Fatal(err)
			}
			final, err := store.LoadPreparedChunk(context.Background(), claim, checkpoint.TotalChunks-1, now)
			if err != nil || !final.InventoryComplete {
				t.Fatalf("stored final=%+v err=%v", final, err)
			}
			derived, err := rule.FinalWatermark(claim, final)
			if err != nil || finalWatermarkText(derived) != finalWatermarkText(emitted[0]) {
				t.Fatalf("rule gave %s (err=%v) on the stored final, emitted %s",
					finalWatermarkText(derived), err, finalWatermarkText(emitted[0]))
			}
		})
	}
}

// Every chunked route the git family serves has a final watermark rule, so no chunked route publishes an older
// binary's watermark unchecked.
func TestEveryChunkedRouteHasAFinalWatermarkRule(t *testing.T) {
	found := 0
	for _, provider := range MatrixProviders() {
		for _, capability := range Capabilities(provider) {
			descriptor, ok := Descriptor(provider, capability.Dataset)
			if !ok || !descriptor.Chunked {
				continue
			}
			found++
			route, ok := SelectGitFamilyRoute(provider, capability.Dataset, GitFamilyDeps{})
			if !ok {
				t.Fatalf("%s/%s is chunked but not a git-family route", provider, capability.Dataset)
			}
			if _, ok := route.Handler.(finalWatermarkRuleShape); !ok {
				t.Errorf("%s/%s handler %T has no final watermark rule", provider, capability.Dataset, route.Handler)
			}
		}
	}
	if found != 5 {
		t.Fatalf("chunked routes found=%d, want 5 (github files, cicd, tests; gitlab cicd, tests)", found)
	}
}
