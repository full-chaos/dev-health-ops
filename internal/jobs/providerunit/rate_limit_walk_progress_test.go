package providerunit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-14252. A rate-limit episode (10 deferrals or 2 h) used to span the
// whole life of a unit, so a long chunked walk under a tight token budget
// failed with the rate_limit category although every attempt committed
// chunks. The episode now restarts when the unit's chunk checkpoint moved
// since the last rate-limit deferral; a walk that stopped moving still fails.

type rateLimitWalk struct {
	repository *memoryUnitRepository
	metrics    *providerfoundation.Metrics
	logs       *bytes.Buffer
	attempts   int
	err        error
}

// driveRateLimitedWalk runs Handler.Work until the unit stops snoozing or
// steps run out. progress moves the checkpoint for an attempt (nil = the unit
// has no checkpoint at all); every attempt ends on a provider rate limit.
func driveRateLimitedWalk(
	t *testing.T, retryAfter time.Duration, steps int,
	progress func(attempt int) *providersync.RateLimitProgressMark,
	seed func(*memoryUnitRepository, time.Time),
) rateLimitWalk {
	t.Helper()
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	repository := newMemoryUnitRepository(unit)
	if seed != nil {
		seed(repository, now)
	}
	var live *providersync.RateLimitProgressMark
	repository.checkpointProgress = func() *providersync.RateLimitProgressMark { return live }
	walk := rateLimitWalk{repository: repository, metrics: providerfoundation.NewMetrics(), logs: &bytes.Buffer{}}
	handler := &Handler{
		Repository:      repository,
		ProviderMetrics: walk.metrics,
		LeaseDuration:   time.Minute,
		Heartbeat:       10 * time.Second,
		Now:             func() time.Time { return now },
		BuildExecutor: func(*providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			walk.attempts++
			live = progress(walk.attempts)
			return providersync.CompleteRouteExecutor{}, rateLimitError(retryAfter)
		},
	}
	for step := 0; step < steps; step++ {
		execution := providerExecution(unit, now, 5)
		execution.Definition.MaxAttempts = 5
		execution.Logger = slog.New(slog.NewJSONHandler(walk.logs, nil))
		err := handler.Work(context.Background(), execution)
		if _, snoozed := jobruntime.SnoozeDelay(err); !snoozed {
			walk.err = err
			return walk
		}
		now = repository.availableAt
	}
	return walk
}

func onePerAttempt(attempt int) *providersync.RateLimitProgressMark {
	return &providersync.RateLimitProgressMark{PreparedChunks: int64(attempt), NextOrdinal: int64(attempt)}
}

func standingStill(int) *providersync.RateLimitProgressMark {
	return &providersync.RateLimitProgressMark{PreparedChunks: 7, NextOrdinal: 7}
}

func noCheckpoint(int) *providersync.RateLimitProgressMark { return nil }

func (walk rateLimitWalk) mustStillDefer(t *testing.T) {
	t.Helper()
	if walk.err != nil || walk.repository.failures != 0 || walk.repository.status != "dispatching" {
		t.Fatalf("walk failed after %d progressing attempts: status=%q category=%q deferrals=%d err=%v",
			walk.attempts, walk.repository.status, walk.repository.lastFailCategory, walk.repository.rateLimitDeferrals, walk.err)
	}
}

func (walk rateLimitWalk) mustFailRateLimit(t *testing.T, attempts int) {
	t.Helper()
	if walk.repository.status != "failed" || walk.repository.lastFailCategory != RateLimitCategory || walk.attempts != attempts {
		t.Fatalf("status=%q category=%q attempts=%d (want failed %q at attempt %d) err=%v",
			walk.repository.status, walk.repository.lastFailCategory, walk.attempts, RateLimitCategory, attempts, walk.err)
	}
	if walk.repository.rateLimitEpisodeRestarts != 0 {
		t.Fatalf("a walk without progress restarted its episode %d times", walk.repository.rateLimitEpisodeRestarts)
	}
}

// T1: GitHub secondary limits answer with a short Retry-After, so a
// progressing walk used to spend 10 deferrals in minutes.
func TestRateLimitEpisodeCountCapDoesNotFailAProgressingChunkedWalk(t *testing.T) {
	t.Parallel()
	walk := driveRateLimitedWalk(t, time.Minute, 25, onePerAttempt, nil)
	walk.mustStillDefer(t)
	// The first deferral writes the first mark; every later one saw progress.
	if walk.repository.rateLimitEpisodeRestarts != 24 || walk.repository.rateLimitDeferrals != 1 {
		t.Fatalf("restarts=%d deferrals=%d, want 24 restarts and a fresh episode",
			walk.repository.rateLimitEpisodeRestarts, walk.repository.rateLimitDeferrals)
	}
}

// T2: the primary limit resets hourly, so a progressing walk that needed a
// third token window passed 2 h from its first deferral.
func TestRateLimitEpisodeWallCapDoesNotFailAProgressingChunkedWalk(t *testing.T) {
	t.Parallel()
	walk := driveRateLimitedWalk(t, time.Hour, 6, onePerAttempt, nil)
	walk.mustStillDefer(t)
}

// T3: a walk whose checkpoint does not move keeps one episode and fails at
// the count cap. An equal mark is not progress (kills `>` -> `>=`).
func TestRateLimitEpisodeCountCapStillFailsAWalkWithoutProgress(t *testing.T) {
	t.Parallel()
	walk := driveRateLimitedWalk(t, time.Minute, 25, standingStill, nil)
	walk.mustFailRateLimit(t, rateLimitMaxDeferrals+1)
}

func TestRateLimitEpisodeWallCapStillFailsAWalkWithoutProgress(t *testing.T) {
	t.Parallel()
	walk := driveRateLimitedWalk(t, time.Hour, 6, standingStill, nil)
	walk.mustFailRateLimit(t, 3)
}

// A unit with no checkpoint row (every non-chunked route) keeps today's caps.
func TestRateLimitEpisodeWithoutACheckpointKeepsTheCaps(t *testing.T) {
	t.Parallel()
	walk := driveRateLimitedWalk(t, time.Minute, 25, noCheckpoint, nil)
	walk.mustFailRateLimit(t, rateLimitMaxDeferrals+1)
	if walk.repository.rateLimitProgressMark != nil {
		t.Fatalf("a unit without a checkpoint stored mark %+v", *walk.repository.rateLimitProgressMark)
	}
}

// Ruling item 4: a checkpoint that starts again lower (a new generation, or a
// delete and fresh insert) is not progress against the old mark: no restart,
// the episode keeps counting, and the lower mark is stored.
func TestRateLimitEpisodeLowerCheckpointIsNotProgress(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		live providersync.RateLimitProgressMark
	}{
		{"both lower", providersync.RateLimitProgressMark{PreparedChunks: 2, NextOrdinal: 2}},
		{"prepared higher, committed lower", providersync.RateLimitProgressMark{PreparedChunks: 9, NextOrdinal: 2}},
		{"prepared lower, committed higher", providersync.RateLimitProgressMark{PreparedChunks: 2, NextOrdinal: 9}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			live := test.live
			walk := driveRateLimitedWalk(t, time.Minute, 1,
				func(int) *providersync.RateLimitProgressMark { return &live },
				func(repository *memoryUnitRepository, now time.Time) {
					first := now.Add(-time.Hour)
					repository.rateLimitDeferrals, repository.rateLimitFirstSeen = 4, &first
					repository.rateLimitProgressMark = &providersync.RateLimitProgressMark{PreparedChunks: 5, NextOrdinal: 5}
				})
			walk.mustStillDefer(t)
			if walk.repository.rateLimitEpisodeRestarts != 0 || walk.repository.rateLimitDeferrals != 5 {
				t.Fatalf("restarts=%d deferrals=%d, want no restart and 5", walk.repository.rateLimitEpisodeRestarts, walk.repository.rateLimitDeferrals)
			}
			if mark := walk.repository.rateLimitProgressMark; mark == nil || *mark != live {
				t.Fatalf("stored mark=%v, want the live checkpoint %+v", mark, live)
			}
		})
	}
}

// Ruling item 5: a row deferred before the mark existed carries no mark. Its
// first deferral writes one and counts on; only the next can restart.
func TestRateLimitEpisodePreDeployRowWritesTheMarkWithoutARestart(t *testing.T) {
	t.Parallel()
	walk := driveRateLimitedWalk(t, time.Minute, 2, onePerAttempt,
		func(repository *memoryUnitRepository, now time.Time) {
			first := now.Add(-30 * time.Minute)
			repository.rateLimitDeferrals, repository.rateLimitFirstSeen = 3, &first
		})
	walk.mustStillDefer(t)
	if walk.repository.rateLimitEpisodeRestarts != 1 || walk.repository.rateLimitDeferrals != 1 {
		t.Fatalf("restarts=%d deferrals=%d, want the first deferral to count (4) and the second to restart (1)",
			walk.repository.rateLimitEpisodeRestarts, walk.repository.rateLimitDeferrals)
	}
	if mark := walk.repository.rateLimitProgressMark; mark == nil || *mark != *onePerAttempt(2) {
		t.Fatalf("stored mark=%v, want %+v", mark, *onePerAttempt(2))
	}

	first := driveRateLimitedWalk(t, time.Minute, 1, onePerAttempt,
		func(repository *memoryUnitRepository, now time.Time) {
			started := now.Add(-30 * time.Minute)
			repository.rateLimitDeferrals, repository.rateLimitFirstSeen = 3, &started
		})
	if first.repository.rateLimitEpisodeRestarts != 0 || first.repository.rateLimitDeferrals != 4 ||
		!first.repository.rateLimitFirstSeen.Equal(time.Date(2026, 11, 19, 11, 30, 0, 0, time.UTC)) {
		t.Fatalf("first deferral of a pre-deploy row: restarts=%d deferrals=%d first_seen=%v",
			first.repository.rateLimitEpisodeRestarts, first.repository.rateLimitDeferrals, first.repository.rateLimitFirstSeen)
	}
}

// Ruling item 3: each restart is loud, one INFO line with the unit and both
// marks, and one counter increment.
func TestRateLimitEpisodeRestartLogsAndCounts(t *testing.T) {
	t.Parallel()
	walk := driveRateLimitedWalk(t, time.Minute, 3, onePerAttempt, nil)
	walk.mustStillDefer(t)
	var lines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(walk.logs.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "sync_provider_unit_rate_limit_episode_restarted" {
			lines = append(lines, record)
		}
	}
	if len(lines) != 2 {
		t.Fatalf("restart lines=%d, want 2:\n%s", len(lines), walk.logs.String())
	}
	last := lines[1]
	if last["level"] != "INFO" || last["sync_unit_id"] != walk.repository.unit.ID || last["prior_deferrals"] != float64(1) ||
		last["old_prepared_chunks"] != float64(2) || last["old_next_ordinal"] != float64(2) ||
		last["new_prepared_chunks"] != float64(3) || last["new_next_ordinal"] != float64(3) {
		t.Fatalf("restart line=%v", last)
	}
	var output strings.Builder
	if err := walk.metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	want := `dev_health_provider_rate_limit_episode_restart_total{provider="` + walk.repository.unit.Provider +
		`",dataset="` + walk.repository.unit.Dataset + `"} 2`
	if !strings.Contains(output.String(), want) {
		t.Fatalf("missing %q in:\n%s", want, output.String())
	}
}

func TestRateLimitProgressMarkComparesThePair(t *testing.T) {
	t.Parallel()
	mark := func(prepared, next int64) *providersync.RateLimitProgressMark {
		return &providersync.RateLimitProgressMark{PreparedChunks: prepared, NextOrdinal: next}
	}
	for _, test := range []struct {
		name             string
		checkpoint, last *providersync.RateLimitProgressMark
		want             bool
	}{
		{"equal", mark(5, 5), mark(5, 5), false},
		{"prepared up", mark(6, 5), mark(5, 5), true},
		{"committed up", mark(5, 6), mark(5, 5), true},
		{"both up", mark(6, 6), mark(5, 5), true},
		{"prepared up committed down", mark(9, 4), mark(5, 5), false},
		{"committed up prepared down", mark(4, 9), mark(5, 5), false},
		{"both down", mark(1, 1), mark(5, 5), false},
		{"no checkpoint", nil, mark(5, 5), false},
		{"no mark", mark(6, 6), nil, false},
		{"neither", nil, nil, false},
	} {
		episode := providersync.RateLimitEpisode{Checkpoint: test.checkpoint, Mark: test.last}
		if got := episode.Progressed(); got != test.want {
			t.Errorf("%s: Progressed()=%v, want %v", test.name, got, test.want)
		}
	}
}

// --- Done-when 2: a real chunked walk through repeated rate limits. ---

// pagedWalkHandler is a chunked route of `pages` provider pages, one chunk per
// page. Its cursor is the next page, so a resumed attempt asks the provider
// only for pages the checkpoint has not committed.
type pagedWalkHandler struct {
	pages        int
	destinations []string
}

func (pagedWalkHandler) Collect(
	context.Context, providersync.Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (providersync.CompleteRouteBatch, error) {
	return providersync.CompleteRouteBatch{}, errors.New("the paged walk is chunked only")
}

func (handler pagedWalkHandler) CollectChunks(
	ctx context.Context, claim providersync.Claim, _ providerfoundation.Credential,
	client *providerfoundation.HTTPClient, _ time.Time, resumeCursor string,
	emit func(providersync.ChunkRouteEmission) error,
) error {
	page := 0
	if resumeCursor != "" {
		parsed, err := strconv.Atoi(strings.TrimPrefix(resumeCursor, "page:"))
		if err != nil {
			return err
		}
		page = parsed
	}
	for ; page < handler.pages; page++ {
		response, err := client.Do(ctx, http.MethodGet, "/walk/"+strconv.Itoa(page), nil)
		if err != nil {
			return err
		}
		_ = response.Body.Close()
		effects := make([]providersync.EffectBatch, 0, len(handler.destinations))
		for _, destination := range handler.destinations {
			effect, err := providersync.BuildEffectBatch(destination, providersync.EffectReplaySafe, []json.RawMessage{
				json.RawMessage(fmt.Sprintf(`{"org_id":%q,"page":%d}`, claim.OrgID, page)),
			})
			if err != nil {
				return err
			}
			effects = append(effects, effect)
		}
		final := page == handler.pages-1
		batch := providersync.CompleteRouteBatch{Effects: effects}
		if final {
			batch.Result = map[string]any{"pages": handler.pages}
		}
		if err := emit(providersync.ChunkRouteEmission{
			Batch: batch, Final: final,
			CursorBefore: "page:" + strconv.Itoa(page), CursorAfter: "page:" + strconv.Itoa(page+1),
		}); err != nil {
			return err
		}
	}
	return nil
}

type matchingComparator struct{}

func (matchingComparator) CompareCompleteRoute(
	context.Context, providersync.Claim, providersync.CompleteRouteBatch,
) (providersync.ShadowComparison, error) {
	return providersync.ShadowComparison{Match: true}, nil
}

// budgetedWalkDoer answers every third request with a GitHub primary-limit 429
// and counts the pages it served.
type budgetedWalkDoer struct {
	mu       sync.Mutex
	requests int
	served   map[string]int
}

func (doer *budgetedWalkDoer) Do(request *http.Request) (*http.Response, error) {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	if request.URL.Path == "/repos/acme/api" {
		return jsonResponse(request, http.StatusOK, strings.NewReader(`{"full_name":"acme/api","default_branch":"main"}`)), nil
	}
	doer.requests++
	if doer.requests%3 == 0 {
		response := jsonResponse(request, http.StatusTooManyRequests, strings.NewReader(`{"message":"API rate limit exceeded"}`))
		response.Header.Set("Retry-After", "3600")
		response.Header.Set("X-RateLimit-Remaining", "0")
		return response, nil
	}
	doer.served[request.URL.Path]++
	return jsonResponse(request, http.StatusOK, strings.NewReader(`{}`)), nil
}

// fixedOwnerRepository claims with one owner so the in-process chunk ledger,
// which fences on the owner as Postgres fences on the lease, accepts every
// attempt of the same unit.
type fixedOwnerRepository struct{ *memoryUnitRepository }

const fixedWalkOwner = "00000000-0000-4000-8000-000000014252"

func (repository fixedOwnerRepository) Claim(
	ctx context.Context, request providersync.ClaimRequest,
) (providersync.Claim, error) {
	request.Owner = fixedWalkOwner
	return repository.memoryUnitRepository.Claim(ctx, request)
}

func TestChunkedWalkResumesThroughRepeatedRateLimitsAndFinishes(t *testing.T) {
	t.Parallel()
	const pages = 30
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	unit := providerUnit()
	capability, ok := providersync.Capability("github", "cicd")
	if !ok {
		t.Fatal("github/cicd capability missing")
	}
	descriptor, ok := providersync.Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked || !descriptor.Plannable {
		t.Fatalf("github/cicd descriptor chunked=%v plannable=%v present=%v", descriptor.Chunked, descriptor.Plannable, ok)
	}
	unit.SourceExternalID, unit.SourceName = "acme/api", "acme/api"
	// "tests" is an alias of the plannable cicd unit, which carries the GitHub TestOps walk.
	unit.Provider, unit.Dataset, unit.CostClass = "github", "cicd", capability.CostClass
	memory := newMemoryUnitRepository(unit)
	repository := fixedOwnerRepository{memory}
	doer := &budgetedWalkDoer{served: map[string]int{}}
	var ledger *providersync.InProcessChunkLedger
	var ledgerClaim providersync.Claim
	memory.checkpointProgress = func() *providersync.RateLimitProgressMark {
		if ledger == nil {
			return nil
		}
		checkpoint, err := ledger.LoadChunkCheckpoint(context.Background(), ledgerClaim, now)
		if err != nil {
			return nil
		}
		return &providersync.RateLimitProgressMark{
			PreparedChunks: int64(checkpoint.PreparedChunks), NextOrdinal: int64(checkpoint.NextOrdinal),
		}
	}
	metrics := providerfoundation.NewMetrics()
	handler := &Handler{
		Repository:      repository,
		ProviderMetrics: metrics,
		LeaseDuration:   time.Minute,
		Heartbeat:       10 * time.Second,
		Now:             func() time.Time { return now },
		BuildExecutor: func(session *providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			if ledger == nil {
				ledger, ledgerClaim = providersync.NewInProcessChunkLedger(session.Claim), session.Claim
			}
			return providersync.CompleteRouteExecutor{
				Credentials: providerfoundation.CredentialResolver{
					Repository: githubCredentialRepository{unit: session.Claim.Unit},
					Decryptor:  githubCredentialDecryptor{},
				},
				Doer:         fakehttp.Client(doer),
				Retry:        providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
				Budget:       testBudgetStore{},
				BudgetLimits: map[providersync.CostClass]int{capability.CostClass: 1},
				BudgetTTL:    time.Minute,
				Gate: func(providersync.Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
					return testBackoffGate{}
				},
				Handler:           pagedWalkHandler{pages: pages, destinations: descriptor.Destinations},
				Comparator:        matchingComparator{},
				Committer:         providersync.EffectCommitter{Ledger: ledger, Sink: testEffectSink{}, Readback: testEffectReadback{}, Now: func() time.Time { return now }},
				HeartbeatInterval: 10 * time.Second,
				Now:               func() time.Time { return now },
			}, nil
		},
	}
	started := now
	attempts, deferrals := 0, 0
	for memory.status != "success" {
		attempts++
		if attempts > 3*pages {
			t.Fatalf("the walk did not finish in %d attempts", attempts)
		}
		execution := providerExecution(unit, now, 5)
		execution.Definition.MaxAttempts = 5
		err := handler.Work(context.Background(), execution)
		if err == nil {
			break
		}
		if _, snoozed := jobruntime.SnoozeDelay(err); !snoozed {
			t.Fatalf("attempt %d: status=%q category=%q deferrals=%d err=%v",
				attempts, memory.status, memory.lastFailCategory, memory.rateLimitDeferrals, err)
		}
		deferrals++
		now = memory.availableAt
	}
	if memory.status != "success" {
		t.Fatalf("status=%q after %d attempts", memory.status, attempts)
	}
	if deferrals <= rateLimitMaxDeferrals || now.Sub(started) < rateLimitMaxTotalWait {
		t.Fatalf("deferrals=%d over %s: the walk must cross both old caps", deferrals, now.Sub(started))
	}
	if len(memory.rateLimitObservations) != deferrals {
		t.Fatalf("rate-limit deferrals=%d, snoozes=%d: every snooze must be a rate-limit deferral",
			len(memory.rateLimitObservations), deferrals)
	}
	for page := 0; page < pages; page++ {
		if served := doer.served["/walk/"+strconv.Itoa(page)]; served != 1 {
			t.Fatalf("page %d served %d times: a resumed attempt must start at the checkpoint", page, served)
		}
	}
	checkpoint, err := ledger.LoadChunkCheckpoint(context.Background(), ledgerClaim, now)
	if err != nil {
		t.Fatal(err)
	}
	if !checkpoint.InventoryComplete || checkpoint.TotalChunks != pages || checkpoint.NextOrdinal != pages {
		t.Fatalf("checkpoint=%+v, want %d committed chunks", checkpoint, pages)
	}
	if memory.rateLimitEpisodeRestarts == 0 {
		t.Fatal("no episode restart recorded")
	}
}
