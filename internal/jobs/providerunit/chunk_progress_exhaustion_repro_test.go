package providerunit

import (
	"bytes"
	"context"
	"encoding/json"
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

// Repro: a long chunked github cicd walk whose attempts each commit chunks
// still ends provider_unit_exhausted / provider_transient on the 5th ordinary
// failure, however far its checkpoint advanced between failures. The rate-limit
// arm restarts its episode on checkpoint progress (CHAOS-14252); the ordinary
// retry arm has no such reset (providerunit.go retry ladder).
//
// What this harness fakes: Handler.Work, the chunked streaming executor, the
// effect committer and the in-process chunk ledger are real; the HTTP client is
// real over a fake Doer. River is NOT run: the attempt number is a counter that
// follows River's rule (a snooze keeps it, an ordinary retryable return adds
// one). The repository is the in-memory fake; its attempt field models the
// sync_run_units.attempts column by the SQL (claim +1, continuation and
// progress refund -1 clamped at 0, release-for-retry unchanged:
// repository_postgres.go claimUnitSQL, deferForChunkContinuationSQL,
// releaseForRetrySQL), and its progress refund reads the in-process ledger's
// checkpoint, the durable checkpoint of this harness.

// exhaustReproRepository wraps the class repository with a fixed claim owner
// (the in-process ledger fences on it) and counts releases.
type exhaustReproRepository struct {
	*classRepository
	releases int
}

func (repository *exhaustReproRepository) Claim(ctx context.Context, request providersync.ClaimRequest) (providersync.Claim, error) {
	request.Owner = fixedWalkOwner
	return repository.classRepository.Claim(ctx, request)
}

func (repository *exhaustReproRepository) ReleaseForRetry(ctx context.Context, claim providersync.Claim, now time.Time) error {
	err := repository.classRepository.ReleaseForRetry(ctx, claim, now)
	if err == nil {
		repository.releases++
	}
	return err
}

// flakyPageDoer answers a chosen provider page with 503 for the next `budget`
// requests of that page (budget = the client's in-attempt tries, so one unit
// attempt fails provider_transient), then serves it. Every other request is 200.
type flakyPageDoer struct {
	mu     sync.Mutex
	budget int
	left   map[int]int
	status map[int]int // page -> 5xx status; default 503
}

func (doer *flakyPageDoer) Do(request *http.Request) (*http.Response, error) {
	if page, ok := strings.CutPrefix(request.URL.Path, "/walk/"); ok {
		index, _ := strconv.Atoi(page)
		doer.mu.Lock()
		remaining, flaky := doer.left[index]
		if flaky && remaining > 0 {
			doer.left[index] = remaining - 1
			doer.mu.Unlock()
			code := http.StatusServiceUnavailable
			if override := doer.status[index]; override != 0 {
				code = override
			}
			return jsonResponse(request, code, strings.NewReader(`{}`)), nil
		}
		doer.mu.Unlock()
	}
	if request.URL.Path == "/repos/acme/api" {
		return jsonResponse(request, http.StatusOK, strings.NewReader(`{"full_name":"acme/api","default_branch":"main"}`)), nil
	}
	return jsonResponse(request, http.StatusOK, strings.NewReader(`{}`)), nil
}

const exhaustReproHTTPTries = 3

type exhaustWalk struct {
	repository     *exhaustReproRepository
	ledger         *providersync.InProcessChunkLedger
	ledgerClaim    providersync.Claim
	attempt        int // River job.Attempt as the adapter passes it
	snoozes        int
	refunds        int
	failures       int
	runAgeAtRefund []time.Duration
	runAgeAtFail   []time.Duration
	ordinalAtFail  []int
	classAtFail    []string
	accounting     []string // violations of the attempts arithmetic
	terminal       bool
	lastReturnPerm bool
}

func driveTransientPageWalk(t *testing.T, pages int, flakyPages ...int) exhaustWalk {
	return driveTransientPageWalkStatus(t, pages, nil, flakyPages...)
}

func driveTransientPageWalkStatus(t *testing.T, pages int, status map[int]int, flakyPages ...int) exhaustWalk {
	t.Helper()
	tries := map[int]int{}
	for _, page := range flakyPages {
		tries[page] = exhaustReproHTTPTries
	}
	return driveFlakyWalk(t, flakyWalk{pages: pages, status: status, tries: tries})
}

// flakyWalk shapes one harness walk: tries is the 5xx answer count per page
// (exhaustReproHTTPTries fails one attempt), runAge the run's age at the start,
// store wraps the in-process ledger as the executor's chunk store.
type flakyWalk struct {
	pages  int
	status map[int]int
	tries  map[int]int
	runAge time.Duration
	store  func(*providersync.InProcessChunkLedger) providersync.EffectLedger
	// metrics and logger, when set, are the handler's and every attempt's.
	metrics *providerfoundation.Metrics
	logger  *slog.Logger
}

func driveFlakyWalk(t *testing.T, shape flakyWalk) exhaustWalk {
	t.Helper()
	pages, status := shape.pages, shape.status
	now := time.Date(2026, 11, 23, 13, 0, 0, 0, time.UTC)
	unit := providerUnit()
	capability, ok := providersync.Capability("github", "cicd")
	if !ok {
		t.Fatal("github/cicd capability missing")
	}
	descriptor, ok := providersync.Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked || !descriptor.Plannable {
		t.Fatalf("github/cicd descriptor chunked=%v plannable=%v", descriptor.Chunked, descriptor.Plannable)
	}
	unit.SourceExternalID, unit.SourceName = "acme/api", "acme/api"
	unit.Provider, unit.Dataset, unit.CostClass = "github", "cicd", capability.CostClass
	doer := &flakyPageDoer{budget: exhaustReproHTTPTries, left: map[int]int{}, status: status}
	for page, count := range shape.tries {
		doer.left[page] = count
	}
	walk := exhaustWalk{
		repository: &exhaustReproRepository{classRepository: &classRepository{memoryUnitRepository: newMemoryUnitRepository(unit)}},
		attempt:    1,
	}
	walk.repository.runCreatedAt = now.Add(-shape.runAge)
	if shape.runAge == 0 {
		walk.repository.runCreatedAt = now
	}
	walk.repository.chunkNextOrdinal = func(at time.Time) (int, bool) {
		if walk.ledger == nil {
			return 0, false
		}
		checkpoint, err := walk.ledger.LoadChunkCheckpoint(context.Background(), walk.ledgerClaim, at)
		if err != nil {
			return 0, false
		}
		return checkpoint.NextOrdinal, true
	}
	handler := &Handler{
		Repository:      walk.repository,
		LeaseDuration:   time.Minute,
		Heartbeat:       10 * time.Second,
		Now:             func() time.Time { return now },
		ProviderMetrics: shape.metrics,
		BuildExecutor: func(session *providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			if walk.ledger == nil {
				walk.ledger, walk.ledgerClaim = providersync.NewInProcessChunkLedger(session.Claim), session.Claim
			}
			var ledger providersync.EffectLedger = walk.ledger
			if shape.store != nil {
				ledger = shape.store(walk.ledger)
			}
			return providersync.CompleteRouteExecutor{
				Credentials: providerfoundation.CredentialResolver{
					Repository: githubCredentialRepository{unit: session.Claim.Unit},
					Decryptor:  githubCredentialDecryptor{},
				},
				Doer:         fakehttp.Client(doer),
				Retry:        providerfoundation.RetryPolicy{MaxAttempts: exhaustReproHTTPTries, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
				Budget:       testBudgetStore{},
				BudgetLimits: map[providersync.CostClass]int{capability.CostClass: 1},
				BudgetTTL:    time.Minute,
				Gate: func(providersync.Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
					return testBackoffGate{}
				},
				Handler:    pagedWalkHandler{pages: pages, destinations: descriptor.Destinations},
				Comparator: matchingComparator{},
				Committer: providersync.EffectCommitter{
					Ledger: ledger, Sink: &droppingSink{}, Readback: testEffectReadback{}, Now: func() time.Time { return now },
				},
				HeartbeatInterval: 10 * time.Second,
				Now:               func() time.Time { return now },
			}, nil
		},
	}
	for step := 0; step < 20*pages; step++ {
		before, releasesBefore := walk.repository.attempt, walk.repository.releases
		refundsBefore := walk.repository.progressRefunds
		execution := providerExecution(unit, now, walk.attempt)
		execution.Definition.MaxAttempts = 5
		execution.Definition.RetryPolicy = "bounded_exponential_jitter"
		execution.Logger = shape.logger
		err := handler.Work(context.Background(), execution)
		if err == nil {
			return walk
		}
		if _, snoozed := jobruntime.SnoozeDelay(err); snoozed {
			walk.snoozes++
			if walk.repository.progressRefunds != refundsBefore {
				walk.refunds++
				walk.runAgeAtRefund = append(walk.runAgeAtRefund, now.Sub(walk.repository.runCreatedAt))
			}
			if got := walk.repository.attempt; got != before {
				walk.accounting = append(walk.accounting, "continuation: attempts "+strconv.Itoa(before)+" -> "+strconv.Itoa(got)+", want net 0 over claim + continuation")
			}
			now = walk.repository.availableAt
			continue
		}
		walk.failures++
		ordinal := -1
		if walk.ledger != nil {
			if checkpoint, loadErr := walk.ledger.LoadChunkCheckpoint(context.Background(), walk.ledgerClaim, now); loadErr == nil {
				ordinal = checkpoint.NextOrdinal
			}
		}
		walk.ordinalAtFail = append(walk.ordinalAtFail, ordinal)
		walk.classAtFail = append(walk.classAtFail, causeClass(err, false))
		walk.runAgeAtFail = append(walk.runAgeAtFail, now.Sub(walk.repository.runCreatedAt))
		walk.lastReturnPerm = isPermanentReturn(err)
		if walk.repository.status == "failed" {
			walk.terminal = true
			return walk
		}
		if got := walk.repository.attempt; got != before+1 || walk.repository.releases != releasesBefore+1 {
			walk.accounting = append(walk.accounting, "release-for-retry: attempts "+strconv.Itoa(before)+" -> "+strconv.Itoa(got)+
				", releases "+strconv.Itoa(releasesBefore)+" -> "+strconv.Itoa(walk.repository.releases)+", want attempts +1 and one release")
		}
		walk.attempt++ // an ordinary retry consumes one River attempt
		now = now.Add(30 * time.Minute)
	}
	t.Fatalf("walk did not end in %d steps", 20*pages)
	return walk
}

const (
	exhaustReproPages    = 200
	exhaustReproRedPages = 500
)

// CONTROL: two attempt-failing 503 pages far apart, each after the attempt
// committed chunks, are refunded (CHAOS-14574): the unit completes with no
// ordinary failure, and the attempts arithmetic holds at every step. Before
// the refund this cell wanted 2 ordinary failures and success on attempt 3.
func TestChunkedWalkSurvivesTwoTransientAttemptFailuresControl(t *testing.T) {
	walk := driveTransientPageWalk(t, exhaustReproPages, 2, 100)
	if walk.terminal || walk.repository.status != "success" {
		t.Fatalf("control: unit ended %q category=%q cause_class=%q at attempt %d after %d failures", walk.repository.status,
			walk.repository.lastFailCategory, walk.repository.causeClass, walk.attempt, walk.failures)
	}
	if walk.failures != 0 || walk.refunds != 2 || walk.attempt != 1 {
		t.Fatalf("control: failures=%d refunds=%d attempt=%d, want 0 ordinary failures, 2 progress refunds, River attempt 1",
			walk.failures, walk.refunds, walk.attempt)
	}
	if len(walk.accounting) != 0 {
		t.Fatalf("attempts arithmetic: %v", walk.accounting)
	}
}

// CONTROL (zero progress): a 503 on the first page of the first attempt fails
// an attempt that committed nothing, so it counts as an ordinary failure as
// before: the unit completes on River attempt 2 with no refund.
func TestChunkedWalkZeroProgressTransientFailureStillCountsControl(t *testing.T) {
	walk := driveTransientPageWalk(t, exhaustReproPages, 0)
	if walk.terminal || walk.repository.status != "success" {
		t.Fatalf("unit ended %q category=%q at attempt %d", walk.repository.status, walk.repository.lastFailCategory, walk.attempt)
	}
	if walk.failures != 1 || walk.refunds != 0 || walk.attempt != 2 {
		t.Fatalf("failures=%d refunds=%d attempt=%d, want 1 ordinary failure, no refund, success on River attempt 2",
			walk.failures, walk.refunds, walk.attempt)
	}
	if len(walk.classAtFail) != 1 || walk.classAtFail[0] != causeClassProviderTransient {
		t.Fatalf("class at each failure %v, want [provider_transient]", walk.classAtFail)
	}
	if len(walk.accounting) != 0 {
		t.Fatalf("attempts arithmetic: %v", walk.accounting)
	}
}

// Five provider_transient failures of attempts that each start and fail at the
// same checkpoint (page 64, right after a continuation) made no progress, so
// none is refunded and the unit is exhausted at River attempt 5, as before.
func TestFiveTransientFailuresWithoutProgressExhaustAtFive(t *testing.T) {
	walk := driveFlakyWalk(t, flakyWalk{pages: exhaustReproPages, tries: map[int]int{64: 5 * exhaustReproHTTPTries}})
	if !walk.terminal || walk.repository.lastFailCategory != "provider_unit_exhausted" {
		t.Fatalf("unit ended %q category=%q at attempt %d, want failed provider_unit_exhausted",
			walk.repository.status, walk.repository.lastFailCategory, walk.attempt)
	}
	if walk.failures != 5 || walk.refunds != 0 || walk.attempt != 5 {
		t.Fatalf("failures=%d refunds=%d attempt=%d, want 5 ordinary failures, no refund, River attempt 5",
			walk.failures, walk.refunds, walk.attempt)
	}
	for index, ordinal := range walk.ordinalAtFail {
		if ordinal != 64 {
			t.Fatalf("checkpoint next_ordinal at each failure %v, want 64 at every failure (failure %d)", walk.ordinalAtFail, index+1)
		}
	}
	if len(walk.accounting) != 0 {
		t.Fatalf("attempts arithmetic: %v", walk.accounting)
	}
}

// A run 1 s short of the 24 h cap: the first progress failure is refunded, its
// snooze carries the run past the cap, and every later progress failure counts
// again, so the unit is exhausted at River attempt 5.
func TestProgressRefundsStopAtTheRunAgeCap(t *testing.T) {
	walk := driveFlakyWalk(t, flakyWalk{
		pages: exhaustReproRedPages, runAge: progressRefundRunAgeCap - time.Second,
		tries: map[int]int{2: exhaustReproHTTPTries, 100: exhaustReproHTTPTries, 200: exhaustReproHTTPTries,
			300: exhaustReproHTTPTries, 400: exhaustReproHTTPTries, 450: exhaustReproHTTPTries},
	})
	if !walk.terminal || walk.repository.lastFailCategory != "provider_unit_exhausted" || walk.attempt != 5 {
		t.Fatalf("unit ended %q category=%q at attempt %d, want failed provider_unit_exhausted at River attempt 5",
			walk.repository.status, walk.repository.lastFailCategory, walk.attempt)
	}
	if walk.refunds != 1 || walk.failures != 5 {
		t.Fatalf("refunds=%d failures=%d, want 1 refund under the cap and 5 ordinary failures past it", walk.refunds, walk.failures)
	}
	for _, age := range walk.runAgeAtRefund {
		if age >= progressRefundRunAgeCap {
			t.Fatalf("refund at run age %v, want every refund under %v", age, progressRefundRunAgeCap)
		}
	}
	for _, age := range walk.runAgeAtFail {
		if age < progressRefundRunAgeCap {
			t.Fatalf("ordinary failure at run age %v, want every failure at or past %v", age, progressRefundRunAgeCap)
		}
	}
	if len(walk.accounting) != 0 {
		t.Fatalf("attempts arithmetic: %v", walk.accounting)
	}
}

// lostAdvanceStore drops the checkpoint advance of one ordinal once: the chunk
// was fetched, prepared and written to the sink, and the attempt counted it,
// but the durable checkpoint never moved past it.
type lostAdvanceStore struct {
	*providersync.InProcessChunkLedger
	ordinal int
	lost    bool
}

func (store *lostAdvanceStore) MarkChunkCommitted(
	ctx context.Context, claim providersync.Claim, ordinal int, digest string, now time.Time,
) error {
	if ordinal == store.ordinal && !store.lost {
		store.lost = true
		return nil
	}
	return store.InProcessChunkLedger.MarkChunkCommitted(ctx, claim, ordinal, digest, now)
}

// An attempt that starts at checkpoint 64, fetches and writes chunk 64, but
// whose checkpoint commit of it did not land, then fails provider_transient on
// page 65: the durable checkpoint did not move, so no refund.
func TestTransientFailureAfterALostCheckpointCommitIsNotRefunded(t *testing.T) {
	var store *lostAdvanceStore
	walk := driveFlakyWalk(t, flakyWalk{
		pages: exhaustReproPages, tries: map[int]int{65: exhaustReproHTTPTries},
		store: func(ledger *providersync.InProcessChunkLedger) providersync.EffectLedger {
			if store == nil {
				store = &lostAdvanceStore{InProcessChunkLedger: ledger, ordinal: 64}
			}
			return store
		},
	})
	if store == nil || !store.lost {
		t.Fatal("the checkpoint advance of chunk 64 was never dropped")
	}
	if walk.terminal || walk.repository.status != "success" {
		t.Fatalf("unit ended %q category=%q at attempt %d", walk.repository.status, walk.repository.lastFailCategory, walk.attempt)
	}
	if walk.failures != 1 || walk.refunds != 0 || walk.attempt != 2 {
		t.Fatalf("failures=%d refunds=%d attempt=%d, want the page-65 failure counted (no refund), success on River attempt 2",
			walk.failures, walk.refunds, walk.attempt)
	}
	if len(walk.ordinalAtFail) != 1 || walk.ordinalAtFail[0] != 64 || walk.classAtFail[0] != causeClassProviderTransient {
		t.Fatalf("checkpoint at failure %v class %v, want [64] [provider_transient]", walk.ordinalAtFail, walk.classAtFail)
	}
	if len(walk.accounting) != 0 {
		t.Fatalf("attempts arithmetic: %v", walk.accounting)
	}
}

// Was RED before CHAOS-14574 (defect proof): five attempt-failing 5xx pages (503 and 502 mixed, as on prod), each 100 committed chunks
// after the last (more than the 64-chunk attempt bound, so continuations sit between), with continuations between. Every attempt committed chunks and
// the checkpoint advanced; the healthy outcome is a finished unit. Today the 5th
// ordinary failure ends it provider_unit_exhausted / provider_transient.
func TestChunkedWalkWithCheckpointProgressDoesNotExhaustOnFiveSparseTransientFailures(t *testing.T) {
	walk := driveTransientPageWalkStatus(t, exhaustReproRedPages,
		map[int]int{100: http.StatusBadGateway, 300: http.StatusBadGateway}, 2, 100, 200, 300, 400)
	if len(walk.accounting) != 0 {
		t.Fatalf("attempts arithmetic: %v", walk.accounting)
	}
	if walk.terminal || walk.repository.status != "success" {
		t.Fatalf("unit ended %q category=%q cause_class=%q at River attempt %d of 5 after %d ordinary failures "+
			"(class at each failure %v, checkpoint next_ordinal at each failure %v) and %d continuations (attempt-neutral snoozes); "+
			"every attempt committed chunks, want the unit to finish",
			walk.repository.status, walk.repository.lastFailCategory, walk.repository.causeClass,
			walk.attempt, walk.failures, walk.classAtFail, walk.ordinalAtFail, walk.snoozes)
	}
}

// A progress refund is one "continued" lifecycle line and one unit deferral,
// both with the closed reason progress_refund, the cause class and the start
// ordinal, and no error operand.
func TestProgressRefundRecordsTheDeferralReasonOnTheLineAndTheMetric(t *testing.T) {
	var logs bytes.Buffer
	metrics := providerfoundation.NewMetrics()
	walk := driveFlakyWalk(t, flakyWalk{
		pages: exhaustReproPages, tries: map[int]int{2: exhaustReproHTTPTries},
		metrics: metrics, logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	})
	if walk.refunds != 1 || walk.failures != 0 {
		t.Fatalf("refunds=%d failures=%d, want 1 refund", walk.refunds, walk.failures)
	}
	var refundLines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "sync_provider_unit_finished" &&
			record["deferral_reason"] == "progress_refund" {
			refundLines = append(refundLines, record)
		}
	}
	if len(refundLines) != 1 {
		t.Fatalf("progress_refund lines=%d, want 1 in:\n%s", len(refundLines), logs.String())
	}
	line := refundLines[0]
	if line["result"] != "continued" || line["cause_class"] != "provider_transient" || line["start_ordinal"] != float64(0) {
		t.Fatalf("refund line=%v; want result=continued cause_class=provider_transient start_ordinal=0", line)
	}
	if _, present := line["error_detail"]; present {
		t.Fatalf("refund line carries an error operand: %v", line)
	}
	var rendered bytes.Buffer
	if err := metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	want := `dev_health_provider_unit_deferred_total{provider="github",dataset="cicd",reason="progress_refund"} 1`
	if !strings.Contains(rendered.String(), want) {
		t.Fatalf("missing %q in:\n%s", want, rendered.String())
	}
}
