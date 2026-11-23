package providerunit

import (
	"context"
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
// one). The repository is the in-memory fake; unitAttempts models the
// sync_run_units.attempts column by the SQL (claim +1, continuation -1 clamped
// at 0, release-for-retry unchanged: repository_postgres.go claimUnitSQL,
// deferForChunkContinuationSQL, releaseForRetrySQL).

// exhaustReproRepository wraps the class repository with a fixed claim owner
// (the in-process ledger fences on it) and the attempts column model.
type exhaustReproRepository struct {
	*classRepository
	unitAttempts int
	releases     int
}

func (repository *exhaustReproRepository) Claim(ctx context.Context, request providersync.ClaimRequest) (providersync.Claim, error) {
	request.Owner = fixedWalkOwner
	claim, err := repository.classRepository.Claim(ctx, request)
	if err == nil {
		repository.unitAttempts++
	}
	return claim, err
}

func (repository *exhaustReproRepository) DeferChunkContinuation(
	ctx context.Context, claim providersync.Claim, availableAt, now time.Time,
) error {
	err := repository.classRepository.DeferChunkContinuation(ctx, claim, availableAt, now)
	if err == nil && repository.unitAttempts > 0 {
		repository.unitAttempts--
	}
	return err
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
	failures       int
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
	for _, page := range flakyPages {
		doer.left[page] = exhaustReproHTTPTries
	}
	walk := exhaustWalk{
		repository: &exhaustReproRepository{classRepository: &classRepository{memoryUnitRepository: newMemoryUnitRepository(unit)}},
		attempt:    1,
	}
	handler := &Handler{
		Repository:    walk.repository,
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(session *providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			if walk.ledger == nil {
				walk.ledger, walk.ledgerClaim = providersync.NewInProcessChunkLedger(session.Claim), session.Claim
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
					Ledger: walk.ledger, Sink: &droppingSink{}, Readback: testEffectReadback{}, Now: func() time.Time { return now },
				},
				HeartbeatInterval: 10 * time.Second,
				Now:               func() time.Time { return now },
			}, nil
		},
	}
	for step := 0; step < 20*pages; step++ {
		before, releasesBefore := walk.repository.unitAttempts, walk.repository.releases
		execution := providerExecution(unit, now, walk.attempt)
		execution.Definition.MaxAttempts = 5
		err := handler.Work(context.Background(), execution)
		if err == nil {
			return walk
		}
		if _, snoozed := jobruntime.SnoozeDelay(err); snoozed {
			walk.snoozes++
			if got := walk.repository.unitAttempts; got != before {
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
		walk.lastReturnPerm = isPermanentReturn(err)
		if walk.repository.status == "failed" {
			walk.terminal = true
			return walk
		}
		if got := walk.repository.unitAttempts; got != before+1 || walk.repository.releases != releasesBefore+1 {
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

// CONTROL: two attempt-failing 503 pages far apart are survived; the unit
// completes on the 3rd attempt, and the attempts arithmetic holds at every step.
func TestChunkedWalkSurvivesTwoTransientAttemptFailuresControl(t *testing.T) {
	walk := driveTransientPageWalk(t, exhaustReproPages, 2, 100)
	if walk.terminal || walk.repository.status != "success" {
		t.Fatalf("control: unit ended %q category=%q cause_class=%q at attempt %d after %d failures", walk.repository.status,
			walk.repository.lastFailCategory, walk.repository.causeClass, walk.attempt, walk.failures)
	}
	if walk.failures != 2 || walk.attempt != 3 || walk.snoozes < 1 {
		t.Fatalf("control: failures=%d attempt=%d snoozes=%d, want 2 failures, success on attempt 3, >= 1 continuation",
			walk.failures, walk.attempt, walk.snoozes)
	}
	for index, class := range walk.classAtFail {
		if class != "provider_transient" {
			t.Errorf("failure %d class=%q, want provider_transient (the 503 premise)", index+1, class)
		}
	}
	if len(walk.accounting) != 0 {
		t.Fatalf("attempts arithmetic: %v", walk.accounting)
	}
}

// RED (defect proof): five attempt-failing 5xx pages (503 and 502 mixed, as on prod), each 100 committed chunks
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
