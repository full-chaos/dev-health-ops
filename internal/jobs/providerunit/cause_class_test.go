package providerunit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// classRepository records the cause class and refuses a failure write that
// arrives after the job deadline, as the real repository does: its lease
// expiry is capped at the job deadline and the write needs an unexpired lease.
type classRepository struct {
	*memoryUnitRepository
	deadline       time.Time
	causeClass     string
	classCalls     int
	writeCtxErr    error
	writeHadLimit  bool
	failCompletedA time.Time
}

func (repository *classRepository) FailWithCauseClass(
	ctx context.Context, claim providersync.Claim, category, class string,
	_ time.Time, completedAt time.Time,
) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.status != "running" || repository.lastClaim.Owner != claim.Owner {
		return providersync.ErrLeaseLost
	}
	if !repository.deadline.IsZero() && !completedAt.Before(repository.deadline) {
		return providersync.ErrLeaseLost
	}
	repository.writeCtxErr = ctx.Err()
	_, repository.writeHadLimit = ctx.Deadline()
	repository.classCalls++
	repository.failures++
	repository.lastFailCategory = category
	repository.causeClass = class
	repository.failCompletedA = completedAt
	repository.status = "failed"
	return nil
}

func githubBlameUnit() providersync.Unit {
	unit := githubFilesUnit()
	capability, ok := providersync.Capability("github", "blame")
	if !ok {
		panic("github/blame capability missing")
	}
	unit.Dataset = "blame"
	unit.CostClass = capability.CostClass
	return unit
}

type emptyBlameCoverage struct{}

func (emptyBlameCoverage) Progress(
	context.Context, providersync.Claim, string, string, string,
) (providersync.GitHubBlameProgressState, error) {
	return providersync.GitHubBlameProgressState{}, nil
}

func (emptyBlameCoverage) HasGenerationProgress(
	context.Context, providersync.Claim, string,
) (bool, error) {
	return false, nil
}

type doerFunc func(*http.Request) (*http.Response, error)

func (doer doerFunc) Do(request *http.Request) (*http.Response, error) { return doer(request) }

func jsonResponse(request *http.Request, status int, body io.Reader) *http.Response {
	return &http.Response{
		StatusCode: status, Request: request, Body: io.NopCloser(body),
		Header: http.Header{"Content-Type": []string{"application/json"}},
	}
}

// githubRouteExecutor is githubFilesTraversalExecutor with the route handler
// and the provider transport chosen by the test.
func githubRouteExecutor(
	now time.Time, handler providersync.CompleteRouteHandler, doer providerfoundation.HTTPDoer,
	builds *atomic.Int32,
) ExecutorFactory {
	return func(session *providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
		builds.Add(1)
		return providersync.CompleteRouteExecutor{
			Credentials: providerfoundation.CredentialResolver{
				Repository: githubCredentialRepository{unit: session.Claim.Unit},
				Decryptor:  githubCredentialDecryptor{},
			},
			Doer: fakehttp.Client(doer),
			Retry: providerfoundation.RetryPolicy{
				MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
			},
			Budget:       testBudgetStore{},
			BudgetLimits: map[providersync.CostClass]int{providersync.CostMedium: 1, providersync.CostHeavy: 1},
			BudgetTTL:    time.Minute,
			Gate: func(providersync.Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
				return testBackoffGate{}
			},
			Handler:           handler,
			Comparator:        providersync.ProductionContractComparator{},
			Committer:         providersync.EffectCommitter{Ledger: &testEffectLedger{}, Sink: testEffectSink{}, Readback: testEffectReadback{}, Now: func() time.Time { return now }},
			HeartbeatInterval: 400 * time.Millisecond,
			Now:               func() time.Time { return now },
		}, nil
	}
}

// oversizedTreeDoer is the provider of the measured blame failure: every
// request is small except the recursive tree, which is above the 2 MiB cap.
func oversizedTreeDoer(treeCalls *atomic.Int32) doerFunc {
	return func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/repos/acme/api":
			return jsonResponse(request, http.StatusOK,
				strings.NewReader(`{"full_name":"acme/api","default_branch":"main"}`)), nil
		case "/repos/acme/api/branches/main":
			return jsonResponse(request, http.StatusOK,
				strings.NewReader(`{"commit":{"sha":"tree-sha"}}`)), nil
		case "/repos/acme/api/git/trees/tree-sha":
			treeCalls.Add(1)
			return jsonResponse(request, http.StatusOK,
				strings.NewReader(`{"tree":[`+strings.Repeat(" ", (2<<20)+1)+`]}`)), nil
		default:
			return nil, fmt.Errorf("unexpected request %s", request.URL.Path)
		}
	}
}

func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &output
}

// The measured defect: a provider response above the byte cap failed the same
// way on all five tries of every hourly run. It must end on the first try, with
// its own class, stored where a reader can see it.
func TestResponseAboveTheByteCapIsTerminalOnTheFirstAttemptWithItsClass(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	repository := &classRepository{memoryUnitRepository: newMemoryUnitRepository(githubBlameUnit())}
	var builds, treeCalls atomic.Int32
	logs := captureWarnings(t)
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now: func() time.Time { return now },
		BuildExecutor: githubRouteExecutor(now,
			providersync.GitHubBlameRouteHandler{Coverage: emptyBlameCoverage{}, MaxFiles: 2},
			oversizedTreeDoer(&treeCalls), &builds),
	}
	execution := providerExecution(repository.unit, now, 1)
	execution.Definition.MaxAttempts = 5

	err := handler.Work(context.Background(), execution)

	var tooLarge *providerfoundation.ObjectTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Fatalf("Work()=%v, want the real cap error", err)
	}
	if builds.Load() != 1 || treeCalls.Load() != 1 {
		t.Fatalf("executions=%d tree requests=%d, want exactly one of each", builds.Load(), treeCalls.Load())
	}
	if repository.status != "failed" || repository.releaseCalls != 0 ||
		repository.lastFailCategory != ResponseTooLargeCategory ||
		repository.causeClass != ResponseTooLargeCategory {
		t.Fatalf("status=%s releases=%d category=%q class=%q, want failed/0/%s/%s",
			repository.status, repository.releaseCalls, repository.lastFailCategory,
			repository.causeClass, ResponseTooLargeCategory, ResponseTooLargeCategory)
	}
	assertOneSafeWarning(t, logs.String(), ResponseTooLargeCategory, "git/trees", "acme")
}

func assertOneSafeWarning(t *testing.T, logs, class string, forbidden ...string) {
	t.Helper()
	var warnings []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "provider unit failed terminally") {
			warnings = append(warnings, line)
		}
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "cause_class="+class) ||
		!strings.Contains(warnings[0], "level=WARN") {
		t.Fatalf("terminal WARN lines=%q, want exactly one WARN naming %s", warnings, class)
	}
	for _, word := range forbidden {
		if strings.Contains(warnings[0], word) {
			t.Fatalf("terminal WARN carries %q: %s", word, warnings[0])
		}
	}
}

type collectErrorHandler struct{ err error }

func (handler collectErrorHandler) Collect(
	context.Context, providersync.Claim, providerfoundation.Credential,
	*providerfoundation.HTTPClient, time.Time,
) (providersync.CompleteRouteBatch, error) {
	return providersync.CompleteRouteBatch{}, handler.err
}

// The measured work-items defect: the unit's result is above the bounded write
// contract. The error is built by the real BuildEffectBatch from real rows.
func realBoundExceededError(t *testing.T, rows int) error {
	t.Helper()
	encoded := make([]json.RawMessage, rows)
	for index := range encoded {
		encoded[index] = json.RawMessage(`{"a":1}`)
	}
	_, err := providersync.BuildEffectBatch("work_items", providersync.EffectReplaySafe, encoded)
	return err
}

func TestResultAboveTheWriteContractIsTerminalOnTheFirstAttemptWithItsClass(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	boundErr := realBoundExceededError(t, 100_001)
	if !errors.Is(boundErr, providersync.ErrEffectRecoveryUnsafe) {
		t.Fatalf("real producer returned %v, want the recovery-unsafe sentinel", boundErr)
	}
	repository := &classRepository{memoryUnitRepository: newMemoryUnitRepository(githubFilesUnit())}
	var builds atomic.Int32
	logs := captureWarnings(t)
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now: func() time.Time { return now },
		BuildExecutor: githubRouteExecutor(now, collectErrorHandler{err: boundErr},
			doerFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("unexpected") }), &builds),
	}
	execution := providerExecution(repository.unit, now, 1)
	execution.Definition.MaxAttempts = 5

	err := handler.Work(context.Background(), execution)

	if !errors.Is(err, providersync.ErrEffectRecoveryUnsafe) {
		t.Fatalf("Work()=%v, want the bound error", err)
	}
	if builds.Load() != 1 || repository.status != "failed" || repository.releaseCalls != 0 ||
		repository.lastFailCategory != ResultTooLargeCategory || repository.causeClass != ResultTooLargeCategory {
		t.Fatalf("executions=%d status=%s releases=%d category=%q class=%q",
			builds.Load(), repository.status, repository.releaseCalls,
			repository.lastFailCategory, repository.causeClass)
	}
	assertOneSafeWarning(t, logs.String(), ResultTooLargeCategory)
}

// The other recovery-unsafe sites are not size bounds; they keep the ordinary
// retry path, so a sentinel with no bound behind it is not made terminal.
func TestRecoveryUnsafeWithoutASizeBoundStaysRetryable(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	repository := &classRepository{memoryUnitRepository: newMemoryUnitRepository(githubFilesUnit())}
	var builds atomic.Int32
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now: func() time.Time { return now },
		BuildExecutor: githubRouteExecutor(now, collectErrorHandler{err: providersync.ErrEffectRecoveryUnsafe},
			doerFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("unexpected") }), &builds),
	}
	execution := providerExecution(repository.unit, now, 1)
	execution.Definition.MaxAttempts = 5

	if err := handler.Work(context.Background(), execution); err == nil {
		t.Fatal("Work() = nil, want a retryable error")
	}
	if repository.status == "failed" || repository.releaseCalls != 1 {
		t.Fatalf("status=%s releases=%d, want the ordinary retry", repository.status, repository.releaseCalls)
	}
}

// A try that reaches the job time limit is terminal for this run and records
// its own failure. The lease expires at the deadline, so the write must happen
// before it, on a context that is not the expired work context.
func TestTimeLimitIsTerminalAndTheUnitRecordsItsOwnFailure(t *testing.T) {
	previous := timeLimitReserve
	timeLimitReserve = 400 * time.Millisecond
	t.Cleanup(func() { timeLimitReserve = previous })
	deadline := time.Now().Add(1500 * time.Millisecond)
	repository := &classRepository{
		memoryUnitRepository: newMemoryUnitRepository(githubBlameUnit()), deadline: deadline,
	}
	var builds, requests atomic.Int32
	logs := captureWarnings(t)
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Second, Heartbeat: 400 * time.Millisecond,
		BuildExecutor: githubRouteExecutor(time.Now(),
			providersync.GitHubBlameRouteHandler{Coverage: emptyBlameCoverage{}, MaxFiles: 2},
			doerFunc(func(request *http.Request) (*http.Response, error) {
				requests.Add(1)
				<-request.Context().Done()
				return nil, request.Context().Err()
			}), &builds),
	}
	execution := providerExecution(repository.unit, time.Now(), 1)
	execution.Definition.MaxAttempts = 5
	execution.Deadline = deadline
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	err := handler.Work(ctx, execution)

	if err == nil {
		t.Fatal("Work() = nil, want a terminal error")
	}
	if builds.Load() != 1 || requests.Load() != 1 {
		t.Fatalf("executions=%d requests=%d err=%v logs=%s, want one each", builds.Load(), requests.Load(), err, logs.String())
	}
	if repository.status != "failed" || repository.releaseCalls != 0 ||
		repository.lastFailCategory != TimeLimitCategory || repository.causeClass != TimeLimitCategory {
		t.Fatalf("status=%s releases=%d category=%q class=%q, want failed/0/%s/%s",
			repository.status, repository.releaseCalls, repository.lastFailCategory,
			repository.causeClass, TimeLimitCategory, TimeLimitCategory)
	}
	if repository.writeCtxErr != nil || !repository.writeHadLimit {
		t.Fatalf("failure write context err=%v bounded=%v, want a live, bounded context",
			repository.writeCtxErr, repository.writeHadLimit)
	}
	assertOneSafeWarning(t, logs.String(), TimeLimitCategory)
}

// An exhausted unit keeps its outcome label and names the class that
// exhausted it. An error no classifier knows is "unclassified", never empty.
func TestExhaustedUnitStoresTheClassOfItsLastAttempt(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		err   error
		class string
	}{
		{"unknown error", errors.New("secret-bearing text /org/acme/path"), providersync.CauseClassUnclassified},
		{"provider 503", &providerfoundation.ProviderError{
			Class: providerfoundation.ErrorTransient, StatusCode: 503, Path: "/repos/acme/api",
		}, "provider_transient"},
		{"provider class outside the vocabulary", &providerfoundation.ProviderError{
			Class: providerfoundation.ErrorClass("made_up/x"),
		}, providersync.CauseClassUnclassified},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			repository := &classRepository{memoryUnitRepository: newMemoryUnitRepository(githubFilesUnit())}
			var builds atomic.Int32
			logs := captureWarnings(t)
			handler := &Handler{
				Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
				Now: func() time.Time { return now },
				BuildExecutor: githubRouteExecutor(now, collectErrorHandler{err: testCase.err},
					doerFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("unexpected") }), &builds),
			}
			execution := providerExecution(repository.unit, now, 5)
			execution.Definition.MaxAttempts = 5

			if err := handler.Work(context.Background(), execution); err == nil {
				t.Fatal("Work() = nil, want the exhausted error")
			}
			if repository.status != "failed" ||
				repository.lastFailCategory != GitHubFilesInventoryFailureCategory ||
				repository.causeClass != testCase.class {
				t.Fatalf("status=%s category=%q class=%q, want failed/%s/%s", repository.status,
					repository.lastFailCategory, repository.causeClass,
					GitHubFilesInventoryFailureCategory, testCase.class)
			}
			assertOneSafeWarning(t, logs.String(), testCase.class, "secret", "/org/acme", "/repos/acme")
		})
	}
}

// Before the last attempt an unknown error keeps the ordinary retry: no failure
// row, no WARN.
func TestEarlierAttemptOfAnUnknownErrorStillRetries(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	repository := &classRepository{memoryUnitRepository: newMemoryUnitRepository(githubFilesUnit())}
	var builds atomic.Int32
	logs := captureWarnings(t)
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now: func() time.Time { return now },
		BuildExecutor: githubRouteExecutor(now, collectErrorHandler{err: errors.New("boom")},
			doerFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("unexpected") }), &builds),
	}
	execution := providerExecution(repository.unit, now, 2)
	execution.Definition.MaxAttempts = 5

	if err := handler.Work(context.Background(), execution); err == nil {
		t.Fatal("Work() = nil, want retryable")
	}
	if repository.status == "failed" || repository.releaseCalls != 1 || repository.classCalls != 0 ||
		strings.Contains(logs.String(), "failed terminally") {
		t.Fatalf("status=%s releases=%d classCalls=%d logs=%s", repository.status,
			repository.releaseCalls, repository.classCalls, logs.String())
	}
}

// The classifier works for every wrapping the routes use (%w chains) and for
// every provider's route, because the cap and the bound live in shared code.
func TestCauseClassVocabularyIsClosedAndWrapSafe(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("%w: %w", providersync.ErrGitHubBlameTraversalFailed, err) }
	cases := []struct {
		name     string
		err      error
		timedOut bool
		want     string
	}{
		{"cap", wrap(&providerfoundation.ObjectTooLargeError{Path: "/x", CapBytes: 1, ContentLength: -1}), false, ResponseTooLargeCategory},
		{"bound", wrap(realBoundExceededError(t, 100_001)), false, ResultTooLargeCategory},
		{"deadline", context.DeadlineExceeded, true, TimeLimitCategory},
		{"auth", &providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication}, false, AuthCategory},
		{"rate limited", &providerfoundation.ProviderError{Class: providerfoundation.ErrorRateLimited}, false, "provider_rate_limited"},
		{"budget", providerfoundation.ErrBudgetContended, false, causeClassBudgetContended},
		{"nil", nil, false, providersync.CauseClassUnclassified},
		{"plain", errors.New("x"), false, providersync.CauseClassUnclassified},
	}
	for _, testCase := range cases {
		got := causeClass(testCase.err, testCase.timedOut)
		if got != testCase.want || !providersync.ValidCauseClass(got) {
			t.Errorf("%s: class=%q valid=%v, want %q", testCase.name, got,
				providersync.ValidCauseClass(got), testCase.want)
		}
	}
	for _, bad := range []string{"", "Has/Slash", "../x", "with space", strings.Repeat("a", 65), "9lead"} {
		if providersync.ValidCauseClass(bad) {
			t.Errorf("ValidCauseClass(%q) = true", bad)
		}
	}
	_ = jobruntime.ProviderUnitArgs{}
}

// A first-attempt terminal category that the failure counter does not know
// collapses to "other", and the operator loses the very series that tells the
// causes apart.
func TestNewTerminalCategoriesAreInTheFailureCounterVocabulary(t *testing.T) {
	for _, category := range []string{ResponseTooLargeCategory, ResultTooLargeCategory, TimeLimitCategory} {
		if got := providerfoundation.MetricUnitFailureReasonLabel(category); got != category {
			t.Errorf("MetricUnitFailureReasonLabel(%q) = %q", category, got)
		}
	}
}

// One provider request that times out is transient. It wraps DeadlineExceeded
// but the job's time limit has not been reached, so it keeps the retry path.
func TestSingleRequestTimeoutIsNotTheJobTimeLimit(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	repository := &classRepository{memoryUnitRepository: newMemoryUnitRepository(githubFilesUnit())}
	var builds atomic.Int32
	handler := &Handler{
		Repository: repository, LeaseDuration: time.Minute, Heartbeat: 10 * time.Second,
		Now: func() time.Time { return now },
		BuildExecutor: githubRouteExecutor(now,
			collectErrorHandler{err: fmt.Errorf("request timed out: %w", context.DeadlineExceeded)},
			doerFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("unexpected") }), &builds),
	}
	execution := providerExecution(repository.unit, now, 1)
	execution.Definition.MaxAttempts = 5

	if err := handler.Work(context.Background(), execution); err == nil {
		t.Fatal("Work() = nil, want retryable")
	}
	if repository.status == "failed" || repository.releaseCalls != 1 {
		t.Fatalf("status=%s releases=%d, want the ordinary retry", repository.status, repository.releaseCalls)
	}
}

// The terminal write must not run on the work context: at the time limit that
// context is already expired, and the write would fail before it started.
func TestTerminalFailureWriteIgnoresAnExpiredWorkContext(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	repository := &classRepository{memoryUnitRepository: newMemoryUnitRepository(githubFilesUnit())}
	claim := providersync.Claim{Unit: repository.unit, Owner: "owner"}
	repository.status, repository.lastClaim = "running", claim
	handler := &Handler{Repository: repository}
	expired, cancel := context.WithCancel(context.Background())
	cancel()

	if err := handler.failTerminal(expired, claim, TimeLimitCategory, TimeLimitCategory,
		context.DeadlineExceeded, now, now.Add(time.Second)); err != nil {
		t.Fatalf("failTerminal() = %v", err)
	}
	if repository.writeCtxErr != nil || !repository.writeHadLimit || repository.causeClass != TimeLimitCategory {
		t.Fatalf("write ctx err=%v bounded=%v class=%q", repository.writeCtxErr, repository.writeHadLimit, repository.causeClass)
	}
}
