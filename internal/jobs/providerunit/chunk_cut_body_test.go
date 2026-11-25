package providerunit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// RED: a GitHub Actions run-jobs page whose 200 body is cut by the transport
// mid-JSON (the read returns io.ErrUnexpectedEOF or a reset) is a transport
// fault, not a bad page. Today decodePageTotal (pagination.go) turns it into a
// permanent body_read ProviderError with the read cause dropped; the unit
// handler stores it as cause class unclassified (cap 2, no progress refund), so
// two cuts on a long chunked cicd walk end it provider_unit_exhausted at River
// attempt 2 and failTxOnce deletes its chunk checkpoint.
//
// Harness: the driveFlakyWalk shape (chunk_progress_exhaustion_repro_test.go)
// with a chunked handler whose pages are read through the real
// providerfoundation.CollectGitHubLinkPages on the run-jobs route.

const (
	cutBodyPages     = 200
	cutBodyRunsRoute = "/repos/{owner}/{repo}/actions/runs/{run}/jobs"
)

var cutBodyPagesAt = []int{2, 100}

// The error_detail shape of the failed prod unit, on this harness's route.
const cutBodyProdErrorText = "provider request failed: permanent route=" + cutBodyRunsRoute +
	" reason=body_read: provider pagination response is invalid"

// runJobsWalkHandler is pagedWalkHandler with each page read as a run-jobs
// page: one chunk per page, cursor = next page.
type runJobsWalkHandler struct {
	pages        int
	destinations []string
	pageErrors   *[]string
}

func (runJobsWalkHandler) Collect(
	context.Context, providersync.Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (providersync.CompleteRouteBatch, error) {
	return providersync.CompleteRouteBatch{}, fmt.Errorf("the run-jobs walk is chunked only")
}

func (handler runJobsWalkHandler) CollectChunks(
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
		if _, err := providerfoundation.CollectGitHubLinkPages(ctx, client, providerfoundation.GitHubPageOptions{
			Path:     "/repos/acme/api/actions/runs/" + strconv.Itoa(page) + "/jobs",
			Route:    cutBodyRunsRoute,
			Query:    url.Values{"per_page": {"100"}},
			DataKey:  "jobs",
			MaxPages: 1,
			TotalKey: "total_count",
		}); err != nil {
			*handler.pageErrors = append(*handler.pageErrors, err.Error())
			return err
		}
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

type cutBodyMode int

const (
	cutBodyUnexpectedEOF cutBodyMode = iota + 1
	cutBodyConnReset
	cutBodyInvalidPage
	cutBodyStatus503
)

// cutReader serves a run-jobs page prefix and then fails the read.
type cutReader struct {
	prefix io.Reader
	err    error
}

func (reader *cutReader) Read(p []byte) (int, error) {
	if n, err := reader.prefix.Read(p); n > 0 || err != io.EOF {
		return n, err
	}
	return 0, reader.err
}

// cutBodyDoer answers a chosen run-jobs page with the planted fault for its next
// left[page] requests, then serves it whole.
type cutBodyDoer struct {
	mu   sync.Mutex
	mode cutBodyMode
	left map[int]int
}

const cutBodyPrefix = `{"total_count":1,"jobs":[{"id":7001,"run_id":`

func (doer *cutBodyDoer) Do(request *http.Request) (*http.Response, error) {
	if request.URL.Path == "/repos/acme/api" {
		return jsonResponse(request, http.StatusOK, strings.NewReader(`{"full_name":"acme/api","default_branch":"main"}`)), nil
	}
	rest, ok := strings.CutPrefix(request.URL.Path, "/repos/acme/api/actions/runs/")
	if !ok {
		return jsonResponse(request, http.StatusOK, strings.NewReader(`{}`)), nil
	}
	page, _ := strconv.Atoi(strings.TrimSuffix(rest, "/jobs"))
	doer.mu.Lock()
	remaining := doer.left[page]
	if remaining > 0 {
		doer.left[page] = remaining - 1
	}
	doer.mu.Unlock()
	if remaining > 0 {
		switch doer.mode {
		case cutBodyUnexpectedEOF:
			return jsonResponse(request, http.StatusOK, &cutReader{prefix: strings.NewReader(cutBodyPrefix), err: io.ErrUnexpectedEOF}), nil
		case cutBodyConnReset:
			return jsonResponse(request, http.StatusOK, &cutReader{prefix: strings.NewReader(cutBodyPrefix), err: syscall.ECONNRESET}), nil
		case cutBodyInvalidPage:
			return jsonResponse(request, http.StatusOK, strings.NewReader(`{"total_count":1,"jobs":{"id":7001}}`)), nil
		case cutBodyStatus503:
			return jsonResponse(request, http.StatusServiceUnavailable, strings.NewReader(`{}`)), nil
		}
	}
	return jsonResponse(request, http.StatusOK, strings.NewReader(
		fmt.Sprintf(`{"total_count":1,"jobs":[{"id":%d,"run_id":%d,"status":"completed"}]}`, 7000+page, page))), nil
}

type cutBodyWalk struct {
	repository     *exhaustReproRepository
	pageErrors     []string
	attempt        int
	snoozes        int
	failures       int
	terminal       bool
	classAtFail    []string
	ordinalAtFail  []int
	accounting     []string
	lastReturnPerm bool
}

func driveCutBodyWalk(t *testing.T, mode cutBodyMode, left map[int]int) cutBodyWalk {
	t.Helper()
	now := time.Date(2026, 11, 23, 13, 0, 0, 0, time.UTC)
	unit := providerUnit()
	capability, ok := providersync.Capability("github", "cicd")
	if !ok {
		t.Fatal("github/cicd capability missing")
	}
	descriptor, ok := providersync.Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatalf("github/cicd descriptor chunked=%v", descriptor.Chunked)
	}
	unit.SourceExternalID, unit.SourceName = "acme/api", "acme/api"
	unit.Provider, unit.Dataset, unit.CostClass = "github", "cicd", capability.CostClass
	doer := &cutBodyDoer{mode: mode, left: map[int]int{}}
	for page, count := range left {
		doer.left[page] = count
	}
	walk := cutBodyWalk{
		repository: &exhaustReproRepository{classRepository: &classRepository{memoryUnitRepository: newMemoryUnitRepository(unit)}},
		attempt:    1,
	}
	walk.repository.runCreatedAt = now
	var ledger *providersync.InProcessChunkLedger
	var ledgerClaim providersync.Claim
	walk.repository.chunkNextOrdinal = func(at time.Time) (int, bool) {
		if ledger == nil {
			return 0, false
		}
		checkpoint, err := ledger.LoadChunkCheckpoint(context.Background(), ledgerClaim, at)
		if err != nil {
			return 0, false
		}
		return checkpoint.NextOrdinal, true
	}
	handler := &Handler{
		Repository:    walk.repository,
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
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
				Retry:        providerfoundation.RetryPolicy{MaxAttempts: exhaustReproHTTPTries, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
				Budget:       testBudgetStore{},
				BudgetLimits: map[providersync.CostClass]int{capability.CostClass: 1},
				BudgetTTL:    time.Minute,
				Gate: func(providersync.Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
					return testBackoffGate{}
				},
				Handler:    runJobsWalkHandler{pages: cutBodyPages, destinations: descriptor.Destinations, pageErrors: &walk.pageErrors},
				Comparator: matchingComparator{},
				Committer: providersync.EffectCommitter{
					Ledger: ledger, Sink: &droppingSink{}, Readback: testEffectReadback{}, Now: func() time.Time { return now },
				},
				HeartbeatInterval: 10 * time.Second,
				Now:               func() time.Time { return now },
			}, nil
		},
	}
	for step := 0; step < 20*cutBodyPages; step++ {
		before, releasesBefore := walk.repository.attempt, walk.repository.releases
		execution := providerExecution(unit, now, walk.attempt)
		execution.Definition.MaxAttempts = 5
		execution.Definition.RetryPolicy = "bounded_exponential_jitter"
		err := handler.Work(context.Background(), execution)
		if err == nil {
			return walk
		}
		if _, snoozed := jobruntime.SnoozeDelay(err); snoozed {
			walk.snoozes++
			if got := walk.repository.attempt; got != before {
				walk.accounting = append(walk.accounting, "snooze: attempts "+strconv.Itoa(before)+" -> "+strconv.Itoa(got))
			}
			now = walk.repository.availableAt
			continue
		}
		walk.failures++
		ordinal := -1
		if ledger != nil {
			if checkpoint, loadErr := ledger.LoadChunkCheckpoint(context.Background(), ledgerClaim, now); loadErr == nil {
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
		if got := walk.repository.attempt; got != before+1 || walk.repository.releases != releasesBefore+1 {
			walk.accounting = append(walk.accounting, "release-for-retry: attempts "+strconv.Itoa(before)+" -> "+strconv.Itoa(got))
		}
		walk.attempt++
		now = now.Add(30 * time.Minute)
	}
	t.Fatalf("walk did not end in %d steps", 20*cutBodyPages)
	return walk
}

func onceAt(pages []int) map[int]int {
	left := map[int]int{}
	for _, page := range pages {
		left[page] = 1
	}
	return left
}

func TestChunkedCicdRunJobsCutBodyKeepsTheUnitAndItsCheckpoint(t *testing.T) {
	for _, arm := range []struct {
		name string
		mode cutBodyMode
	}{
		{"planted_cut_body_unexpected_EOF", cutBodyUnexpectedEOF},
		{"planted_cut_body_conn_reset", cutBodyConnReset},
	} {
		t.Run(arm.name, func(t *testing.T) {
			walk := driveCutBodyWalk(t, arm.mode, onceAt(cutBodyPagesAt))
			if len(walk.pageErrors) == 0 {
				t.Fatalf("planted arm %s: no page read failed; the plant missed", arm.name)
			}
			for _, text := range walk.pageErrors {
				if text != cutBodyProdErrorText {
					t.Fatalf("planted arm %s: page error %q, want the prod text %q", arm.name, text, cutBodyProdErrorText)
				}
			}
			if walk.terminal || walk.repository.status != "success" {
				t.Fatalf("planted arm %s on run-jobs pages %v (one cut each, chunks committed between): unit ended %q category=%q cause_class=%q "+
					"at River attempt %d after %d ordinary failures (class at each failure %v, checkpoint next_ordinal at each failure %v, "+
					"permanent return=%v); want the cut body treated as a transport fault and the unit to finish with its checkpoint",
					arm.name, cutBodyPagesAt, walk.repository.status, walk.repository.lastFailCategory, walk.repository.causeClass,
					walk.attempt, walk.failures, walk.classAtFail, walk.ordinalAtFail, walk.lastReturnPerm)
			}
			if len(walk.accounting) != 0 {
				t.Fatalf("planted arm %s: attempts arithmetic: %v", arm.name, walk.accounting)
			}
		})
	}
}

// CONTROL (passes today): the same walk with a 503 page in place of each cut is
// refunded (CHAOS-14574) and finishes.
func TestChunkedCicdRunJobsCutBody503Control(t *testing.T) {
	left := map[int]int{}
	for _, page := range cutBodyPagesAt {
		left[page] = exhaustReproHTTPTries
	}
	walk := driveCutBodyWalk(t, cutBodyStatus503, left)
	if walk.terminal || walk.repository.status != "success" || walk.failures != 0 {
		t.Fatalf("control 503: unit ended %q category=%q cause_class=%q failures=%d classes=%v, want success with 0 ordinary failures",
			walk.repository.status, walk.repository.lastFailCategory, walk.repository.causeClass, walk.failures, walk.classAtFail)
	}
}

// CONTROL (passes today): a whole 200 body that is not a run-jobs page
// (body_decode) on every request is a deterministic bad page; it ends the unit.
func TestChunkedCicdRunJobsInvalidPageStillFailsControl(t *testing.T) {
	walk := driveCutBodyWalk(t, cutBodyInvalidPage, map[int]int{cutBodyPagesAt[1]: 1000})
	if !walk.terminal || walk.repository.lastFailCategory != "provider_unit_exhausted" {
		t.Fatalf("control invalid page: unit ended %q category=%q cause_class=%q at attempt %d, want a terminal provider_unit_exhausted",
			walk.repository.status, walk.repository.lastFailCategory, walk.repository.causeClass, walk.attempt)
	}
}
