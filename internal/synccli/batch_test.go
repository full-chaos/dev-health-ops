package synccli

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// batchHarness fakes the two outside seams of the batch path -- the listing and
// the per-dataset run -- and records what reached them.
type batchHarness struct {
	mu       sync.Mutex
	repos    []providersync.ListedRepository
	listErr  error
	listRuns []providersync.InProcessRun
	listings []providersync.InProcessListing
	runs     []providersync.InProcessRun
	events   []string // "start:<source>" / "end:<source>"
	inFlight int
	peak     int
	fail     func(providersync.InProcessRun) error
	hold     func(providersync.InProcessRun)
	opened   int
}

func (h *batchHarness) executor() Executor {
	return InlineExecutor(InlineDeps{
		OpenStore: func(context.Context, string) (driver.Conn, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.opened++
			return fakeStore{}, nil
		},
		List: func(_ context.Context, run providersync.InProcessRun, listing providersync.InProcessListing) ([]providersync.ListedRepository, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			h.listRuns = append(h.listRuns, run)
			h.listings = append(h.listings, listing)
			return h.repos, h.listErr
		},
		Run: func(_ context.Context, run providersync.InProcessRun) (providersync.CompleteRouteExecutionResult, error) {
			h.mu.Lock()
			h.runs = append(h.runs, run)
			h.inFlight++
			h.peak = max(h.peak, h.inFlight)
			h.events = append(h.events, "start:"+run.SourceExternalID)
			h.mu.Unlock()
			if h.hold != nil {
				h.hold(run)
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			h.inFlight--
			h.events = append(h.events, "end:"+run.SourceExternalID)
			if h.fail != nil {
				return providersync.CompleteRouteExecutionResult{}, h.fail(run)
			}
			return providersync.CompleteRouteExecutionResult{}, nil
		},
	})
}

func (h *batchHarness) ran() map[string][]string {
	out := map[string][]string{}
	for _, run := range h.runs {
		out[run.SourceExternalID] = append(out[run.SourceExternalID], run.Dataset)
	}
	return out
}

func githubRepos(names ...string) []providersync.ListedRepository {
	var repos []providersync.ListedRepository
	for _, name := range names {
		repos = append(repos, providersync.ListedRepository{Name: name[strings.Index(name, "/")+1:], FullName: name})
	}
	return repos
}

var githubBatchArgs = []string{"--provider", "github", "-s", "acme/*", "--auth", "ghp-very-secret"}

func TestBatchListsThenRunsEveryDatasetOfEachRepository(t *testing.T) {
	h := &batchHarness{repos: githubRepos("acme/api", "acme/web")}
	code, stdout, stderr := runVerb(t, "git", h.executor(),
		append([]string{"--group", "acme-org", "--owner", "ignored-owner", "--max-repos", "5", "--since", "2026-01-02"}, githubBatchArgs...), inlineEnv)
	if code != cli.ExitOK || stdout != "" {
		t.Fatalf("exit %d stdout %q stderr %q", code, stdout, stderr)
	}
	want := map[string][]string{
		"acme/api": {"repo-metadata", "commit-stats", "commits", "files"},
		"acme/web": {"repo-metadata", "commit-stats", "commits", "files"},
	}
	if got := h.ran(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ran %v, want %v", got, want)
	}
	if h.opened != 1 {
		t.Fatalf("the store is opened once for the whole batch, got %d", h.opened)
	}
	// --group names the organization, so --owner is not a user to list; the
	// pattern and the cap reach the listing as given.
	listing := h.listings[0].GitHub
	if h.listings[0].GitLab != nil || listing == nil || listing.Org != "acme-org" || listing.User != "" ||
		listing.Pattern != "acme/*" || listing.MaxRepos == nil || *listing.MaxRepos != 5 {
		t.Fatalf("listing = %+v", h.listings[0])
	}
	run := h.runs[0]
	if run.OrgID != "org-1" || run.Provider != "github" || run.Credential["token"] != "ghp-very-secret" ||
		run.SourceName != run.SourceExternalID || run.SinceAt == nil || run.SinceAt.Format("2006-01-02") != "2026-01-02" {
		t.Fatalf("run = %+v", run)
	}
	if h.listRuns[0].Credential["token"] != "ghp-very-secret" {
		t.Fatalf("the listing must use the run's credential: %+v", h.listRuns[0])
	}
}

func TestBatchOwnerIsTheUserToListOnlyWithoutAGroup(t *testing.T) {
	h := &batchHarness{repos: githubRepos("acme/api")}
	if code, _, stderr := runVerb(t, "prs", h.executor(), append([]string{"--owner", "acme"}, githubBatchArgs...), inlineEnv); code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	listing := h.listings[0].GitHub
	if listing.Org != "" || listing.User != "acme" || listing.MaxRepos != nil {
		t.Fatalf("listing = %+v, want user acme and no cap", listing)
	}
}

func TestBatchGitLabRunsEachProjectByNumericID(t *testing.T) {
	h := &batchHarness{repos: []providersync.ListedRepository{
		{Name: "api", FullName: "acme/api", ProjectID: 11},
		{Name: "web", FullName: "acme/web", ProjectID: 12},
	}}
	code, _, stderr := runVerb(t, "incidents", h.executor(),
		[]string{"--provider", "gitlab", "-s", "acme/*", "--auth", "glpat-secret", "--gitlab-url", "https://gl.example", "--group", "acme"}, inlineEnv)
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if got, want := h.ran(), map[string][]string{"11": {"incidents"}, "12": {"incidents"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ran %v, want %v", got, want)
	}
	listing := h.listings[0].GitLab
	if h.listings[0].GitHub != nil || listing == nil || listing.Group != "acme" || listing.Pattern != "acme/*" || listing.MaxProjects != nil {
		t.Fatalf("listing = %+v", h.listings[0])
	}
	if run := h.runs[0]; run.Config["base_url"] != "https://gl.example" || run.Credential["token"] != "glpat-secret" || run.SourceName != "acme/api" && run.SourceName != "acme/web" {
		t.Fatalf("run = %+v", run)
	}
}

// TestBatchProcessesChunksInOrderAndBoundsConcurrency: repositories run in
// chunks of --batch-size (a chunk finishes before the next starts), never more
// than --max-concurrent at a time.
func TestBatchProcessesChunksInOrderAndBoundsConcurrency(t *testing.T) {
	var names []string
	for i := 0; i < 7; i++ {
		names = append(names, fmt.Sprintf("acme/r%d", i))
	}
	release := make(chan struct{})
	entered := make(chan string, 64)
	h := &batchHarness{repos: githubRepos(names...)}
	h.hold = func(run providersync.InProcessRun) {
		if run.Dataset != "prs" {
			return
		}
		entered <- run.SourceExternalID
		<-release
	}
	done := make(chan int)
	go func() {
		code, _, _ := runVerb(t, "prs", h.executor(), append([]string{"--batch-size", "3", "--max-concurrent", "2"}, githubBatchArgs...), inlineEnv)
		done <- code
	}()
	// Chunk 1 is r0..r2, two at a time: exactly two are in flight until one is released.
	first := []string{<-entered, <-entered}
	select {
	case name := <-entered:
		t.Fatalf("%s started while two were in flight with --max-concurrent 2", name)
	case <-time.After(200 * time.Millisecond): // a negative check needs a grace period for a wrong start to show
	}
	release <- struct{}{}
	third := <-entered
	got := map[string]bool{first[0]: true, first[1]: true, third: true}
	if !got["acme/r0"] || !got["acme/r1"] || !got["acme/r2"] {
		t.Fatalf("chunk 1 = %v, want r0..r2 before any later repository", got)
	}
	close(release)
	if code := <-done; code != cli.ExitOK {
		t.Fatalf("exit %d", code)
	}
	if h.peak > 2 {
		t.Fatalf("peak concurrency %d exceeds --max-concurrent 2", h.peak)
	}
	// A repository of a later chunk starts only after every earlier chunk ended.
	ends := map[string]int{}
	for i, event := range h.events {
		if name, ok := strings.CutPrefix(event, "end:"); ok {
			ends[name] = i
		}
	}
	for i, event := range h.events {
		name, ok := strings.CutPrefix(event, "start:")
		if !ok {
			continue
		}
		for later, earlier := range map[string][]string{
			"acme/r3": {"acme/r0", "acme/r1", "acme/r2"}, "acme/r4": {"acme/r0", "acme/r1", "acme/r2"}, "acme/r5": {"acme/r0", "acme/r1", "acme/r2"},
			"acme/r6": {"acme/r0", "acme/r1", "acme/r2", "acme/r3", "acme/r4", "acme/r5"},
		} {
			if name != later {
				continue
			}
			for _, before := range earlier {
				if ends[before] > i {
					t.Fatalf("%s started (event %d) before %s ended (event %d): chunks must not overlap", name, i, before, ends[before])
				}
			}
		}
	}
}

func TestBatchFloorsNonPositiveBatchSizeAndConcurrencyAtOne(t *testing.T) {
	h := &batchHarness{repos: githubRepos("acme/a", "acme/b")}
	code, _, stderr := runVerb(t, "prs", h.executor(),
		append([]string{"--batch-size", "0", "--max-concurrent", "-3", "--use-async", "--rate-limit-delay", "0.5"}, githubBatchArgs...), inlineEnv)
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if len(h.ran()) != 2 || h.peak != 1 {
		t.Fatalf("ran %v peak %d, want both repositories, one at a time", h.ran(), h.peak)
	}
}

// TestBatchFailuresAreIndependentReportedAndExitOne is the Go-worker rule for
// the batch: no swallowed error, each dataset and each repository independent.
func TestBatchFailuresAreIndependentReportedAndExitOne(t *testing.T) {
	h := &batchHarness{repos: githubRepos("acme/a", "acme/b", "acme/c")}
	h.fail = func(run providersync.InProcessRun) error {
		if run.SourceExternalID == "acme/b" && run.Dataset == "commit-stats" {
			return errors.New("provider said no; token ghp-very-secret dsn " + inlineEnv["CLICKHOUSE_URI"])
		}
		if run.SourceExternalID == "acme/c" && run.Dataset == "repo-metadata" {
			return errors.New("metadata refused")
		}
		return nil
	}
	code, _, stderr := runVerb(t, "git", h.executor(), githubBatchArgs, inlineEnv)
	if code != cli.ExitFailure {
		t.Fatalf("exit %d, want 1 when any repository failed: %s", code, stderr)
	}
	all := []string{"repo-metadata", "commit-stats", "commits", "files"}
	want := map[string][]string{"acme/a": all, "acme/b": all, "acme/c": all}
	if got := h.ran(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ran %v, want every dataset of every repository despite the failures", got)
	}
	for _, need := range []string{"acme/b: commit-stats: provider said no", "acme/c: repo-metadata: metadata refused", "2 of 3 repositories failed (2 dataset runs)"} {
		if !strings.Contains(stderr, need) {
			t.Fatalf("stderr %q missing %q", stderr, need)
		}
	}
	for _, secret := range []string{"ghp-very-secret", "ch-secret"} {
		if strings.Contains(stderr, secret) {
			t.Fatalf("stderr echoes %q: %q", secret, stderr)
		}
	}
}

func TestBatchListingFailureFailsBeforeAnyRunAndHidesTheToken(t *testing.T) {
	h := &batchHarness{listErr: errors.New("GET /orgs/acme/repos with ghp-very-secret: 401")}
	code, _, stderr := runVerb(t, "git", h.executor(), githubBatchArgs, inlineEnv)
	if code != cli.ExitFailure || len(h.runs) != 0 {
		t.Fatalf("exit %d runs %d: %s", code, len(h.runs), stderr)
	}
	if !strings.Contains(stderr, "list repositories") || strings.Contains(stderr, "ghp-very-secret") {
		t.Fatalf("stderr %q must name the listing stage and hide the token", stderr)
	}
}

func TestBatchWithNothingMatchedSucceedsAndSaysSo(t *testing.T) {
	h := &batchHarness{}
	code, _, stderr := runVerb(t, "git", h.executor(), githubBatchArgs, inlineEnv)
	if code != cli.ExitOK || len(h.runs) != 0 || !strings.Contains(stderr, `no repositories matched "acme/*"`) {
		t.Fatalf("exit %d runs %d stderr %q", code, len(h.runs), stderr)
	}
}

func TestBatchReportsAListedItemItCannotRun(t *testing.T) {
	h := &batchHarness{repos: []providersync.ListedRepository{{Name: "orphan"}, {Name: "ok", FullName: "acme/ok"}}}
	code, _, stderr := runVerb(t, "prs", h.executor(), githubBatchArgs, inlineEnv)
	if code != cli.ExitFailure || !strings.Contains(stderr, "orphan: listing: the listed repository has no full name") || !strings.Contains(stderr, "1 of 2 repositories failed") {
		t.Fatalf("exit %d stderr %q", code, stderr)
	}
	if got := h.ran(); !reflect.DeepEqual(got, map[string][]string{"acme/ok": {"prs"}}) {
		t.Fatalf("ran %v, want the runnable repository still synced", got)
	}
}

// TestBatchRefusalsRunAndListNothing: what single mode refuses, batch refuses
// before it lists or opens the store.
func TestBatchRefusalsRunAndListNothing(t *testing.T) {
	cases := []struct {
		name, target string
		args         []string
		env          map[string]string
		code         int
		want         string
	}{
		{"github incidents", "incidents", githubBatchArgs, inlineEnv, cli.ExitFailure, githubIncidentsRefusal},
		{"chunked cicd", "cicd", githubBatchArgs, inlineEnv, cli.ExitRefused, ticketChunked},
		{"chunked tests", "tests", []string{"--provider", "gitlab", "-s", "a/*", "--auth", "t"}, inlineEnv, cli.ExitRefused, ticketChunked},
		{"no org", "git", githubBatchArgs, map[string]string{"CLICKHOUSE_URI": inlineEnv["CLICKHOUSE_URI"]}, cli.ExitRefused, ticketDBLookups},
		{"db credentials", "git", []string{"--provider", "github", "-s", "a/*"},
			map[string]string{"CLICKHOUSE_URI": inlineEnv["CLICKHOUSE_URI"], "ORG_ID": "o", "POSTGRES_URI": "postgresql://p"}, cli.ExitRefused, ticketDBLookups},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &batchHarness{repos: githubRepos("acme/a")}
			code, _, stderr := runVerb(t, tc.target, h.executor(), tc.args, tc.env)
			if code != tc.code || !strings.Contains(stderr, tc.want) {
				t.Fatalf("exit %d stderr %q, want exit %d naming %q", code, stderr, tc.code, tc.want)
			}
			if len(h.listings) != 0 || len(h.runs) != 0 || h.opened != 0 {
				t.Fatalf("a refusal must list, run and open nothing: %d/%d/%d", len(h.listings), len(h.runs), h.opened)
			}
		})
	}
}

func TestClampIntSaturatesInsteadOfWrapping(t *testing.T) {
	huge, _ := new(big.Int).SetString("99999999999999999999999", 10)
	if got := clampInt(huge); got != 1<<31-1 {
		t.Fatalf("clampInt(huge) = %d", got)
	}
	if got := clampInt(huge.Neg(huge)); got != -(1 << 31) {
		t.Fatalf("clampInt(-huge) = %d", got)
	}
}
