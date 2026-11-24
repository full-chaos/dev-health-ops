package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// prefetchCellDoer serves runs of GitHub artifacts (id = run*100 + k) through the /zip redirect and the blob host, and
// records every request's start and end in order, the in-flight count, and which artifact downloads overlapped.
type prefetchCellDoer struct {
	t      *testing.T
	runs   int
	perRun int
	listed map[int]int64 // listed size_in_bytes; missing = 1000
	// noLocation: the /zip answers 302 with no Location (a skip-class failure).
	noLocation map[int]bool
	// readError: the blob body fails mid-read (a partial body).
	readError map[int]bool
	// zipTransportError: every /zip attempt fails in transport (a fatal download).
	zipTransportError map[int]bool
	// zipFirst: the first /zip answer of the id (status, Retry-After, body); later answers redirect.
	zipFirst map[int]prefetchCellAnswer
	// unsafeBounds: the archive opens and holds more reports than a run allows (the :3105 fatal return).
	unsafeBounds map[int]bool
	// holdBlob: the blob call of key is held until the /zip of value starts, or a 2 s liveness guard passes.
	holdBlob map[int]int
	latency  func(id int) time.Duration

	mu          sync.Mutex
	events      []string
	startedAt   map[string]time.Time
	zipStarted  map[int]chan struct{}
	firstServed map[int]bool
	active      map[int]int // artifact id -> its requests in flight
	overlaps    map[[2]int]bool
	inFlight    int
	maxInFlight int
	requests    map[string]int
}

type prefetchCellAnswer struct {
	status     int
	retryAfter string
	body       string
}

func newPrefetchCellDoer(t *testing.T, runs, perRun int) *prefetchCellDoer {
	return &prefetchCellDoer{
		t: t, runs: runs, perRun: perRun, listed: map[int]int64{}, noLocation: map[int]bool{}, readError: map[int]bool{},
		zipTransportError: map[int]bool{}, zipFirst: map[int]prefetchCellAnswer{}, unsafeBounds: map[int]bool{},
		holdBlob: map[int]int{}, startedAt: map[string]time.Time{}, zipStarted: map[int]chan struct{}{},
		firstServed: map[int]bool{}, active: map[int]int{}, overlaps: map[[2]int]bool{}, requests: map[string]int{},
	}
}

func (doer *prefetchCellDoer) zipStartedChannel(id int) chan struct{} {
	channel, ok := doer.zipStarted[id]
	if !ok {
		channel = make(chan struct{})
		doer.zipStarted[id] = channel
	}
	return channel
}

func (doer *prefetchCellDoer) begin(label string, id int) {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	doer.events = append(doer.events, "start:"+label)
	doer.startedAt[label+"#"+strconv.Itoa(len(doer.events))] = time.Now()
	doer.requests[label]++
	doer.inFlight++
	if doer.inFlight > doer.maxInFlight {
		doer.maxInFlight = doer.inFlight
	}
	if id > 0 {
		for other, n := range doer.active {
			if other != id && n > 0 {
				pair := [2]int{min(id, other), max(id, other)}
				doer.overlaps[pair] = true
			}
		}
		doer.active[id]++
		if strings.HasPrefix(label, "zip:") {
			channel := doer.zipStartedChannel(id)
			select {
			case <-channel:
			default:
				close(channel)
			}
		}
	}
}

func (doer *prefetchCellDoer) end(label string, id int) {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	doer.events = append(doer.events, "end:"+label)
	doer.inFlight--
	if id > 0 {
		doer.active[id]--
	}
}

func (doer *prefetchCellDoer) listedSize(id int) int64 {
	if size, ok := doer.listed[id]; ok {
		return size
	}
	return 1000
}

func (doer *prefetchCellDoer) artifactsFixture(run int) string {
	var body strings.Builder
	body.WriteString(`{"artifacts":[`)
	for k := 1; k <= doer.perRun; k++ {
		id := run*100 + k
		if k > 1 {
			body.WriteByte(',')
		}
		size := doer.listedSize(id)
		sizeJSON := strconv.FormatInt(size, 10)
		if size < 0 {
			sizeJSON = "null"
		}
		fmt.Fprintf(&body, `{"id":%d,"name":"test-results-%d","expired":false,"size_in_bytes":%s}`, id, id, sizeJSON)
	}
	body.WriteString(`]}`)
	return body.String()
}

func (doer *prefetchCellDoer) Do(request *http.Request) (*http.Response, error) {
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		doer.begin("repo", 0)
		defer doer.end("repo", 0)
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		doer.begin("runs", 0)
		defer doer.end("runs", 0)
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, doer.runs)), nil
	case strings.HasSuffix(path, "/jobs"):
		doer.begin("jobs", 0)
		defer doer.end("jobs", 0)
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		segments := strings.Split(strings.Trim(path, "/"), "/")
		run, err := strconv.Atoi(segments[len(segments)-2])
		if err != nil {
			doer.t.Errorf("artifacts path %q", path)
		}
		label := "list:" + strconv.Itoa(run)
		doer.begin(label, 0)
		defer doer.end(label, 0)
		return githubTestsHTTPResponse(request, header, doer.artifactsFixture(run)), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id := githubTestsArtifactIDFromPath(doer.t, path)
		label := "zip:" + strconv.Itoa(id)
		doer.begin(label, id)
		defer doer.end(label, id)
		if doer.latency != nil {
			time.Sleep(doer.latency(id))
		}
		if doer.zipTransportError[id] {
			return nil, errors.New("prefetch cell: transport reset")
		}
		doer.mu.Lock()
		answer, first := doer.zipFirst[id]
		served := doer.firstServed[id]
		doer.firstServed[id] = true
		doer.mu.Unlock()
		if first && !served {
			answerHeader := http.Header{"Content-Type": {"application/json"}}
			if answer.retryAfter != "" {
				answerHeader.Set("Retry-After", answer.retryAfter)
			}
			return &http.Response{StatusCode: answer.status, Header: answerHeader, Body: io.NopCloser(strings.NewReader(answer.body)), Request: request}, nil
		}
		redirect := http.Header{}
		if !doer.noLocation[id] {
			redirect.Set("Location", "https://blob.example/"+strconv.Itoa(id)+".zip")
		}
		return &http.Response{StatusCode: http.StatusFound, Header: redirect, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
	case request.URL.Host == "blob.example":
		id := githubTestsArtifactIDFromBlobPath(doer.t, path)
		label := "blob:" + strconv.Itoa(id)
		doer.begin(label, id)
		defer doer.end(label, id)
		if wait, ok := doer.holdBlob[id]; ok {
			doer.mu.Lock()
			channel := doer.zipStartedChannel(wait)
			doer.mu.Unlock()
			select {
			case <-channel:
			case <-time.After(2 * time.Second): // liveness guard only: turns a serial walk's wait into a red, not a hang
			}
		}
		if doer.latency != nil {
			time.Sleep(doer.latency(id))
		}
		blobHeader := http.Header{"Content-Type": {"application/zip"}}
		if doer.readError[id] {
			return &http.Response{StatusCode: http.StatusOK, Header: blobHeader, Body: io.NopCloser(&githubTestsErrorReader{}), Request: request}, nil
		}
		members := map[string]string{"junit.xml": githubTestsNamedMultiSuiteJUnit("a"+strconv.Itoa(id)+"-", 1)}
		if doer.unsafeBounds[id] {
			members = make(map[string]string, githubTestsMaxReportsPerRun+1)
			for i := 0; i <= githubTestsMaxReportsPerRun; i++ {
				members["report-"+strconv.Itoa(i)+".xml"] = githubTestsMultiSuiteJUnit(1)
			}
		}
		return &http.Response{StatusCode: http.StatusOK, Header: blobHeader, Body: io.NopCloser(bytes.NewReader(githubTestsZip(doer.t, members))), Request: request}, nil
	default:
		doer.t.Errorf("unexpected request %s", request.URL.String())
		return nil, errors.New("unexpected request")
	}
}

func (doer *prefetchCellDoer) eventIndex(event string) int {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	for index, seen := range doer.events {
		if seen == event {
			return index
		}
	}
	return -1
}

func (doer *prefetchCellDoer) wire() int {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	total := 0
	for _, n := range doer.requests {
		total += n
	}
	return total
}

// prefetchCellStore is a budget store with the ValkeyBudgetStore Lua semantics (deny when the key's count reaches its
// limit, never wait). denyPrefetch denies that many acquires of a "-prefetch" key first.
type prefetchCellStore struct {
	mu           sync.Mutex
	count        map[string]int
	max          map[string]int
	denied       map[string]int
	acquired     map[string]int
	denyPrefetch int
}

type prefetchCellReservation struct {
	store *prefetchCellStore
	key   string
	once  sync.Once
}

func newPrefetchCellStore() *prefetchCellStore {
	return &prefetchCellStore{count: map[string]int{}, max: map[string]int{}, denied: map[string]int{}, acquired: map[string]int{}}
}

func (store *prefetchCellStore) Acquire(_ context.Context, key providerfoundation.BudgetKey) (providerfoundation.Reservation, error) {
	if key.Validate() != nil {
		return nil, providerfoundation.ErrBudgetUnavailable
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	name := key.String()
	if strings.HasSuffix(key.CostClass, "-prefetch") && store.denyPrefetch > 0 {
		store.denyPrefetch--
		store.denied[name]++
		return nil, providerfoundation.ErrBudgetContended
	}
	if store.count[name] >= key.Limit {
		store.denied[name]++
		return nil, providerfoundation.ErrBudgetContended
	}
	store.count[name]++
	store.acquired[name]++
	store.max[name] = max(store.max[name], store.count[name])
	return &prefetchCellReservation{store: store, key: name}, nil
}

func (reservation *prefetchCellReservation) Release(context.Context) error {
	reservation.once.Do(func() {
		reservation.store.mu.Lock()
		defer reservation.store.mu.Unlock()
		reservation.store.count[reservation.key]--
	})
	return nil
}

func (store *prefetchCellStore) snapshot() (counts, maxes, denied, acquired map[string]int) {
	store.mu.Lock()
	defer store.mu.Unlock()
	clone := func(source map[string]int) map[string]int {
		out := make(map[string]int, len(source))
		for key, value := range source {
			out[key] = value
		}
		return out
	}
	return clone(store.count), clone(store.max), clone(store.denied), clone(store.acquired)
}

const (
	prefetchCellMediumKey   = "provider_budget:github:f0f0f0f0:api.github.com:medium"
	prefetchCellPrefetchKey = "provider_budget:github:f0f0f0f0:api.github.com:medium-prefetch"
)

func prefetchCellClient(t *testing.T, doer *prefetchCellDoer, budget providerfoundation.BudgetStore, class string, attempts int) *providerfoundation.HTTPClient {
	t.Helper()
	client := githubTestsClient(t, fakehttp.Client(doer))
	client.Retry = providerfoundation.RetryPolicy{MaxAttempts: attempts, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}
	client.Metrics = providerfoundation.NewMetrics()
	client.Budget = budget
	limit, _ := providerfoundation.CostClassBudgetLimit(class)
	client.BudgetKey = providerfoundation.BudgetKey{Provider: "github", OrgID: "f0f0f0f0", Host: "api.github.com", CostClass: class, Limit: limit, TTL: time.Minute}
	return client
}

// prefetchCellWalk drives the cicd chunked route to its final emission and returns every emission (batch, cursors,
// final flag) as JSON lines, and the route error.
func prefetchCellWalk(t *testing.T, client *providerfoundation.HTTPClient) ([]string, error) {
	t.Helper()
	claim := nativeTestClaim("github", "cicd")
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	var emissions []string
	resume := ""
	for pass := 0; pass < 200; pass++ {
		last, finalSeen := resume, false
		err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				encoded, marshalErr := json.Marshal(struct {
					Batch                     CompleteRouteBatch
					CursorBefore, CursorAfter string
					Final, Barrier            bool
				}{emission.Batch, emission.CursorBefore, emission.CursorAfter, emission.Final, emission.Barrier})
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				emissions = append(emissions, string(encoded))
				last = emission.CursorAfter
				if emission.Final {
					finalSeen = true
				}
				return nil
			})
		if finalSeen || err != nil {
			return emissions, err
		}
		resume = last
	}
	t.Fatal("the walk never reached a final emission")
	return nil, nil
}

func prefetchCellEqual(t *testing.T, name string, got, want []string) {
	t.Helper()
	if strings.Join(got, "\n") == strings.Join(want, "\n") {
		return
	}
	if len(got) != len(want) {
		t.Fatalf("%s: %d emissions, want %d (the serial walk)", name, len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: emission %d differs from the serial walk:\n got %s\nwant %s", name, i, got[i], want[i])
		}
	}
}

func prefetchCellBudgetDenied(t *testing.T, client *providerfoundation.HTTPClient) string {
	t.Helper()
	var text bytes.Buffer
	if err := client.Metrics.WritePrometheus(&text); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(text.String(), "\n") {
		if strings.HasPrefix(line, `dev_health_provider_budget_denied_total{provider="github"}`) {
			return strings.TrimSpace(strings.TrimPrefix(line, `dev_health_provider_budget_denied_total{provider="github"}`))
		}
	}
	return "absent"
}

// R1, red on the serial walk: while the walk reads artifact 101's blob, the next artifact's /zip has already started.
// The WANT is request ORDER; the blob hold's 2 s guard only turns the serial walk's wait into a red.
func TestGitHubTestsPrefetchStartsTheNextArtifactWhileTheWalkReadsTheCurrentOne(t *testing.T) {
	serialDoer := newPrefetchCellDoer(t, 1, 3)
	serial, err := prefetchCellWalk(t, prefetchCellClient(t, serialDoer, nil, "medium", 1))
	if err != nil {
		t.Fatal(err)
	}
	doer := newPrefetchCellDoer(t, 1, 3)
	doer.holdBlob[101] = 102
	got, err := prefetchCellWalk(t, prefetchCellClient(t, doer, newPrefetchCellStore(), "medium", 1))
	if err != nil {
		t.Fatal(err)
	}
	zipNext, blobEnd := doer.eventIndex("start:zip:102"), doer.eventIndex("end:blob:101")
	t.Logf("events: %v", doer.events)
	if zipNext < 0 || blobEnd < 0 || zipNext > blobEnd {
		t.Fatalf("start:zip:102 at %d, end:blob:101 at %d: want the next /zip to start before the current blob ends", zipNext, blobEnd)
	}
	prefetchCellEqual(t, "R1", got, serial)
}

// C1-C3, P2-C: the in-flight bound per key and where the prefetch never runs.
func TestGitHubTestsPrefetchBound(t *testing.T) {
	for _, tc := range []struct {
		name          string
		class         string
		budget        func() providerfoundation.BudgetStore
		wantMax       int
		wantPrefetchN bool
	}{
		{"medium: walk 1 slot of the medium key + 1 prefetch slot", "medium", func() providerfoundation.BudgetStore { return newPrefetchCellStore() }, 2, true},
		{"heavy (tests): no prefetch row, serial", "heavy", func() providerfoundation.BudgetStore { return newPrefetchCellStore() }, 1, false},
		{"no budget: serial", "medium", func() providerfoundation.BudgetStore { return nil }, 1, false},
		{"in-process blocking store (dho sync): serial", "medium", func() providerfoundation.BudgetStore { return &InProcessBudgetStore{} }, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doer := newPrefetchCellDoer(t, 1, 6)
			doer.latency = func(int) time.Duration { return 2 * time.Millisecond }
			if tc.wantMax == 2 {
				doer.holdBlob[101] = 102 // makes the overlap certain; the 2 s guard only fires if it never comes
			}
			budget := tc.budget()
			if _, err := prefetchCellWalk(t, prefetchCellClient(t, doer, budget, tc.class, 1)); err != nil {
				t.Fatal(err)
			}
			if tc.wantMax == 1 && doer.maxInFlight != 1 {
				t.Fatalf("max in flight %d, want 1 (serial)", doer.maxInFlight)
			}
			if tc.wantMax == 2 && doer.maxInFlight != 2 {
				t.Fatalf("max in flight %d, want 2 (anti-vacuity: the prefetch overlapped)", doer.maxInFlight)
			}
			store, ok := budget.(*prefetchCellStore)
			if !ok {
				return
			}
			_, maxes, denied, acquired := store.snapshot()
			t.Logf("store max %v, denied %v, acquired %v, doer max in flight %d", maxes, denied, acquired, doer.maxInFlight)
			if maxes[prefetchCellMediumKey] > 1 && tc.class == "medium" {
				t.Fatalf("the walk held %d medium slots, want <= 1", maxes[prefetchCellMediumKey])
			}
			if maxes[prefetchCellPrefetchKey] > 1 {
				t.Fatalf("prefetch slots %d, want <= 1", maxes[prefetchCellPrefetchKey])
			}
			if (acquired[prefetchCellPrefetchKey] > 0) != tc.wantPrefetchN {
				t.Fatalf("prefetch acquires %d, want prefetch = %v", acquired[prefetchCellPrefetchKey], tc.wantPrefetchN)
			}
		})
	}
}

// C1b (pm P1-2): a sibling medium unit of the same bucket calls the provider for the whole walk: never denied.
func TestGitHubTestsPrefetchLeavesTheSiblingMediumUnitItsSlot(t *testing.T) {
	doer := newPrefetchCellDoer(t, 2, 6)
	doer.latency = func(int) time.Duration { return 3 * time.Millisecond }
	store := newPrefetchCellStore()
	walkClient := prefetchCellClient(t, doer, store, "medium", 1)
	siblingDoer := newPrefetchCellDoer(t, 1, 1)
	sibling := prefetchCellClient(t, siblingDoer, store, "medium", 1)
	var stop atomic.Bool
	var siblingCalls, siblingDenied atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		for !stop.Load() {
			response, err := sibling.DoRoute(context.Background(), http.MethodGet, "/repos/{owner}/{repo}", "/repos/acme/api", nil)
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			siblingCalls.Add(1)
			if errors.Is(err, providerfoundation.ErrBudgetContended) {
				siblingDenied.Add(1)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	_, err := prefetchCellWalk(t, walkClient)
	stop.Store(true)
	<-done
	if err != nil {
		t.Fatal(err)
	}
	_, maxes, _, acquired := store.snapshot()
	t.Logf("sibling calls %d, denied %d; store max %v, acquired %v; walk max in flight %d", siblingCalls.Load(), siblingDenied.Load(), maxes, acquired, doer.maxInFlight)
	if acquired[prefetchCellPrefetchKey] == 0 || siblingCalls.Load() < 10 {
		t.Fatalf("vacuous: prefetch acquires %d, sibling calls %d", acquired[prefetchCellPrefetchKey], siblingCalls.Load())
	}
	if siblingDenied.Load() != 0 {
		t.Fatalf("the sibling medium unit was denied %d times, want 0", siblingDenied.Load())
	}
}

// C4, C5a, C5f, C8, run boundary: every emission equals the serial walk's; the listed-size rule and the run boundary
// hold under random latency.
func TestGitHubTestsPrefetchEmissionsEqualTheSerialWalk(t *testing.T) {
	capBytes := githubTestsMaxDownloadSize
	configure := func(doer *prefetchCellDoer) {
		doer.noLocation[102] = true                       // skip class
		doer.listed[105] = int64(float64(capBytes) * 0.6) // 105 + 106 exceed the cap together
		doer.listed[106] = int64(float64(capBytes) * 0.6)
		doer.listed[201] = -1          // unknown size
		doer.listed[203] = 166_407_486 // above the cap (sitting 3 max): range or refusal path
		doer.listed[204] = 88_559_868  // sitting 3 max artifact
	}
	serialDoer := newPrefetchCellDoer(t, 2, 6)
	configure(serialDoer)
	serialClient := prefetchCellClient(t, serialDoer, nil, "medium", 1)
	serialClient.Ledger = providerfoundation.NewRequestLedger()
	serial, serialErr := prefetchCellWalk(t, serialClient)
	if serialErr != nil {
		t.Fatalf("the serial walk failed: %v (the fixture must walk both runs to the end)", serialErr)
	}
	serialLedger, _ := serialClient.Ledger.Snapshot()
	for seed := int64(1); seed <= 5; seed++ {
		random := rand.New(rand.NewSource(seed))
		delays := map[int]time.Duration{}
		for run := 1; run <= 2; run++ {
			for k := 1; k <= 6; k++ {
				delays[run*100+k] = time.Duration(random.Intn(3000)) * time.Microsecond
			}
		}
		doer := newPrefetchCellDoer(t, 2, 6)
		configure(doer)
		doer.latency = func(id int) time.Duration { return delays[id] }
		doer.holdBlob[101] = 102
		store := newPrefetchCellStore()
		client := prefetchCellClient(t, doer, store, "medium", 1)
		client.Ledger = providerfoundation.NewRequestLedger()
		got, err := prefetchCellWalk(t, client)
		// The prefetch's requests reach the unit's request ledger: the same entries as the serial walk's.
		if ledger, _ := client.Ledger.Snapshot(); fmt.Sprint(ledger) != fmt.Sprint(serialLedger) || len(ledger) == 0 {
			t.Fatalf("seed %d: unit ledger %v, want the serial walk's %v", seed, ledger, serialLedger)
		}
		if fmt.Sprint(err) != fmt.Sprint(serialErr) {
			t.Fatalf("seed %d: err %v, want the serial walk's %v", seed, err, serialErr)
		}
		// The walk's order is the sort by declared size: run 1 = 101-104, 105, 106; run 2 = 202, 205, 206, 204 (88.6 MB),
		// 203 (166 MB, above the cap), 201 (unknown size, last). Prefetched beside the walk's own download: 102 (a
		// skip-class failure), 104, 205, 204. Never: 106 (105 + 106 pass the cap), 201 (unknown), 203 (above the cap).
		if _, _, _, acquired := store.snapshot(); acquired[prefetchCellPrefetchKey] != 4 {
			t.Fatalf("seed %d: %d prefetches, want 4 (102, 104, 205, 204)", seed, acquired[prefetchCellPrefetchKey])
		}
		prefetchCellEqual(t, fmt.Sprintf("seed %d", seed), got, serial)
		var pairs []string
		for pair := range doer.overlaps {
			pairs = append(pairs, fmt.Sprint(pair))
			if pair[0]/100 != pair[1]/100 {
				t.Fatalf("seed %d: artifacts %v of two runs overlapped (a prefetch crossed a run)", seed, pair)
			}
		}
		sort.Strings(pairs)
		// Only a walk download and its prefetch may overlap. 101/102 always overlap (the blob hold).
		allowed := map[string]bool{"[101 102]": true, "[103 104]": true, "[202 205]": true, "[204 206]": true}
		for _, pair := range pairs {
			if !allowed[pair] {
				t.Fatalf("seed %d: %s overlapped, want only %v", seed, pair, allowed)
			}
		}
		if !doer.overlaps[[2]int{101, 102}] {
			t.Fatalf("seed %d: 101 and 102 did not overlap (vacuous)", seed)
		}
		t.Logf("seed %d: overlapped %v", seed, pairs)
	}
}

// C5b, C5h: a fatal artifact while the next one is prefetched: the serial error, nothing in flight, every slot back.
func TestGitHubTestsPrefetchFatalArtifactClosesThePrefetch(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*prefetchCellDoer)
	}{
		{"transport error on the /zip (a fatal download)", func(doer *prefetchCellDoer) { doer.zipTransportError[103] = true }},
		{"unsafe archive bounds (the :3105 return)", func(doer *prefetchCellDoer) { doer.unsafeBounds[103] = true }},
		{"partial body of the walk's own blob", func(doer *prefetchCellDoer) { doer.readError[103] = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serialDoer := newPrefetchCellDoer(t, 1, 4)
			tc.configure(serialDoer)
			_, serialErr := prefetchCellWalk(t, prefetchCellClient(t, serialDoer, nil, "medium", 1))
			if serialErr == nil {
				t.Fatal("the serial walk did not fail: the fixture is wrong")
			}
			doer := newPrefetchCellDoer(t, 1, 4)
			tc.configure(doer)
			// The walk downloads 103 itself (102 was prefetched) and 104 is prefetched beside it, slow: the run ends
			// with 104 in flight.
			doer.latency = func(id int) time.Duration {
				if id == 104 {
					return 20 * time.Millisecond
				}
				return 0
			}
			store := newPrefetchCellStore()
			_, err := prefetchCellWalk(t, prefetchCellClient(t, doer, store, "medium", 1))
			if fmt.Sprint(err) != fmt.Sprint(serialErr) {
				t.Fatalf("err %v, want the serial walk's %v", err, serialErr)
			}
			counts, _, _, acquired := store.snapshot()
			doer.mu.Lock()
			inFlight := doer.inFlight
			doer.mu.Unlock()
			if inFlight != 0 {
				t.Fatalf("%d requests in flight at return, want 0", inFlight)
			}
			for key, n := range counts {
				if n != 0 {
					t.Fatalf("budget %s holds %d slots at return, want 0", key, n)
				}
			}
			if acquired[prefetchCellPrefetchKey] < 2 {
				t.Fatalf("prefetch acquires %d, want 2 (102, then 104 in flight at the fatal return)", acquired[prefetchCellPrefetchKey])
			}
			extra := doer.wire() - serialDoer.wire()
			t.Logf("wire: serial %d, prefetch %d (extra %d); prefetch acquires %d", serialDoer.wire(), doer.wire(), extra, acquired[prefetchCellPrefetchKey])
			// The bound (pm P2-A): one artifact's whole download; here /zip + blob.
			if extra < 0 || extra > 2 {
				t.Fatalf("extra wire requests %d, want 0..2 (one prefetched download)", extra)
			}
		})
	}
}

// C5c (pm P2-3, P2-B): a denied prefetch slot: the walk downloads that artifact itself, the emissions and the request
// count equal the serial walk's, and the denial never reaches the unit budget-denied series.
func TestGitHubTestsPrefetchDeniedSlotStaysSerialAndIsNoUnitDenial(t *testing.T) {
	serialDoer := newPrefetchCellDoer(t, 1, 4)
	serial, err := prefetchCellWalk(t, prefetchCellClient(t, serialDoer, nil, "medium", 1))
	if err != nil {
		t.Fatal(err)
	}
	doer := newPrefetchCellDoer(t, 1, 4)
	store := newPrefetchCellStore()
	store.denyPrefetch = 1
	client := prefetchCellClient(t, doer, store, "medium", 1)
	got, err := prefetchCellWalk(t, client)
	if err != nil {
		t.Fatal(err)
	}
	prefetchCellEqual(t, "C5c", got, serial)
	_, _, denied, acquired := store.snapshot()
	if denied[prefetchCellPrefetchKey] != 1 || acquired[prefetchCellPrefetchKey] == 0 {
		t.Fatalf("prefetch denied %d, acquired %d: want 1 denial and later prefetches", denied[prefetchCellPrefetchKey], acquired[prefetchCellPrefetchKey])
	}
	if doer.wire() != serialDoer.wire() {
		t.Fatalf("wire %d, want the serial %d", doer.wire(), serialDoer.wire())
	}
	if got := prefetchCellBudgetDenied(t, client); got != "absent" && got != "0" {
		t.Fatalf("dev_health_provider_budget_denied_total{provider=\"github\"} = %s, want no unit denial", got)
	}
}

// prefetchCellGate is a backoff gate in memory: Penalize holds every caller until the penalty passes.
type prefetchCellGate struct {
	mu        sync.Mutex
	until     time.Time
	penalized []time.Time
}

func (gate *prefetchCellGate) Wait(context.Context) (time.Duration, error) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return max(time.Until(gate.until), 0), nil
}

func (gate *prefetchCellGate) Penalize(_ context.Context, delay time.Duration) error {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	gate.penalized = append(gate.penalized, time.Now())
	if until := time.Now().Add(delay); until.After(gate.until) {
		gate.until = until
	}
	return nil
}

// C5d, C5e, gate (pm): a 429 or a secondary 403 on one /zip penalizes the shared gate once, the prefetch's next call
// waits for it like the walk's, and the emissions equal the serial walk's.
func TestGitHubTestsPrefetchHonoursTheSharedGate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		answer prefetchCellAnswer
	}{
		{"429 with Retry-After", prefetchCellAnswer{status: http.StatusTooManyRequests, retryAfter: "1", body: `{"message":"slow down"}`}},
		{"403 secondary rate limit, no Retry-After", prefetchCellAnswer{status: http.StatusForbidden, body: `{"message":"You have exceeded a secondary rate limit. Please wait a few minutes before you try again."}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			walk := func(store providerfoundation.BudgetStore) (*prefetchCellDoer, *prefetchCellGate, []string, error) {
				doer := newPrefetchCellDoer(t, 1, 4)
				doer.zipFirst[103] = tc.answer
				gate := &prefetchCellGate{}
				client := prefetchCellClient(t, doer, store, "medium", 2)
				client.Gate = gate
				emissions, err := prefetchCellWalk(t, client)
				return doer, gate, emissions, err
			}
			_, serialGate, serial, serialErr := walk(nil)
			doer, gate, got, err := walk(newPrefetchCellStore())
			if fmt.Sprint(err) != fmt.Sprint(serialErr) {
				t.Fatalf("err %v, want the serial walk's %v", err, serialErr)
			}
			prefetchCellEqual(t, tc.name, got, serial)
			if len(gate.penalized) != len(serialGate.penalized) || len(gate.penalized) == 0 {
				t.Fatalf("gate penalized %d times (serial %d), want the serial count, at least 1", len(gate.penalized), len(serialGate.penalized))
			}
			// After the penalty, at most ONE request starts before it has passed (minus clock slack): the other
			// lane's call that had already passed the gate. Its next call, and every later one, waited at the shared
			// gate: a Penalize on the walk's key stops the prefetch's next start, and the other way round.
			penalty, until := gate.penalized[0], gate.until
			doer.mu.Lock()
			defer doer.mu.Unlock()
			var inWindow []string
			for label, started := range doer.startedAt {
				if started.After(penalty) && started.Before(until.Add(-20*time.Millisecond)) {
					inWindow = append(inWindow, fmt.Sprintf("%s +%v", label, started.Sub(penalty)))
				}
			}
			t.Logf("penalty %v; started inside it: %v", until.Sub(penalty), inWindow)
			if len(inWindow) > 1 {
				t.Fatalf("%d requests started inside the penalty, want at most 1 (one already past the gate): %v", len(inWindow), inWindow)
			}
		})
	}
}
