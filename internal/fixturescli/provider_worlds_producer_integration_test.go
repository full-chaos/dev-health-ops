//go:build integration

package fixturescli

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	syncplan "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/workerservice"
)

// The provider worlds of `fixtures generate` (gitlab, jira, linear, and github of the real shape) are not
// dumps of a generator: every row is written by the PRODUCTION producers. The producer below answers the
// provider requests of the real collectors and routes from a file of provider answers (a replay: a
// request no answer names FAILS the run, it is never answered empty), runs them in production order
// into a ClickHouse at the migration head under one fixed clock, runs the work graph's issue <-> pull
// request link builders the way the work-graph build runs them, and dumps every table the run wrote.
//
// The answers are constructed in the shape of the provider's real answers (the CHAOS-9124 convention:
// the shape is recorded, the values are made up), so no value of a real organization is in the tree.
//
// Without -freeze-provider-worlds the test is the freshness check: it runs the same chain and fails
// unless the run writes exactly the committed world, so a world can never drift from what its
// producers write today.
var freezeProviderWorlds = flag.Bool("freeze-provider-worlds", false, "write the provider worlds the producers write, instead of comparing them")

// providerAnswer is one provider answer: the request it answers and the answer.
type providerAnswer struct {
	Method string `json:"method"`
	Host   string `json:"host"`
	Path   string `json:"path"`
	// Query is the request's query in url.Values.Encode form (keys sorted); "*" answers any query.
	Query string `json:"query,omitempty"`
	// GraphQL, for a GraphQL POST, is a text the request's query document must contain, and
	// Variables, when set, the request's variables as JSON object (compared as values).
	GraphQL   string          `json:"graphql,omitempty"`
	Variables json.RawMessage `json:"variables,omitempty"`
	// Phase names the sync this answer belongs to ("" answers in every phase): a world that syncs its
	// catalog twice answers the first sync from the first phase's answers.
	Phase   string          `json:"phase,omitempty"`
	Status  int             `json:"status"`
	Headers http.Header     `json:"headers,omitempty"`
	Body    json.RawMessage `json:"body"`
	// Respond, when set, builds the body from the request (a GraphQL answer whose shape follows the
	// aliases the request asks for, a search answer that depends on its query); ok=false is "this
	// answer does not answer the request".
	Respond func(request replayRequest) (body any, ok bool) `json:"-"`
}

// replayRequest is what a Respond answer reads of a request.
type replayRequest struct {
	Query     url.Values
	Document  string // a GraphQL request's query document
	Variables json.RawMessage
}

// replayDoer answers every request from its answers and records each one. A request no answer names is
// answered 599 and recorded as unmatched; the producer fails the run on any.
type replayDoer struct {
	mu sync.Mutex
	// phase is the phase of the step that runs now.
	phase     string
	answers   []providerAnswer
	used      map[int]int
	unmatched []string
	// seen is every request line, in order: the trace a failed step prints.
	seen []string
}

func newReplayDoer(answers []providerAnswer) *replayDoer {
	return &replayDoer{answers: answers, used: map[int]int{}}
}

func canonicalQuery(raw string) string {
	values, err := url.ParseQuery(raw)
	if err != nil {
		return raw
	}
	return values.Encode()
}

func sameJSONValue(left, right []byte) bool {
	var a, b any
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return false
	}
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return bytes.Equal(ja, jb)
}

func (doer *replayDoer) Do(request *http.Request) (*http.Response, error) {
	var body []byte
	if request.Body != nil {
		read, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		body = read
	}
	var graphQL struct {
		Query     string          `json:"query"`
		Variables json.RawMessage `json:"variables"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &graphQL)
	}
	query := canonicalQuery(request.URL.RawQuery)
	doer.mu.Lock()
	defer doer.mu.Unlock()
	doer.seen = append(doer.seen, request.Method+" "+request.URL.Host+request.URL.Path+"?"+query)
	// An answer that names the exact query wins over one that answers any query.
	order := make([]int, 0, len(doer.answers))
	for index, answer := range doer.answers {
		if answer.Query != "*" {
			order = append(order, index)
		}
	}
	for index, answer := range doer.answers {
		if answer.Query == "*" {
			order = append(order, index)
		}
	}
	for _, index := range order {
		answer := doer.answers[index]
		if answer.Phase != "" && answer.Phase != doer.phase {
			continue
		}
		if answer.Method != request.Method || answer.Host != request.URL.Host || answer.Path != request.URL.Path {
			continue
		}
		if answer.Query != "*" && canonicalQuery(answer.Query) != query {
			continue
		}
		if answer.GraphQL != "" && !strings.Contains(graphQL.Query, answer.GraphQL) {
			continue
		}
		if len(answer.Variables) > 0 && !sameJSONValue(answer.Variables, graphQL.Variables) {
			continue
		}
		responseBody := answer.Body
		if answer.Respond != nil {
			built, ok := answer.Respond(replayRequest{Query: request.URL.Query(), Document: graphQL.Query, Variables: graphQL.Variables})
			if !ok {
				continue
			}
			raw, err := json.Marshal(built)
			if err != nil {
				return nil, err
			}
			responseBody = raw
		}
		doer.used[index]++
		header := http.Header{"Content-Type": []string{"application/json"}}
		for key, values := range answer.Headers {
			header[key] = values
		}
		status := answer.Status
		if status == 0 {
			status = http.StatusOK
		}
		return &http.Response{
			StatusCode: status, Header: header, Request: request,
			Body: io.NopCloser(bytes.NewReader(responseBody)),
		}, nil
	}
	line := request.Method + " " + request.URL.Host + request.URL.Path
	if query != "" {
		line += "?" + query
	}
	if graphQL.Query != "" {
		first := strings.TrimSpace(graphQL.Query)
		if cut := strings.IndexAny(first, "{("); cut > 0 {
			first = strings.TrimSpace(first[:cut])
		}
		line += " graphql=" + first + " variables=" + string(graphQL.Variables)
	}
	doer.unmatched = append(doer.unmatched, line)
	return &http.Response{
		StatusCode: 599, Header: http.Header{"Content-Type": []string{"application/json"}}, Request: request,
		Body: io.NopCloser(strings.NewReader(`{"message":"no recorded answer"}`)),
	}, nil
}

func (doer *replayDoer) lastSeen(n int) []string {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	return doer.seen[max(0, len(doer.seen)-n):]
}

// unusedAnswers lists the answers no request asked for: an answer the producers no longer ask is a
// scenario that silently stopped being exercised.
func (doer *replayDoer) unusedAnswers() []string {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	var out []string
	for index, answer := range doer.answers {
		if doer.used[index] == 0 {
			out = append(out, fmt.Sprintf("%d: %s %s%s?%s %s", index, answer.Method, answer.Host, answer.Path, answer.Query, answer.GraphQL))
		}
	}
	return out
}

// worldStep is one production verb of a world's run.
type worldStep struct {
	// Kind is "sync" (the scheduler's plan of a first sync of the integration's sources and
	// datasets, then every planned unit through the worker's executor, in plan order),
	// "team_catalog" (the worker's team catalog collector of the provider), "work_graph" (the
	// issue <-> pull request link builders), "ownership_derivation" (the post-sync job
	// sync.team_repo_ownership_derivation) or "repo_complexity" (a seeded daily complexity history per
	// repository: the world holds no file text for the complexity job to scan).
	Kind     string
	Provider string
	// Sources are the integration's sources: external id ("owner/repo" for GitHub, the numeric
	// project id for GitLab, the Jira project key, the Linear team key) and name.
	Sources [][2]string
	// Datasets are the integration's enabled datasets.
	Datasets []string
	// DepthDays is the integration's initial sync depth: how far before the first sync it reads.
	DepthDays int
	// Credential and Config are the integration's credential fields and settings. The values are
	// placeholders: the replay never checks a credential.
	Credential  map[string]string
	Config      map[string]string
	SyncOptions map[string]any
	// DatasetOptions are the integration's dataset options every planned unit carries (jira
	// fetch_dev_status).
	DatasetOptions map[string]any
	// Phase selects the answers of this step's sync, and Before how long before the world's instant it
	// runs (its clock): a catalog synced twice shows what a later sync closes.
	Phase  string
	Before time.Duration
}

// providerWorld is one frozen provider world: the parameters `fixtures generate` selects it by, the
// organization and instant it is produced for, the answers and the run.
type providerWorld struct {
	Params   GenerateParams
	OrgID    string
	FrozenAt time.Time
	// Answers are the provider's answers to every request the run makes.
	Answers func() []providerAnswer
	Steps   []worldStep
}

// idSequence makes the ids of a run (sync runs, units, claims) from the world's name, so two runs of the
// same world write the same rows.
type idSequence struct {
	mu   sync.Mutex
	name string
	next int
}

func (ids *idSequence) id() string {
	ids.mu.Lock()
	defer ids.mu.Unlock()
	ids.next++
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("dev-health-fixture-world:%s:%d", ids.name, ids.next))).String()
}

func statusMapping(t *testing.T) *providersync.StatusMapping {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	mapping, err := providersync.LoadStatusMapping(filepath.Join(root, "src", "dev_health_ops", "config", "status_mapping.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return mapping
}

func providerClient(t *testing.T, provider, baseURL string, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		provider, baseURL, doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// runWorldStep runs one production verb of the world against conn.
func runWorldStep(ctx context.Context, t *testing.T, conn driver.Conn, world providerWorld, step worldStep, doer *replayDoer, ids *idSequence, mapping *providersync.StatusMapping) {
	t.Helper()
	doer.mu.Lock()
	doer.phase = step.Phase
	doer.mu.Unlock()
	if step.Before < 0 || (step.Before > 0 && step.Kind != "team_catalog") {
		t.Fatalf("step %s: only a team catalog step runs before the world's instant", step.Kind)
	}
	switch step.Kind {
	case "sync":
		runWorldSync(ctx, t, conn, world, step, doer, ids, mapping)
	case "team_catalog":
		credential := worldCredential(step, ids.id())
		reference := providersync.TeamCatalogReference{
			OrgID: world.OrgID, SyncRunID: ids.id(), IntegrationID: ids.id(), SourceID: ids.id(),
			SyncOptions: step.SyncOptions, Strict: true,
		}
		for _, source := range step.Sources {
			reference.SourceExternalIDs = append(reference.SourceExternalIDs, source[0])
		}
		selections := providersync.TeamCatalogSelections{Teams: true, Projects: true, Members: true}
		result, err := collectTeamCatalog(ctx, t, conn, step, credential, reference, selections, doer, world.FrozenAt.Add(-step.Before))
		if err != nil {
			t.Fatalf("%s team catalog: %v (unmatched requests: %v; last requests: %v)", step.Provider, err, doer.unmatched, doer.lastSeen(6))
		}
		t.Logf("%s team catalog: %+v", step.Provider, result)
	case "work_graph":
		runWorkGraphLinks(ctx, t, conn, world)
	case "ownership_derivation":
		runOwnershipDerivation(ctx, t, conn, world)
	case "repo_complexity":
		runRepoComplexity(ctx, t, conn, world)
	default:
		t.Fatalf("unknown step kind %q", step.Kind)
	}
	if len(doer.unmatched) > 0 {
		t.Fatalf("%s %s: requests no answer names:\n  %s", step.Kind, step.Provider, strings.Join(doer.unmatched, "\n  "))
	}
}

// runWorldSync plans a first scheduled sync of the integration with the scheduler's own planner and runs
// every planned unit through the worker's executor (RunRouteInProcess with the worker's route
// selectors), with the unit's own window and processor flags.
func runWorldSync(ctx context.Context, t *testing.T, conn driver.Conn, world providerWorld, step worldStep, doer *replayDoer, ids *idSequence, mapping *providersync.StatusMapping) {
	t.Helper()
	depth := step.DepthDays
	input := syncplan.PlannerInput{
		OrgID: world.OrgID, IntegrationID: ids.id(), Mode: syncplan.SyncModeIncremental, Now: world.FrozenAt,
		IntegrationDepthDays: &depth,
	}
	sourceNames := map[string]string{}
	for _, source := range step.Sources {
		id := ids.id()
		sourceNames[id] = source[0]
		input.Sources = append(input.Sources, syncplan.PlanSource{ID: id, ExternalID: source[0], Provider: step.Provider, FullName: source[1]})
	}
	for _, dataset := range step.Datasets {
		input.Datasets = append(input.Datasets, syncplan.PlanDataset{Key: dataset})
	}
	units, err := syncplan.BuildScheduledPlan(input)
	if err != nil {
		t.Fatalf("plan %s: %v", step.Provider, err)
	}
	if len(units) == 0 {
		t.Fatalf("plan %s: no unit planned", step.Provider)
	}
	for _, unit := range units {
		if unit.WindowEnd == nil {
			t.Fatalf("plan %s/%s: a unit without a window end", unit.Provider, unit.Dataset)
		}
		external := sourceNames[unit.SourceID]
		run := providersync.InProcessRun{
			OrgID: world.OrgID, Provider: unit.Provider, Dataset: unit.Dataset,
			SourceExternalID: external, SourceName: external,
			SinceAt: unit.WindowStart, BeforeAt: *unit.WindowEnd, ProcessorFlags: unit.ProcessorFlags, DatasetOptions: step.DatasetOptions,
			Credential: step.Credential, Config: step.Config, Conn: conn, Doer: fakehttp.Client(doer),
			Retry: providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
			Now:   func() time.Time { return world.FrozenAt }, IDs: ids.id,
		}
		selector := func(session *providersync.LeaseSession) (providersync.GitFamilyRoute, bool, error) {
			if route, ok := providersync.SelectGitFamilyRoute(unit.Provider, unit.Dataset, providersync.GitFamilyDeps{Conn: conn, Lease: session}); ok {
				return route, true, nil
			}
			return providersync.SelectWorkItemsRoute(unit.Provider, unit.Dataset, providersync.WorkItemsRouteDeps{
				Conn: conn, Lease: session, Metrics: providerfoundation.NewMetrics(), StatusMapping: mapping,
			})
		}
		result, err := providersync.RunRouteInProcess(ctx, run, selector)
		if err != nil {
			t.Fatalf("%s/%s %s: %v (unmatched requests: %v; last requests: %v)", unit.Provider, unit.Dataset, external, err, doer.unmatched, doer.lastSeen(6))
		}
		t.Logf("%s/%s %s: %+v", unit.Provider, unit.Dataset, external, result)
		if len(doer.unmatched) > 0 {
			t.Fatalf("%s/%s %s: requests no answer names:\n  %s", unit.Provider, unit.Dataset, external, strings.Join(doer.unmatched, "\n  "))
		}
	}
}

// runWorkGraphLinks runs the issue <-> pull request link producers in the order the work-graph build
// registers them (workerservice/workgraph.go: issue_pr_links, then the fast-path, text-parse and
// heuristic issue_pr_edges steps), over the world's whole history as a backfill build's scope does.
func runWorkGraphLinks(ctx context.Context, t *testing.T, conn driver.Conn, world providerWorld) {
	t.Helper()
	now := func() time.Time { return world.FrozenAt }
	to := world.FrozenAt
	from := to.AddDate(0, 0, -400)
	loader, err := issueprlinks.NewLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := issueprlinks.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	links, err := issueprlinks.NewService(loader, writer, nil)
	if err != nil {
		t.Fatal(err)
	}
	links.SetClock(now)
	outcome, err := links.Produce(ctx, world.OrgID, issueprlinks.Window{From: &from, To: &to})
	if err != nil {
		t.Fatalf("issue_pr_links: %v", err)
	}
	t.Logf("issue_pr_links: %+v", outcome)
	edgesLoader, err := issuepredges.NewLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	edgesWriter, err := issueprlinks.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	edges, err := issuepredges.NewService(edgesLoader, conn, edgesWriter, nil)
	if err != nil {
		t.Fatal(err)
	}
	edges.SetClock(now)
	window := issuepredges.Window{From: &from, To: &to, HeuristicDaysWindow: 7, HeuristicConfidence: 0.3}
	fast, err := edges.ProduceFastPathEdges(ctx, world.OrgID, window)
	if err != nil {
		t.Fatalf("issue_pr_edges fast path: %v", err)
	}
	text, err := edges.ProduceTextParseEdges(ctx, world.OrgID, window)
	if err != nil {
		t.Fatalf("issue_pr_edges text parse: %v", err)
	}
	heuristic, err := edges.ProduceHeuristicEdges(ctx, world.OrgID, window)
	if err != nil {
		t.Fatalf("issue_pr_edges heuristic: %v", err)
	}
	t.Logf("issue_pr_edges: fast=%+v text=%+v heuristic=%+v", fast, text, heuristic)
}

// runOwnershipDerivation runs the post-sync job sync.team_repo_ownership_derivation's service for the
// world's organization, as the worker runs it after a sync, on the world's clock.
func runOwnershipDerivation(ctx context.Context, t *testing.T, conn driver.Conn, world providerWorld) {
	t.Helper()
	service := providersync.TeamRepoOwnershipDerivationService{Conn: conn, Now: func() time.Time { return world.FrozenAt }}
	written, retracted, ready, arms, stats, err := service.DeriveWithStats(ctx, world.OrgID)
	if err != nil {
		t.Fatalf("team_repo_ownership_derivation: %v", err)
	}
	if !ready {
		t.Fatal("team_repo_ownership_derivation: inputs not ready: the world's sync wrote no input the derivation reads")
	}
	t.Logf("team_repo_ownership_derivation: written=%d retracted=%d arms=%v stats=%+v", written, retracted, arms, stats)
}

// produceWorld runs the world's producers into a fresh ClickHouse and dumps every table they wrote.
func produceWorld(t *testing.T, world providerWorld) FrozenWorld {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ch := startClickHouse(t)
	requireUTCServer(t, ch.httpDSN)
	stopMerges(t, ch.httpDSN)
	before := rowCounts(t, ch.httpDSN)
	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(ch.instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	doer := newReplayDoer(world.Answers())
	ids := &idSequence{name: WorldFile(world.Params)}
	mapping := statusMapping(t)
	for _, step := range world.Steps {
		runWorldStep(ctx, t, conn, world, step, doer, ids, mapping)
	}
	if unused := doer.unusedAnswers(); len(unused) > 0 {
		t.Fatalf("answers no producer asked for (a scenario that is not exercised):\n  %s", strings.Join(unused, "\n  "))
	}
	after := rowCounts(t, ch.httpDSN)
	derived := derivedTables(t, ch.httpDSN)
	frozen := FrozenWorld{
		FrozenAt: world.FrozenAt.UTC().Format(producerClockLayout),
		OrgID:    world.OrgID,
		Params:   world.Params,
	}
	var names []string
	for name, count := range after {
		if count != before[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if before[name] != "0" {
			t.Fatalf("table %s held %s rows before the run: a world is dumped from an empty table only", name, before[name])
		}
		table := withoutProducerServerStamped(t, ch.httpDSN, dumpWorldTable(t, ch.httpDSN, name))
		frozen.Tables = append(frozen.Tables, WorldTable{FrozenTable: table, Derived: derived[name]})
	}
	return frozen
}

// producerServerStamped are the columns the Go producers leave to the server's DEFAULT now(): a
// world holds no value for them, so a load stamps them at load time, as a sync does. Each is checked
// to be a now() default of the live schema.
var producerServerStamped = map[string]map[string]bool{
	"teams": {"last_synced": true},
}

func withoutProducerServerStamped(t *testing.T, dsn string, table FrozenTable) FrozenTable {
	t.Helper()
	drop := producerServerStamped[table.Name]
	for column := range drop {
		expression := strings.TrimSpace(clickHouseHTTP(t, dsn, "SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = '"+table.Name+"' AND name = '"+column+"' FORMAT TSV"))
		if !strings.HasPrefix(expression, "now") {
			t.Fatalf("%s.%s has default %q, not a now() stamp: the producer must write it", table.Name, column, expression)
		}
	}
	return table.withoutColumns(drop)
}

// producerCommit is the main commit the freeze's checkout is based on (the merge base of HEAD and
// origin/main): a commit every reader can resolve. It is a stamp only: the freshness run compares
// rows, and it is what pins a world to its producers.
func producerCommit(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "merge-base", "HEAD", "origin/main").Output()
	if err != nil {
		t.Fatalf("git merge-base HEAD origin/main: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func encodeWorld(t *testing.T, world FrozenWorld) []byte {
	t.Helper()
	var plain bytes.Buffer
	encoder := json.NewEncoder(&plain)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(world); err != nil {
		t.Fatal(err)
	}
	var packed bytes.Buffer
	writer, err := gzip.NewWriterLevel(&packed, gzip.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	// A fixed header: the bytes depend on the rows only, so the digest does too.
	writer.ModTime = time.Time{}
	if _, err := writer.Write(plain.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return packed.Bytes()
}

// sameWorldRows: the same organization, instant, tables and rows (the producer stamp aside).
func sameWorldRows(committed, produced FrozenWorld) bool {
	if committed.OrgID != produced.OrgID || committed.FrozenAt != produced.FrozenAt || len(committed.Tables) != len(produced.Tables) {
		return false
	}
	left, _ := json.Marshal(committed.Tables)
	right, _ := json.Marshal(produced.Tables)
	return bytes.Equal(left, right)
}

// TestProviderWorldsAreWhatTheProducersWrite runs every provider world's producers and compares what
// they write with the committed world, table by table and row by row; with -freeze-provider-worlds it
// writes the worlds instead (and prints the digests to pin).
func TestProviderWorldsAreWhatTheProducersWrite(t *testing.T) {
	for _, world := range append(append([]providerWorld{}, providerWorlds...), demoWorldAsOf()) {
		t.Run(WorldFile(world.Params), func(t *testing.T) {
			produced := produceWorld(t, world)
			file := WorldFile(world.Params)
			if *freezeProviderWorlds {
				// Only a freeze stamps the producer: the freshness run compares rows and runs where no git
				// checkout or origin/main exists (CI's test shards).
				produced.Producer = producerCommit(t)
				encoded := encodeWorld(t, produced)
				// A world whose rows are unchanged keeps its file (and its producer stamp and digest).
				if committed, err := LoadFrozenWorld(world.Params); err == nil && sameWorldRows(committed, produced) {
					t.Logf("%s: unchanged", file)
					return
				}
				if err := os.WriteFile(file, encoded, 0o644); err != nil {
					t.Fatal(err)
				}
				sum := sha256.Sum256(encoded)
				t.Logf("froze %s: sha256 %s", file, hex.EncodeToString(sum[:]))
				return
			}
			committed, err := LoadFrozenWorld(world.Params)
			if err != nil {
				t.Fatal(err)
			}
			if sameWorldRows(committed, produced) {
				return
			}
			if committed.OrgID != produced.OrgID || committed.FrozenAt != produced.FrozenAt {
				t.Fatalf("world header: committed %s %s, produced %s %s", committed.OrgID, committed.FrozenAt, produced.OrgID, produced.FrozenAt)
			}
			want := map[string]WorldTable{}
			for _, table := range committed.Tables {
				want[table.Name] = table
			}
			got := map[string]WorldTable{}
			for _, table := range produced.Tables {
				got[table.Name] = table
			}
			for name := range want {
				if _, ok := got[name]; !ok {
					t.Errorf("the committed world holds %s; the producers no longer write it", name)
				}
			}
			for name, table := range got {
				committedTable, ok := want[name]
				if !ok {
					t.Errorf("the producers write %s; the committed world does not hold it", name)
					continue
				}
				left, _ := json.Marshal(committedTable)
				right, _ := json.Marshal(table)
				if !bytes.Equal(left, right) {
					t.Errorf("%s: the producers write other rows than the committed world holds (rerun with -freeze-provider-worlds and review the diff)", name)
				}
			}
		})
	}
}

// soleIntegrationCensus answers the snapshot close gate for a world: each provider has one integration
// in the organization, so no other active integration of the same provider exists.
type soleIntegrationCensus struct{}

func (soleIntegrationCensus) CountActiveSiblingIntegrations(context.Context, string, string, string) (int, error) {
	return 0, nil
}

// collectTeamCatalog runs the worker's own team catalog collector of the provider (the registry the
// worker builds, behind the team id carry) with the client the worker resolves for it.
func collectTeamCatalog(ctx context.Context, t *testing.T, conn driver.Conn, step worldStep, credential providerfoundation.Credential,
	reference providersync.TeamCatalogReference, selections providersync.TeamCatalogSelections, doer *replayDoer, now time.Time,
) (providersync.TeamCatalogResult, error) {
	t.Helper()
	replay := fakehttp.Client(doer).(*http.Client)
	collector, ok := workerservice.NativeTeamCatalogCollectors(conn, nil, soleIntegrationCensus{}, replay, replay.Transport)[step.Provider]
	if !ok {
		t.Fatalf("the worker has no team catalog collector for %s", step.Provider)
	}
	if err := providersync.RequireCarried(collector); err != nil {
		t.Fatal(err)
	}
	// The Linear catalog stamps each project at the instant it observed it (its handler's own clock,
	// time.Now in the worker): the world pins that clock to the world's instant too.
	if carry, ok := collector.(providersync.CarryFirstTeamCatalogCollector); ok {
		if linear, ok := carry.Collector.(providersync.LinearTeamCatalogCollector); ok {
			linear.Handler.Now = func() time.Time { return now }
			carry.Collector = linear
			collector = carry
		}
	}
	retry := providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}
	lease := providerfoundation.LeaseGuardFunc(func(ctx context.Context) error { return ctx.Err() })
	var client *providerfoundation.HTTPClient
	var err error
	switch step.Provider {
	case "github":
		client, err = providerfoundation.NewGitHubClient(credential, fakehttp.Client(doer), retry, lease)
	case "gitlab":
		client, err = providerfoundation.NewGitLabClient(credential, fakehttp.Client(doer), retry, lease)
	case "jira":
		client, err = providerfoundation.NewJiraClient(credential, fakehttp.Client(doer), retry, lease)
	case "linear":
		client, err = providerfoundation.NewLinearClient(credential, fakehttp.Client(doer), retry, lease)
	default:
		t.Fatalf("no client for %s", step.Provider)
	}
	if err != nil {
		t.Fatal(err)
	}
	return collector.CollectTeamCatalog(ctx, reference, credential, client, selections, now)
}

func worldCredential(step worldStep, id string) providerfoundation.Credential {
	fields := map[string]secrets.Value{}
	for key, value := range step.Credential {
		fields[key] = secrets.NewValue(value)
	}
	return providerfoundation.NewCredential(step.Provider, id, step.Config, fields)
}
