package providersync

// The whole-day fetch window of the four work-item routes (CHAOS-8808).
//
// EXPECTED SIDE. The Python sync job is deleted, so there is no live Python to
// call. testdata/work_item_fetch_window.frozen-python.golden.json is a
// RECORDED answer: testdata/work_item_fetch_window_recorder.py reads the
// Python files with `git show` at commit 0dcecf34a^ (b702598e8959, the last
// tree before the Go provider runtime #1738), takes the statements and
// functions out of the source by name and EXECUTES them:
//
//	processors/dataset_adapters.py  _window_day, _window_backfill_days
//	metrics/job_work_items.py       _date_range; days, since_dt, until_dt (:436-438)
//	utils/datetime.py               to_utc
//	providers/jira/provider.py      the two date reductions; client.py build_jira_jql
//	providers/github/provider.py    since, until, within_active_window
//	providers/gitlab/provider.py    updated_after
//	providers/linear/provider.py    updated_after, updated_before; client.py the gte/lte lines
//
// Recorded with (from the repository root, full history needed):
//
//	python3 internal/providersync/testdata/work_item_fetch_window_recorder.py \
//	    0dcecf34a^ > internal/providersync/testdata/work_item_fetch_window.frozen-python.golden.json
//	go run ./internal/testsupport/recordedfiles/manifest -kind python-recorded \
//	    internal/providersync/testdata/work_item_fetch_window.frozen-python.golden.json
//
// No expected value in that file is written by hand. Every leaf is tagged
// {"t","v"} and compared with internal/testsupport/oraclecompare.
//
// ACTUAL SIDE. Each provider's REAL route runs against a fake HTTP doer and
// the test reads the filter out of the request the route sent.
//
// NOT covered here: how the Python client libraries (PyGithub, python-gitlab)
// wrote an instant as text. Instants are compared as instants; only Jira's
// JQL is compared as text.

import (
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/oraclecompare"
)

const workItemFetchWindowGolden = "testdata/work_item_fetch_window.frozen-python.golden.json"

// workItemFetchWindowGoldenCases is every case the recorder holds. A case
// that is in the file and not here, or here and not in the file, fails.
var workItemFetchWindowGoldenCases = []string{
	"hourly_window_inside_a_day",
	"window_crosses_midnight_utc",
	"multi_day_catch_up",
	"first_sync_of_90_days",
	"window_ends_exactly_at_midnight",
	"instant_with_an_offset",
}

type workItemFetchWindowDay string

func (day workItemFetchWindowDay) OracleDate() string { return string(day) }

func workItemFetchWindowDayOf(value time.Time) workItemFetchWindowDay {
	return workItemFetchWindowDay(value.UTC().Format("2006-01-02"))
}

// The Go side of each compared dict. The json names are the recorder's.
type workItemFetchWindowGoWindow struct {
	Day          workItemFetchWindowDay `json:"day"`
	BackfillDays int                    `json:"backfill_days"`
	FirstDay     workItemFetchWindowDay `json:"first_day"`
	LastDay      workItemFetchWindowDay `json:"last_day"`
	DayCount     int                    `json:"day_count"`
	SinceDt      time.Time              `json:"since_dt"`
	UntilDt      time.Time              `json:"until_dt"`
}

type workItemFetchWindowGoKept struct {
	UpdatedAt time.Time `json:"updated_at"`
	Kept      bool      `json:"kept"`
}

type workItemFetchWindowGoGitHub struct {
	Since *time.Time                           `json:"since"`
	Kept  map[string]workItemFetchWindowGoKept `json:"kept"`
}

type workItemFetchWindowGoGitLab struct {
	UpdatedAfter  *time.Time `json:"updated_after"`
	UpdatedBefore *time.Time `json:"updated_before"`
}

type workItemFetchWindowGoLinear struct {
	GTE *time.Time `json:"gte"`
	LTE *time.Time `json:"lte"`
}

type workItemFetchWindowGoJira struct {
	JQL string `json:"jql"`
}

type workItemFetchWindowGoOwnOffset struct {
	FirstDay workItemFetchWindowDay `json:"first_day"`
	LastDay  workItemFetchWindowDay `json:"last_day"`
}

type workItemFetchWindowRecordedCase struct {
	Name   string         `json:"name"`
	Unit   map[string]any `json:"unit"`
	Python map[string]any `json:"python"`
	Own    map[string]any `json:"python_with_own_offset"`
}

func loadWorkItemFetchWindowGolden(t *testing.T) map[string]workItemFetchWindowRecordedCase {
	t.Helper()
	raw, err := os.ReadFile(workItemFetchWindowGolden)
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		RecordedFrom map[string]string                 `json:"recorded_from"`
		Program      string                            `json:"program"`
		Cases        []workItemFetchWindowRecordedCase `json:"cases"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		t.Fatal(err)
	}
	if file.RecordedFrom["commit"] != "b702598e89599d707b709e451d543c4211c9d4ee" {
		t.Fatalf("the golden was recorded from %q, not from 0dcecf34a^", file.RecordedFrom["commit"])
	}
	cases := map[string]workItemFetchWindowRecordedCase{}
	names := []string{}
	for _, recorded := range file.Cases {
		cases[recorded.Name] = recorded
		names = append(names, recorded.Name)
	}
	if !reflect.DeepEqual(names, workItemFetchWindowGoldenCases) {
		t.Fatalf("golden cases=%v want %v", names, workItemFetchWindowGoldenCases)
	}
	return cases
}

// workItemFetchWindowRecordedInstant reads one tagged datetime leaf.
func workItemFetchWindowRecordedInstant(t *testing.T, leaf any) time.Time {
	t.Helper()
	tagged, ok := leaf.(map[string]any)
	if !ok || tagged["t"] != "datetime" {
		t.Fatalf("not a datetime leaf: %#v", leaf)
	}
	value, err := time.Parse(time.RFC3339Nano, tagged["v"].(string))
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func assertWorkItemFetchWindowEqualsPython(t *testing.T, what string, python any, goValue any) {
	t.Helper()
	if python == nil {
		t.Fatalf("%s: the golden holds no Python answer", what)
	}
	encoded := oraclecompare.TypedEncode(t, reflect.ValueOf(goValue))
	if !oraclecompare.TypedValuesEqual(python, encoded) {
		pythonJSON, _ := json.Marshal(python)
		goJSON, _ := json.Marshal(encoded)
		t.Fatalf("%s differs from the frozen Python\npython=%s\ngo    =%s", what, pythonJSON, goJSON)
	}
}

func workItemFetchWindowClaim(provider string, since, before time.Time) Claim {
	claim := nativeTestClaim(provider, "work-items")
	claim.SinceAt, claim.BeforeAt = &since, &before
	return claim
}

// --- the four routes, each observed through the request it sends ---

var workItemFetchWindowGitHubChild = regexp.MustCompile(`^/repos/acme/api/issues/(\d+)/(events|comments)$`)

type workItemFetchWindowGitHubDoer struct {
	t *testing.T
	// probes are the updated_at instants of issues 1..n.
	probes        []time.Time
	issueRequests []*http.Request
	eventsAsked   map[int]bool
}

func (doer *workItemFetchWindowGitHubDoer) Do(request *http.Request) (*http.Response, error) {
	reply := func(body any) (*http.Response, error) {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(string(encoded))), Request: request,
		}, nil
	}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return reply(map[string]any{"id": 4567, "full_name": "acme/api"})
	case path == "/repos/acme/api/issues":
		doer.issueRequests = append(doer.issueRequests, request.Clone(request.Context()))
		listed := []map[string]any{}
		for index, updated := range doer.probes {
			listed = append(listed, map[string]any{
				"number": index + 1, "title": "probe " + strconv.Itoa(index+1), "body": "", "state": "open",
				"created_at": "2026-01-05T09:00:00Z", "updated_at": updated.UTC().Format(time.RFC3339),
				"user": map[string]any{"login": "reporter"}, "labels": []any{}, "assignees": []any{},
			})
		}
		return reply(listed)
	}
	if match := workItemFetchWindowGitHubChild.FindStringSubmatch(path); match != nil {
		number, _ := strconv.Atoi(match[1])
		if match[2] == "events" {
			doer.eventsAsked[number] = true
		}
		return reply([]any{})
	}
	doer.t.Errorf("unexpected GitHub request %s", request.URL.String())
	return reply(map[string]any{})
}

// observeGitHubWorkItemFetchWindow runs the real collector. The end of the
// window is a client-side filter, so it is observed by what the collector
// keeps: an issue is kept when the collector asks for its events.
func observeGitHubWorkItemFetchWindow(
	t *testing.T, since, before time.Time, probeLabels []string, probes []time.Time,
) workItemFetchWindowGoGitHub {
	t.Helper()
	claim := workItemFetchWindowClaim("github", since, before)
	claim.DatasetOptions = map[string]any{
		"include_issues": true, "include_pull_requests": false,
		"fetch_comments": false, "fetch_milestones": false,
	}
	doer := &workItemFetchWindowGitHubDoer{t: t, probes: probes, eventsAsked: map[int]bool{}}
	_, err := (GitHubWorkItemsRESTCollector{}).Collect(context.Background(), claim,
		gitHubPullRequestClient(t, fakehttp.Client(doer), "https://api.github.com"), before.Add(time.Minute))
	if err != nil {
		t.Fatalf("github collect: %v", err)
	}
	if len(doer.issueRequests) != 1 {
		t.Fatalf("github issue list requests=%d want 1", len(doer.issueRequests))
	}
	observed := workItemFetchWindowGoGitHub{Kept: map[string]workItemFetchWindowGoKept{}}
	query := doer.issueRequests[0].URL.Query()
	if query.Has("since") {
		value, parseErr := time.Parse(time.RFC3339, query.Get("since"))
		if parseErr != nil {
			t.Fatalf("github since=%q: %v", query.Get("since"), parseErr)
		}
		observed.Since = &value
	}
	for index, label := range probeLabels {
		observed.Kept[label] = workItemFetchWindowGoKept{UpdatedAt: probes[index], Kept: doer.eventsAsked[index+1]}
	}
	return observed
}

func observeGitLabWorkItemFetchWindow(t *testing.T, since, before time.Time) (workItemFetchWindowGoGitLab, []string) {
	t.Helper()
	claim := workItemFetchWindowClaim("gitlab", since, before)
	doer := &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}
	_, err := (GitLabWorkItemsRouteHandler{
		StatusMapping: loadRealStatusMapping(t), PerPage: 2, MaxPages: 10, NestedMaxPages: 10,
	}).Collect(context.Background(), claim,
		providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, fakehttp.Client(doer)), before.Add(time.Minute))
	if err != nil {
		t.Fatalf("gitlab collect: %v", err)
	}
	observed := workItemFetchWindowGoGitLab{}
	rawQueries := []string{}
	listRequests := 0
	for _, request := range doer.requests {
		if !strings.HasSuffix(request.URL.Path, "/issues") {
			continue
		}
		listRequests++
		query := request.URL.Query()
		rawQueries = append(rawQueries, "updated_after="+query.Get("updated_after"))
		after, parseErr := time.Parse(time.RFC3339Nano, query.Get("updated_after"))
		if parseErr != nil {
			t.Fatalf("gitlab updated_after=%q: %v", query.Get("updated_after"), parseErr)
		}
		if observed.UpdatedAfter != nil && !observed.UpdatedAfter.Equal(after) {
			t.Fatalf("gitlab issue pages disagree on updated_after: %v, %v", observed.UpdatedAfter, after)
		}
		observed.UpdatedAfter = &after
		if query.Has("updated_before") {
			beforeValue, beforeErr := time.Parse(time.RFC3339Nano, query.Get("updated_before"))
			if beforeErr != nil {
				t.Fatalf("gitlab updated_before=%q: %v", query.Get("updated_before"), beforeErr)
			}
			observed.UpdatedBefore = &beforeValue
		}
	}
	if listRequests == 0 {
		t.Fatal("the GitLab route sent no issue list request")
	}
	return observed, rawQueries
}

type workItemFetchWindowLinearDoer struct {
	issueBodies []string
}

func (doer *workItemFetchWindowLinearDoer) Do(request *http.Request) (*http.Response, error) {
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	body := string(raw)
	reply := `{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	if strings.Contains(body, "teams") {
		reply = `{"data":{"teams":{"nodes":[{"id":"team-eng","key":"ENG","name":"Engineering"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
	} else {
		doer.issueBodies = append(doer.issueBodies, body)
	}
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(reply)), Request: request,
	}, nil
}

func observeLinearWorkItemFetchWindow(t *testing.T, since, before time.Time) (workItemFetchWindowGoLinear, map[string]any) {
	t.Helper()
	claim := workItemFetchWindowClaim("linear", since, before)
	claim.SourceExternalID = "global-discovery"
	doer := &workItemFetchWindowLinearDoer{}
	noCycles := false
	_, err := (LinearWorkItemsRouteHandler{GlobalDiscovery: true, FetchCycles: &noCycles}).Collect(
		context.Background(), claim, providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, fakehttp.Client(doer)), before.Add(time.Minute))
	if err != nil {
		t.Fatalf("linear collect: %v", err)
	}
	if len(doer.issueBodies) != 1 {
		t.Fatalf("linear issue requests=%d want 1", len(doer.issueBodies))
	}
	var sent struct {
		Variables struct {
			Filter struct {
				UpdatedAt map[string]any `json:"updatedAt"`
			} `json:"filter"`
		} `json:"variables"`
	}
	if err := json.Unmarshal([]byte(doer.issueBodies[0]), &sent); err != nil {
		t.Fatal(err)
	}
	observed := workItemFetchWindowGoLinear{}
	for key, target := range map[string]**time.Time{"gte": &observed.GTE, "lte": &observed.LTE} {
		text, ok := sent.Variables.Filter.UpdatedAt[key].(string)
		if !ok {
			continue
		}
		value, parseErr := time.Parse(time.RFC3339Nano, text)
		if parseErr != nil {
			t.Fatalf("linear %s=%q: %v", key, text, parseErr)
		}
		*target = &value
	}
	if len(sent.Variables.Filter.UpdatedAt) != 2 {
		t.Fatalf("linear updatedAt filter=%v want gte and lte only", sent.Variables.Filter.UpdatedAt)
	}
	return observed, sent.Variables.Filter.UpdatedAt
}

func observeJiraWorkItemFetchWindow(t *testing.T, since, before time.Time) workItemFetchWindowGoJira {
	t.Helper()
	claim := workItemFetchWindowClaim("jira", since, before)
	claim.SourceExternalID = "OPS"
	claim.DatasetOptions = map[string]any{"fetch_comments": false, "sprint_field": "customfield_10020"}
	doer := &jiraWorkItemsDoer{t: t}
	client := jiraWorkItemsTestClient(t, fakehttp.Client(doer),
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	_, err := (JiraWorkItemsRouteHandler{
		StatusMapping: loadRealStatusMapping(t), Identity: jiraRouteIdentity,
	}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, before.Add(time.Minute))
	if err != nil {
		t.Fatalf("jira collect: %v", err)
	}
	observed := []string{}
	for _, rawPath := range doer.paths {
		parsed, parseErr := url.Parse(rawPath)
		if parseErr == nil && parsed.Path == "/rest/api/3/search/jql" {
			observed = append(observed, parsed.Query().Get("jql"))
		}
	}
	if len(observed) != 1 {
		t.Fatalf("jira search requests=%d want 1", len(observed))
	}
	return workItemFetchWindowGoJira{JQL: observed[0]}
}

// goWorkItemFetchWindowDays is the Go helper's window in the recorder's shape.
func goWorkItemFetchWindowDays(t *testing.T, since, before time.Time) workItemFetchWindowGoWindow {
	t.Helper()
	window := workItemWholeDayFetchWindow(workItemFetchWindowClaim("github", since, before))
	if window.Since == nil || window.Until == nil {
		t.Fatalf("window=%+v", window)
	}
	if window.Since.Location() != time.UTC || window.Until.Location() != time.UTC {
		t.Fatalf("the fetch window is not in UTC: %v %v", window.Since.Location(), window.Until.Location())
	}
	days := int(workItemFetchWindowUTCDay(*window.Until).Sub(*window.Since)/(24*time.Hour)) + 1
	return workItemFetchWindowGoWindow{
		Day: workItemFetchWindowDayOf(*window.Until), BackfillDays: days,
		FirstDay: workItemFetchWindowDayOf(*window.Since), LastDay: workItemFetchWindowDayOf(*window.Until),
		DayCount: days, SinceDt: *window.Since, UntilDt: *window.Until,
	}
}

// TestWorkItemRoutesSendTheFetchWindowOfTheFrozenPython is the parity proof:
// for every recorded window, the helper and the request of each of the four
// real routes equal what the frozen Python computed and sent.
func TestWorkItemRoutesSendTheFetchWindowOfTheFrozenPython(t *testing.T) {
	golden := loadWorkItemFetchWindowGolden(t)
	for _, name := range workItemFetchWindowGoldenCases {
		recorded := golden[name]
		t.Run(name, func(t *testing.T) {
			since := workItemFetchWindowRecordedInstant(t, recorded.Unit["since_at"])
			before := workItemFetchWindowRecordedInstant(t, recorded.Unit["before_at"])
			if name == "instant_with_an_offset" {
				// The unit's instants in a zone that is not UTC: the same
				// instants, another wall clock and another calendar day.
				zone := time.FixedZone("plus-two", 2*60*60)
				since, before = since.In(zone), before.In(zone)
				if since.Day() == since.UTC().Day() {
					t.Fatal("the offset case must name another day than UTC does")
				}
			}

			assertWorkItemFetchWindowEqualsPython(t, "window", recorded.Python["window"],
				goWorkItemFetchWindowDays(t, since, before))

			pythonGitHub, _ := recorded.Python["github"].(map[string]any)
			pythonKept, _ := pythonGitHub["kept"].(map[string]any)
			labels := make([]string, 0, len(pythonKept))
			for label := range pythonKept {
				labels = append(labels, label)
			}
			sort.Strings(labels)
			if len(labels) != 3 {
				t.Fatalf("github probes=%v want 3", labels)
			}
			probes := make([]time.Time, 0, len(labels))
			for _, label := range labels {
				probe, _ := pythonKept[label].(map[string]any)
				probes = append(probes, workItemFetchWindowRecordedInstant(t, probe["updated_at"]))
			}
			assertWorkItemFetchWindowEqualsPython(t, "github", recorded.Python["github"],
				observeGitHubWorkItemFetchWindow(t, since, before, labels, probes))

			gitlab, _ := observeGitLabWorkItemFetchWindow(t, since, before)
			assertWorkItemFetchWindowEqualsPython(t, "gitlab", recorded.Python["gitlab"], gitlab)

			linear, _ := observeLinearWorkItemFetchWindow(t, since, before)
			assertWorkItemFetchWindowEqualsPython(t, "linear", recorded.Python["linear"], linear)

			assertWorkItemFetchWindowEqualsPython(t, "jira", recorded.Python["jira"],
				observeJiraWorkItemFetchWindow(t, since, before))
		})
	}
}

// TestWorkItemFetchWindowTakesTheDayInUTCNotInTheInstantsOwnZone pins the one
// declared difference from Python. Python took `.date()` in the instant's own
// offset; its instants were UTC (timestamptz), so the two agreed. Go converts
// to UTC first. For every recorded case but the offset one the two rules give
// the same days; for the offset one they MUST differ, or the case proves
// nothing.
func TestWorkItemFetchWindowTakesTheDayInUTCNotInTheInstantsOwnZone(t *testing.T) {
	golden := loadWorkItemFetchWindowGolden(t)
	for _, name := range workItemFetchWindowGoldenCases {
		recorded := golden[name]
		since := workItemFetchWindowRecordedInstant(t, recorded.Unit["since_at"])
		before := workItemFetchWindowRecordedInstant(t, recorded.Unit["before_at"])
		window := goWorkItemFetchWindowDays(t, since.In(time.FixedZone("plus-two", 2*60*60)), before)
		encoded := oraclecompare.TypedEncode(t, reflect.ValueOf(workItemFetchWindowGoOwnOffset{
			FirstDay: window.FirstDay, LastDay: window.LastDay,
		}))
		equal := oraclecompare.TypedValuesEqual(any(recorded.Own), encoded)
		if want := name != "instant_with_an_offset"; equal != want {
			t.Errorf("%s: Go days equal to Python's own-offset days = %v, want %v (python=%v go=%v)",
				name, equal, want, recorded.Own, encoded)
		}
	}
}

// --- one pin per route: the exact request for ONE hourly window ---

var (
	workItemFetchWindowHourlySince  = time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	workItemFetchWindowHourlyBefore = time.Date(2026, 6, 17, 13, 0, 0, 0, time.UTC)
)

func TestGitHubWorkItemsRouteSendsTheWholeDayFetchWindowForAnHourlyUnit(t *testing.T) {
	labels := []string{"start_of_the_day", "after_the_unit_end", "last_second_of_the_day", "next_day"}
	probes := []time.Time{
		time.Date(2026, 6, 17, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 17, 13, 0, 1, 0, time.UTC),
		time.Date(2026, 6, 17, 23, 59, 59, 0, time.UTC),
		time.Date(2026, 6, 18, 0, 0, 0, 0, time.UTC),
	}
	observed := observeGitHubWorkItemFetchWindow(t,
		workItemFetchWindowHourlySince, workItemFetchWindowHourlyBefore, labels, probes)
	if observed.Since == nil || observed.Since.Format(time.RFC3339Nano) != "2026-06-17T00:00:00Z" {
		t.Errorf("GitHub since=%v want 2026-06-17T00:00:00Z (00:00 UTC of the unit's day)", observed.Since)
	}
	want := map[string]bool{
		"start_of_the_day": true, "after_the_unit_end": true, "last_second_of_the_day": true, "next_day": false,
	}
	for label, kept := range want {
		if observed.Kept[label].Kept != kept {
			t.Errorf("GitHub issue %s (updated %s) kept=%v want %v",
				label, observed.Kept[label].UpdatedAt.Format(time.RFC3339), observed.Kept[label].Kept, kept)
		}
	}
}

func TestGitLabWorkItemsRouteSendsTheWholeDayFetchWindowForAnHourlyUnit(t *testing.T) {
	observed, raw := observeGitLabWorkItemFetchWindow(t,
		workItemFetchWindowHourlySince, workItemFetchWindowHourlyBefore)
	for _, query := range raw {
		if query != "updated_after=2026-06-17T00:00:00Z" {
			t.Errorf("GitLab issue list %s want updated_after=2026-06-17T00:00:00Z", query)
		}
	}
	if observed.UpdatedBefore != nil {
		t.Errorf("GitLab sent updated_before=%v; the route sends a start only", observed.UpdatedBefore)
	}
}

func TestLinearWorkItemsRouteSendsTheWholeDayFetchWindowForAnHourlyUnit(t *testing.T) {
	_, filter := observeLinearWorkItemFetchWindow(t,
		workItemFetchWindowHourlySince, workItemFetchWindowHourlyBefore)
	want := map[string]any{"gte": "2026-06-17T00:00:00Z", "lte": "2026-06-17T23:59:59.999999Z"}
	if !reflect.DeepEqual(filter, want) {
		t.Errorf("Linear updatedAt filter=%v want %v", filter, want)
	}
}

func TestJiraWorkItemsRouteSendsTheWholeDayFetchWindowForAnHourlyUnit(t *testing.T) {
	observed := observeJiraWorkItemFetchWindow(t,
		workItemFetchWindowHourlySince, workItemFetchWindowHourlyBefore)
	want := "project = 'OPS' AND (updated >= '2026-06-17' OR (statusCategory != Done AND created <= '2026-06-17')) ORDER BY updated DESC"
	if observed.JQL != want {
		t.Errorf("Jira JQL=%q want %q", observed.JQL, want)
	}
}

// TestJiraWorkItemsJQLIsTheSameAsBeforeTheFetchWindowHelper proves that the
// helper did not change Jira's request: for every window the JQL equals the
// one built from the unit's own instants reduced to dates, which is what the
// route did before it used the helper.
func TestJiraWorkItemsJQLIsTheSameAsBeforeTheFetchWindowHelper(t *testing.T) {
	zone := time.FixedZone("minus-nine", -9*60*60)
	for _, window := range workItemFetchWindowRelationCases(zone) {
		claim := workItemFetchWindowClaim("jira", window.since, window.before)
		before := fmt.Sprintf(
			"project = 'OPS' AND (updated >= '%s' OR (statusCategory != Done AND created <= '%s')) ORDER BY updated DESC",
			window.since.UTC().Format("2006-01-02"), window.before.UTC().Format("2006-01-02"))
		if got := jiraWorkItemsJQL(claim, "OPS"); got != before {
			t.Errorf("%s: JQL=%q, before the helper=%q", window.name, got, before)
		}
	}
}

// --- the helper by itself ---

func TestWorkItemWholeDayFetchWindowAlignsBothEndsToUTCDays(t *testing.T) {
	// A process zone that is not UTC: a helper that used local time would
	// name another instant for the start of the day.
	previous := time.Local
	time.Local = time.FixedZone("minus-seven", -7*60*60)
	t.Cleanup(func() { time.Local = previous })

	cases := []struct {
		name, since, before, wantSince, wantUntil string
	}{
		{"hourly", "2026-06-17T12:00:00Z", "2026-06-17T13:00:00Z", "2026-06-17T00:00:00Z", "2026-06-17T23:59:59.999999Z"},
		{"early_hour", "2026-06-17T01:30:00Z", "2026-06-17T02:30:00Z", "2026-06-17T00:00:00Z", "2026-06-17T23:59:59.999999Z"},
		{"late_hour", "2026-06-17T22:30:00Z", "2026-06-17T23:30:00Z", "2026-06-17T00:00:00Z", "2026-06-17T23:59:59.999999Z"},
		{"crosses_midnight", "2026-06-17T23:30:00Z", "2026-06-18T00:30:00Z", "2026-06-17T00:00:00Z", "2026-06-18T23:59:59.999999Z"},
		{"ends_at_midnight", "2026-06-17T23:00:00Z", "2026-06-18T00:00:00Z", "2026-06-17T00:00:00Z", "2026-06-18T23:59:59.999999Z"},
		{"starts_at_midnight", "2026-06-17T00:00:00Z", "2026-06-17T01:00:00Z", "2026-06-17T00:00:00Z", "2026-06-17T23:59:59.999999Z"},
		{"last_nanosecond_of_a_day", "2026-06-17T23:59:59.999999999Z", "2026-06-18T00:00:00.000000001Z", "2026-06-17T00:00:00Z", "2026-06-18T23:59:59.999999Z"},
		{"offset_instants", "2026-06-17T01:30:00+02:00", "2026-06-17T02:30:00+02:00", "2026-06-16T00:00:00Z", "2026-06-17T23:59:59.999999Z"},
		{"month_end", "2026-02-28T23:10:00Z", "2026-03-01T00:10:00Z", "2026-02-28T00:00:00Z", "2026-03-01T23:59:59.999999Z"},
	}
	for _, testCase := range cases {
		since, err := time.Parse(time.RFC3339Nano, testCase.since)
		if err != nil {
			t.Fatal(err)
		}
		before, err := time.Parse(time.RFC3339Nano, testCase.before)
		if err != nil {
			t.Fatal(err)
		}
		window := workItemWholeDayFetchWindow(workItemFetchWindowClaim("linear", since, before))
		if window.Since == nil || window.Since.Format(time.RFC3339Nano) != testCase.wantSince {
			t.Errorf("%s: since=%v want %s", testCase.name, window.Since, testCase.wantSince)
		}
		if window.Until == nil || window.Until.Format(time.RFC3339Nano) != testCase.wantUntil {
			t.Errorf("%s: until=%v want %s", testCase.name, window.Until, testCase.wantUntil)
		}
	}

	// A missing bound stays missing: the helper never narrows a fetch.
	open := nativeTestClaim("github", "work-items")
	open.SinceAt, open.BeforeAt = nil, nil
	if window := workItemWholeDayFetchWindow(open); window.Since != nil || window.Until != nil {
		t.Errorf("a claim with no window got a fetch window: %+v", window)
	}
}

// --- the deriver's day loop and the fetch window name the same days ---

type workItemFetchWindowRelationCase struct {
	name          string
	since, before time.Time
}

func workItemFetchWindowRelationCases(zone *time.Location) []workItemFetchWindowRelationCase {
	at := func(text string) time.Time {
		value, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			panic(err)
		}
		return value
	}
	return []workItemFetchWindowRelationCase{
		{"hourly", at("2026-06-17T12:00:00Z"), at("2026-06-17T13:00:00Z")},
		{"first_hour_of_a_day", at("2026-06-17T00:00:00Z"), at("2026-06-17T01:00:00Z")},
		{"last_hour_of_a_day", at("2026-06-17T23:00:00Z"), at("2026-06-18T00:00:00Z")},
		{"crosses_midnight", at("2026-06-17T23:30:00Z"), at("2026-06-18T00:30:00Z")},
		{"multi_day", at("2026-06-14T09:15:27.123456Z"), at("2026-06-17T13:00:00Z")},
		{"multi_day_ends_at_midnight", at("2026-06-14T09:15:27Z"), at("2026-06-18T00:00:00Z")},
		{"first_sync_90_days", at("2026-03-19T13:00:00Z"), at("2026-06-17T13:00:00Z")},
		{"one_nanosecond_after_midnight", at("2026-06-17T12:00:00Z"), at("2026-06-18T00:00:00.000000001Z")},
		{"zoned_instants", at("2026-06-17T20:30:00Z").In(zone), at("2026-06-17T21:30:00Z").In(zone)},
	}
}

// TestWorkItemDerivedDaysStayInsideTheFetchWindow fails if the day loop of the
// sync deriver (githubWorkItemDerivedDays, used for all four providers) and
// the fetch window stop naming the same days.
//
// They agree with ONE exception that exists today and is pinned here as it
// is: the fetch window ends on the day of the end instant itself (the frozen
// Python adapter rule), and the day loop ends on the day of the instant before
// it. So for a window that ends exactly at 00:00:00 UTC the fetch window holds
// one day more than the loop derives. That day is fetched and not derived; no
// derived day is ever outside the fetch window.
func TestWorkItemDerivedDaysStayInsideTheFetchWindow(t *testing.T) {
	zone := time.FixedZone("minus-nine", -9*60*60)
	exceptions := 0
	for _, testCase := range workItemFetchWindowRelationCases(zone) {
		claim := workItemFetchWindowClaim("github", testCase.since, testCase.before)
		days, err := githubWorkItemDerivedDays(claim, testCase.before.Add(time.Minute))
		if err != nil || len(days) == 0 {
			t.Fatalf("%s: days=%v err=%v", testCase.name, days, err)
		}
		window := workItemWholeDayFetchWindow(claim)
		for _, day := range days {
			endOfDay := day.Add(24*time.Hour - time.Microsecond)
			if day.Before(*window.Since) || endOfDay.After(*window.Until) {
				t.Errorf("%s: derived day %s is outside the fetch window %s .. %s", testCase.name,
					day.Format("2006-01-02"), window.Since.Format(time.RFC3339Nano), window.Until.Format(time.RFC3339Nano))
			}
		}
		if !days[0].Equal(*window.Since) {
			t.Errorf("%s: first derived day %s, fetch window starts %s", testCase.name,
				days[0].Format("2006-01-02"), window.Since.Format(time.RFC3339Nano))
		}
		lastFetchDay := workItemFetchWindowUTCDay(*window.Until)
		endsAtMidnight := testCase.before.Equal(workItemFetchWindowUTCDay(testCase.before))
		wantLast := lastFetchDay
		if endsAtMidnight {
			wantLast = lastFetchDay.AddDate(0, 0, -1)
			exceptions++
		}
		if !days[len(days)-1].Equal(wantLast) {
			t.Errorf("%s: last derived day %s, want %s (fetch window ends %s, ends at midnight=%v)", testCase.name,
				days[len(days)-1].Format("2006-01-02"), wantLast.Format("2006-01-02"),
				window.Until.Format(time.RFC3339Nano), endsAtMidnight)
		}
	}
	if exceptions != 2 {
		t.Fatalf("cases that end exactly at midnight=%d want 2: the exception is not exercised", exceptions)
	}
}

// --- census: nobody reads the claim's window for a request but the helper ---

// workItemWindowReads is every read of claim.SinceAt / claim.BeforeAt in the
// work-item files of this package, by file and function, with what the read
// is for. None of them but the helper's builds a provider request.
//
// A new read fails TestWorkItemRoutesReadTheWindowOnlyThroughTheFetchWindowHelper.
// If the new read puts the window into a provider request, it must go
// through workItemWholeDayFetchWindow instead; otherwise add it here with its
// purpose.
var workItemWindowReads = map[string]string{
	"work_item_fetch_window.go workItemWholeDayFetchWindow SinceAt=2 BeforeAt=2":      "THE helper: the only request use",
	"github_work_items_composition.go githubWorkItemDerivedDays SinceAt=3 BeforeAt=3": "the deriver's day loop",
	"github_work_items_route.go Collect SinceAt=0 BeforeAt=2":                         "the watermark the unit reports",
	"gitlab_work_item_derived.go Derive SinceAt=0 BeforeAt=2":                         "the watermark the deriver reports",
	"gitlab_work_items_route.go Collect SinceAt=0 BeforeAt=3":                         "claim validation; watermark check",
	"jira_atlassian_route.go Collect SinceAt=2 BeforeAt=3":                            "claim validation; watermark check",
	"jira_work_item_derived.go Derive SinceAt=0 BeforeAt=2":                           "claim validation; the watermark",
	"jira_work_items_route.go Collect SinceAt=3 BeforeAt=4":                           "claim validation; the watermark",
	"linear_work_items_composition.go Collect SinceAt=0 BeforeAt=2":                   "watermark check",
	"linear_work_items_route.go Collect SinceAt=0 BeforeAt=2":                         "claim validation; the watermark",
}

func workItemWindowReadCensus(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	scanned := 0
	counts := map[string]map[string]int{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		if !strings.Contains(name, "work_item") && name != "jira_atlassian_route.go" {
			continue
		}
		scanned++
		parsed, parseErr := parser.ParseFile(token.NewFileSet(), name, nil, parser.SkipObjectResolution)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		for _, declaration := range parsed.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			key := name + " " + function.Name.Name
			ast.Inspect(function.Body, func(node ast.Node) bool {
				selector, isSelector := node.(*ast.SelectorExpr)
				if !isSelector || (selector.Sel.Name != "SinceAt" && selector.Sel.Name != "BeforeAt") {
					return true
				}
				if counts[key] == nil {
					counts[key] = map[string]int{}
				}
				counts[key][selector.Sel.Name]++
				return true
			})
		}
	}
	// A census that read nothing would report "no reads" and pass.
	if scanned < 20 {
		t.Fatalf("the census read %d work-item files; it must read the package's work-item sources", scanned)
	}
	lines := make([]string, 0, len(counts))
	for key, count := range counts {
		lines = append(lines, fmt.Sprintf("%s SinceAt=%d BeforeAt=%d", key, count["SinceAt"], count["BeforeAt"]))
	}
	sort.Strings(lines)
	return lines
}

func TestWorkItemRoutesReadTheWindowOnlyThroughTheFetchWindowHelper(t *testing.T) {
	found := workItemWindowReadCensus(t)
	want := make([]string, 0, len(workItemWindowReads))
	for line := range workItemWindowReads {
		want = append(want, line)
	}
	sort.Strings(want)
	if !reflect.DeepEqual(found, want) {
		t.Fatalf("reads of the claim's window in the work-item files changed.\n"+
			"A provider request takes its window from workItemWholeDayFetchWindow and from nowhere else.\n"+
			"found:\n  %s\nwant:\n  %s", strings.Join(found, "\n  "), strings.Join(want, "\n  "))
	}
}
