package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

type gitLabWorkItemsDoer struct {
	responses map[string][]string
	requests  []*http.Request
	// nextHeader makes the fake answer like GitLab does: X-Next-Page names the
	// next page when that page holds rows, and is EMPTY on the last page.
	nextHeader bool
	// forceNextHeader overrides X-Next-Page for one response key
	// ("<path>?page=N"), to model a provider whose header disagrees with its body.
	forceNextHeader map[string]string
}

func (doer *gitLabWorkItemsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.requests = append(doer.requests, request.Clone(request.Context()))
	path := request.URL.Path
	key := path
	if request.URL.Query().Get("page") != "" {
		key = path + "?page=" + request.URL.Query().Get("page")
	}
	values := doer.responses[key]
	if len(values) == 0 && request.URL.Query().Get("page") != "1" {
		key = path + "?page=1"
		values = doer.responses[key]
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("missing fake GitLab response for %s", request.URL.String())
	}
	body := values[0]
	doer.responses[key] = values[1:]
	header := make(http.Header)
	if doer.nextHeader {
		header.Set("X-Next-Page", "")
		if page, err := strconv.Atoi(request.URL.Query().Get("page")); err == nil {
			if next := doer.responses[path+"?page="+strconv.Itoa(page+1)]; len(next) > 0 && next[0] != "[]" {
				header.Set("X-Next-Page", strconv.Itoa(page+1))
			}
		}
	}
	if forced, ok := doer.forceNextHeader[key]; ok {
		header.Set("X-Next-Page", forced)
	}
	return &http.Response{
		StatusCode: http.StatusOK, Status: "200 OK", Header: header,
		Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

func gitLabWorkItemsClient(t *testing.T, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"gitlab", "https://gitlab.example", fakehttp.Client(doer),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func gitLabWorkItemResponses() map[string][]string {
	root := "/api/v4/projects/123"
	return map[string][]string{
		root:                        {`{"id":123,"path":"api","path_with_namespace":"acme/api","name":"api"}`},
		root + "/milestones?page=1": {`[{"id":7,"title":"July","state":"closed","start_date":"2026-07-01","due_date":"2026-07-31"}]`, `[]`},
		root + "/issues?page=1":     {`[{"iid":42,"title":"Fix the API","description":"blocks #7 and see CHAOS-9","state":"opened","created_at":"2026-07-02T09:00:00Z","updated_at":"2026-07-03T09:00:00Z","closed_at":null,"labels":["bug","in progress","priority::high"],"assignees":[{"email":"Alice@EXAMPLE.COM","username":"alice","name":"Alice"}],"author":{"email":"bob@example.com","username":"bob","name":"Bob"},"web_url":"https://gitlab.example/acme/api/-/issues/42","weight":3,"milestone":{"id":7,"title":"July"},"due_date":"2026-07-20"}]`, `[]`},
		root + "/issues/42/resource_label_events?page=1":        {`[{"action":"add","created_at":"2026-07-02T10:00:00Z","label":{"name":"done"}}]`, `[]`},
		root + "/issues/42/resource_state_events?page=1":        {`[{"state":"reopened","created_at":"2026-07-03T10:00:00Z","user":{"username":"bob","name":"Bob"}}]`, `[]`},
		root + "/issues/42/links?page=1":                        {`[{"link_type":"blocks","iid":7,"references":{"full":"acme/api#7"}}]`, `[]`},
		root + "/issues/42/closed_by?page=1":                    {`[{"iid":9,"references":{"full":"acme/api!9"}}]`, `[]`},
		root + "/issues/42/notes?page=1":                        {`[{"system":true,"body":"label changed","created_at":"2026-07-02T11:00:00Z"},{"id":501,"system":false,"body":"hello 🌍","created_at":"2026-07-02T12:00:00Z","author":{"username":"alice"}}]`, `[]`},
		root + "/merge_requests?page=1":                         {`[{"iid":9,"title":"Ship the API","description":"","state":"opened","created_at":"2026-07-04T09:00:00Z","updated_at":"2026-07-04T10:00:00Z","closed_at":null,"merged_at":null,"labels":["priority::low"],"assignees":[],"author":{"username":"alice"},"web_url":"https://gitlab.example/acme/api/-/merge_requests/9","milestone":null}]`, `[]`},
		root + "/merge_requests/9/resource_state_events?page=1": {`[{"state":"opened","created_at":"2026-07-04T09:00:00Z","user":{"username":"alice"}},{"state":"merged","created_at":"2026-07-05T09:00:00Z","user":{"username":"bob"}}]`, `[]`},
		root + "/merge_requests/9/notes?page=1":                 {`[{"id":502,"system":false,"body":"ship it","created_at":"2026-07-04T12:00:00Z","author":{"username":"bob"}}]`, `[]`},
	}
}

// The GitLab work-items unit stores the six raw facts and the ai_attribution
// effect, and no table the daily job computes from stored rows.
//
// The ai_attribution effect of this route is present and EMPTY, also for a
// payload that holds a merge request with labels: merge-request AI attribution
// is written by the prs unit alone, and this route never fills the slice.
func TestGitLabWorkItemsRouteNormalizesSixRawFactsAndLeavesDerivedTablesToDailyJob(t *testing.T) {
	doer := &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}
	claim := nativeTestClaim("gitlab", "work-items")
	now := time.Date(2026, 8, 3, 12, 0, 0, 987654321, time.UTC)
	client := gitLabWorkItemsClient(t, fakehttp.Client(doer))
	leftToDaily := providerfoundation.NewMetrics()
	client.Metrics = leftToDaily
	batch, err := (GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 2, MaxPages: 10, NestedMaxPages: 10}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		client, now,
	)
	if err != nil {
		paths := make([]string, 0, len(doer.requests))
		for _, request := range doer.requests {
			paths = append(paths, request.URL.String())
		}
		t.Fatalf("err=%v requests=%s", err, strings.Join(paths, "\n"))
	}
	byDestination := map[string]EffectBatch{}
	for _, effect := range batch.Effects {
		byDestination[effect.Destination] = effect
	}
	gotDestinations := make([]string, 0, len(batch.Effects))
	for _, effect := range batch.Effects {
		gotDestinations = append(gotDestinations, effect.Destination)
	}
	slices.Sort(gotDestinations)
	if want := workItemRouteDestinations(); !slices.Equal(gotDestinations, want) {
		t.Fatalf("destinations=%v want=%v", gotDestinations, want)
	}
	for _, destination := range githubWorkItemDerivedDestinations {
		if _, present := byDestination[destination]; present {
			t.Fatalf("the unit built an effect for the derived table %q", destination)
		}
	}
	aiAttribution, present := byDestination["ai_attribution"]
	if !present || len(aiAttribution.Rows) != 0 || aiAttribution.Recovery != EffectReadbackRequired {
		t.Fatalf("ai_attribution effect present=%t rows=%d recovery=%s want present, empty, readback-required",
			present, len(aiAttribution.Rows), aiAttribution.Recovery)
	}
	for _, key := range []string{
		"team_inheritance", "team_attribution_written", "derived_destinations_implemented",
		"derived_destinations_unimplemented", "watermark_held_for_derived_gap",
	} {
		if _, present := batch.Result[key]; present {
			t.Fatalf("result still carries %q: %+v", key, batch.Result)
		}
	}
	assertWorkItemDerivedTablesLeftToDailyJob(t, leftToDaily, "gitlab", 1)
	if len(byDestination["work_items"].Rows) != 2 ||
		len(byDestination["work_item_transitions"].Rows) != 3 ||
		len(byDestination["work_item_dependencies"].Rows) != 3 ||
		len(byDestination["work_item_reopen_events"].Rows) != 1 ||
		len(byDestination["work_item_interactions"].Rows) != 2 ||
		len(byDestination["sprints"].Rows) != 1 {
		t.Fatalf("raw effects=%+v", byDestination)
	}
	closingRows := 0
	for _, raw := range byDestination["work_item_dependencies"].Rows {
		var dependency gitlabWorkItemDependencyRow
		if err := jsonUnmarshalEffect(raw, &dependency); err != nil {
			t.Fatal(err)
		}
		if dependency.RelationshipTypeRaw == "gitlab_closing_reference" {
			closingRows++
			if dependency.SourceWorkItemID != "gitlab:acme/api!9" || dependency.TargetWorkItemID != "gitlab:acme/api#42" {
				t.Fatalf("closing row=%+v", dependency)
			}
		}
	}
	if closingRows != 1 {
		t.Fatalf("gitlab_closing_reference rows=%d want=1 (the issue's closed_by fetch must reach the dependency effect)", closingRows)
	}
	var issue gitlabWorkItemRow
	for _, raw := range byDestination["work_items"].Rows {
		var candidate gitlabWorkItemRow
		if err := jsonUnmarshalEffect(raw, &candidate); err != nil {
			t.Fatal(err)
		}
		if candidate.WorkItemID == "gitlab:acme/api#42" {
			issue = candidate
		}
	}
	if issue.WorkItemID != "gitlab:acme/api#42" || issue.Provider != "gitlab" || issue.Type != "bug" ||
		issue.Status != "in_progress" || issue.PriorityRaw == nil || *issue.PriorityRaw != "high" ||
		issue.Assignees[0] != "alice@example.com" || issue.Reporter == nil || *issue.Reporter != "bob@example.com" ||
		issue.StoryPoints == nil || *issue.StoryPoints != 3 || issue.SprintID == nil || *issue.SprintID != "7" {
		t.Fatalf("issue=%+v", issue)
	}
	if wantDue := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC); issue.DueAt == nil || !issue.DueAt.Equal(wantDue) {
		t.Fatalf("issue due_at=%v want %v (the issue's own due_date)", issue.DueAt, wantDue)
	}
	var sprint gitlabSprintRow
	if err := jsonUnmarshalEffect(byDestination["sprints"].Rows[0], &sprint); err != nil {
		t.Fatal(err)
	}
	if wantEnd := time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC); sprint.EndedAt == nil || !sprint.EndedAt.Equal(wantEnd) {
		t.Fatalf("sprint ended_at=%v want %v (the milestone's date-only due_date)", sprint.EndedAt, wantEnd)
	}
	var interaction gitlabWorkItemInteractionRow
	for _, raw := range byDestination["work_item_interactions"].Rows {
		var candidate gitlabWorkItemInteractionRow
		if err := jsonUnmarshalEffect(raw, &candidate); err != nil {
			t.Fatal(err)
		}
		if candidate.Actor != nil && *candidate.Actor == "gitlab:alice" {
			interaction = candidate
			break
		}
	}
	if interaction.BodyLength != len([]rune("hello 🌍")) || interaction.Provider != "gitlab" {
		t.Fatalf("interaction=%+v", interaction)
	}
	var dependency gitlabWorkItemDependencyRow
	for _, raw := range byDestination["work_item_dependencies"].Rows {
		var candidate gitlabWorkItemDependencyRow
		if err := jsonUnmarshalEffect(raw, &candidate); err != nil {
			t.Fatal(err)
		}
		if candidate.TargetWorkItemID == "gitlab:acme/api#7" {
			dependency = candidate
			break
		}
	}
	if dependency.RelationshipType != "blocks" || dependency.RelationshipSemanticsVersion != "canonical-blocks.v2" ||
		dependency.SourceWorkItemID != issue.WorkItemID || dependency.TargetWorkItemID != "gitlab:acme/api#7" {
		t.Fatalf("dependency=%+v", dependency)
	}
	summary, ok := batch.Result["gitlab_work_items"].(GitLabWorkItemsResult)
	if !ok || summary.WorkItemsSynced != 2 || len(summary.RawDestinations) != 6 {
		t.Fatalf("typed summary=%T/%+v", batch.Result["gitlab_work_items"], batch.Result["gitlab_work_items"])
	}
	// The watermark is the end of the window whose raw rows the unit stored.
	if claim.BeforeAt == nil || batch.Watermark == nil || !batch.Watermark.Equal(claim.BeforeAt.UTC()) {
		t.Fatalf("watermark=%v want the claim bound %v", batch.Watermark, claim.BeforeAt)
	}
	for _, request := range doer.requests {
		if strings.HasSuffix(request.URL.Path, "/issues") || strings.HasSuffix(request.URL.Path, "/merge_requests") {
			query := request.URL.Query()
			// CHAOS-8805: the fetch carries both ends of the window.
			if query.Get("updated_after") != "2026-07-01T00:00:00Z" || query.Get("updated_before") != "2026-07-31T23:59:59Z" || query.Get("state") != "all" {
				t.Fatalf("window query=%s", request.URL.String())
			}
		}
	}
}

func TestGitLabWorkItemsRouteRejectsPaginationCapAndMissingMapping(t *testing.T) {
	claim := nativeTestClaim("gitlab", "work-items")
	responses := gitLabWorkItemResponses()
	responses["/api/v4/projects/123/issues?page=1"] = []string{`[{"iid":1,"title":"one"}]`}
	responses["/api/v4/projects/123/issues?page=2"] = []string{`[{"iid":2,"title":"two"}]`}
	responses["/api/v4/projects/123/issues?page=3"] = []string{`[]`}
	no := false
	_, err := (GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 1, MaxPages: 1, FetchComments: &no, FetchHistory: &no, FetchLabels: &no, FetchLinks: &no, FetchMilestones: &no, IncludeMRs: &no}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, fakehttp.Client(&gitLabWorkItemsDoer{responses: responses})), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != ErrPaginationCapExceeded {
		t.Fatalf("cap error=%v", err)
	}
	_, err = (GitLabWorkItemsRouteHandler{}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, fakehttp.Client(&gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()})), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != ErrInvalidConfiguration {
		t.Fatalf("mapping error=%v", err)
	}
}

func TestGitLabWorkItemsRouteRejectsInvalidScopeBeforeRequests(t *testing.T) {
	canonical := nativeTestClaim("gitlab", "work-items")
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	validCredential := providerfoundation.Credential{Provider: "gitlab", ID: canonical.CredentialID}
	validClient := gitLabWorkItemsClient(t, fakehttp.Client(&gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}))
	wrongClient := *validClient
	wrongClient.Provider = "github"
	noBefore := canonical
	noBefore.BeforeAt = nil
	wrongProvider := nativeTestClaim("github", "work-items")
	// Keep the later GitLab project binding syntactically valid so a mutated
	// provider guard is observed by the no-I/O assertion, even though the
	// normalizer will still reject the foreign claim after the request.
	wrongProvider.SourceExternalID = "123"
	cases := []struct {
		name       string
		claim      Claim
		credential providerfoundation.Credential
		client     *providerfoundation.HTTPClient
		handler    GitLabWorkItemsRouteHandler
	}{
		{name: "wrong claim provider", claim: wrongProvider, credential: validCredential, client: validClient, handler: GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t)}},
		{name: "wrong claim dataset", claim: nativeTestClaim("gitlab", "commits"), credential: validCredential, client: validClient, handler: GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t)}},
		{name: "wrong credential provider", claim: canonical, credential: providerfoundation.Credential{Provider: "github", ID: canonical.CredentialID}, client: validClient, handler: GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t)}},
		{name: "credential mismatch", claim: canonical, credential: providerfoundation.Credential{Provider: "gitlab", ID: firstRunID}, client: validClient, handler: GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t)}},
		{name: "wrong client provider", claim: canonical, credential: validCredential, client: &wrongClient, handler: GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t)}},
		{name: "nil client", claim: canonical, credential: validCredential, client: nil, handler: GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t)}},
		{name: "zero normalized at", claim: canonical, credential: validCredential, client: validClient, handler: GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t)}},
		{name: "missing before bound", claim: noBefore, credential: validCredential, client: validClient, handler: GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t)}},
		{name: "missing status mapping", claim: canonical, credential: validCredential, client: validClient, handler: GitLabWorkItemsRouteHandler{}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			doer := &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}
			client := test.client
			if client != nil {
				copy := *client
				copy.Doer = fakehttp.Client(doer)
				client = &copy
			}
			normalizedAt := now
			if test.name == "zero normalized at" {
				normalizedAt = time.Time{}
			}
			_, err := test.handler.Collect(context.Background(), test.claim, test.credential, client, normalizedAt)
			if err != ErrInvalidConfiguration {
				t.Fatalf("error=%v", err)
			}
			if len(doer.requests) != 0 {
				t.Fatalf("invalid scope issued %d request(s)", len(doer.requests))
			}
		})
	}
}

func TestGitLabWorkItemsRouteAcceptsOnlyCanonicalClaimAcrossFiveAliases(t *testing.T) {
	for _, dataset := range workitemcontract.FamilyDatasets() {
		dataset := dataset
		t.Run(dataset, func(t *testing.T) {
			claim := nativeTestClaim("gitlab", dataset)
			_, err := (GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t)}).Collect(
				context.Background(), claim,
				providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
				gitLabWorkItemsClient(t, fakehttp.Client(&gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()})),
				time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
			)
			if dataset == "work-items" {
				if err != nil {
					t.Fatalf("canonical claim error=%v", err)
				}
				return
			}
			if err != ErrInvalidConfiguration {
				t.Fatalf("alias error=%v", err)
			}
		})
	}
}

func jsonUnmarshalEffect(raw []byte, target any) error {
	return json.Unmarshal(raw, target)
}

// CHAOS-9782: a milestone owned by an ancestor group becomes a sprints row keyed by
// its group (the key a project's target names), once however often it is listed,
// and the issue planned on it carries its id.
func TestGitLabWorkItemsRouteWritesGroupMilestoneSprintRows(t *testing.T) {
	responses := gitLabWorkItemResponses()
	root := "/api/v4/projects/123"
	const group = `{"id":31,"title":"Group Q3","state":"active","start_date":"2026-07-01","due_date":"2026-09-30","group_id":1,"web_url":"https://gitlab.example/groups/acme/-/milestones/2"}`
	responses[root+"/milestones?page=1"] = []string{`[{"id":7,"title":"July","state":"closed","start_date":"2026-07-01","due_date":"2026-07-31"},` + group + `,` + group + `]`, `[]`}
	issues := responses[root+"/issues?page=1"]
	issues[0] = strings.Replace(issues[0], `"milestone":{"id":7,"title":"July"}`, `"milestone":{"id":31,"title":"Group Q3"}`, 1)
	doer := &gitLabWorkItemsDoer{responses: responses}
	claim := nativeTestClaim("gitlab", "work-items")
	client := gitLabWorkItemsClient(t, fakehttp.Client(doer))
	batch, err := (GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 3, MaxPages: 10, NestedMaxPages: 10}).Collect(
		context.Background(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID}, client,
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{}
	for _, effect := range batch.Effects {
		if effect.Destination != "sprints" {
			continue
		}
		for _, raw := range effect.Rows {
			var sprint gitlabSprintRow
			if err := jsonUnmarshalEffect(raw, &sprint); err != nil {
				t.Fatal(err)
			}
			keys = append(keys, sprint.SprintID)
		}
	}
	slices.Sort(keys)
	if want := []string{gitlabMilestoneSprintID("acme/api", "7"), gitlabMilestoneSprintID("acme", "31")}; !slices.Equal(keys, want) {
		t.Fatalf("sprint keys = %v, want %v (group milestone once, under its group)", keys, want)
	}
	for _, effect := range batch.Effects {
		if effect.Destination != "work_items" {
			continue
		}
		for _, raw := range effect.Rows {
			var item gitlabWorkItemRow
			if err := jsonUnmarshalEffect(raw, &item); err != nil {
				t.Fatal(err)
			}
			if item.WorkItemID == "gitlab:acme/api#42" && (item.SprintID == nil || *item.SprintID != "31") {
				t.Fatalf("issue sprint id = %v, want 31", item.SprintID)
			}
		}
	}
	for _, request := range doer.requests {
		if strings.HasSuffix(request.URL.Path, "/milestones") {
			query := request.URL.Query()
			if query.Get("include_ancestors") != "true" || query.Get("include_parent_milestones") != "true" {
				t.Fatalf("milestone read %s lacks the ancestor parameters", request.URL.String())
			}
		}
	}
}

// A unit that fails binding its project (here: the project endpoint is missing)
// still names its window at Info: the line is not behind the project lookup.
func TestGitLabWorkItemsRouteLogsItsWindowEvenWhenTheProjectLookupFails(t *testing.T) {
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	claim := nativeTestClaim("gitlab", "work-items")
	client := gitLabWorkItemsClient(t, fakehttp.Client(&gitLabWorkItemsDoer{responses: map[string][]string{}}))
	handler := GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 2, MaxPages: 10, NestedMaxPages: 10}
	if _, err := handler.Collect(context.Background(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		client, time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("the failed project lookup returned no error")
	}
	line := logged.String()
	if !strings.Contains(line, "msg=gitlab_work_items_window") || !strings.Contains(line, "updated_after=2026-07-01T00:00:00Z") ||
		!strings.Contains(line, "updated_before=2026-07-31T23:59:59Z") {
		t.Fatalf("a unit failing at the project lookup named no window: %q", line)
	}
}
