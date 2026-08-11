package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

type gitLabWorkItemsDoer struct {
	responses map[string][]string
	requests  []*http.Request
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
	return &http.Response{
		StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

func gitLabWorkItemsClient(t *testing.T, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"gitlab", "https://gitlab.example", doer,
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
		root + "/issues?page=1":     {`[{"iid":42,"title":"Fix the API","description":"blocks #7 and see CHAOS-9","state":"opened","created_at":"2026-07-02T09:00:00Z","updated_at":"2026-07-03T09:00:00Z","closed_at":null,"labels":["bug","in progress","priority::high"],"assignees":[{"email":"Alice@EXAMPLE.COM","username":"alice","name":"Alice"}],"author":{"email":"bob@example.com","username":"bob","name":"Bob"},"web_url":"https://gitlab.example/acme/api/-/issues/42","weight":3,"milestone":{"id":7,"title":"July"}}]`, `[]`},
		root + "/issues/42/resource_label_events?page=1":        {`[{"action":"add","created_at":"2026-07-02T10:00:00Z","label":{"name":"done"}}]`, `[]`},
		root + "/issues/42/resource_state_events?page=1":        {`[{"state":"reopened","created_at":"2026-07-03T10:00:00Z","user":{"username":"bob","name":"Bob"}}]`, `[]`},
		root + "/issues/42/links?page=1":                        {`[{"link_type":"blocks","iid":7,"references":{"full":"acme/api#7"}}]`, `[]`},
		root + "/issues/42/notes?page=1":                        {`[{"system":true,"body":"label changed","created_at":"2026-07-02T11:00:00Z"},{"system":false,"body":"hello 🌍","created_at":"2026-07-02T12:00:00Z","author":{"username":"alice"}}]`, `[]`},
		root + "/merge_requests?page=1":                         {`[{"iid":9,"title":"Ship the API","description":"","state":"opened","created_at":"2026-07-04T09:00:00Z","updated_at":"2026-07-04T10:00:00Z","closed_at":null,"merged_at":null,"labels":["priority::low"],"assignees":[],"author":{"username":"alice"},"web_url":"https://gitlab.example/acme/api/-/merge_requests/9","milestone":null}]`, `[]`},
		root + "/merge_requests/9/resource_state_events?page=1": {`[{"state":"opened","created_at":"2026-07-04T09:00:00Z","user":{"username":"alice"}},{"state":"merged","created_at":"2026-07-05T09:00:00Z","user":{"username":"bob"}}]`, `[]`},
		root + "/merge_requests/9/notes?page=1":                 {`[{"system":false,"body":"ship it","created_at":"2026-07-04T12:00:00Z","author":{"username":"bob"}}]`, `[]`},
	}
}

func TestGitLabWorkItemsRouteNormalizesSixRawFactsAndReportsDerivedGap(t *testing.T) {
	doer := &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}
	claim := nativeTestClaim("gitlab", "work-items")
	now := time.Date(2026, 8, 3, 12, 0, 0, 987654321, time.UTC)
	batch, err := (GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 2, MaxPages: 10, NestedMaxPages: 10}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, doer), now,
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
	if len(byDestination) != 6 || len(byDestination["work_items"].Rows) != 2 ||
		len(byDestination["work_item_transitions"].Rows) != 3 ||
		len(byDestination["work_item_dependencies"].Rows) != 2 ||
		len(byDestination["work_item_reopen_events"].Rows) != 1 ||
		len(byDestination["work_item_interactions"].Rows) != 2 ||
		len(byDestination["sprints"].Rows) != 1 {
		t.Fatalf("raw effects=%+v", byDestination)
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
	if !ok || summary.WorkItemsSynced != 2 || len(summary.RawDestinations) != 6 ||
		len(summary.DerivedDestinationsUnimplemented) != 10 || !summary.WatermarkHeldForDerivedGap {
		t.Fatalf("typed summary=%T/%+v", batch.Result["gitlab_work_items"], batch.Result["gitlab_work_items"])
	}
	if batch.Watermark != nil || batch.Result["watermark_held_for_derived_gap"] != true ||
		len(batch.Result["derived_destinations_unimplemented"].([]string)) != 10 {
		t.Fatalf("watermark/result=%v/%+v", batch.Watermark, batch.Result)
	}
	for _, request := range doer.requests {
		if strings.HasSuffix(request.URL.Path, "/issues") || strings.HasSuffix(request.URL.Path, "/merge_requests") {
			query := request.URL.Query()
			if query.Get("updated_after") == "" || query.Get("updated_before") != "" || query.Get("state") != "all" {
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
		gitLabWorkItemsClient(t, &gitLabWorkItemsDoer{responses: responses}), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != ErrPaginationCapExceeded {
		t.Fatalf("cap error=%v", err)
	}
	_, err = (GitLabWorkItemsRouteHandler{}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != ErrInvalidConfiguration {
		t.Fatalf("mapping error=%v", err)
	}
}

func TestGitLabWorkItemsRouteRejectsInvalidScopeBeforeRequests(t *testing.T) {
	canonical := nativeTestClaim("gitlab", "work-items")
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	validCredential := providerfoundation.Credential{Provider: "gitlab", ID: canonical.CredentialID}
	validClient := gitLabWorkItemsClient(t, &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()})
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
				copy.Doer = doer
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
				gitLabWorkItemsClient(t, &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}),
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
