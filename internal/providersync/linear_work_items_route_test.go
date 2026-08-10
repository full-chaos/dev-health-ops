package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type linearWorkItemsDoer struct {
	responses []string
	requests  []*http.Request
}

func boolPointer(value bool) *bool { return &value }

func (doer *linearWorkItemsDoer) Do(request *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	request.Body = io.NopCloser(strings.NewReader(string(body)))
	doer.requests = append(doer.requests, request.Clone(request.Context()))
	if len(doer.responses) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	body = []byte(doer.responses[0])
	doer.responses = doer.responses[1:]
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(string(body))),
		Request:    request,
	}, nil
}

func linearWorkItemsClient(t *testing.T, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"linear", "https://api.linear.app", doer,
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func linearTeamResponse() string {
	return `{"data":{"teams":{"nodes":[{"id":"team-eng","key":"ENG","name":"Engineering"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
}

func TestLinearWorkItemsRouteNormalizesLiveIssueAndHistory(t *testing.T) {
	t.Parallel()
	doer := &linearWorkItemsDoer{responses: []string{linearTeamResponse(), `{
  "data": {"issues": {"nodes": [{
    "id":"lin-issue-42","identifier":"ENG-42","title":"Preserve the Linear work-item contract",
    "description":"A non-empty issue exercises the canonical normalizer.","priority":2,"estimate":5,
    "createdAt":"2026-07-25T09:00:00Z","updatedAt":"2026-07-28T16:30:00Z","startedAt":"2026-07-26T10:00:00Z",
    "completedAt":null,"canceledAt":null,"dueDate":"2026-08-01T00:00:00Z","url":"https://linear.app/fullchaos/issue/ENG-42",
    "state":{"name":"In Progress","type":"started"},"labels":{"nodes":[{"name":"bug"},{"name":"priority::high"}]},
    "assignee":{"email":"alice@example.com","name":"Alice"},"creator":{"email":"bob@example.com","name":"Bob"},
    "team":{"id":"team-eng","key":"ENG","name":"Engineering"},"project":{"id":"project-platform","name":"Platform"},
    "cycle":{"id":"cycle-7","name":"Sprint 7","number":7},"parent":{"identifier":"ENG-1"},
    "history":{"nodes":[
      {"createdAt":"2026-07-26T10:00:00Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"In Progress","type":"started"},"actor":{"email":"alice@example.com","name":"Alice"}},
      {"createdAt":"2026-07-27T11:00:00Z","fromState":{"name":"Done","type":"completed"},"toState":{"name":"In Progress","type":"started"},"actor":{"email":"bob@example.com","name":"Bob"}}
    ]}
  }],"pageInfo":{"hasNextPage":false,"endCursor":null}}}
}`}}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 987654321, time.UTC)
	noCycles := false
	batch, err := (LinearWorkItemsRouteHandler{PerPage: 50, MaxPages: 10, FetchCycles: &noCycles}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 2 {
		t.Fatalf("requests=%d", len(doer.requests))
	}
	var requestBody struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}
	if err := json.NewDecoder(doer.requests[1].Body).Decode(&requestBody); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(requestBody.Query, "query LinearWorkItems") ||
		requestBody.Variables["first"] != float64(50) ||
		requestBody.Variables["after"] != nil {
		t.Fatalf("request=%+v", requestBody)
	}
	filter, ok := requestBody.Variables["filter"].(map[string]any)
	if !ok {
		t.Fatalf("filter=%T", requestBody.Variables["filter"])
	}
	if filter["team"].(map[string]any)["key"].(map[string]any)["in"].([]any)[0] != "ENG" {
		t.Fatalf("team filter=%+v", filter)
	}
	byDestination := map[string]EffectBatch{}
	for _, effect := range batch.Effects {
		byDestination[effect.Destination] = effect
	}
	if len(byDestination) != 6 || len(byDestination["work_items"].Rows) != 1 ||
		len(byDestination["work_item_transitions"].Rows) != 2 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row linearWorkItemRow
	if err := json.Unmarshal(byDestination["work_items"].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row.WorkItemID != "linear:ENG-42" || row.Provider != "linear" || row.Type != "bug" ||
		row.Status != "in_progress" || row.StatusRaw == nil || *row.StatusRaw != "In Progress" ||
		row.NativeTeamKey == nil || *row.NativeTeamKey != "ENG" || row.ProjectID == nil ||
		*row.ProjectID != "project-platform" || row.Assignees[0] != "alice@example.com" ||
		row.Reporter == nil || *row.Reporter != "bob@example.com" || row.StoryPoints == nil ||
		*row.StoryPoints != 5 || row.PriorityRaw == nil || *row.PriorityRaw != "high" ||
		row.ServiceClass == nil || *row.ServiceClass != "fixed_date" ||
		!row.LastSynced.Equal(normalizedAt.UTC().Truncate(time.Millisecond)) {
		t.Fatalf("row=%+v", row)
	}
	var transition linearWorkItemTransitionRow
	if err := json.Unmarshal(byDestination["work_item_transitions"].Rows[1], &transition); err != nil {
		t.Fatal(err)
	}
	if transition.WorkItemID != row.WorkItemID || transition.FromStatus != "done" ||
		transition.ToStatus != "in_progress" || transition.Actor == nil ||
		*transition.Actor != "bob@example.com" {
		t.Fatalf("transition=%+v", transition)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) ||
		batch.Evidence.Provider != "linear" || batch.Evidence.Dataset != "work-items" ||
		batch.Evidence.Requests != 2 || batch.Evidence.Pages != 2 || batch.Evidence.Records != 4 {
		t.Fatalf("watermark=%v evidence=%+v", batch.Watermark, batch.Evidence)
	}
}

func TestLinearWorkItemsRouteFailsClosedOnGraphQLErrorsAndCaps(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	for _, testCase := range []struct {
		name      string
		responses []string
		handler   LinearWorkItemsRouteHandler
		want      error
	}{
		{name: "graphql error", responses: []string{`{"errors":[{"message":"bad filter"}]}`}, handler: LinearWorkItemsRouteHandler{FetchCycles: boolPointer(false)}, want: providerfoundation.ErrGraphQLResponse},
		{name: "pagination cap", responses: []string{linearTeamResponse(), `{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"cursor-1"}}}}`, `{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`}, handler: LinearWorkItemsRouteHandler{MaxPages: 1, FetchCycles: boolPointer(false)}, want: ErrPaginationCapExceeded},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			doer := &linearWorkItemsDoer{responses: testCase.responses}
			_, err := testCase.handler.Collect(context.Background(), claim,
				providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
				linearWorkItemsClient(t, doer), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))
			if err == nil || !strings.Contains(err.Error(), testCase.want.Error()) {
				t.Fatalf("error=%v want=%v", err, testCase.want)
			}
		})
	}
}

func TestLinearWorkItemsRouteCollectsRawLinearSurfaces(t *testing.T) {
	t.Parallel()
	doer := &linearWorkItemsDoer{responses: []string{
		`{"data":{"teams":{"nodes":[{"id":"team-eng","key":"ENG","name":"Engineering"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"cycles":{"nodes":[{"id":"cycle-7","number":7,"name":"","startsAt":"2026-07-25T09:00:00Z","endsAt":"2026-08-01T09:00:00Z","completedAt":"2026-08-01T09:00:00Z","progress":1,"team":{"id":"team-eng","key":"ENG","name":"Engineering"}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"issues":{"nodes":[{
			"id":"lin-issue-42","identifier":"ENG-42","title":"Raw Linear surfaces",
			"description":"exercise raw effects","priority":2,"estimate":3,
			"createdAt":"2026-07-25T09:00:00Z","updatedAt":"2026-07-28T16:30:00Z",
			"startedAt":"2026-07-26T10:00:00Z","completedAt":null,"canceledAt":null,
			"dueDate":null,"url":"https://linear.app/fullchaos/issue/ENG-42","archivedAt":null,
			"state":{"name":"In Progress","type":"started"},
			"labels":{"nodes":[{"name":"bug"}]},
			"assignee":{"email":"alice@example.com","name":"Alice"},
			"creator":{"email":"bob@example.com","name":"Bob"},
			"team":{"id":"team-eng","key":"ENG","name":"Engineering"},
			"project":{"id":"project-platform","name":"Platform"},"cycle":{"id":"cycle-7","name":"Sprint 7","number":7},
			"parent":null,
			"history":{"nodes":[
				{"createdAt":"2026-07-26T10:00:00Z","fromState":{"name":"Todo","type":"unstarted"},"toState":{"name":"In Progress","type":"started"},"actor":{"email":"alice@example.com","name":"Alice"}},
				{"createdAt":"2026-07-27T11:00:00Z","fromState":{"name":"Done","type":"completed"},"toState":{"name":"In Progress","type":"started"},"actor":{"email":"bob@example.com","name":"Bob"}}
			]},
			"comments":{"nodes":[{"body":"hello 🌍","createdAt":"2026-07-27T12:00:00Z","user":{"email":"alice@example.com","name":"Alice"}},{"body":"","createdAt":"2026-07-27T13:00:00Z","user":null}],"pageInfo":{"hasNextPage":false,"endCursor":null}},
			"attachments":{"nodes":[{"url":"https://github.com/acme/repo/pull/9","sourceType":"github"},{"url":"https://evil.example/acme/repo/pull/10","sourceType":"github"},{"url":"https://github.com/acme/repo/pull/9","sourceType":"github"}],"pageInfo":{"hasNextPage":false,"endCursor":null}},
			"relations":{"nodes":[{"type":"blocked_by","issue":{"identifier":"ENG-42"},"relatedIssue":{"identifier":"ENG-1"}}],"pageInfo":{"hasNextPage":false,"endCursor":null}},
			"inverseRelations":{"nodes":[{"type":"blocked_by","issue":{"identifier":"ENG-42"},"relatedIssue":{"identifier":"ENG-1"}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}
		}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 987654321, time.UTC)
	batch, err := (LinearWorkItemsRouteHandler{}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	byDestination := map[string]EffectBatch{}
	for _, effect := range batch.Effects {
		byDestination[effect.Destination] = effect
	}
	wantDestinations := []string{"work_items", "work_item_transitions", "work_item_dependencies", "work_item_reopen_events", "work_item_interactions", "sprints"}
	for _, destination := range wantDestinations {
		if _, ok := byDestination[destination]; !ok {
			t.Fatalf("missing destination %q: %+v", destination, byDestination)
		}
	}
	if len(byDestination["work_item_dependencies"].Rows) != 2 ||
		len(byDestination["work_item_reopen_events"].Rows) != 1 ||
		len(byDestination["work_item_interactions"].Rows) != 1 ||
		len(byDestination["sprints"].Rows) != 1 {
		t.Fatalf("raw effects=%+v", byDestination)
	}
	var dependency linearWorkItemDependencyRow
	if err := json.Unmarshal(byDestination["work_item_dependencies"].Rows[1], &dependency); err != nil {
		t.Fatal(err)
	}
	if dependency.SourceWorkItemID != "linear:ENG-1" || dependency.TargetWorkItemID != "linear:ENG-42" || dependency.RelationshipType != "blocks" {
		t.Fatalf("dependency=%+v", dependency)
	}
	var interaction linearWorkItemInteractionRow
	if err := json.Unmarshal(byDestination["work_item_interactions"].Rows[0], &interaction); err != nil {
		t.Fatal(err)
	}
	if interaction.BodyLength != len([]rune("hello 🌍")) || interaction.Actor == nil || *interaction.Actor != "alice@example.com" {
		t.Fatalf("interaction=%+v", interaction)
	}
	var sprint linearSprintRow
	if err := json.Unmarshal(byDestination["sprints"].Rows[0], &sprint); err != nil {
		t.Fatal(err)
	}
	if sprint.SprintID != "linear:cycle:cycle-7" || sprint.State == nil || *sprint.State != "closed" || sprint.NativeTeamKey == nil || *sprint.NativeTeamKey != "ENG" {
		t.Fatalf("sprint=%+v", sprint)
	}
	if batch.Evidence.Requests != 3 || batch.Evidence.Pages != 3 || batch.Evidence.Records != 8 {
		t.Fatalf("evidence=%+v", batch.Evidence)
	}
}

func TestLinearAttachmentIdentityUsesPythonTrustedNetlocBoundary(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name       string
		attachment linearAttachmentPayload
		want       string
	}{
		{
			name: "gitlab nested project",
			attachment: linearAttachmentPayload{
				URL: "https://gitlab.com/group/subgroup/project/-/merge_requests/17", SourceType: "gitlab",
			},
			want: "gitlab:group/subgroup/project!17",
		},
		{
			name: "explicit port is not public netloc",
			attachment: linearAttachmentPayload{
				URL: "https://github.com:443/acme/repo/pull/9", SourceType: "github",
			},
		},
		{
			name: "userinfo cannot borrow trusted host",
			attachment: linearAttachmentPayload{
				URL: "https://github.com@evil.example/acme/repo/pull/9", SourceType: "github",
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := linearAttachmentWorkItemID(testCase.attachment); got != testCase.want {
				t.Fatalf("attachment id=%q want=%q", got, testCase.want)
			}
		})
	}
}

func TestLinearWorkItemsRouteFetchControlsSuppressOptionalRawFacts(t *testing.T) {
	t.Parallel()
	no := false
	doer := &linearWorkItemsDoer{responses: []string{linearTeamResponse(), `{"data":{"issues":{"nodes":[{
		"id":"lin-issue-44","identifier":"ENG-44","title":"Optional facts",
		"createdAt":"2026-07-25T09:00:00Z","updatedAt":"2026-07-28T16:30:00Z",
		"state":{"name":"In Progress","type":"started"},
		"labels":{"nodes":[]},"history":{"nodes":[{"createdAt":"2026-07-27T11:00:00Z","fromState":{"name":"Done","type":"completed"},"toState":{"name":"In Progress","type":"started"},"actor":null}]},
		"comments":{"nodes":[{"body":"comment","createdAt":"2026-07-27T12:00:00Z","user":null}],"pageInfo":{"hasNextPage":false,"endCursor":null}},
		"attachments":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},
		"relations":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},
		"inverseRelations":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}
	}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`}}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	batch, err := (LinearWorkItemsRouteHandler{FetchComments: &no, FetchHistory: &no, FetchCycles: &no}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, effect := range batch.Effects {
		if (effect.Destination == "work_item_transitions" || effect.Destination == "work_item_reopen_events" || effect.Destination == "work_item_interactions") && len(effect.Rows) != 0 {
			t.Fatalf("optional destination %s rows=%d", effect.Destination, len(effect.Rows))
		}
	}
}

func TestLinearWorkItemsRouteCompletesTruncatedNativeRelations(t *testing.T) {
	t.Parallel()
	doer := &linearWorkItemsDoer{responses: []string{
		linearTeamResponse(),
		`{"data":{"issues":{"nodes":[{
			"id":"lin-issue-45","identifier":"ENG-45","title":"Paginated relation",
			"createdAt":"2026-07-25T09:00:00Z","updatedAt":"2026-07-28T16:30:00Z",
			"state":{"name":"In Progress","type":"started"},"labels":{"nodes":[]},
			"history":{"nodes":[]},"comments":{"nodes":[]},"attachments":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},
			"relations":{"nodes":[{"type":"related","issue":{"identifier":"ENG-45"},"relatedIssue":{"identifier":"ENG-2"}}],"pageInfo":{"hasNextPage":true,"endCursor":"relation-cursor"}},
			"inverseRelations":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}
		}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"issue":{"relations":{"nodes":[{"type":"related","issue":{"identifier":"ENG-45"},"relatedIssue":{"identifier":"ENG-2"}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`,
	}}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	batch, err := (LinearWorkItemsRouteHandler{FetchCycles: boolPointer(false)}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	byDestination := map[string]EffectBatch{}
	for _, effect := range batch.Effects {
		byDestination[effect.Destination] = effect
	}
	if len(byDestination["work_item_dependencies"].Rows) != 1 || batch.Evidence.Requests != 3 || batch.Evidence.Pages != 3 {
		t.Fatalf("effects=%+v evidence=%+v", byDestination, batch.Evidence)
	}
}

func TestLinearWorkItemsRoutePaginatesCommentsWithinPythonBound(t *testing.T) {
	t.Parallel()
	doer := &linearWorkItemsDoer{responses: []string{
		linearTeamResponse(),
		`{"data":{"issues":{"nodes":[{
			"id":"lin-issue-46","identifier":"ENG-46","title":"Paginated comments",
			"createdAt":"2026-07-25T09:00:00Z","updatedAt":"2026-07-28T16:30:00Z",
			"state":{"name":"Todo","type":"unstarted"},"labels":{"nodes":[]},
			"comments":{"nodes":[{"body":"first","createdAt":"2026-07-27T12:00:00Z","user":null}],"pageInfo":{"hasNextPage":true,"endCursor":"comment-cursor"}},
			"attachments":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},
			"relations":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},
			"inverseRelations":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}
		}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"issue":{"comments":{"nodes":[{"body":"first","createdAt":"2026-07-27T12:00:00Z","user":null}],"pageInfo":{"hasNextPage":true,"endCursor":"comment-cursor-2"}}}}}`,
		`{"data":{"issue":{"comments":{"nodes":[{"body":"second 🌍","createdAt":"2026-07-27T13:00:00Z","user":null}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`,
	}}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	batch, err := (LinearWorkItemsRouteHandler{FetchCycles: boolPointer(false)}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	byDestination := map[string]EffectBatch{}
	for _, effect := range batch.Effects {
		byDestination[effect.Destination] = effect
	}
	if len(byDestination["work_item_interactions"].Rows) != 2 || batch.Evidence.Requests != 4 || batch.Evidence.Pages != 4 {
		t.Fatalf("interactions=%d evidence=%+v", len(byDestination["work_item_interactions"].Rows), batch.Evidence)
	}
	var second linearWorkItemInteractionRow
	if err := json.Unmarshal(byDestination["work_item_interactions"].Rows[1], &second); err != nil {
		t.Fatal(err)
	}
	if second.BodyLength != len([]rune("second 🌍")) {
		t.Fatalf("unicode body length=%d", second.BodyLength)
	}
}

func TestLinearWorkItemsRouteFailsClosedWhenCommentsExceedBound(t *testing.T) {
	t.Parallel()
	doer := &linearWorkItemsDoer{responses: []string{
		linearTeamResponse(),
		`{"data":{"issues":{"nodes":[{"id":"lin-issue-47","identifier":"ENG-47","title":"Too many comments","createdAt":"2026-07-25T09:00:00Z","updatedAt":"2026-07-28T16:30:00Z","state":{"name":"Todo","type":"unstarted"},"labels":{"nodes":[]},"comments":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"comment-cursor"}},"attachments":{"nodes":[]},"relations":{"nodes":[]},"inverseRelations":{"nodes":[]}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
		`{"data":{"issue":{"comments":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"comment-cursor-2"}}}}}`,
		`{"data":{"issue":{"comments":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"comment-cursor-3"}}}}}`,
	}}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	_, err := (LinearWorkItemsRouteHandler{FetchCycles: boolPointer(false)}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("error=%v, want comment pagination cap", err)
	}
}

func TestLinearReferenceTeamAndSprintCacheMirrorPythonResolution(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = "ENG"
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	nativeKey := "ENG"
	active := "active"
	referenceSprint := linearSprintRow{
		Provider: "linear", SprintID: "linear:cycle:cached", Name: stringPointer("Cached cycle"),
		State: &active, NativeTeamKey: &nativeKey, LastSynced: now, OrgID: claim.OrgID,
	}
	doer := &linearWorkItemsDoer{responses: []string{
		`{"data":{"issues":{"nodes":[{"id":"lin-issue-48","identifier":"ENG-48","title":"Reference sprint","createdAt":"2026-07-25T09:00:00Z","updatedAt":"2026-07-28T16:30:00Z","state":{"name":"Todo","type":"unstarted"},"labels":{"nodes":[]},"attachments":{"nodes":[]},"relations":{"nodes":[]},"inverseRelations":{"nodes":[]}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}}
	handler := LinearWorkItemsRouteHandler{
		ReferenceTeams:   []LinearReferenceTeam{{Provider: "", ID: "team-eng", Name: "Engineering", ProjectKeys: []string{"ENG"}}},
		ReferenceSprints: []linearSprintRow{referenceSprint},
	}
	batch, err := handler.Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 1 || batch.Evidence.Requests != 1 {
		t.Fatalf("reference cache unexpectedly queried API: requests=%d evidence=%+v", len(doer.requests), batch.Evidence)
	}
	for _, effect := range batch.Effects {
		if effect.Destination == "sprints" && len(effect.Rows) != 1 {
			t.Fatalf("cached sprint effect=%+v", effect)
		}
	}
}

func TestLinearReferenceTeamRejectsForeignProviderAndUsesFallbacks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		rows []LinearReferenceTeam
		key  string
		want linearTeamPayload
		ok   bool
	}{
		{name: "project key", rows: []LinearReferenceTeam{{Provider: "linear", ID: "team-1", Name: "Platform", ProjectKeys: []string{"ENG"}}}, key: "ENG", want: linearTeamPayload{ID: "team-1", Key: "ENG", Name: "Platform"}, ok: true},
		{name: "blank fields", rows: []LinearReferenceTeam{{Provider: "", ProjectKeys: []string{"ENG"}}}, key: "ENG", want: linearTeamPayload{ID: "ENG", Key: "ENG", Name: "ENG"}, ok: true},
		{name: "foreign provider", rows: []LinearReferenceTeam{{Provider: "github", ID: "ENG"}}, key: "ENG", ok: false},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got, ok := linearReferenceTeamPayload(testCase.rows, testCase.key)
			if ok != testCase.ok || (ok && got != testCase.want) {
				t.Fatalf("got=%+v ok=%v want=%+v ok=%v", got, ok, testCase.want, testCase.ok)
			}
		})
	}
}

func TestLinearWorkItemsRouteRejectsInvalidScopeAndLease(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("linear", "work-items")
	client := linearWorkItemsClient(t, &linearWorkItemsDoer{responses: []string{
		linearTeamResponse(),
		`{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`,
	}})
	validCredential := providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID}
	normalizedAt := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	for _, testCase := range []struct {
		name       string
		ctx        context.Context
		claim      Claim
		credential providerfoundation.Credential
		client     *providerfoundation.HTTPClient
	}{
		{name: "wrong provider", ctx: context.Background(), claim: func() Claim { c := claim; c.Provider = "github"; return c }(), credential: validCredential, client: client},
		{name: "wrong dataset", ctx: context.Background(), claim: func() Claim { c := claim; c.Dataset = "comments"; return c }(), credential: validCredential, client: client},
		{name: "credential provider mismatch", ctx: context.Background(), claim: claim, credential: providerfoundation.Credential{Provider: "github", ID: claim.CredentialID}, client: client},
		{name: "nil context", ctx: nil, claim: claim, credential: validCredential, client: client},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := (LinearWorkItemsRouteHandler{}).Collect(
				testCase.ctx, testCase.claim, testCase.credential,
				testCase.client, normalizedAt,
			)
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	leaseLostClient, err := providerfoundation.NewHTTPClient(
		"linear", "https://api.linear.app", &linearWorkItemsDoer{},
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error {
			return providerfoundation.ErrLeaseLost
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (LinearWorkItemsRouteHandler{}).Collect(
		context.Background(), claim, validCredential, leaseLostClient, normalizedAt,
	); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("lease error=%v", err)
	}
}

func TestLinearWorkItemClosedAtPrefersCompletedAt(t *testing.T) {
	claim := nativeTestClaim("linear", "work-items")
	completed := "2026-07-31T09:00:00Z"
	canceled := "2026-08-01T09:00:00Z"
	payload := linearWorkItemPayload{
		Identifier: "ENG-99", Title: "Both terminal timestamps",
		CreatedAt: "2026-07-29T09:00:00Z", UpdatedAt: "2026-08-01T09:00:00Z",
		CompletedAt: &completed, CanceledAt: &canceled,
		State: &linearStatePayload{Name: "Done", Type: "completed"},
	}
	row, _, err := normalizeLinearWorkItem(
		claim, payload, time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if row.ClosedAt == nil || !row.ClosedAt.Equal(time.Date(2026, 7, 31, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("closed_at=%v", row.ClosedAt)
	}
}
