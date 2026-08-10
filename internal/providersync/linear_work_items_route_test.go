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

func TestLinearWorkItemsRouteNormalizesLiveIssueAndHistory(t *testing.T) {
	t.Parallel()
	doer := &linearWorkItemsDoer{responses: []string{`{
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
	batch, err := (LinearWorkItemsRouteHandler{PerPage: 50, MaxPages: 10}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, doer), normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.requests) != 1 {
		t.Fatalf("requests=%d", len(doer.requests))
	}
	var requestBody struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}
	if err := json.NewDecoder(doer.requests[0].Body).Decode(&requestBody); err != nil {
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
	if len(byDestination) != 2 || len(byDestination["work_items"].Rows) != 1 ||
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
		batch.Evidence.Requests != 1 || batch.Evidence.Pages != 1 || batch.Evidence.Records != 3 {
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
		{name: "graphql error", responses: []string{`{"errors":[{"message":"bad filter"}]}`}, handler: LinearWorkItemsRouteHandler{}, want: providerfoundation.ErrGraphQLResponse},
		{name: "pagination cap", responses: []string{`{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"cursor-1"}}}}`, `{"data":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`}, handler: LinearWorkItemsRouteHandler{MaxPages: 1}, want: ErrPaginationCapExceeded},
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

func TestLinearWorkItemsRouteRejectsInvalidScopeAndLease(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("linear", "work-items")
	client := linearWorkItemsClient(t, &linearWorkItemsDoer{responses: []string{
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
