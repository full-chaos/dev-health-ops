package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func closingMRs(t *testing.T, raw string) []gitlabClosingMergeRequestPayload {
	t.Helper()
	var payloads []gitlabClosingMergeRequestPayload
	if err := json.Unmarshal([]byte(raw), &payloads); err != nil {
		t.Fatal(err)
	}
	return payloads
}

// CHAOS-8526: GitLab's closed_by response becomes MR-source / issue-target rows of raw kind gitlab_closing_reference,
// the orientation every other PRIMARY producer uses (the PR/MR is the edge SOURCE).
func TestNormalizeGitLabClosingMergeRequests(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "work-items")
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	issue := "gitlab:acme/api#42"
	rows := normalizeGitLabClosingMergeRequests(claim, issue, "acme/api", closingMRs(t, `[
		{"iid":9,"references":{"full":"acme/api!9"}},
		{"iid":9,"references":{"full":"acme/api!9"}},
		{"iid":3,"references":{"full":"other/fork!3"}},
		{"iid":4,"references":{"full":""}},
		{"iid":5,"references":{"full":"!5"}},
		{"iid":0,"references":{"full":"acme/api!0"}}
	]`), at)
	got := make([][2]string, 0, len(rows))
	for _, row := range rows {
		got = append(got, [2]string{row.SourceWorkItemID, row.TargetWorkItemID})
		if row.RelationshipType != "relates_to" || row.RelationshipTypeRaw != "gitlab_closing_reference" ||
			row.RelationshipSemanticsVersion != "canonical-blocks.v2" || row.OrgID != claim.OrgID || !row.LastSynced.Equal(at) {
			t.Fatalf("row=%+v", row)
		}
	}
	want := [][2]string{
		{"gitlab:acme/api!9", issue}, {"gitlab:other/fork!3", issue}, {"gitlab:acme/api!4", issue}, {"gitlab:acme/api!5", issue},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows=%v want=%v (duplicates and iid 0 dropped; a cross-project MR keeps its own path; a missing path falls back to the issue's project)", got, want)
	}
}

// The real producer's rows (not hand-authored ones) go through the real Derive: the issue and the MR it closes end up as
// one native link in work_graph_issue_pr, and an MR in another project resolves to ITS repo.
func TestGitLabClosingReferenceRowsBecomeNativeLinks(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "work-items")
	at := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	issue := "gitlab:acme/api#42"
	rows := normalizeGitLabClosingMergeRequests(claim, issue, "acme/api", closingMRs(t,
		`[{"iid":9,"references":{"full":"acme/api!9"}},{"iid":3,"references":{"full":"other/fork!3"}}]`), at)
	apiRepo, forkRepo := uuid.MustParse("44444444-4444-4444-8444-444444444444"), uuid.MustParse("55555555-5555-4555-8555-555555555555")
	inputs := issueprlinks.Inputs{
		OrgID:        claim.OrgID,
		Repos:        []issueprlinks.RepoRow{{OrgID: claim.OrgID, ID: apiRepo, Repo: "acme/api"}, {OrgID: claim.OrgID, ID: forkRepo, Repo: "other/fork"}},
		PullRequests: []issueprlinks.PullRequestRow{{OrgID: claim.OrgID, RepoID: apiRepo, Number: 9}, {OrgID: claim.OrgID, RepoID: forkRepo, Number: 3}},
		WorkItems:    []issueprlinks.WorkItemRow{{OrgID: claim.OrgID, WorkItemID: issue}},
	}
	for _, row := range rows {
		inputs.Dependencies = append(inputs.Dependencies, issueprlinks.DependencyRow{
			OrgID: row.OrgID, SourceWorkItemID: row.SourceWorkItemID, TargetWorkItemID: row.TargetWorkItemID,
			RelationshipTypeRaw: row.RelationshipTypeRaw, LastSynced: row.LastSynced,
		})
	}
	result := issueprlinks.Derive(inputs)
	if !result.Balanced() || result.Written() != 2 {
		t.Fatalf("wrote %d links (rejections %v), want 2", result.Written(), result.Rejected)
	}
	got := map[uint32]uuid.UUID{}
	for _, link := range result.Links {
		if link.WorkItemID != issue || link.Provenance != issueprlinks.ProvenanceNative || link.Evidence != "gitlab_closing_reference" {
			t.Fatalf("link=%+v", link)
		}
		got[link.PRNumber] = link.RepoID
	}
	if got[9] != apiRepo || got[3] != forkRepo {
		t.Fatalf("links resolved to repos %v, want MR 9 -> %s and MR 3 -> %s", got, apiRepo, forkRepo)
	}
}

// The closed_by response carries MRs of every state (opened, merged, closed without merging). The GitHub linker
// (extractGitHubClosingIssueReferences) emits a row for every closingIssuesReferences entry whatever the PR's state, and
// so does this one: provider-agnostic parity, CHAOS-8526. The state is the provider's own, not a filter of ours.
func TestNormalizeGitLabClosingMergeRequestsKeepsEveryMergeRequestState(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "work-items")
	rows := normalizeGitLabClosingMergeRequests(claim, "gitlab:acme/api#42", "acme/api", closingMRs(t, `[
		{"iid":1,"state":"opened","references":{"full":"acme/api!1"}},
		{"iid":2,"state":"merged","references":{"full":"acme/api!2"}},
		{"iid":3,"state":"closed","references":{"full":"acme/api!3"}}
	]`), time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC))
	if len(rows) != 3 {
		t.Fatalf("rows=%d want 3 (open, merged and closed-unmerged all link, as on GitHub)", len(rows))
	}
}

// closedByStatusDoer answers the closed_by endpoint with a fixed status and delegates everything else.
type closedByStatusDoer struct {
	inner  *gitLabWorkItemsDoer
	status int
	body   string // the answer body; empty means a small error object
}

func (doer closedByStatusDoer) Do(request *http.Request) (*http.Response, error) {
	if strings.HasSuffix(request.URL.Path, "/closed_by") {
		return &http.Response{
			StatusCode: doer.status, Status: http.StatusText(doer.status), Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(firstNonEmpty(doer.body, `{"message":"closed_by"}`))), Request: request,
		}, nil
	}
	return doer.inner.Do(request)
}

// The closed_by fetch of one issue failing is not swallowed and does not poison the batch: it is logged with the issue,
// counted under its own outcome, reported incomplete, and the other rows are kept. The watermark depends on the class
// (D4771): a TERMINAL answer (404/403, not readable with this credential) advances it, since retrying cannot help; a
// TRANSIENT one (5xx, 429, network) holds it, so the next run asks again.
func TestGitLabWorkItemsRouteClosedByFailureClassSplitsTheWatermark(t *testing.T) {
	cases := map[string]struct {
		status        int // 0: answers normally
		body          string
		wantOutcome   string
		wantWatermark bool
		wantTerminal  int
		wantTransient int
	}{
		"answers":                       {0, "", "synced", true, 0, 0},
		"terminal 404":                  {http.StatusNotFound, "", "terminal_unavailable", true, 1, 0},
		"terminal 403":                  {http.StatusForbidden, "", "terminal_unavailable", true, 1, 0},
		"transient 503":                 {http.StatusServiceUnavailable, "", "transient_failed", false, 0, 1},
		"transient 429":                 {http.StatusTooManyRequests, "", "transient_failed", false, 0, 1},
		"transient 500":                 {http.StatusInternalServerError, "", "transient_failed", false, 0, 1},
		"transient transport (no body)": {-1, "", "transient_failed", false, 0, 1},
		"terminal page cap exceeded":    {http.StatusOK, `[{"iid":9,"references":{"full":"acme/api!9"}},{"iid":10,"references":{"full":"acme/api!10"}}]`, "terminal_page_cap", true, 1, 0},
		"terminal undecodable answer":   {http.StatusOK, `["not an object"]`, "terminal_undecodable", true, 1, 0},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
			if err != nil {
				t.Fatal(err)
			}
			deriver := GitLabWorkItemDeriver{Source: &githubMultiDayOracleSource{}, statusMapping: loadRealStatusMapping(t), investmentClassifier: classifier}
			responses := gitLabWorkItemResponses()
			root := "/api/v4/projects/123"
			responses[root+"/merge_requests?page=1"] = []string{
				`[{"iid":9,"title":"Ship the API","description":"","state":"opened","created_at":"2026-07-04T09:00:00Z","updated_at":"2026-07-04T10:00:00Z","labels":["priority::low"],"assignees":[],"author":{"username":"alice","bot":false},"source_branch":"feature/ship-api"}]`, `[]`,
			}
			if c.status == -1 {
				delete(responses, root+"/issues/42/closed_by?page=1")
			}
			inner := &gitLabWorkItemsDoer{responses: responses}
			var doer providerfoundation.HTTPDoer = inner
			if c.status > 0 {
				doer = closedByStatusDoer{inner: inner, status: c.status, body: c.body}
			}
			claim := nativeTestClaim("gitlab", "work-items")
			claim.OrgID = "77777777-7777-4777-8777-777777777777"
			client := gitLabWorkItemsClient(t, fakehttp.Client(doer))
			client.Metrics = providerfoundation.NewMetrics()
			batch, err := (GitLabWorkItemsRouteHandler{
				StatusMapping: loadRealStatusMapping(t), Derived: deriver, PerPage: 2, MaxPages: 10, NestedMaxPages: 10,
			}).Collect(context.Background(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
				client, time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))
			if err != nil {
				t.Fatalf("a closed_by failure must not fail the batch: %v", err)
			}
			if (batch.Watermark != nil) != c.wantWatermark {
				t.Fatalf("watermark=%v want advanced=%v", batch.Watermark, c.wantWatermark)
			}
			if batch.Result["closing_reference_fetch_terminal"] != c.wantTerminal || batch.Result["closing_reference_fetch_transient"] != c.wantTransient {
				t.Fatalf("terminal=%v transient=%v want %d/%d", batch.Result["closing_reference_fetch_terminal"], batch.Result["closing_reference_fetch_transient"], c.wantTerminal, c.wantTransient)
			}
			incomplete, hasIncomplete := batch.Result["incomplete"].([]string)
			if failed := c.wantTerminal + c.wantTransient; failed > 0 {
				if !hasIncomplete || len(incomplete) != 1 || incomplete[0] != "gitlab:acme/api#42" {
					t.Fatalf("incomplete=%v want the issue ref (the row is reported incomplete in both classes)", batch.Result["incomplete"])
				}
				if dependencies := batch.Result["closing_reference_dependencies_synced"]; dependencies != 0 {
					t.Fatalf("closing rows synced=%v want 0", dependencies)
				}
			} else if hasIncomplete || batch.Result["closing_reference_dependencies_synced"] != 1 {
				t.Fatalf("answering closed_by: result=%v", batch.Result)
			}
			var exposition strings.Builder
			if err := client.Metrics.WritePrometheus(&exposition); err != nil {
				t.Fatal(err)
			}
			if want := `dev_health_gitlab_closing_mr_fetch_total{outcome="` + c.wantOutcome + `"} 1`; !strings.Contains(exposition.String(), want) {
				t.Fatalf("metrics lack %s:\n%s", want, exposition.String())
			}
		})
	}
}

// The classifier of a failed closed_by fetch (D4771): terminal reasons repeat on every run and let the watermark advance;
// everything else, an error that is not a ProviderError included, is transient and holds it.
func TestGitLabClosingFetchOutcome(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct {
		err  error
		want string
	}{
		"plain error is not a ProviderError": {errors.New("boom"), "transient_failed"},
		"wrapped plain error":                {fmt.Errorf("closed_by: %w", errors.New("boom")), "transient_failed"},
		"404":                                {&providerfoundation.ProviderError{StatusCode: http.StatusNotFound}, "terminal_unavailable"},
		"wrapped 403":                        {fmt.Errorf("closed_by: %w", &providerfoundation.ProviderError{StatusCode: http.StatusForbidden}), "terminal_unavailable"},
		"401 is not terminal here":           {&providerfoundation.ProviderError{StatusCode: http.StatusUnauthorized}, "transient_failed"},
		"429":                                {&providerfoundation.ProviderError{StatusCode: http.StatusTooManyRequests}, "transient_failed"},
		"503":                                {&providerfoundation.ProviderError{StatusCode: http.StatusServiceUnavailable}, "transient_failed"},
		"page cap":                           {ErrPaginationCapExceeded, "terminal_page_cap"},
		"undecodable":                        {fmt.Errorf("closed_by: %w", providerfoundation.ErrNormalizationInvalid), "terminal_undecodable"},
	} {
		if got := gitLabClosingFetchOutcome(c.err); got != c.want {
			t.Errorf("%s: outcome %q want %q", name, got, c.want)
		}
	}
}

// closedByFailingDoer fails the closed_by request with an error whose text is a distinctive plain sentinel.
type closedByFailingDoer struct{ inner *gitLabWorkItemsDoer }

const closedByErrorSentinel = "closed-by-detail-marker"

func (doer closedByFailingDoer) Do(request *http.Request) (*http.Response, error) {
	if strings.HasSuffix(request.URL.Path, "/closed_by") {
		return nil, errors.New(closedByErrorSentinel)
	}
	return doer.inner.Do(request)
}

// The failed-fetch log line carries the error's class and type, never its text (CHAOS-7933, D4270): neither the sentinel
// text of the transport error, nor the text the provider layer wraps it in ("provider request failed"), nor a "cause" key.
func TestGitLabClosedByFailureLogCarriesNoErrorText(t *testing.T) {
	var out strings.Builder
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&out, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	deriver := GitLabWorkItemDeriver{Source: &githubMultiDayOracleSource{}, statusMapping: loadRealStatusMapping(t), investmentClassifier: classifier}
	responses := gitLabWorkItemResponses()
	root := "/api/v4/projects/123"
	responses[root+"/merge_requests?page=1"] = []string{
		`[{"iid":9,"title":"Ship the API","description":"","state":"opened","created_at":"2026-07-04T09:00:00Z","updated_at":"2026-07-04T10:00:00Z","labels":["priority::low"],"assignees":[],"author":{"username":"alice","bot":false},"source_branch":"feature/ship-api"}]`, `[]`,
	}
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	if _, err := (GitLabWorkItemsRouteHandler{
		StatusMapping: loadRealStatusMapping(t), Derived: deriver, PerPage: 2, MaxPages: 10, NestedMaxPages: 10,
	}).Collect(context.Background(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, fakehttp.Client(closedByFailingDoer{inner: &gitLabWorkItemsDoer{responses: responses}})),
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	failureLines := 0
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if strings.Contains(line, closedByErrorSentinel) || strings.Contains(line, "provider request failed") || strings.Contains(line, `"cause"`) {
			t.Fatalf("a log line carries error text or a cause key: %s", line)
		}
		if strings.Contains(line, "providersync.gitlab.closing_mr_fetch_failed") {
			failureLines++
			if !strings.Contains(line, `"issue":"gitlab:acme/api#42"`) || !strings.Contains(line, `"error_class"`) || !strings.Contains(line, `"error_type"`) {
				t.Fatalf("the failure line lacks the issue, the error class or the error type: %s", line)
			}
		}
	}
	if failureLines != 1 {
		t.Fatalf("failure lines=%d want 1\n%s", failureLines, out.String())
	}
}
