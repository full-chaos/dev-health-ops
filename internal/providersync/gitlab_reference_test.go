package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func TestParseGitLabReference(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct {
		full   string
		marker byte
		path   string
		iid    uint32
		ok     bool
	}{
		"merge request":        {"acme/api!9", '!', "acme/api", 9, true},
		"subgroup":             {"acme/platform/api!12", '!', "acme/platform/api", 12, true},
		"issue":                {"acme/api#7", '#', "acme/api", 7, true},
		"max uint32":           {"acme/api!4294967295", '!', "acme/api", 4294967295, true},
		"empty":                {"", '!', "", 0, false},
		"wrong marker":         {"acme/api#7", '!', "", 0, false},
		"no marker":            {"acme/api", '!', "", 0, false},
		"empty path":           {"!9", '!', "", 0, false},
		"dot-dot first":        {"../acme/api!9", '!', "", 0, false},
		"dot-dot middle":       {"acme/../api!9", '!', "", 0, false},
		"dot segment":          {"acme/./api!9", '!', "", 0, false},
		"leading slash":        {"/acme/api!9", '!', "", 0, false},
		"trailing slash":       {"acme/api/!9", '!', "", 0, false},
		"empty segment":        {"acme//api!9", '!', "", 0, false},
		"whitespace in path":   {"acme/ api!9", '!', "", 0, false},
		"trailing whitespace":  {"acme/api!9 ", '!', "", 0, false},
		"absolute URL":         {"https://x.test/acme/api!9", '!', "", 0, false},
		"issue iid zero":       {"acme/api#0", '#', "", 0, false},
		"iid zero":             {"acme/api!0", '!', "", 0, false},
		"iid beyond uint32":    {"acme/api!4294967296", '!', "", 0, false},
		"iid leading zero":     {"acme/api!09", '!', "", 0, false},
		"iid signed":           {"acme/api!+9", '!', "", 0, false},
		"issue iid beyond u32": {"acme/api#4294967296", '#', "", 0, false},
	} {
		got, err := parseGitLabReference(c.full, c.marker)
		if (err == nil) != c.ok || (c.ok && (got.Path != c.path || got.IID != c.iid)) {
			t.Errorf("%s: parse(%q)=%+v err=%v want ok=%v path=%q iid=%d", name, c.full, got, err, c.ok, c.path, c.iid)
		}
	}
}

// A /links target without references.full is not linked: the source issue's project does not stand in for it.
func TestNormalizeGitLabDependenciesNeverGuessesTheLinkTargetProject(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "work-items")
	var links []gitlabIssueLinkPayload
	for _, raw := range []string{
		`{"link_type":"blocks","iid":7}`,
		`{"link_type":"blocks","iid":7,"references":{"full":""}}`,
		`{"link_type":"blocks","iid":7,"references":{"full":"../acme/api#7"}}`,
		`{"link_type":"blocks","iid":7,"references":{"full":"acme/api#8"}}`,
		`{"link_type":"blocks","iid":4294967296,"references":{"full":"acme/api#4294967296"}}`,
		`{"link_type":"blocks","iid":3,"references":{"full":"other/proj#3"}}`,
	} {
		var link gitlabIssueLinkPayload
		if err := json.Unmarshal([]byte(raw), &link); err != nil {
			t.Fatal(err)
		}
		links = append(links, link)
	}
	rows := normalizeGitLabDependencies(claim, "gitlab:acme/api#5", "acme/api", "", links, time.Now())
	unsupported := 0
	for _, link := range links {
		if _, ok := link.targetWorkItemID(); !ok {
			unsupported++
		}
	}
	if unsupported != 5 || len(rows) != 1 || rows[0].TargetWorkItemID != "gitlab:other/proj#3" {
		t.Fatalf("unsupported=%d rows=%+v want 5 unsupported and one row to other/proj#3", unsupported, rows)
	}
}

type linksBodyDoer struct {
	inner *gitLabWorkItemsDoer
	body  string
}

func (doer linksBodyDoer) Do(request *http.Request) (*http.Response, error) {
	if strings.HasSuffix(request.URL.Path, "/issues/42/links") && request.URL.Query().Get("page") == "1" {
		return &http.Response{
			StatusCode: http.StatusOK, Status: "OK", Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(doer.body)), Request: request,
		}, nil
	}
	return doer.inner.Do(request)
}

// Through the production route: a /links entry without references.full is counted as an unsupported shape and writes no
// dependency row.
func TestGitLabWorkItemsRouteCountsLinksWithoutReferencesAsUnsupported(t *testing.T) {
	classifier, err := NewInvestmentClassifier(investmentConfigPath(t, "real"))
	if err != nil {
		t.Fatal(err)
	}
	deriver := GitLabWorkItemDeriver{Source: &githubMultiDayOracleSource{}, statusMapping: loadRealStatusMapping(t), investmentClassifier: classifier}
	inner := &gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()}
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	client := gitLabWorkItemsClient(t, fakehttp.Client(linksBodyDoer{inner: inner, body: `[{"link_type":"blocks","iid":7}]`}))
	client.Metrics = providerfoundation.NewMetrics()
	batch, err := (GitLabWorkItemsRouteHandler{
		StatusMapping: loadRealStatusMapping(t), Derived: deriver, PerPage: 2, MaxPages: 10, NestedMaxPages: 10,
	}).Collect(context.Background(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		client, time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if batch.Result["issue_links_unsupported_shape"] != 1 {
		t.Fatalf("result=%v want issue_links_unsupported_shape=1", batch.Result)
	}
}

// The route's parser and Derive's ParsePRSource accept and reject the same merge-request references, so a reference the
// route calls good is never rejected later as unparseable_source.
func TestParseGitLabReferenceAgreesWithDerive(t *testing.T) {
	t.Parallel()
	for _, full := range []string{
		"acme/api!9", "acme/api!4294967295", "acme/api!4294967296", "acme/api!0", "acme/api!09", "acme/api!+9", "acme/api!-9",
		"acme/api!", "!9", "acme/api!9 ", "acme/api!٣", "acme/api!1e3", "a/b!c!9",
	} {
		reference, err := parseGitLabReference(full, gitlabMergeRequestMarker)
		source, ok := issueprlinks.ParsePRSource("gitlab:" + full)
		if err == nil && (!ok || source.PRNumber != reference.IID || source.RepoSlug != reference.Path) {
			t.Errorf("%q: the route accepts %+v, Derive parses ok=%v %+v", full, reference, ok, source)
		}
	}
}

// A GitLab merge-request URL whose project or number fails the one reference parser is rejected at the Jira and Linear
// sites: no id is minted, so nothing is written or counted as synced.
func TestScmURLSitesRejectGitLabURLsThatFailTheReferenceParser(t *testing.T) {
	t.Setenv("JIRA_TRUSTED_SCM_HOSTS", "")
	t.Setenv("LINEAR_TRUSTED_SCM_HOSTS", "")
	for name, c := range map[string]struct {
		url      string
		want     string
		rejected bool
	}{
		"good":                 {"https://gitlab.com/acme/api/-/merge_requests/9", "gitlab:acme/api!9", false},
		"non-numeric number":   {"https://gitlab.com/acme/api/-/merge_requests/abc", "", true},
		"number beyond uint32": {"https://gitlab.com/acme/api/-/merge_requests/4294967296", "", true},
		"leading zero":         {"https://gitlab.com/acme/api/-/merge_requests/09", "", true},
		"zero":                 {"https://gitlab.com/acme/api/-/merge_requests/0", "", true},
		"percent-encoded path": {"https://gitlab.com/acme/a%20pi/-/merge_requests/9", "", true},
		"dot-dot project":      {"https://gitlab.com/acme/../-/merge_requests/9", "", true},
		"not a merge request":  {"https://gitlab.com/acme/api/-/issues/9", "", false},
	} {
		jiraSource, jiraRejected := jiraDevStatusPullRequestSource(c.url)
		if jiraSource != c.want || jiraRejected != c.rejected {
			t.Errorf("jira %s: %q rejected=%v want %q rejected=%v", name, jiraSource, jiraRejected, c.want, c.rejected)
		}
		linearSource, linearRejected := linearAttachmentSource(linearAttachmentPayload{SourceType: "gitlab", URL: c.url})
		if linearSource != c.want || linearRejected != c.rejected {
			t.Errorf("linear %s: %q rejected=%v want %q rejected=%v", name, linearSource, linearRejected, c.want, c.rejected)
		}
	}
	for _, url := range []string{
		"https://gitlab.com/acme/api/-/merge_requests/abc", "https://gitlab.com/acme/api/-/merge_requests/4294967296",
		"https://gitlab.com/acme/api/-/merge_requests/09",
	} {
		if got := jiraDevStatusPullRequestSourceID(url); got != "" {
			t.Errorf("jira id for %s = %q, want none", url, got)
		}
		if got := linearAttachmentWorkItemID(linearAttachmentPayload{SourceType: "gitlab", URL: url}); got != "" {
			t.Errorf("linear id for %s = %q, want none", url, got)
		}
	}
}
