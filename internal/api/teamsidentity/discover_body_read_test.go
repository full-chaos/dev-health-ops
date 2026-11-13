package teamsidentity

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// wantBodyCap is providerfoundation's maxProviderPageBody (pagination.go:16).
const wantBodyCap = 32 << 20

type bodyMode string

const (
	modeRead       bodyMode = "read"
	modeDecode     bodyMode = "decode"
	modeOverLength bodyMode = "oversize-content-length"
	modeOverStream bodyMode = "oversize-stream"
)

type failingReader struct{ sent bool }

func (r *failingReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		return copy(p, "{"), nil
	}
	return 0, errors.New("planted read failure")
}

type zeroStream struct{ read *atomic.Int64 }

// zeroStream ends after streamEnd bytes so an unbounded reader stops (a red
// run on a tree without the cap must not exhaust memory).
const streamEnd = 2 * wantBodyCap

func (z zeroStream) Read(p []byte) (int, error) {
	done := z.read.Load()
	if done >= streamEnd {
		return 0, io.EOF
	}
	p = p[:min(int64(len(p)), streamEnd-done)]
	for i := range p {
		p[i] = ' '
	}
	z.read.Add(int64(len(p)))
	return len(p), nil
}

// siteDoer answers every request with a valid fixture except the target one,
// which gets a 200 whose body is bad in the way mode says.
type siteDoer struct {
	target func(*http.Request) bool
	good   func(*http.Request) string
	mode   bodyMode
	read   atomic.Int64
}

func (d *siteDoer) Do(request *http.Request) (*http.Response, error) {
	response := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Request: request, ContentLength: -1}
	if !d.target(request) {
		body := d.good(request)
		response.Body, response.ContentLength = io.NopCloser(strings.NewReader(body)), int64(len(body))
		return response, nil
	}
	switch d.mode {
	case modeRead:
		response.Body = io.NopCloser(&failingReader{})
	case modeDecode:
		response.Body = io.NopCloser(strings.NewReader("not json"))
	case modeOverLength:
		response.Body, response.ContentLength = io.NopCloser(strings.NewReader("[]")), wantBodyCap+1
	case modeOverStream:
		response.Body = io.NopCloser(zeroStream{read: &d.read})
	}
	return response, nil
}

func pathSuffix(suffix string) func(*http.Request) bool {
	return func(r *http.Request) bool { return strings.HasSuffix(r.URL.Path, suffix) }
}

func anyRequest(*http.Request) bool { return true }

func noneGood(*http.Request) string { return "{}" }

type bodySite struct {
	name   string
	route  string
	target func(*http.Request) bool
	good   func(*http.Request) string
	run    func(context.Context) error
}

func discoveryClientFor(t *testing.T, provider string) *providerfoundation.HTTPClient {
	t.Helper()
	var (
		client *providerfoundation.HTTPClient
		err    error
	)
	switch provider {
	case "github":
		client, err = providerfoundation.NewGitHubClient(githubTestCredential(), discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{})
	case "gitlab":
		client, err = providerfoundation.NewGitLabClient(gitlabTestCredential(), discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{})
	}
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func bodySites(t *testing.T) []bodySite {
	token := secrets.NewValue("token")
	return []bodySite{
		{"github team detail", "/orgs/{org}/teams/{slug}", anyRequest, noneGood, func(ctx context.Context) error {
			_, err := githubTeamDetailFor(ctx, discoveryClientFor(t, "github"), "acme", "eng")
			return err
		}},
		{"github list page", "/orgs/{org}/teams/{slug}/repos", anyRequest, noneGood, func(ctx context.Context) error {
			_, err := githubListAllPages(ctx, discoveryClientFor(t, "github"), "team repos", "acme", "/orgs/{org}/teams/{slug}/repos", "/orgs/acme/teams/eng/repos?per_page=100", func(map[string]any) string { return "" })
			return err
		}},
		{"gitlab group", "/groups/{group}", anyRequest, noneGood, func(ctx context.Context) error {
			_, err := gitlabGetGroup(ctx, discoveryClientFor(t, "gitlab"), "grp")
			return err
		}},
		{"gitlab list page", "/groups/{group}/subgroups", anyRequest, noneGood, func(ctx context.Context) error {
			_, _, err := gitlabListGroupPages(ctx, discoveryClientFor(t, "gitlab"), "grp", "/groups/{group}/subgroups", "/api/v4/groups/7/subgroups", 100)
			return err
		}},
		{"linear teams", "/graphql", anyRequest, noneGood, func(ctx context.Context) error {
			_, err := discoverLinear(ctx, linearTestCredential())
			return err
		}},
		{"members github team detail", "/orgs/{org}/teams/{slug}", anyRequest, noneGood, func(ctx context.Context) error {
			_, err := discoverMembersGitHub(ctx, token, "acme", "eng")
			return err
		}},
		{"members github page", "/orgs/{org}/teams/{slug}/members", pathSuffix("/members"), noneGood, func(ctx context.Context) error {
			_, err := discoverMembersGitHub(ctx, token, "acme", "eng")
			return err
		}},
		{"members github user", "/users/{login}", pathSuffix("/users/a"), func(r *http.Request) string {
			if strings.HasSuffix(r.URL.Path, "/members") {
				return `[{"login":"a"}]`
			}
			return "{}"
		}, func(ctx context.Context) error {
			_, err := discoverMembersGitHub(ctx, token, "acme", "eng")
			return err
		}},
		{"members gitlab page", "/groups/{group}/members", pathSuffix("/members"), func(*http.Request) string { return `{"id":7}` }, func(ctx context.Context) error {
			_, err := discoverMembersGitLab(ctx, token, "", "grp")
			return err
		}},
		{"members linear graphql", "/graphql", anyRequest, noneGood, func(ctx context.Context) error {
			_, err := discoverMembersLinear(ctx, token, "ENG")
			return err
		}},
		{"members jira project", "/rest/api/3/project/{key}", anyRequest, noneGood, func(ctx context.Context) error {
			_, err := discoverMembersJira(ctx, jiraTestCredential(), "ABC")
			return err
		}},
		{"infer jira search page", "/rest/api/3/search/jql", anyRequest, noneGood, func(ctx context.Context) error {
			_, err := inferJiraMembers(ctx, jiraTestCredential(), "ABC", 30)
			return err
		}},
	}
}

// A 2xx body that fails to read, decode or stays under no cap: the error is a
// *ProviderError that names the route template, keeps errors.Is for the
// sentinel, and carries no request path (org, team, login or key).
func TestDiscoveryBodyFailuresCarryRouteAndKeepTheSentinel(t *testing.T) {
	for _, mode := range []bodyMode{modeRead, modeDecode, modeOverLength, modeOverStream} {
		for _, site := range bodySites(t) {
			t.Run(string(mode)+"/"+site.name, func(t *testing.T) {
				doer := &siteDoer{target: site.target, good: site.good, mode: mode}
				withDiscoveryClient(t, doer)
				err := site.run(context.Background())
				if err == nil {
					t.Fatalf("a 200 with a %s body returned no error", mode)
				}
				var providerErr *providerfoundation.ProviderError
				if !errors.As(err, &providerErr) {
					t.Fatalf("error %q is not a *ProviderError", err)
				}
				if want := "route=" + site.route; !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q lacks %q", err, want)
				}
				wantReason := map[bodyMode]string{modeRead: "body_read", modeDecode: "body_decode", modeOverLength: "body_too_large", modeOverStream: "body_too_large"}[mode]
				if providerErr.Reason != wantReason {
					t.Fatalf("reason = %q, want %q (error %q)", providerErr.Reason, wantReason, err)
				}
				if !errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
					t.Fatalf("errors.Is(ErrNormalizationInvalid) lost: %q", err)
				}
				for _, planted := range []string{"acme", "/eng", "ABC", "grp"} {
					if strings.Contains(err.Error(), planted) {
						t.Fatalf("error %q carries the request name %q", err, planted)
					}
				}
				if mode == modeOverStream {
					if got := doer.read.Load(); got > wantBodyCap+1 {
						t.Fatalf("read %d bytes of an endless body, cap %d", got, wantBodyCap)
					}
				}
			})
		}
	}
}
