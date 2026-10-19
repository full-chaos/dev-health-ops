package teamsidentity

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const linkNextURL = "https://api.github.com/orgs/acme/teams?page=2"

func TestGithubNextLinkPresentParamVariants(t *testing.T) {
	cases := []struct {
		name     string
		header   string
		wantURL  string
		wantNext bool
	}{
		{"quoted", `<` + linkNextURL + `>; rel="next"`, linkNextURL, true},
		{"unquoted", `<` + linkNextURL + `>; rel=next`, linkNextURL, true},
		{"upper-case key", `<` + linkNextURL + `>; REL="next"`, linkNextURL, true},
		{"upper-case value", `<` + linkNextURL + `>; rel="NEXT"`, linkNextURL, true},
		{"spaces around equals", `<` + linkNextURL + `>; rel = "next"`, linkNextURL, true},
		{"param before rel", `<` + linkNextURL + `>; title="x"; rel="next"`, linkNextURL, true},
		{"next after last", `<https://api.github.com/orgs/acme/teams?page=9>; rel="last", <` + linkNextURL + `>; REL = "next"`, linkNextURL, true},
		{"rel list is not next", `<` + linkNextURL + `>; rel="next last"`, "", false},
		{"single quotes are not next", `<` + linkNextURL + `>; rel='next'`, "", false},
		{"quoted comma in an earlier param leaves a fragment: next with no URL", `<https://api.github.com/orgs/o/teams?page=1>; title="x, y"; REL="NEXT"`, "", true},
		{"url segment without angle brackets: next with no URL", `https://api.github.com/orgs/o/teams?page=2; rel="next"`, "", true},
		{"last only", `<` + linkNextURL + `>; rel="last"`, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotURL, gotNext := githubNextLinkPresent(tc.header)
			if gotNext != tc.wantNext || gotURL != tc.wantURL {
				t.Fatalf("githubNextLinkPresent(%q) = (%q, %v), want (%q, %v)", tc.header, gotURL, gotNext, tc.wantURL, tc.wantNext)
			}
		})
	}
}

func linkTwoFields(r *http.Request) http.Header {
	base := r.URL.Scheme + "://" + r.URL.Host + r.URL.Path
	out := http.Header{}
	out.Add("Link", `<`+base+`?page=9>; rel="last"`)
	out.Add("Link", `<`+base+`?per_page=100&page=2>; rel="next"`)
	return out
}

func TestEveryLinkWalkReadsANextLinkInALaterLinkField(t *testing.T) {
	token := secrets.NewValue("token")
	page := func(r *http.Request, first, second string) (http.Header, string) {
		if strings.Contains(r.URL.RawQuery, "page=2") {
			return nil, second
		}
		return linkTwoFields(r), first
	}
	t.Run("github org teams", func(t *testing.T) {
		doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
			return page(r, `[{"slug":"a","name":"A"}]`, `[{"slug":"b","name":"B"}]`)
		}}
		got, err := githubListAllPages(context.Background(), hostileClient(t, "github", doer), "org teams", "o", "/orgs/{org}/teams", "/orgs/o/teams?per_page=100",
			func(entry githubTeamListEntry) string { return entry.Slug })
		if err != nil || len(got) != 2 || doer.requests.Load() != 2 {
			t.Fatalf("rows=%d requests=%d err=%v, want 2 rows from 2 requests", len(got), doer.requests.Load(), err)
		}
	})
	t.Run("github members", func(t *testing.T) {
		doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
			if !strings.HasSuffix(r.URL.Path, "/members") {
				return nil, `{"url":"https://api.example.test/orgs/o/teams/t"}`
			}
			return page(r, `[{"login":"u1"}]`, `[{"login":"u2"}]`)
		}}
		withDiscoveryDoer(t, doer)
		got, err := discoverMembersGitHub(context.Background(), token, "o", "t")
		if err != nil || len(got) != 2 {
			t.Fatalf("members=%d requests=%d err=%v, want 2 members", len(got), doer.requests.Load(), err)
		}
	})
	t.Run("gitlab members", func(t *testing.T) {
		doer := &funcDoer{answer: func(r *http.Request, n int) (http.Header, string) {
			if !strings.HasSuffix(r.URL.Path, "/members") {
				return nil, `{"id":7,"full_path":"g","name":"G"}`
			}
			return page(r, `[{"id":1,"username":"u1"}]`, `[{"id":2,"username":"u2"}]`)
		}}
		withDiscoveryDoer(t, doer)
		got, err := discoverMembersGitLab(context.Background(), token, "", "g")
		if err != nil || len(got) != 2 {
			t.Fatalf("members=%d requests=%d err=%v, want 2 members", len(got), doer.requests.Load(), err)
		}
	})
}
