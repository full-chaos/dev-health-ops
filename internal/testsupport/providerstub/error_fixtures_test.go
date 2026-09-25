package providerstub

import (
	"strconv"
	"testing"
	"time"
)

func TestErrorFixturesAnswerTheirStatusAndHeaders(t *testing.T) {
	fixtures, err := LoadDir("fixtures")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		host, target string
		want         int
		header, val  string
	}{
		{"api.github.com", "/orgs/zz-429/repos", 429, "Retry-After", "1"}, {"api.github.com", "/orgs/zz-500", 500, "", ""},
		{"api.github.com", "/users/zz-403/repos", 403, "X-Ratelimit-Remaining", "0"}, {"api.github.com", "/orgs/zz-secondary", 403, "Retry-After", "1"},
		{"gitlab.com", "/api/v4/groups/zz-429", 429, "Retry-After", "1"}, {"gitlab.com", "/api/v4/groups/zz-500/projects", 500, "", ""},
		{"gitlab.com", "/api/v4/groups/zz-403", 403, "", ""}, {"api.github.com", "/orgs/zz-unknown", 599, "", ""},
	} {
		got := get(t, fixtures, tc.host, "GET", tc.target, nil)
		if got.Code != tc.want {
			t.Errorf("%s %s answered %d, want %d", tc.host, tc.target, got.Code, tc.want)
		}
		if tc.header != "" && got.Header().Get(tc.header) != tc.val {
			t.Errorf("%s %s header %s = %q, want %q", tc.host, tc.target, tc.header, got.Header().Get(tc.header), tc.val)
		}
	}
}

// A primary-limit fixture must carry a reset time that has already passed: PyGithub's GithubRetry sleeps until
// X-RateLimit-Reset, so a far-future value (the first version used 2100) wedges the Python plane for decades.
func TestPrimaryLimitFixturesCarryAnAlreadyPassedReset(t *testing.T) {
	fixtures, err := LoadDir("fixtures")
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"/orgs/zz-403", "/orgs/zz-403/repos", "/users/zz-403", "/users/zz-403/repos"} {
		got := get(t, fixtures, "api.github.com", "GET", target, nil)
		reset, err := strconv.ParseInt(got.Header().Get("X-Ratelimit-Reset"), 10, 64)
		if err != nil || got.Code != 403 {
			t.Fatalf("%s: status %d reset %q", target, got.Code, got.Header().Get("X-Ratelimit-Reset"))
		}
		if reset >= time.Now().Unix() {
			t.Errorf("%s: X-RateLimit-Reset %d is not in the past: a client that sleeps until it (PyGithub) never returns", target, reset)
		}
	}
}
