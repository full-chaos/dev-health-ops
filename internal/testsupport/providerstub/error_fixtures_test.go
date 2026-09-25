package providerstub

import "testing"

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
