package providerstub

import "testing"

func TestDiscoveryFixturesAnswerEachCaseOnTheRightProvider(t *testing.T) {
	fixtures, err := LoadDir("fixtures")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		host, target string
		want         int
	}{
		{"api.github.com", "/user/repos?per_page=100&affiliation=owner", 200},
		{"api.github.com", "/orgs/zz-venue", 200}, {"api.github.com", "/orgs/zz-venue/repos?type=all", 200},
		{"api.github.com", "/orgs/zz-401/repos", 401}, {"api.github.com", "/users/zz-401/repos", 401},
		{"api.github.com", "/orgs/zz-fallback/repos", 404}, {"api.github.com", "/users/zz-fallback/repos", 200},
		{"gitlab.com", "/api/v4/projects?membership=true&page=1", 200}, {"gitlab.com", "/c/ok/api/v4/projects?membership=true", 200},
		{"gitlab.com", "/api/v4/groups/zz-venue", 200}, {"gitlab.com", "/api/v4/groups/zz-venue/projects", 200},
		{"gitlab.com", "/c/ok/api/v4/groups/zz-venue/projects", 200}, {"gitlab.com", "/api/v4/groups/zz-401/projects", 401},
		{"gitlab.com", "/api/v4/projects", 599}, // the fixture requires membership=true
		{"api.github.com", "/api/v4/projects?membership=true", 599},
	} {
		if got := get(t, fixtures, tc.host, "GET", tc.target, nil); got.Code != tc.want {
			t.Errorf("%s %s answered %d, want %d", tc.host, tc.target, got.Code, tc.want)
		}
	}
}
