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
		{"api.github.com", "/c/ok/user/repos?per_page=100&affiliation=owner", 200}, {"api.github.com", "/c/ok/orgs/zz-venue/repos?type=all", 200},
		{"api.github.com", "/c/401/user/repos", 401}, {"api.github.com", "/c/fallback/orgs/zz-venue/repos", 404}, {"api.github.com", "/c/fallback/users/zz-venue/repos", 200},
		{"gitlab.com", "/c/ok/api/v4/projects?membership=true&page=1", 200}, {"gitlab.com", "/c/ok/api/v4/groups/zz-venue/projects", 200},
		{"gitlab.com", "/c/401/api/v4/projects?membership=true", 401},
		{"gitlab.com", "/c/ok/api/v4/projects", 599}, // the fixture requires membership=true
		{"api.github.com", "/c/ok/api/v4/projects?membership=true", 599},
	} {
		if got := get(t, fixtures, tc.host, "GET", tc.target, nil); got.Code != tc.want {
			t.Errorf("%s %s answered %d, want %d", tc.host, tc.target, got.Code, tc.want)
		}
	}
}
