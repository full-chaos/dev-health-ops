package providerstub

import "testing"

func TestJiraDiscoveryFixturesAnswerOnAnyTenantHost(t *testing.T) {
	fixtures, err := LoadDir("fixtures")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		host, target string
		want         int
	}{
		{"zz-venue.atlassian.net", "/rest/api/3/project/search?maxResults=100&startAt=0", 200},
		{"zz-venue.atlassian.net", "/rest/api/3/project", 200}, {"zz-venue.atlassian.net", "/rest/api/3/myself", 200},
		{"api.github.com", "/rest/api/3/project/search", 599},
	} {
		if got := get(t, fixtures, tc.host, "GET", tc.target, nil); got.Code != tc.want {
			t.Errorf("%s %s answered %d, want %d", tc.host, tc.target, got.Code, tc.want)
		}
	}
}
