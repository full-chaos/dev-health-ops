package providerstub

import (
	"strings"
	"testing"
)

func TestLinearViewerFixturesSelectByKeyAndOperation(t *testing.T) {
	fixtures, err := LoadDir("fixtures")
	if err != nil {
		t.Fatal(err)
	}
	viewer := `{"query":"{ viewer { id email name } }"}`
	for _, tc := range []struct {
		key, body string
		want      int
		contains  string
	}{
		{"stub-linear-key-ok", viewer, 200, "Stub Linear"}, {"stub-linear-key-401", viewer, 401, "not authenticated"},
		{"stub-linear-key-500", viewer, 500, "upstream failure"}, {"stub-linear-key-noviewer", viewer, 200, `"viewer": null`},
		// another operation is not the viewer fixtures' business: the starter teams fixture (declared later) answers it
		{"stub-linear-key-ok", `{"query":"{ teams { nodes { id } } }"}`, 200, "zz-team-1"},
	} {
		got := send(t, fixtures, "api.linear.app", "POST", "/graphql", tc.body, map[string]string{"Authorization": tc.key})
		if got.Code != tc.want || !strings.Contains(got.Body.String(), tc.contains) {
			t.Errorf("%s %s answered %d %s, want %d containing %q", tc.key, tc.body, got.Code, got.Body.String(), tc.want, tc.contains)
		}
	}
}
