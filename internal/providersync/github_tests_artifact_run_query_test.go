package providersync

import (
	"testing"
	"time"
)

// The report phase lists runs from the start of the claim's first day up to the
// claim's upper bound. With no upper bound it walked on to the present, so a
// catch-up window counted every later run against the listing cap.
func TestGitHubTestsArtifactRunQueryBoundsTheWindowAndNeverFiltersByBranch(t *testing.T) {
	since := time.Date(2026, 7, 22, 10, 30, 0, 0, time.UTC)
	before := time.Date(2026, 7, 22, 11, 30, 0, 0, time.UTC)
	for name, test := range map[string]struct {
		since, before *time.Time
		want          string
	}{
		"both":        {&since, &before, "2026-07-22..2026-07-22T11:30:00Z"},
		"since only":  {&since, nil, ">=2026-07-22"},
		"before only": {nil, &before, "*..2026-07-22T11:30:00Z"},
		"neither":     {nil, nil, ""},
	} {
		query := githubTestsArtifactRunQuery(Claim{Unit: Unit{SinceAt: test.since, BeforeAt: test.before}})
		if got := query.Get("created"); got != test.want {
			t.Errorf("%s: created=%q, want %q", name, got, test.want)
		}
		if query.Has("branch") {
			t.Errorf("%s: the report run listing filters by branch: %s", name, query.Encode())
		}
	}
}
