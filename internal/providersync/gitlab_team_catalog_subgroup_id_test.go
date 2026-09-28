package providersync

import "testing"

// TestGitLabSubgroupIDDiffersFromThePythonVerbsByDesign names and pins the one
// structural divergence CHAOS-6907's live-python-oracle test (internal/synccli)
// deliberately does not exercise row by row: the legacy `dev-hops sync teams
// --provider gitlab` verb (providers/teams.py) ids a subgroup as "gl:" plus
// the group's own SHORT `path` (python-gitlab's `subgroup.path`, never
// `full_path`), while this catalog's gitlabTeamID always uses the full,
// namespaced path. For a TOP-LEVEL group the two spellings are identical (no
// namespace prefix) -- which is why every scenario in the oracle test uses a
// top-level group instead of a nested one: a subgroup's rows would sort under
// two different ids in the two ClickHouse databases the oracle diffs, breaking
// its by-id row matching, not proving a real product defect.
func TestGitLabSubgroupIDDiffersFromThePythonVerbsByDesign(t *testing.T) {
	if got, want := gitlabTeamID("acme/platform"), "gl:acme/platform"; got != want {
		t.Fatalf("go subgroup id = %q, want %q (full_path-keyed)", got, want)
	}
	if got, want := gitlabTeamID("platform"), "gl:platform"; got != want {
		t.Fatalf("go top-level id = %q, want %q", got, want)
	}
	// Python's own id for the "acme/platform" subgroup is "gl:platform" (the
	// leaf segment only) -- not reproduced here since it requires no Go code
	// path; it is the row-shape divergence RISK-NOTES names for CHAOS-6907.
}
