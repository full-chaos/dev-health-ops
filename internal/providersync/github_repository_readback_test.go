package providersync

import (
	"testing"
	"time"
)

func repositoryReadbackFixture(now time.Time) repositoryRow {
	return repositoryRow{
		ID: "c7198fbc-1945-3717-05d8-eb78866b4e79", OrgID: "org-acme",
		Repo: "Acme/API", CreatedAt: now, LastSynced: now, Provider: "github",
		Settings: `{"source":"github"}`, Tags: `["github"]`,
	}
}

func repositoryReadbackVersion(expected repositoryRow) repositoryVersion {
	return repositoryVersion{
		Row:        expected,
		Ref:        "",
		SourceID:   "",
		LastSynced: expected.LastSynced,
		Found:      true,
	}
}

// TestRepositoryReadbackToleratesPreMergeHistory is the wedge regression.
// ReplacingMergeTree keeps every unmerged version of a key, so earlier sync
// occurrences legitimately sit next to this one. Comparing against all
// physical versions would report a conflict for a perfectly healthy table and
// block recovery; only the winning version may decide.
func TestRepositoryReadbackToleratesPreMergeHistory(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	expected := repositoryReadbackFixture(now)

	// The winning version is this effect, even though older versions of the
	// same key remain unmerged underneath it.
	if got := compareRepositoryVersion(
		expected, repositoryReadbackVersion(expected),
	); got != EffectExact {
		t.Fatalf("winning version = %s want %s", got, EffectExact)
	}

	// A duplicate insert of the identical row does not change the winner.
	duplicate := repositoryReadbackVersion(expected)
	if got := compareRepositoryVersion(expected, duplicate); got != EffectExact {
		t.Fatalf("duplicate = %s want %s", got, EffectExact)
	}
}

func TestRepositoryReadbackClassifiesEveryVersionRelationship(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	expected := repositoryReadbackFixture(now)

	stale := repositoryReadbackVersion(expected)
	stale.LastSynced = now.Add(-time.Hour)
	stale.Row.LastSynced = stale.LastSynced

	newer := repositoryReadbackVersion(expected)
	newer.LastSynced = now.Add(time.Hour)

	differentContent := repositoryReadbackVersion(expected)
	differentContent.Row.Settings = `{"source":"external_ingest"}`

	externallyStamped := repositoryReadbackVersion(expected)
	externallyStamped.SourceID = "11111111-1111-4111-8111-111111111111"

	withRef := repositoryReadbackVersion(expected)
	withRef.Ref = "refs/heads/main"

	for name, test := range map[string]struct {
		actual repositoryVersion
		want   EffectInspection
	}{
		"absent key":                 {repositoryVersion{}, EffectAbsent},
		"zero timestamp aggregate":   {repositoryVersion{Found: true}, EffectAbsent},
		"only an older version":      {stale, EffectAbsent},
		"a newer occurrence wins":    {newer, EffectConflict},
		"same version, new content":  {differentContent, EffectConflict},
		"external ingest stamped it": {externallyStamped, EffectConflict},
		"unexpected ref":             {withRef, EffectConflict},
	} {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := compareRepositoryVersion(expected, test.actual); got != test.want {
				t.Fatalf("%s = %s want %s", name, got, test.want)
			}
		})
	}
}
