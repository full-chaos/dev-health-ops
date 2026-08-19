//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

// This file proves the READBACK half of the two-layer CHAOS-3494 assertion.
//
// The manifest half lives in github_work_items_composition_test.go: an n-day
// window emits n byte-identical work_item_team_attributions rows, mirroring
// Python's in-loop call to a builder that takes no `day`. This file proves what
// happens to those duplicates at persistence, against the REAL schema.
//
// It is the assertion the composition layer could not safely inherit. If a
// duplicate-carrying batch answered Conflict, the effect committer would rewrite
// the destination on every recovery pass forever, and mirroring the Python defect
// would have turned a harmless write amplification into an unbounded one.

// githubWorkItemMultiDayAttributionRows is what GitHubWorkItemDeriver produces
// for an n-day window: the SAME row, n times. computed_at is constant across the
// loop because Python passes one computed_at into every day's call, which is
// precisely why the copies are byte-identical rather than merely similar.
func githubWorkItemMultiDayAttributionRows(days int) []githubWorkItemTeamAttributionRow {
	repoID := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	teamID, teamName := "t1", "Team One"
	row := githubWorkItemTeamAttributionRow{
		WorkItemID: "acme/api#1", Provider: "github", Source: "repo_ownership",
		IsPrimary: 1, Confidence: "high", Evidence: "repo:acme/api",
		// Non-zero nanoseconds: the column is DateTime64(3), so a builder that
		// failed to quantize would store a different value than it compared.
		ComputedAt: time.Date(2026, 8, 5, 0, 30, 0, 123456789, time.UTC),
		RepoID:     &repoID, TeamID: &teamID, TeamName: &teamName,
		OrgID: githubDerivedIntegrationOrg,
	}
	rows := make([]githubWorkItemTeamAttributionRow, 0, days)
	for range days {
		rows = append(rows, row)
	}
	return rows
}

// TestGitHubWorkItemTeamAttributionsAnswerExactForMultiDayDuplicates is the
// blocking question for the composition layer, answered by execution rather than
// by reading the comparator.
func TestGitHubWorkItemTeamAttributionsAnswerExactForMultiDayDuplicates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)
	sink := GitHubWorkItemTeamAttributionsClickHouseEffects{
		Conn: conn, Lease: githubDerivedIntegrationLease(),
	}

	const days = 3
	rows := githubWorkItemMultiDayAttributionRows(days)
	effect := githubDerivedIntegrationEffect(t, githubTeamAttributionsDestination, rows)
	identity := githubDerivedIntegrationIdentity(githubTeamAttributionsDestination, len(rows))

	// Non-vacuity: the batch must really carry the duplicates. If a future
	// change deduplicated at the manifest layer this test would otherwise
	// silently become an ordinary single-row readback.
	if len(effect.Rows) != days {
		t.Fatalf("effect carries %d rows, want %d byte-identical duplicates",
			len(effect.Rows), days)
	}
	for index := 1; index < len(effect.Rows); index++ {
		if string(effect.Rows[index]) != string(effect.Rows[0]) {
			t.Fatalf("row[%d] differs from row[0]; the duplicates are not identical:\n%s\n%s",
				index, effect.Rows[index], effect.Rows[0])
		}
	}

	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil ||
		inspection != EffectAbsent {
		t.Fatalf("before write: inspection = %v, err = %v, want EffectAbsent", inspection, err)
	}
	if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
	// THE VERDICT. Conflict here would mean the committer rewrites this
	// destination on every recovery pass, forever.
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil ||
		inspection != EffectExact {
		t.Fatalf("duplicate-carrying batch: inspection = %v, err = %v, want EffectExact",
			inspection, err)
	}

	// Replay the identical batch, as recovery does. A rewrite loop shows up
	// here as a verdict that stops being Exact once a second generation of
	// duplicates is on disk.
	if err := sink.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := sink.InspectGitHubWorkItemEffect(ctx, identity, effect); err != nil ||
		inspection != EffectExact {
		t.Fatalf("replayed duplicate batch: inspection = %v, err = %v, want EffectExact",
			inspection, err)
	}

	// And the storage side of the split: n identical rows in the manifest
	// collapse to exactly ONE stored row. Asserted against the real table, not
	// inferred from the ReplacingMergeTree declaration.
	// The fence names the FULL sorting key -- (org_id, repo_id, work_item_id,
	// ifNull(team_id,''), source) per 051_team_attribution_dimensions.sql:91.
	// A partial fence cannot produce a false PASS here (this table has exactly
	// one row), but fencing the whole key is the standard: a partial one stops
	// being equivalent the moment a sibling row exists, and that is not a
	// property a reader should have to re-derive.
	var stored uint64
	if err := conn.QueryRow(ctx, `
SELECT count() FROM work_item_team_attributions FINAL
WHERE org_id = ? AND repo_id = ? AND work_item_id = ?
  AND ifNull(team_id, '') = ? AND source = ?`,
		githubDerivedIntegrationOrg,
		uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		"acme/api#1", "t1", "repo_ownership",
	).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != 1 {
		t.Fatalf("stored rows = %d, want 1: %d-day write amplification did not collapse",
			stored, days)
	}
}
