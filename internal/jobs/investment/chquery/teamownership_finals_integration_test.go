//go:build integration

package chquery

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// donorTables are the four ReplacingMergeTree tables the donor query reads.
// Every test below stops their merges before seeding, so each physical
// version stays on disk and the query's own dedup is the only thing choosing
// between them. A background merge would collapse the versions first and let
// a missing FINAL pass unnoticed.
var donorTables = []string{"teams", "repos", "team_repo_ownership", "work_item_team_attributions"}

func stopDonorTableMerges(t *testing.T, ctx context.Context, conn driver.Conn) {
	t.Helper()
	for _, table := range donorTables {
		mustExec(t, ctx, conn, "SYSTEM STOP MERGES "+table)
	}
}

// withoutClause removes one exact fragment from the production query and
// fails the test when the fragment is absent, so a reshaped query cannot
// silently turn a "stripped" query back into the production one.
func withoutClause(t *testing.T, query, fragment, replacement string) string {
	t.Helper()
	stripped := strings.Replace(query, fragment, replacement, 1)
	if stripped == query {
		t.Fatalf("fragment not found in the donor query; its text changed shape:\n%s", fragment)
	}
	return stripped
}

const latestGenerationFilter = `    WHERE computed_at = latest_computed_at
`

// previousTeamRepoDonorsQuery is the donor query as it stood before the
// attributions and teams reads were hoisted out of the two UNION ALL arms. It
// is kept ONLY as the before/after oracle for the two-version corpus below
// and must never be reintroduced into the production query: it evaluates the
// attributions FINAL, the latest-generation scan and the teams FINAL once per
// arm.
const previousTeamRepoDonorsQuery = `
WITH attributions AS (
    SELECT work_item_id, team_id, source, is_primary
    FROM work_item_team_attributions FINAL
    WHERE org_id = {org_id:String} AND work_item_id IN {work_item_ids:Array(String)}
      AND (work_item_id, computed_at) IN (
          SELECT work_item_id, max(computed_at)
          FROM work_item_team_attributions
          WHERE org_id = {org_id:String} AND work_item_id IN {work_item_ids:Array(String)}
          GROUP BY work_item_id
      )
),
active_teams AS (
    SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1
),
ownership AS (
    SELECT team_id, provider, repo_id, lower(repo_full_name) AS repo_name_lower
    FROM team_repo_ownership FINAL
    WHERE org_id = {org_id:String}
      AND source IN ('native', 'jira_legacy', 'provider_access', 'inferred')
      AND valid_from <= {as_of:DateTime64(3, 'UTC')}
      AND (valid_to IS NULL OR valid_to > {as_of:DateTime64(3, 'UTC')})
),
scoped_repos AS (
    SELECT id, provider, lower(repo) AS repo_lower FROM repos FINAL WHERE org_id = {org_id:String}
),
donors AS (
    SELECT a.work_item_id AS work_item_id, a.team_id AS team_id, a.source AS source,
           a.is_primary AS is_primary, r.id AS repo_id
    FROM attributions AS a
    INNER JOIN active_teams AS t ON t.id = a.team_id
    INNER JOIN ownership AS o ON o.team_id = a.team_id AND o.repo_id IS NOT NULL
    INNER JOIN scoped_repos AS r ON r.provider = o.provider AND r.id = o.repo_id

    UNION ALL

    SELECT a.work_item_id AS work_item_id, a.team_id AS team_id, a.source AS source,
           a.is_primary AS is_primary, r.id AS repo_id
    FROM attributions AS a
    INNER JOIN active_teams AS t ON t.id = a.team_id
    INNER JOIN ownership AS o ON o.team_id = a.team_id AND o.repo_id IS NULL
    INNER JOIN scoped_repos AS r ON r.provider = o.provider AND r.repo_lower = o.repo_name_lower
)
SELECT DISTINCT work_item_id, assumeNotNull(team_id), repo_id
FROM donors
WHERE is_primary = 1 AND source IN ('native_team', 'issue_project', 'project_ownership', 'repo_ownership')
  AND team_id IS NOT NULL AND team_id != ''
  AND repo_id != toUUID('00000000-0000-0000-0000-000000000000')
ORDER BY work_item_id, repo_id, team_id`

func donorKeys(donors []TeamRepoDonor) []string {
	keys := make([]string, 0, len(donors))
	for _, d := range donors {
		keys = append(keys, d.WorkItemID+"/"+d.TeamID+"/"+d.RepoID.String())
	}
	return keys
}

// TestEachDonorDedupIsLoadBearing proves, one table at a time, that a newer
// physical version can retract a donation that an older unmerged version
// still grants. For each case the production query must return nothing, and
// the same query with only that one dedup removed must return the stale
// donation. The second half is what makes the first half mean something: it
// shows the fixture reaches the join and that the named dedup, not some other
// clause, is what excludes the stale row. None of these dedups is redundant
// under the outer DISTINCT, because the outer DISTINCT removes duplicate
// tuples but cannot remove a tuple that only a stale version produces.
func TestEachDonorDedupIsLoadBearing(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	stopDonorTableMerges(t, ctx, conn)
	at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	team := func(id string, active uint8, updated time.Time) {
		mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES (?,generateUUIDv4(),'n','linear',?,?,?)`,
			id, active, updated, orgAlpha)
	}
	repo := func(id, name string, synced time.Time) {
		mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id,last_synced) VALUES (?,?,'github',?,?)`,
			id, name, orgAlpha, synced)
	}
	ownership := func(teamID, name string, validTo any, updated time.Time) {
		mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at) VALUES (?,'github',?,?,'exact','provider_access',?,?,?)`,
			orgAlpha, teamID, name, at.Add(-time.Hour), validTo, updated)
	}
	attribution := func(issue, teamID, source string, primary uint8, computed time.Time) {
		mustExec(t, ctx, conn, `INSERT INTO work_item_team_attributions (org_id,repo_id,work_item_id,provider,team_id,source,is_primary,confidence,evidence,computed_at) VALUES (?,toUUID('00000000-0000-0000-0000-000000000000'),?,'linear',?,?,?,'high','fixture',?)`,
			orgAlpha, issue, teamID, source, primary, computed)
	}

	// Each case owns its team, repository and work item, so stripping one
	// dedup cannot leak a stale row from another case into its result.
	cases := []struct {
		name, issue, team, repoID, fragment, replacement string
		seed                                             func(issue, teamID, repoID string)
	}{
		{
			// A team deactivated by a newer version must stop donating.
			name: "teams", issue: "wi-teams", team: "team-deactivated",
			repoID:   "a1111111-1111-4111-8111-111111111111",
			fragment: "FROM teams FINAL", replacement: "FROM teams",
			seed: func(issue, teamID, repoID string) {
				team(teamID, 1, at.Add(-time.Minute))
				team(teamID, 0, at)
				repo(repoID, "acme/teams-case", at)
				ownership(teamID, "acme/teams-case", nil, at)
				attribution(issue, teamID, "native_team", 1, at)
			},
		},
		{
			// Ownership is retracted by writing the same key again with valid_to
			// set and a newer updated_at; the older open-ended version must not
			// keep donating.
			name: "team_repo_ownership", issue: "wi-ownership", team: "team-retracted",
			repoID:   "a2222222-2222-4222-8222-222222222222",
			fragment: "FROM team_repo_ownership FINAL", replacement: "FROM team_repo_ownership",
			seed: func(issue, teamID, repoID string) {
				team(teamID, 1, at)
				repo(repoID, "acme/ownership-case", at)
				ownership(teamID, "acme/ownership-case", nil, at.Add(-time.Minute))
				ownership(teamID, "acme/ownership-case", at.Add(-time.Second), at)
				attribution(issue, teamID, "native_team", 1, at)
			},
		},
		{
			// A renamed repository keeps its id but not its name. Name-only
			// ownership that still names the old spelling must not match the
			// stale catalog version: the join is to the CURRENT catalog.
			name: "repos", issue: "wi-repos", team: "team-renamed-repo",
			repoID:   "a3333333-3333-4333-8333-333333333333",
			fragment: "FROM repos FINAL", replacement: "FROM repos",
			seed: func(issue, teamID, repoID string) {
				team(teamID, 1, at)
				repo(repoID, "acme/old-name", at.Add(-time.Minute))
				repo(repoID, "acme/new-name", at)
				ownership(teamID, "acme/old-name", nil, at)
				attribution(issue, teamID, "native_team", 1, at)
			},
		},
		{
			// Same sorting key AND same computed_at: only FINAL's
			// last-inserted-wins rule picks the correction over the original.
			// The latest-generation filter keeps both, because both carry the
			// latest computed_at.
			name: "work_item_team_attributions", issue: "wi-tie", team: "team-tie",
			repoID:   "a4444444-4444-4444-8444-444444444444",
			fragment: "FROM work_item_team_attributions FINAL\n", replacement: "FROM work_item_team_attributions\n",
			seed: func(issue, teamID, repoID string) {
				team(teamID, 1, at)
				repo(repoID, "acme/tie-case", at)
				ownership(teamID, "acme/tie-case", nil, at)
				attribution(issue, teamID, "native_team", 1, at)
				attribution(issue, teamID, "native_team", 0, at)
			},
		},
		{
			// A newer generation whose row has a DIFFERENT sorting key (another
			// source) is never collapsed with the older one by FINAL, so only
			// the latest-generation filter drops the older eligible row.
			name: "latest generation filter", issue: "wi-generation", team: "team-generation",
			repoID:   "a5555555-5555-4555-8555-555555555555",
			fragment: latestGenerationFilter, replacement: "",
			seed: func(issue, teamID, repoID string) {
				team(teamID, 1, at)
				repo(repoID, "acme/generation-case", at)
				ownership(teamID, "acme/generation-case", nil, at)
				attribution(issue, teamID, "native_team", 1, at.Add(-time.Minute))
				attribution(issue, teamID, "unassigned", 1, at)
			},
		},
	}
	for _, c := range cases {
		c.seed(c.issue, c.team, c.repoID)
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ids := []string{c.issue}
			production, err := reader.FetchTeamRepoDonors(ctx, ids, orgAlpha, at)
			if err != nil {
				t.Fatal(err)
			}
			if len(production) != 0 {
				t.Fatalf("production query donated %v from a version a newer one retracted", donorKeys(production))
			}
			stripped := runDonorQuery(t, ctx, conn, withoutClause(t, teamRepoDonorsQuery, c.fragment, c.replacement), ids, orgAlpha, at)
			want := []string{c.issue + "/" + c.team + "/" + c.repoID}
			if !reflect.DeepEqual(donorKeys(stripped), want) {
				t.Fatalf("without this dedup the stale version should donate exactly %v, got %v; "+
					"if it is now empty the dedup may have become redundant and should be re-examined",
					want, donorKeys(stripped))
			}
		})
	}
}

// seedTwoVersionDonorCorpus writes a donor corpus in which every row of every
// table exists as at least two physical versions: an exact re-insert, a newer
// version with unchanged content, and the retracting versions the dedups must
// resolve. It returns the work item ids to query.
func seedTwoVersionDonorCorpus(t *testing.T, ctx context.Context, conn driver.Conn, at time.Time) []string {
	t.Helper()
	repoA := "b1111111-1111-4111-8111-111111111111"
	repoB := "b2222222-2222-4222-8222-222222222222"
	repoC := "b3333333-3333-4333-8333-333333333333"
	repoRenamed := "b4444444-4444-4444-8444-444444444444"

	for version := 0; version < 2; version++ {
		stamp := at.Add(time.Duration(version-2) * time.Second)
		mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES
            ('team-a',generateUUIDv4(),'A','linear',1,?,?),
            ('team-b',generateUUIDv4(),'B','jira',1,?,?),
            ('team-gone',generateUUIDv4(),'Gone','linear',1,?,?)`,
			stamp, orgAlpha, stamp, orgAlpha, stamp, orgAlpha)
		mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id,last_synced) VALUES
            (?,'Acme/Alpha','github',?,?),
            (?,'acme/beta','github',?,?),
            (?,'acme/gamma','gitlab',?,?),
            (?,'acme/renamed-old','github',?,?)`,
			repoA, orgAlpha, stamp, repoB, orgAlpha, stamp, repoC, orgAlpha, stamp, repoRenamed, orgAlpha, stamp)
		mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at) VALUES
            (?,'github','team-a',NULL,'acme/alpha','exact','provider_access',?,NULL,?),
            (?,'github','team-a',?,'stale-name','exact','native',?,NULL,?),
            (?,'gitlab','team-b',NULL,'acme/gamma','exact','inferred',?,NULL,?),
            (?,'github','team-b',NULL,'acme/beta','exact','provider_access',?,NULL,?),
            (?,'github','team-b',NULL,'acme/renamed-old','exact','provider_access',?,NULL,?),
            (?,'github','team-gone',NULL,'acme/beta','exact','provider_access',?,NULL,?)`,
			orgAlpha, at.Add(-time.Hour), stamp,
			orgAlpha, repoB, at.Add(-time.Hour), stamp,
			orgAlpha, at.Add(-time.Hour), stamp,
			orgAlpha, at.Add(-time.Hour), stamp,
			orgAlpha, at.Add(-time.Hour), stamp,
			orgAlpha, at.Add(-time.Hour), stamp)
		for _, generation := range []time.Duration{-time.Minute, 0} {
			computed := at.Add(generation)
			mustExec(t, ctx, conn, `INSERT INTO work_item_team_attributions (org_id,repo_id,work_item_id,provider,team_id,source,is_primary,confidence,evidence,computed_at) VALUES
                (?,toUUID('00000000-0000-0000-0000-000000000000'),'wi-a','linear','team-a','native_team',1,'high','e',?),
                (?,toUUID('00000000-0000-0000-0000-000000000000'),'wi-a','linear','team-b','repo_ownership',0,'medium','e',?),
                (?,toUUID('00000000-0000-0000-0000-000000000000'),'wi-b','jira','team-b','issue_project',1,'high','e',?),
                (?,toUUID('00000000-0000-0000-0000-000000000000'),'wi-gone','linear','team-gone','project_ownership',1,'high','e',?)`,
				orgAlpha, computed, orgAlpha, computed, orgAlpha, computed, orgAlpha, computed)
		}
	}
	// The retracting versions, each newer than both content versions above.
	mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES ('team-gone',generateUUIDv4(),'Gone','linear',0,?,?)`, at, orgAlpha)
	mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id,last_synced) VALUES (?,'acme/renamed-new','github',?,?)`, repoRenamed, orgAlpha, at)
	mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at) VALUES (?,'github','team-b',NULL,'acme/beta','exact','provider_access',?,?,?)`,
		orgAlpha, at.Add(-time.Hour), at.Add(-time.Second), at)
	// A later generation for wi-b that keeps the donor but in a new key, plus a
	// same-instant tie on wi-a's primary row whose content is unchanged.
	mustExec(t, ctx, conn, `INSERT INTO work_item_team_attributions (org_id,repo_id,work_item_id,provider,team_id,source,is_primary,confidence,evidence,computed_at) VALUES
        (?,toUUID('00000000-0000-0000-0000-000000000000'),'wi-a','linear','team-a','native_team',1,'high','e',?),
        (?,toUUID('00000000-0000-0000-0000-000000000000'),'wi-b','jira','team-b','native_team',1,'high','e',?)`,
		orgAlpha, at, orgAlpha, at.Add(time.Minute))
	return []string{"wi-a", "wi-b", "wi-gone"}
}

func assertDonorOraclesAgree(t *testing.T, ctx context.Context, conn driver.Conn, state string, ids []string, at time.Time, current []TeamRepoDonor) {
	t.Helper()
	for _, oracle := range []struct{ name, query string }{
		{"per-arm FINALs", previousTeamRepoDonorsQuery},
		{"DISTINCT after the join", fanOutTeamRepoDonorsQuery},
	} {
		if got := runDonorQuery(t, ctx, conn, oracle.query, ids, orgAlpha, at); !reflect.DeepEqual(got, current) {
			t.Fatalf("%s corpus: donor query diverged from its %s form:\n%s: %v\ncurrent: %v", state, oracle.name, oracle.name, donorKeys(got), donorKeys(current))
		}
	}
}

// TestDonorOutputIsIndependentOfMergeState reads the two-version corpus while
// every version is still an unmerged part, then forces every table to its
// fully merged state and reads again. The two results must be byte-equal:
// the donor query may not depend on whether ClickHouse has merged yet. Each
// earlier form of the query is an oracle on both states, so a reshaping that
// resolves a version differently than its predecessors shows up here.
func TestDonorOutputIsIndependentOfMergeState(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	stopDonorTableMerges(t, ctx, conn)
	at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	ids := seedTwoVersionDonorCorpus(t, ctx, conn, at)

	var parts uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.parts WHERE active AND database = currentDatabase() AND table IN ('teams','repos','team_repo_ownership','work_item_team_attributions')`).Scan(&parts); err != nil {
		t.Fatal(err)
	}
	if parts < uint64(2*len(donorTables)) {
		t.Fatalf("only %d active parts; the corpus is not unmerged, so this read proves nothing", parts)
	}

	unmerged, err := reader.FetchTeamRepoDonors(ctx, ids, orgAlpha, at)
	if err != nil {
		t.Fatal(err)
	}
	assertDonorOraclesAgree(t, ctx, conn, "unmerged", ids, at, unmerged)
	for _, table := range donorTables {
		mustExec(t, ctx, conn, "SYSTEM START MERGES "+table)
		mustExec(t, ctx, conn, "OPTIMIZE TABLE "+table+" FINAL")
	}
	merged, err := reader.FetchTeamRepoDonors(ctx, ids, orgAlpha, at)
	if err != nil {
		t.Fatal(err)
	}
	assertDonorOraclesAgree(t, ctx, conn, "merged", ids, at, merged)
	if !reflect.DeepEqual(unmerged, merged) {
		t.Fatalf("donor output depends on merge state:\nunmerged: %v\nmerged:   %v", donorKeys(unmerged), donorKeys(merged))
	}
	want := []string{
		"wi-a/team-a/b1111111-1111-4111-8111-111111111111",
		"wi-a/team-a/b2222222-2222-4222-8222-222222222222",
		"wi-b/team-b/b3333333-3333-4333-8333-333333333333",
	}
	if !reflect.DeepEqual(donorKeys(merged), want) {
		t.Fatalf("donors = %v, want %v", donorKeys(merged), want)
	}
}

// TestDonorRowsAreNotMultipliedByDuplicateEvidence feeds one donor tuple
// through every duplication path at once: two physical versions of each row,
// two eligible ownership sources for one repository, and both join branches
// (by id and by name) resolving to that same repository. Materialization
// counts donor rows and splits effort over them, so each (work item, team,
// repository) must come back exactly once. A 2x fan-out anywhere shows up as
// a count mismatch here.
func TestDonorRowsAreNotMultipliedByDuplicateEvidence(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	stopDonorTableMerges(t, ctx, conn)
	at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	repoOne := "c1111111-1111-4111-8111-111111111111"
	repoTwo := "c2222222-2222-4222-8222-222222222222"

	for version := 0; version < 2; version++ {
		stamp := at.Add(time.Duration(version) * time.Second)
		mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES ('team',generateUUIDv4(),'T','linear',1,?,?)`, stamp, orgAlpha)
		mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id,last_synced) VALUES (?,'acme/one','github',?,?),(?,'acme/two','github',?,?)`,
			repoOne, orgAlpha, stamp, repoTwo, orgAlpha, stamp)
		mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at) VALUES
            (?,'github','team',?,'acme/one','exact','native',?,NULL,?),
            (?,'github','team',NULL,'ACME/ONE','exact','provider_access',?,NULL,?),
            (?,'github','team',NULL,'acme/one','exact','inferred',?,NULL,?),
            (?,'github','team',NULL,'acme/two','exact','provider_access',?,NULL,?)`,
			orgAlpha, repoOne, at.Add(-time.Hour), stamp,
			orgAlpha, at.Add(-time.Hour), stamp,
			orgAlpha, at.Add(-time.Hour), stamp,
			orgAlpha, at.Add(-time.Hour), stamp)
		for issue := 0; issue < 3; issue++ {
			mustExec(t, ctx, conn, `INSERT INTO work_item_team_attributions (org_id,repo_id,work_item_id,provider,team_id,source,is_primary,confidence,evidence,computed_at) VALUES (?,toUUID('00000000-0000-0000-0000-000000000000'),?,'linear','team','native_team',1,'high','e',?)`,
				orgAlpha, fmt.Sprintf("wi-%d", issue), at)
		}
	}

	ids := []string{"wi-0", "wi-1", "wi-2"}
	donors, err := reader.FetchTeamRepoDonors(ctx, ids, orgAlpha, at)
	if err != nil {
		t.Fatal(err)
	}
	// Three work items, each owning exactly two repositories through one team.
	if len(donors) != len(ids)*2 {
		t.Fatalf("got %d donor rows, want %d (one per work item and repository): %v", len(donors), len(ids)*2, donorKeys(donors))
	}
	seen := map[string]int{}
	for _, key := range donorKeys(donors) {
		seen[key]++
		if seen[key] > 1 {
			t.Fatalf("donor %s returned %d times", key, seen[key])
		}
	}
}

// TestDonorQueryReadsEachDedupedTableOnce pins the cost shape of the donor
// query. Every dedup it runs is load-bearing (see
// TestEachDonorDedupIsLoadBearing), so the cheaper form is not fewer FINALs
// but fewer evaluations of them. ClickHouse inlines a WITH subquery at every
// reference, so a CTE named in both UNION ALL arms scans, sorts and merges its
// table once per arm, and an IN subquery is one more full scan on top. The
// attributions table -- by far the largest input -- must be scanned once in
// total, and teams once; only ownership and repositories, which the two
// equi-join arms genuinely need separately, may be read twice.
func TestDonorQueryReadsEachDedupedTableOnce(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	// ClickHouse plans an empty table as ReadNothing, so every table needs a
	// row for its reads to appear in the plan at all. Attributions get enough
	// distinct rows that one extra scan is unmistakable in read_rows, while
	// staying inside one index granule so a scan cannot be partially skipped.
	const attributionRows = 5000
	mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES ('team',generateUUIDv4(),'T','linear',1,?,?)`, at, orgAlpha)
	mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id,last_synced) VALUES ('d1111111-1111-4111-8111-111111111111','acme/one','github',?,?)`, orgAlpha, at)
	mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at) VALUES (?,'github','team','acme/one','exact','provider_access',?,NULL,?)`, orgAlpha, at.Add(-time.Hour), at)
	mustExec(t, ctx, conn, fmt.Sprintf(`INSERT INTO work_item_team_attributions (org_id,repo_id,work_item_id,provider,team_id,source,is_primary,confidence,evidence,computed_at)
SELECT ?, toUUID('00000000-0000-0000-0000-000000000000'), concat('wi-', toString(number)), 'linear', 'team', 'native_team', 1, 'high', 'e', ? FROM numbers(%d)`, attributionRows), orgAlpha, at)
	ids := make([]string, attributionRows)
	for i := range ids {
		ids[i] = fmt.Sprintf("wi-%d", i)
	}
	params := []any{
		clickhouse.Named("org_id", orgAlpha),
		clickhouse.Named("as_of", at.UTC().Format("2006-01-02 15:04:05.000")),
	}
	explainCtx, err := withDonorWorkItems(ctx, ids)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := conn.Query(explainCtx, "EXPLAIN PLAN\n"+teamRepoDonorsQuery, params...)
	if err != nil {
		t.Fatalf("EXPLAIN PLAN: %v", err)
	}
	reads := map[string]int{}
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line)
		plan.WriteString("\n")
		if !strings.Contains(line, "ReadFromMergeTree") {
			continue
		}
		for _, table := range donorTables {
			if strings.Contains(line, "."+table+")") {
				reads[table]++
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	want := map[string]int{
		"work_item_team_attributions": 1,
		"teams":                       1,
		"team_repo_ownership":         2,
		"repos":                       2,
	}
	if !reflect.DeepEqual(reads, want) {
		t.Errorf("plan table reads = %v, want %v\nplan:\n%s", reads, want, plan.String())
	}

	// EXPLAIN PLAN does not draw the scan behind an IN (subquery) set, so the
	// plan alone would miss a latest-generation filter re-added as one. The
	// executed query's read_rows does count it.
	const tag = "donor-query-read-shape"
	donors, err := reader.FetchTeamRepoDonors(clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": tag})), ids, orgAlpha, at)
	if err != nil {
		t.Fatal(err)
	}
	if len(donors) != attributionRows {
		t.Fatalf("got %d donors, want %d; the fixture is not reaching the join", len(donors), attributionRows)
	}
	mustExec(t, ctx, conn, "SYSTEM FLUSH LOGS")
	var readRows uint64
	if err := conn.QueryRow(ctx, `SELECT read_rows FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = ? ORDER BY event_time_microseconds DESC LIMIT 1`, tag).Scan(&readRows); err != nil {
		t.Fatalf("read query_log: %v", err)
	}
	// The requested ids arrive as an external table, and read_rows counts its
	// rows too: one per id, which is what is taken off before the bound.
	if scanned := readRows - uint64(len(ids)); readRows < uint64(len(ids)) || scanned < attributionRows || scanned >= 2*attributionRows {
		t.Fatalf("donor query read %d rows (%d of them requested ids) over %d attribution rows; the attributions table must be scanned exactly once",
			readRows, len(ids), attributionRows)
	}
}
