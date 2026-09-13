//go:build integration

package chquery

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// oldTeamRepoDonorsQuery is the join shape this package replaced: repos and
// ownership joined through a single OR predicate (id equality on one side,
// case-insensitive name equality on the other). It is kept here ONLY as the
// equivalence oracle for TestEquiJoinRewriteMatchesOrPredicateOracle and must
// never be reintroduced into the production query.
const oldTeamRepoDonorsQuery = `
SELECT DISTINCT a.work_item_id, assumeNotNull(a.team_id), r.id
FROM (
    SELECT work_item_id, team_id, source, is_primary
    FROM work_item_team_attributions FINAL
    WHERE org_id = {org_id:String} AND work_item_id IN {work_item_ids:Array(String)}
      AND (work_item_id, computed_at) IN (
          SELECT work_item_id, max(computed_at)
          FROM work_item_team_attributions
          WHERE org_id = {org_id:String} AND work_item_id IN {work_item_ids:Array(String)}
          GROUP BY work_item_id
      )
) AS a
INNER JOIN (SELECT id FROM teams FINAL WHERE org_id = {org_id:String} AND is_active = 1) AS t
    ON t.id = a.team_id
INNER JOIN (
    SELECT team_id, provider, repo_id, repo_full_name
    FROM team_repo_ownership FINAL
    WHERE org_id = {org_id:String}
      AND source IN ('native', 'jira_legacy', 'provider_access', 'inferred')
      AND valid_from <= {as_of:DateTime64(3, 'UTC')}
      AND (valid_to IS NULL OR valid_to > {as_of:DateTime64(3, 'UTC')})
) AS o ON o.team_id = a.team_id
INNER JOIN (SELECT id, provider, repo FROM repos FINAL WHERE org_id = {org_id:String}) AS r
    ON r.provider = o.provider AND
       (r.id = o.repo_id OR (o.repo_id IS NULL AND lower(r.repo) = lower(o.repo_full_name)))
WHERE a.is_primary = 1 AND a.source IN ('native_team', 'issue_project', 'project_ownership', 'repo_ownership')
  AND a.team_id IS NOT NULL AND a.team_id != ''
  AND r.id != toUUID('00000000-0000-0000-0000-000000000000')
ORDER BY a.work_item_id, r.id, a.team_id`

// runDonorQuery executes the given query text and decodes rows through the
// exact same path FetchTeamRepoDonors uses, so a divergence between the old
// and new query text is the only thing a comparison can be catching.
func runDonorQuery(
	t *testing.T, ctx context.Context, conn driver.Conn, query string,
	workItemIDs []string, orgID string, asOf time.Time,
) []TeamRepoDonor {
	t.Helper()
	// Earlier forms kept as oracles bind the ids as an Array(String) parameter;
	// the production form reads them from the external table. Each gets only
	// the input its text names, so an oracle cannot pass on the other's input.
	args := []any{clickhouse.Named("org_id", orgID), clickhouse.Named("as_of", asOf.UTC().Format("2006-01-02 15:04:05.000"))}
	if strings.Contains(query, "{work_item_ids:Array(String)}") {
		args = append(args, clickhouse.Named("work_item_ids", workItemIDs))
	} else {
		var err error
		if ctx, err = withDonorWorkItems(ctx, workItemIDs); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		t.Fatalf("donor query: %v\n%s", err, query)
	}
	defer func() { _ = rows.Close() }()
	donors := make([]TeamRepoDonor, 0)
	for rows.Next() {
		var donor TeamRepoDonor
		if err := rows.Scan(&donor.WorkItemID, &donor.TeamID, &donor.RepoID); err != nil {
			t.Fatalf("scan donor row: %v", err)
		}
		donor.WorkItemID = pythonparity.DecodeClickHouseStringValue(donor.WorkItemID)
		donor.TeamID = pythonparity.DecodeClickHouseStringValue(donor.TeamID)
		donors = append(donors, donor)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate donor rows: %v", err)
	}
	return donors
}

// TestEquiJoinRewriteMatchesOrPredicateOracle seeds one work item per branch
// the repos join can take, plus a work item that donates through BOTH
// branches at once, and asserts the new two-branch equi-join query returns
// exactly what the old single OR-predicate join returned. The old query text
// is the oracle, not a hand-written "want" list, so the comparison cannot
// drift out of sync with what the old join actually did.
func TestEquiJoinRewriteMatchesOrPredicateOracle(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	at := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	repoByID := "11111111-1111-4111-8111-111111111111"
	repoByName := "22222222-2222-4222-8222-222222222222"
	repoBothA := "33333333-3333-4333-8333-333333333333"
	repoBothB := "44444444-4444-4444-8444-444444444444"
	repoZero := "00000000-0000-0000-0000-000000000000"

	// Each scenario gets its OWN team. team_repo_ownership joins on team_id
	// alone, so sharing one team across scenarios would fan every one of that
	// team's ownership rows out to every work item attributed to it --
	// masking exactly the per-branch exclusions this fixture means to isolate.
	mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES
        ('team-id',generateUUIDv4(),'ByID','linear',1,?,?),
        ('team-name',generateUUIDv4(),'ByName','linear',1,?,?),
        ('team-both',generateUUIDv4(),'Both','linear',1,?,?),
        ('team-zero',generateUUIDv4(),'Zero','linear',1,?,?),
        ('team-expired',generateUUIDv4(),'Expired','linear',1,?,?),
        ('team-inactive',generateUUIDv4(),'Inactive','linear',0,?,?)`,
		at, orgAlpha, at, orgAlpha, at, orgAlpha, at, orgAlpha, at, orgAlpha, at, orgAlpha)

	mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id) VALUES
        (?,'acme/by-id','github',?),
        (?,'Acme/By-Name','github',?),
        (?,'acme/both-a','github',?),
        (?,'acme/both-b','github',?),
        (?,'acme/zero','github',?)`,
		repoByID, orgAlpha, repoByName, orgAlpha, repoBothA, orgAlpha, repoBothB, orgAlpha, repoZero, orgAlpha)

	ownershipByID := func(team, repoID string) {
		mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at) VALUES (?,'github',?,?,'stale-name-ignored-when-id-set','exact','provider_access',?,NULL,?)`,
			orgAlpha, team, repoID, at.Add(-time.Hour), at)
	}
	ownershipByName := func(team, name string) {
		mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at) VALUES (?,'github',?,?,'exact','provider_access',?,NULL,?)`,
			orgAlpha, team, name, at.Add(-time.Hour), at)
	}

	// Branch A only: ownership names the repo by id.
	ownershipByID("team-id", repoByID)
	// Branch B only: ownership has no repo_id, matched by lower(name) -- the
	// stored name differs in case from the repos row.
	ownershipByName("team-name", "acme/by-name")
	// Both branches for one team: one row by id, one by name, so the two
	// UNION ALL arms must each contribute without dropping or duplicating the
	// other's row.
	ownershipByID("team-both", repoBothA)
	ownershipByName("team-both", "acme/both-b")
	// Branch B row whose matched repo carries the all-zero id -- excluded by
	// the id-fence AFTER the join, exercising that the union still respects
	// the shared post-join filter.
	ownershipByName("team-zero", "acme/zero")
	// Retracted ownership (valid_to already elapsed as of `at`): must not
	// donate through either branch.
	mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at) VALUES (?,'github','team-expired',?,'expired','exact','provider_access',?,?,?)`,
		orgAlpha, repoByID, at.Add(-2*time.Hour), at.Add(-time.Minute), at)
	// A team with otherwise-eligible ownership that is INACTIVE: must not
	// donate regardless of which branch its ownership would otherwise take.
	ownershipByID("team-inactive", repoByID)

	attr := func(issue, team, source string, primary uint8) {
		mustExec(t, ctx, conn, `INSERT INTO work_item_team_attributions (org_id,repo_id,work_item_id,provider,team_id,source,is_primary,confidence,evidence,computed_at) VALUES (?,toUUID('00000000-0000-0000-0000-000000000000'),?,'linear',?,?,?,'high','fixture',?)`,
			orgAlpha, issue, team, source, primary, at)
	}
	attr("id-match", "team-id", "native_team", 1)
	attr("name-match", "team-name", "native_team", 1)
	attr("both-branches", "team-both", "native_team", 1)
	attr("zero-uuid-repo", "team-zero", "native_team", 1)
	attr("expired-ownership", "team-expired", "native_team", 1)
	attr("inactive-team-donor", "team-inactive", "native_team", 1)
	// Non-primary attribution to the SAME otherwise fully eligible id-branch
	// ownership "id-match" uses: only is_primary = 0 can be excluding this one.
	attr("non-primary", "team-id", "native_team", 0)

	ids := []string{
		"id-match", "name-match", "both-branches", "zero-uuid-repo",
		"expired-ownership", "inactive-team-donor", "non-primary",
	}

	oldResult := runDonorQuery(t, ctx, conn, oldTeamRepoDonorsQuery, ids, orgAlpha, at)
	newResult, err := reader.FetchTeamRepoDonors(ctx, ids, orgAlpha, at)
	if err != nil {
		t.Fatal(err)
	}

	if len(oldResult) == 0 {
		t.Fatal("oracle query returned nothing; the fixture is not exercising the join at all")
	}
	if !reflect.DeepEqual(oldResult, newResult) {
		t.Fatalf("equi-join rewrite diverged from the OR-predicate oracle:\nold: %+v\nnew: %+v", oldResult, newResult)
	}

	want := map[string]bool{
		"id-match/team-id/" + repoByID:         true,
		"name-match/team-name/" + repoByName:   true,
		"both-branches/team-both/" + repoBothA: true,
		"both-branches/team-both/" + repoBothB: true,
	}
	got := map[string]bool{}
	for _, d := range newResult {
		got[d.WorkItemID+"/"+d.TeamID+"/"+d.RepoID.String()] = true
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("donor set = %v, want %v", got, want)
	}
}

// TestEquiJoinPlanHasNoDisjunctiveJoinCondition pins the shape of the fix at
// the query-plan level: the repos-to-ownership join must be two branches each
// joined on a plain equality key, never a single join whose condition
// contains an OR across the two key columns. An OR there is not a hash-join
// key ClickHouse can use directly; it is what forced the previous plan's
// repos/ownership join down to the single-threaded, non-spilling algorithm
// this test also checks is gone.
func TestEquiJoinPlanHasNoDisjunctiveJoinCondition(t *testing.T) {
	_, conn, ctx := newTestReader(t)
	at := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES ('team',generateUUIDv4(),'Team','linear',1,?,?)`, at, orgAlpha)
	mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id) VALUES ('11111111-1111-4111-8111-111111111111','acme/good','github',?)`, orgAlpha)
	mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at) VALUES (?,'github','team','acme/good','exact','provider_access',?,NULL,?)`, orgAlpha, at.Add(-time.Hour), at)
	mustExec(t, ctx, conn, `INSERT INTO work_item_team_attributions (org_id,repo_id,work_item_id,provider,team_id,source,is_primary,confidence,evidence,computed_at) VALUES (?,toUUID('00000000-0000-0000-0000-000000000000'),'wi','linear','team','native_team',1,'high','fixture',?)`, orgAlpha, at)

	explainCtx, err := withDonorWorkItems(ctx, []string{"wi"})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(explainCtx, "EXPLAIN PLAN actions=1\n"+teamRepoDonorsQuery,
		clickhouse.Named("org_id", orgAlpha),
		clickhouse.Named("as_of", at.UTC().Format("2006-01-02 15:04:05.000")))
	if err != nil {
		t.Fatalf("EXPLAIN PLAN: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line)
		plan.WriteString("\n")
	}
	text := plan.String()

	// Scoped to "Join conditions:" lines specifically: the ownership filter's
	// own "(valid_to IS NULL OR valid_to > ...)" is an unrelated, legitimate
	// OR and must not trip this check.
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, "Join conditions:") && strings.Contains(line, " OR ") {
			t.Fatalf("a join condition is still disjunctive, the exact shape that made the old join a filtered cross product:\n%s\nfull plan:\n%s", line, text)
		}
	}
	if !strings.Contains(text, "Union") {
		t.Fatalf("plan has no Union node; expected the two equi-join branches to be unioned:\n%s", text)
	}
	if !strings.Contains(text, "repo_id = CAST(id AS Nullable(UUID))") {
		t.Fatalf("plan is missing the id-branch equality join key:\n%s", text)
	}
	if !strings.Contains(text, "lower(repo_full_name) = lower(repo)") {
		t.Fatalf("plan is missing the name-branch equality join key:\n%s", text)
	}
}

// TestFinalIsLoadBearingForAttributionVersionTies is the FINAL-drop question
// the join rewrite raised: does the (work_item_id, computed_at) IN (SELECT
// max(...)) filter already make work_item_team_attributions FINAL redundant?
//
// It does not. That filter selects the latest computed_at PER WORK ITEM, but
// the ReplacingMergeTree key is finer than that (org_id, repo_id,
// work_item_id, team_id, source), so two physical rows can share one key AND
// one computed_at -- a genuine version tie, e.g. a same-instant correction --
// and only FINAL resolves which one is current. Dropping FINAL does not
// average or randomly pick between the tied rows: it returns BOTH, so a
// donation the correction retracted (is_primary flipped to 0) still passes
// the is_primary = 1 filter through the stale row. FINAL is kept in
// production for exactly this reason.
func TestFinalIsLoadBearingForAttributionVersionTies(t *testing.T) {
	reader, conn, ctx := newTestReader(t)
	// Stop merges so both tied physical rows survive on disk long enough for
	// FINAL, not a background merge, to be what resolves them.
	mustExec(t, ctx, conn, `SYSTEM STOP MERGES work_item_team_attributions`)
	at := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	mustExec(t, ctx, conn, `INSERT INTO teams (id,team_uuid,name,provider,is_active,updated_at,org_id) VALUES ('team',generateUUIDv4(),'Team','linear',1,?,?)`, at, orgAlpha)
	mustExec(t, ctx, conn, `INSERT INTO repos (id,repo,provider,org_id) VALUES ('11111111-1111-4111-8111-111111111111','acme/good','github',?)`, orgAlpha)
	mustExec(t, ctx, conn, `INSERT INTO team_repo_ownership (org_id,provider,team_id,repo_full_name,match_type,source,valid_from,valid_to,updated_at) VALUES (?,'github','team','acme/good','exact','provider_access',?,NULL,?)`, orgAlpha, at.Add(-time.Hour), at)

	// Two physical rows sharing the EXACT sorting key (org_id, repo_id,
	// work_item_id, team_id, source) and the EXACT version (computed_at): a
	// same-instant correction that flips is_primary from 1 to 0.
	insertAttribution := func(isPrimary uint8, evidence string) {
		mustExec(t, ctx, conn, `INSERT INTO work_item_team_attributions (org_id,repo_id,work_item_id,provider,team_id,source,is_primary,confidence,evidence,computed_at) VALUES (?,toUUID('00000000-0000-0000-0000-000000000000'),'wi','linear','team','native_team',?,'high',?,?)`,
			orgAlpha, isPrimary, evidence, at)
	}
	insertAttribution(1, "original")
	insertAttribution(0, "same-instant correction")

	donors, err := reader.FetchTeamRepoDonors(ctx, []string{"wi"}, orgAlpha, at)
	if err != nil {
		t.Fatal(err)
	}
	if len(donors) != 0 {
		t.Fatalf(
			"production query (FINAL kept) donated %v; the tie must resolve to the "+
				"is_primary=0 correction and donate nothing", donors,
		)
	}

	// The same fixture with FINAL removed from work_item_team_attributions
	// alone (everything else identical to the production query): the stale
	// is_primary=1 row is never collapsed away, so it still satisfies the
	// outer is_primary = 1 filter and donates -- the false positive dropping
	// FINAL would ship.
	noFinalQuery := strings.Replace(
		teamRepoDonorsQuery,
		"FROM work_item_team_attributions FINAL\n",
		"FROM work_item_team_attributions\n",
		1,
	)
	if noFinalQuery == teamRepoDonorsQuery {
		t.Fatal("the FINAL-stripping replacement did not match; the production query text changed shape")
	}
	withoutFinal := runDonorQuery(t, ctx, conn, noFinalQuery, []string{"wi"}, orgAlpha, at)
	if len(withoutFinal) != 1 {
		t.Fatalf(
			"expected dropping FINAL to resurrect exactly the one stale donation, got %v; "+
				"if this is now empty too, FINAL may genuinely have become redundant here and "+
				"the production query's FINAL should be revisited instead of assumed necessary",
			withoutFinal,
		)
	}
}
