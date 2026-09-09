//go:build integration

package analytics

// Seeded real-engine test for the SankeyResult.coverage port
// (sankeycoverage.go). Same harness as
// investmentquality_seeded_integration_test.go --
// internal/testsupport/containers.StartClickHouse, the digest-pinned
// image every Go Testcontainers test in this repo uses.
//
// WHY A REAL ENGINE IS THE ONLY USEFUL TEST HERE: the Python original
// carries two CHAOS-4241 bugs that were invisible for an unknown period
// because the branch was UNREACHABLE (`if request.use_investment:` was
// once unconditional, so the row-count path never ran for the investment
// case at all). Both are semantic, not syntactic -- a unit test asserting
// on generated SQL text would happily assert the buggy text. This test's
// scenario is built so that each bug flips a DIFFERENT expected number:
//
//   - repo fan-out double-counting: wu-team-2repos contributes TWO joined
//     rows whose fractional weights sum back to exactly 1.0. Counting
//     joined rows instead of summing weights makes total 5.0, not 4.0.
//   - display-expression vs RAW repo id: wu-team-norepo has a NULL
//     repo_id and no repo-effort row, so it must count as repo-UNassigned.
//     Testing the display expression (`ifNull(nullIf(r.repo, ''), ...)`,
//     which is never SQL NULL) makes repo_coverage 1.0, not 0.75.
//   - the effort_value <= 0 arm (CHAOS-4241 codex round 2):
//     wu-zeroeffort-2repos would divide to 0 and vanish from the
//     denominator, making total 3.0, not 4.0. It deliberately carries TWO
//     repo rows so the arm's 1/N shape is actually pinned: with one row
//     1.0/1 == 1.0, so a regression to a constant 1.0 would pass; with
//     two, a constant 1.0 sums to 2.0 and the total becomes 5.0.
//
// Membership-scope tables are created but left EMPTY, so marker_count = 0,
// scope_enabled = 0 and the scope filter is a no-op -- this test isolates
// coverage arithmetic rather than re-testing the scope gate, which
// investmentmembershipscope_test.go already covers.

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Column types AND sorting keys mirror the live schema exactly (verified
// against the running dev stack's DESCRIBE TABLE / SHOW CREATE TABLE, not
// guessed). Three traps this fixture has to reproduce rather than
// simplify:
//
//   - repo_id is Nullable(UUID) on work_unit_repo_effort, and a MergeTree
//     rejects a nullable column in the sorting key outright ("Sorting key
//     contains nullable columns, but merge tree setting
//     `allow_nullable_key` is disabled", code 44). Production does NOT
//     enable that setting -- it wraps the column,
//     `ifNull(toString(repo_id), ”)` -- so this fixture wraps it the same
//     way. Enabling allow_nullable_key here would make the fixture pass
//     under a schema production does not have.
//   - work_item_team_attributions wraps its Nullable team_id in the key
//     for the same reason, and its repo_id is a NON-nullable UUID there.
//   - that table's source/confidence are Enum8, not String.
const seededCoverageExtraDDL = `
CREATE TABLE work_unit_repo_effort (
    work_unit_id String,
    repo_id Nullable(UUID),
    effort_metric String,
    effort_value Float64,
    allocation_weight Float64,
    allocation_source String,
    repo_source Nullable(String),
    categorization_run_id String,
    computed_at DateTime64(3, 'UTC'),
    org_id String
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, work_unit_id, ifNull(toString(repo_id), ''));

CREATE TABLE work_item_team_attributions (
    org_id String,
    repo_id UUID,
    work_item_id String,
    provider String,
    team_id Nullable(String),
    team_name Nullable(String),
    source Enum8('native_team' = 1, 'linked_issue' = 2, 'project_ownership' = 3, 'repo_ownership' = 4, 'assignee_membership' = 5, 'unassigned' = 6, 'issue_project' = 7, 'manual_fallback' = 8, 'author_membership' = 9),
    is_primary UInt8,
    confidence Enum8('high' = 1, 'medium' = 2, 'low' = 3, 'manual' = 4, 'none' = 5),
    evidence String,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, repo_id, work_item_id, ifNull(team_id, ''), source);

CREATE TABLE repos (
    id UUID,
    repo String,
    ref Nullable(String),
    created_at DateTime64(3, 'UTC'),
    settings Nullable(String),
    tags Nullable(String),
    last_synced DateTime64(3, 'UTC'),
    org_id String,
    provider String,
    source_id Nullable(UUID)
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, id);
`

const (
	seededCoverageRepo1 = "11111111-1111-1111-1111-111111111111"
	seededCoverageRepo2 = "22222222-2222-2222-2222-222222222222"
	// CHAOS-5483: a third repo so ONE team-fallback unit can fan out across
	// THREE repos while the other fans out across two. Two would make the
	// 1/N share 0.5, which is also what a broken constant-0.5 arm produces;
	// three makes that unit's share 1/3. The two DIFFERENT widths are what
	// give the fan-out ratio a denominator worth computing -- see
	// TestResolveSankeyCoverage_SeededRealClickHouse_SplitPartitionsExactly.
	seededCoverageRepo3 = "33333333-3333-3333-3333-333333333333"
	seededCoverageTS    = "2026-01-02 00:00:00.000"
)

// seededCoverageUnit is one work_unit_investments row. Only the columns
// the coverage query reads carry meaningful values; the rest are valid
// filler.
type seededCoverageUnit struct {
	workUnitID  string
	effortValue float64
	repoID      string // "" => NULL (no scalar repo fallback)
	issueRef    string // the single structural-evidence issue ref
}

func seedCoverageUnits(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID string, units []seededCoverageUnit) {
	t.Helper()
	rows := make([]string, 0, len(units))
	for _, u := range units {
		repo := "NULL"
		if u.repoID != "" {
			repo = fmt.Sprintf("toUUID('%s')", u.repoID)
		}
		evidence := fmt.Sprintf(`{"issues":["%s"],"prs":[]}`, u.issueRef)
		rows = append(rows, fmt.Sprintf(
			"('%s', toDateTime64('%s', 3, 'UTC'), toDateTime64('2026-01-03 00:00:00.000', 3, 'UTC'), %s, 'github', 'churn_loc', %v, map('feature_delivery', 1.0), map('feature_delivery.build', 1.0), '%s', 0.5, 'moderate', 'ok', '', 'v1', 'h', 'run-1', toDateTime64('%s', 3, 'UTC'), 'pr', 'seeded', '%s')",
			u.workUnitID, seededCoverageTS, repo, u.effortValue, evidence, seededCoverageTS, orgID))
	}
	insert := fmt.Sprintf(
		"INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, work_unit_type, work_unit_name, org_id) VALUES %s",
		strings.Join(rows, ", "))
	if err := conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed work_unit_investments: %v", err)
	}
}

// TestResolveSankeyCoverage_SeededRealClickHouse_ExactShares is the
// durable, CI-enrolled regression test for the coverage port. Four work
// units with hand-computed fractional weights summing to 4.0, of which
// 2.0 resolve a team and 3.0 resolve a repo.
func TestResolveSankeyCoverage_SeededRealClickHouse_ExactShares(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, stmt := range splitSQLStatements(seededQualitySchemaDDL + seededCoverageExtraDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	const orgID = "seeded-sankey-coverage"

	seedCoverageUnits(t, ctx, conn, orgID, []seededCoverageUnit{
		// 60/100 + 40/100 = 1.0 across two repo rows -- proves the
		// wure fan-out is not double-counted.
		{workUnitID: "wu-team-2repos", effortValue: 100, repoID: seededCoverageRepo1, issueRef: "linear:ALPHA-1"},
		// 50/50 = 1.0, team unresolved.
		{workUnitID: "wu-noteam-1repo", effortValue: 50, repoID: seededCoverageRepo1, issueRef: "linear:NOTEAM-1"},
		// No repo-effort row and a NULL scalar repo_id: weight 1.0 via
		// the LEFT JOIN fallback, and repo-UNassigned.
		{workUnitID: "wu-team-norepo", effortValue: 25, repoID: "", issueRef: "linear:ALPHA-1"},
		// effort_value = 0 exercises the 1.0/repo_row_count arm, and it
		// carries TWO repo rows on purpose: with only one, 1.0/1 == 1.0 and
		// a regression to a constant 1.0 would still satisfy the oracle.
		// With two, the arm must produce 0.5 + 0.5; a constant 1.0 yields
		// 2.0 and pushes the total to 5.0.
		{workUnitID: "wu-zeroeffort-2repos", effortValue: 0, repoID: seededCoverageRepo1, issueRef: "linear:NOTEAM-1"},
	})

	if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO work_unit_repo_effort
        (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES
        ('wu-team-2repos',      toUUID('%[1]s'), 'churn_loc', 60, 0.6, 'evidence', 'run-1', toDateTime64('%[3]s', 3, 'UTC'), '%[4]s'),
        ('wu-team-2repos',      toUUID('%[2]s'), 'churn_loc', 40, 0.4, 'evidence', 'run-1', toDateTime64('%[3]s', 3, 'UTC'), '%[4]s'),
        ('wu-noteam-1repo',     toUUID('%[1]s'), 'churn_loc', 50, 1.0, 'evidence', 'run-1', toDateTime64('%[3]s', 3, 'UTC'), '%[4]s'),
        ('wu-zeroeffort-2repos', toUUID('%[1]s'), 'churn_loc',  0, 1.0, 'evidence', 'run-1', toDateTime64('%[3]s', 3, 'UTC'), '%[4]s'),
        ('wu-zeroeffort-2repos', toUUID('%[2]s'), 'churn_loc',  0, 1.0, 'evidence', 'run-1', toDateTime64('%[3]s', 3, 'UTC'), '%[4]s')`,
		seededCoverageRepo1, seededCoverageRepo2, seededCoverageTS, orgID)); err != nil {
		t.Fatalf("seed work_unit_repo_effort: %v", err)
	}

	// Only linear:ALPHA-1 gets an attribution row; linear:NOTEAM-1
	// deliberately gets none, so its units resolve to "unassigned"
	// through a genuine LEFT JOIN miss.
	if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO work_item_team_attributions
        (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at) VALUES
        ('%[3]s', toUUID('%[1]s'), 'linear:ALPHA-1', 'linear', 'ALPHA', 'Alpha', 'native_team', 1, 'high', '', toDateTime64('%[2]s', 3, 'UTC'))`,
		seededCoverageRepo1, seededCoverageTS, orgID)); err != nil {
		t.Fatalf("seed work_item_team_attributions: %v", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO repos
        (id, repo, ref, created_at, settings, tags, last_synced, org_id, provider, source_id) VALUES
        (toUUID('%[1]s'), 'acme/one', NULL, toDateTime64('%[3]s', 3, 'UTC'), NULL, NULL, toDateTime64('%[3]s', 3, 'UTC'), '%[4]s', 'github', NULL),
        (toUUID('%[2]s'), 'acme/two', NULL, toDateTime64('%[3]s', 3, 'UTC'), NULL, NULL, toDateTime64('%[3]s', 3, 'UTC'), '%[4]s', 'github', NULL)`,
		seededCoverageRepo1, seededCoverageRepo2, seededCoverageTS, orgID)); err != nil {
		t.Fatalf("seed repos: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	req, err := SankeyRequestFromInput(model.SankeyRequestInput{
		Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme},
		Measure: model.MeasureInputCount,
		DateRange: &model.DateRangeInput{
			StartDate: mustGraphQLDate("2026-01-01"),
			EndDate:   mustGraphQLDate("2026-01-08"),
		},
		MaxNodes: 16,
		MaxEdges: 100,
	})
	if err != nil {
		t.Fatalf("SankeyRequestFromInput: %v", err)
	}

	got := resolveSankeyCoverage(ctx, client, orgID, req, 60, true, nil)
	if got == nil {
		t.Fatal("expected populated SankeyCoverage for the seeded org/window, got nil -- coverage degraded, check the investment_coverage.query_failed log")
	}

	// Hand-computed:
	//   total        = 0.6 + 0.4 + 1.0 + 1.0 + 1.0 = 4.0
	//   assigned_team= (0.6 + 0.4) + 1.0           = 2.0  (both ALPHA units)
	//   assigned_repo= (0.6 + 0.4) + 1.0 + 1.0     = 3.0  (all but wu-team-norepo)
	const (
		wantTeam = 2.0 / 4.0
		wantRepo = 3.0 / 4.0
		tol      = 1e-9
	)
	if math.Abs(got.TeamCoverage-wantTeam) > tol {
		t.Errorf("TeamCoverage = %v, want %v", got.TeamCoverage, wantTeam)
	}
	if math.Abs(got.RepoCoverage-wantRepo) > tol {
		t.Errorf("RepoCoverage = %v, want %v", got.RepoCoverage, wantRepo)
	}

	// CHAOS-5483 rider on the ORIGINAL oracle: every wure row above was
	// seeded before repo_source existed and carries NULL. Those rows are
	// in the headline numerator (they have a repo_id), so the split must
	// put all of them on the DIRECT side -- assigning them to neither
	// half, which a positive `repo_source IN (own_edges, ancestor:%,
	// children)` predicate would do, silently makes direct + fallback
	// undercount the headline by 100% on any pre-089 org. This assertion
	// is the cheapest place that mistake gets caught.
	if got.DirectRepoCoverage == nil || got.TeamFallbackRepoCoverage == nil {
		t.Fatalf("split fields must be populated on the investment path: direct=%v fallback=%v", got.DirectRepoCoverage, got.TeamFallbackRepoCoverage)
	}
	if math.Abs(*got.DirectRepoCoverage-wantRepo) > tol {
		t.Errorf("DirectRepoCoverage = %v, want %v (every seeded repo_source is NULL, so the whole headline is direct)", *got.DirectRepoCoverage, wantRepo)
	}
	if *got.TeamFallbackRepoCoverage != 0 {
		t.Errorf("TeamFallbackRepoCoverage = %v, want 0 (no team:%% rows seeded)", *got.TeamFallbackRepoCoverage)
	}
	if got.RepoFanoutReposPerUnit == nil || *got.RepoFanoutReposPerUnit != 0 {
		t.Errorf("RepoFanoutReposPerUnit = %v, want a populated 0 (no fallback rows is a MEASUREMENT, not an absence)", got.RepoFanoutReposPerUnit)
	}
}

// seededSplitUnitSourceRow is one work_unit_repo_effort row for the
// CHAOS-5483 split fixture, written out longhand rather than through a
// helper so each row's provenance and generation are readable beside the
// number it is supposed to produce.
type seededSplitUnitSourceRow struct {
	workUnitID  string
	repoID      string
	effortValue float64
	repoSource  string // "" => SQL NULL
	computedAt  string
}

// TestResolveSankeyCoverage_SeededRealClickHouse_SplitPartitionsExactly is
// CHAOS-5483's RED/GREEN test: the direct/hierarchy vs team-fallback split
// of the repo-coverage numerator, against a real engine.
//
// WHY A REAL ENGINE, AGAIN. Three of the four properties below are
// ClickHouse semantics, not Go logic, and a SQL-text assertion would
// happily assert the wrong text for every one of them:
//
//   - repo_source is Nullable(String), so `repo_source LIKE 'team:%'` is
//     Nullable(UInt8). sumIf ACCEPTS that without error and then
//     three-valued logic silently drops every NULL-provenance row out of
//     BOTH halves -- 1.4 of 5.0 assigned effort here. Reading the SQL
//     predicts a type error; the engine says otherwise.
//   - a bare argMax over a nullable column SKIPS NULLs, so a repo whose
//     newest generation dropped its provenance keeps an older `team:`
//     label and gets counted as fallback forever. wu-regen-null pins the
//     tuple-wrapped form that fixes it.
//   - the LEFT JOIN miss (wu-norepo) must land in neither half while
//     staying out of the numerator entirely.
//   - the partition itself is a float identity over sums the engine
//     evaluates, not one Go computes.
//
// Hand-computed oracle (weights are repo_effort_value / effort_value):
//
//	wu-direct-2repos   0.6 (own_edges) + 0.4 (NULL provenance)  = 1.0 direct
//	wu-ancestor-1repo  1.0 (ancestor:linear:ROOT-1)             = 1.0 direct
//	wu-children-1repo  1.0 (children)                           = 1.0 direct
//	wu-regen-null      1.0 (newest generation dropped team:)    = 1.0 direct
//	wu-team-3repos     0.333.. x 3 (team:ALPHA,BETA)            = 1.0 fallback
//	wu-team-2repos-b   0.5 x 2 (team:GAMMA)                     = 1.0 fallback
//	wu-norepo          1.0 via the LEFT JOIN fallback, NULL repo_id, unassigned
//
//	repo_total = 7.0   assigned_repo = 6.0   direct = 4.0   fallback = 2.0
//	fanout     = 5 team rows / 2 team units = 2.5
//
// TWO fallback units of DIFFERENT widths (3 repos and 2 repos) is deliberate
// and is the only reason the fan-out assertion discriminates anything. With a
// single fallback unit the denominator is 1, so `countIf / uniqExactIf` and
// `countIf / 1` produce the identical number and the assertion cannot tell a
// real distinct-unit count from a constant. At 5 rows over 2 units the correct
// answer is 2.5; a denominator stuck at 1 yields 5.0, and counting units
// instead of rows yields 2.0 -- three distinguishable values.
func TestResolveSankeyCoverage_SeededRealClickHouse_SplitPartitionsExactly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, stmt := range splitSQLStatements(seededQualitySchemaDDL + seededCoverageExtraDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	// wu-regen-null needs TWO physical generations of the same
	// (org, work_unit_id, repo_id) to survive to the argMax. The table is
	// a ReplacingMergeTree keyed on exactly that tuple, so a background
	// merge would collapse them and quietly turn this into a test of
	// nothing. Stopping merges makes the measurement actually happen, and
	// the row-count precondition below fails LOUDLY if it did not.
	if err := conn.Exec(ctx, "SYSTEM STOP MERGES work_unit_repo_effort"); err != nil {
		t.Fatalf("SYSTEM STOP MERGES work_unit_repo_effort: %v -- without it the wu-regen-null argMax case is vacuous", err)
	}

	const (
		orgID = "seeded-coverage-split"
		genT1 = "2026-01-02 00:00:00.000"
		genT2 = "2026-01-02 06:00:00.000"
	)

	seedCoverageUnits(t, ctx, conn, orgID, []seededCoverageUnit{
		{workUnitID: "wu-direct-2repos", effortValue: 100, repoID: seededCoverageRepo1, issueRef: "linear:ALPHA-1"},
		{workUnitID: "wu-ancestor-1repo", effortValue: 50, repoID: seededCoverageRepo1, issueRef: "linear:ALPHA-1"},
		{workUnitID: "wu-children-1repo", effortValue: 40, repoID: seededCoverageRepo2, issueRef: "linear:ALPHA-1"},
		{workUnitID: "wu-regen-null", effortValue: 20, repoID: seededCoverageRepo1, issueRef: "linear:ALPHA-1"},
		{workUnitID: "wu-team-3repos", effortValue: 90, repoID: "", issueRef: "linear:ALPHA-1"},
		{workUnitID: "wu-team-2repos-b", effortValue: 60, repoID: "", issueRef: "linear:ALPHA-1"},
		{workUnitID: "wu-norepo", effortValue: 25, repoID: "", issueRef: "linear:ALPHA-1"},
	})

	// wu-team-3repos mirrors what allocateTeamOwnership actually writes
	// (internal/jobs/investment/teamownership.go): one row per owned repo,
	// equal 1/N shares, repo_source "team:" + the sorted contributing team
	// ids joined by comma. Three repos would need three distinct repo
	// UUIDs, but this fixture only declares two in `repos`; the third is
	// declared inline below so the FINAL-deduped repos join still resolves
	// it.
	rows := []seededSplitUnitSourceRow{
		{"wu-direct-2repos", seededCoverageRepo1, 60, "own_edges", genT1},
		{"wu-direct-2repos", seededCoverageRepo2, 40, "", genT1},
		{"wu-ancestor-1repo", seededCoverageRepo1, 50, "ancestor:linear:ROOT-1", genT1},
		{"wu-children-1repo", seededCoverageRepo2, 40, "children", genT1},
		// Older generation carries a team: label; the newer one drops it.
		// A bare argMax(repo_source, computed_at) skips the NULL and
		// returns "team:ALPHA", misfiling 1.0 of direct effort as fallback.
		{"wu-regen-null", seededCoverageRepo1, 20, "team:ALPHA", genT1},
		{"wu-regen-null", seededCoverageRepo1, 20, "", genT2},
		{"wu-team-3repos", seededCoverageRepo1, 30, "team:ALPHA,BETA", genT1},
		{"wu-team-3repos", seededCoverageRepo2, 30, "team:ALPHA,BETA", genT1},
		{"wu-team-3repos", seededCoverageRepo3, 30, "team:ALPHA,BETA", genT1},
		// The SECOND fallback unit, deliberately a DIFFERENT width (2 repos,
		// not 3) and a different team, so the fan-out denominator has
		// something real to count. See this test's doc comment.
		{"wu-team-2repos-b", seededCoverageRepo1, 30, "team:GAMMA", genT1},
		{"wu-team-2repos-b", seededCoverageRepo2, 30, "team:GAMMA", genT1},
	}
	values := make([]string, 0, len(rows))
	for _, r := range rows {
		source := "NULL"
		if r.repoSource != "" {
			source = fmt.Sprintf("'%s'", r.repoSource)
		}
		values = append(values, fmt.Sprintf(
			"('%s', toUUID('%s'), 'churn_loc', %v, 1.0, 'evidence', %s, 'run-1', toDateTime64('%s', 3, 'UTC'), '%s')",
			r.workUnitID, r.repoID, r.effortValue, source, r.computedAt, orgID))
	}
	// `SETTINGS optimize_on_insert = 0` is load-bearing, and MEASURED: with
	// ClickHouse's default (optimize_on_insert = 1) the Replacing merge
	// logic runs over the inserted BLOCK itself, so wu-regen-null's two
	// generations collapse to one before they ever reach a part -- the
	// precondition below caught exactly that on this test's first run.
	// SYSTEM STOP MERGES alone does NOT cover it: that stops background
	// merges between parts, not the insert-time collapse within one.
	if err := conn.Exec(ctx, "INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, repo_source, categorization_run_id, computed_at, org_id) SETTINGS optimize_on_insert = 0 VALUES "+strings.Join(values, ", ")); err != nil {
		t.Fatalf("seed work_unit_repo_effort: %v", err)
	}

	// Rule 4 ("a measurement that did not happen must FAIL, loudly"): if
	// the two wu-regen-null generations were collapsed, the argMax case
	// never ran and every assertion below would pass without testing it.
	var regenRows uint64
	regenCheck, err := conn.Query(ctx, "SELECT count() FROM work_unit_repo_effort WHERE org_id = ? AND work_unit_id = 'wu-regen-null'", orgID)
	if err != nil {
		t.Fatalf("wu-regen-null precondition query: %v", err)
	}
	if !regenCheck.Next() {
		_ = regenCheck.Close()
		t.Fatal("wu-regen-null precondition query returned no rows")
	}
	if err := regenCheck.Scan(&regenRows); err != nil {
		_ = regenCheck.Close()
		t.Fatalf("wu-regen-null precondition scan: %v", err)
	}
	_ = regenCheck.Close()
	if regenRows != 2 {
		t.Fatalf("wu-regen-null has %d physical rows, want 2 -- the ReplacingMergeTree collapsed the generations despite SYSTEM STOP MERGES, so the nullable-argMax case is VACUOUS, not passing", regenRows)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO work_item_team_attributions
        (org_id, repo_id, work_item_id, provider, team_id, team_name, source, is_primary, confidence, evidence, computed_at) VALUES
        ('%[3]s', toUUID('%[1]s'), 'linear:ALPHA-1', 'linear', 'ALPHA', 'Alpha', 'native_team', 1, 'high', '', toDateTime64('%[2]s', 3, 'UTC'))`,
		seededCoverageRepo1, genT1, orgID)); err != nil {
		t.Fatalf("seed work_item_team_attributions: %v", err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO repos
        (id, repo, ref, created_at, settings, tags, last_synced, org_id, provider, source_id) VALUES
        (toUUID('%[1]s'), 'acme/one', NULL, toDateTime64('%[4]s', 3, 'UTC'), NULL, NULL, toDateTime64('%[4]s', 3, 'UTC'), '%[5]s', 'github', NULL),
        (toUUID('%[2]s'), 'acme/two', NULL, toDateTime64('%[4]s', 3, 'UTC'), NULL, NULL, toDateTime64('%[4]s', 3, 'UTC'), '%[5]s', 'github', NULL),
        (toUUID('%[3]s'), 'acme/three', NULL, toDateTime64('%[4]s', 3, 'UTC'), NULL, NULL, toDateTime64('%[4]s', 3, 'UTC'), '%[5]s', 'github', NULL)`,
		seededCoverageRepo1, seededCoverageRepo2, seededCoverageRepo3, genT1, orgID)); err != nil {
		t.Fatalf("seed repos: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	req, err := SankeyRequestFromInput(model.SankeyRequestInput{
		Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme},
		Measure: model.MeasureInputCount,
		DateRange: &model.DateRangeInput{
			StartDate: mustGraphQLDate("2026-01-01"),
			EndDate:   mustGraphQLDate("2026-01-08"),
		},
		MaxNodes: 16,
		MaxEdges: 100,
	})
	if err != nil {
		t.Fatalf("SankeyRequestFromInput: %v", err)
	}

	// PART 1 -- the NUMERATORS, read raw. The resolver divides by
	// repo_total, and a partition asserted only in ratio space would still
	// hold if both halves were scaled by the same wrong factor. The brief
	// asks for the numerators to partition the assigned effort exactly, so
	// they are read and compared as numerators here, before any division.
	compiled, err := compileSankeyCoverage(req, orgID, 60, true, nil)
	if err != nil {
		t.Fatalf("compileSankeyCoverage: %v", err)
	}
	raw, err := client.Query(ctx, compiled.sql, compiled.bindings)
	if err != nil {
		t.Fatalf("execute compiled coverage SQL: %v\n%s", err, compiled.sql)
	}
	if !raw.Next() {
		_ = raw.Close()
		t.Fatal("compiled coverage SQL returned no rows")
	}
	var rawTotal, rawAssignedTeam, rawRepoTotal, rawAssignedRepo float64
	var rawDirect, rawFallback, rawFanout *float64
	if err := raw.Scan(&rawTotal, &rawAssignedTeam, &rawRepoTotal, &rawAssignedRepo, &rawDirect, &rawFallback, &rawFanout); err != nil {
		_ = raw.Close()
		t.Fatalf("scan compiled coverage SQL: %v", err)
	}
	_ = raw.Close()

	const tol = 1e-9
	if rawDirect == nil || rawFallback == nil || rawFanout == nil {
		t.Fatalf("investment path must emit all three split columns non-NULL: direct=%v fallback=%v fanout=%v", rawDirect, rawFallback, rawFanout)
	}
	for _, c := range []struct {
		name string
		got  float64
		want float64
	}{
		{"repo_total", rawRepoTotal, 7.0},
		{"assigned_repo", rawAssignedRepo, 6.0},
		{"direct_repo", *rawDirect, 4.0},
		{"team_fallback_repo", *rawFallback, 2.0},
		{"fanout_repos_per_unit", *rawFanout, 2.5},
	} {
		if math.Abs(c.got-c.want) > tol {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}

	// THE partition property. Tolerance rather than bit-equality is
	// deliberate: the two halves are separate sumIf aggregates, so
	// ClickHouse adds their terms in a different order than assigned_repo's
	// single sumIf does, and IEEE-754 addition is not associative. The
	// claim being pinned is that no row and no weight is lost or
	// double-counted between the halves, which is exact; the last-bit
	// rounding is not part of that claim.
	if diff := math.Abs((*rawDirect + *rawFallback) - rawAssignedRepo); diff > tol {
		t.Errorf("direct + fallback = %v, want assigned_repo = %v (diff %v) -- the split is not a partition", *rawDirect+*rawFallback, rawAssignedRepo, diff)
	}

	// PART 2 -- the same numbers as the resolver returns them, over the
	// SAME denominator RepoCoverage uses.
	got := resolveSankeyCoverage(ctx, client, orgID, req, 60, true, nil)
	if got == nil {
		t.Fatal("expected populated SankeyCoverage, got nil -- coverage degraded, check the investment_coverage.query_failed log")
	}
	if got.DirectRepoCoverage == nil || got.TeamFallbackRepoCoverage == nil || got.RepoFanoutReposPerUnit == nil {
		t.Fatalf("split fields nil on the investment path: direct=%v fallback=%v fanout=%v", got.DirectRepoCoverage, got.TeamFallbackRepoCoverage, got.RepoFanoutReposPerUnit)
	}
	for _, c := range []struct {
		name string
		got  float64
		want float64
	}{
		{"RepoCoverage", got.RepoCoverage, 6.0 / 7.0},
		{"DirectRepoCoverage", *got.DirectRepoCoverage, 4.0 / 7.0},
		{"TeamFallbackRepoCoverage", *got.TeamFallbackRepoCoverage, 2.0 / 7.0},
		{"RepoFanoutReposPerUnit", *got.RepoFanoutReposPerUnit, 2.5},
	} {
		if math.Abs(c.got-c.want) > tol {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if diff := math.Abs((*got.DirectRepoCoverage + *got.TeamFallbackRepoCoverage) - got.RepoCoverage); diff > tol {
		t.Errorf("DirectRepoCoverage + TeamFallbackRepoCoverage = %v, want RepoCoverage = %v (diff %v)", *got.DirectRepoCoverage+*got.TeamFallbackRepoCoverage, got.RepoCoverage, diff)
	}
}

// TestResolveSankeyCoverage_SeededRealClickHouse_FanoutIsArrayJoinInvariant
// pins CHAOS-5483's own regression: the fan-out width must not change when a
// work-category filter is applied, because the filter appends
// `ARRAY JOIN ... subcategory_kv` and that multiplies every joined row by the
// unit's surviving subcategory count.
//
// This is the executed repro of a real defect in the first version of this
// change, kept as the guard rather than thrown away. That version computed
// the width as `countIf(<fallback>) / uniqExactIf(work_unit_id, <fallback>)`
// over joined rows; on the fixture below it reported 3 unfiltered and 6
// filtered for identical underlying data -- one work unit fanned across three
// repos, reported as six. The two SHARE columns never had this problem: the
// ARRAY JOIN scales their numerator and denominator together, so they are
// invariant by construction. A raw row COUNT is not, which is why the
// numerator is now a distinct count over the (work unit, repo) PAIR.
//
// The fixture is deliberately minimal and separate from the split fixture
// above: ONE team-fallback unit, THREE repo rows, and TWO subcategories that
// BOTH survive a `feature_delivery` filter (so the multiplier is exactly 2,
// not an accident of which rows the filter drops). The assertion is equality
// between the two calls, not a hardcoded number -- what matters is that the
// filter cannot move it.
func TestResolveSankeyCoverage_SeededRealClickHouse_FanoutIsArrayJoinInvariant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, stmt := range splitSQLStatements(seededQualitySchemaDDL + seededCoverageExtraDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	const orgID = "seeded-coverage-fanout-invariance"
	repos := []string{seededCoverageRepo1, seededCoverageRepo2, seededCoverageRepo3}

	// Two subcategories, both under feature_delivery, so a
	// work_category=feature_delivery filter keeps BOTH ARRAY JOIN rows.
	if err := conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, work_unit_type, work_unit_name, org_id) VALUES "+
			"('wu-fanout', toDateTime64('%[1]s', 3, 'UTC'), toDateTime64('2026-01-03 00:00:00.000', 3, 'UTC'), NULL, 'github', 'churn_loc', 90, map('feature_delivery', 1.0), map('feature_delivery.build', 0.5, 'feature_delivery.ship', 0.5), '{\"issues\":[\"linear:ALPHA-1\"],\"prs\":[]}', 0.5, 'moderate', 'ok', '', 'v1', 'h', 'run-1', toDateTime64('%[1]s', 3, 'UTC'), 'pr', 'seeded', '%[2]s')",
		seededCoverageTS, orgID)); err != nil {
		t.Fatalf("seed work_unit_investments: %v", err)
	}

	effortRows := make([]string, 0, len(repos))
	repoRows := make([]string, 0, len(repos))
	for _, repo := range repos {
		effortRows = append(effortRows, fmt.Sprintf(
			"('wu-fanout', toUUID('%s'), 'churn_loc', 30, 1.0, 'team_ownership', 'team:ALPHA', 'run-1', toDateTime64('%s', 3, 'UTC'), '%s')",
			repo, seededCoverageTS, orgID))
		repoRows = append(repoRows, fmt.Sprintf(
			"(toUUID('%[1]s'), 'acme/%[1]s', NULL, toDateTime64('%[2]s', 3, 'UTC'), NULL, NULL, toDateTime64('%[2]s', 3, 'UTC'), '%[3]s', 'github', NULL)",
			repo, seededCoverageTS, orgID))
	}
	if err := conn.Exec(ctx, "INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, repo_source, categorization_run_id, computed_at, org_id) SETTINGS optimize_on_insert = 0 VALUES "+strings.Join(effortRows, ", ")); err != nil {
		t.Fatalf("seed work_unit_repo_effort: %v", err)
	}
	if err := conn.Exec(ctx, "INSERT INTO repos (id, repo, ref, created_at, settings, tags, last_synced, org_id, provider, source_id) VALUES "+strings.Join(repoRows, ", ")); err != nil {
		t.Fatalf("seed repos: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	req, err := SankeyRequestFromInput(model.SankeyRequestInput{
		Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme},
		Measure: model.MeasureInputCount,
		DateRange: &model.DateRangeInput{
			StartDate: mustGraphQLDate("2026-01-01"),
			EndDate:   mustGraphQLDate("2026-01-08"),
		},
		MaxNodes: 16,
		MaxEdges: 100,
	})
	if err != nil {
		t.Fatalf("SankeyRequestFromInput: %v", err)
	}

	fanoutFor := func(label string, filters *model.FilterInput) float64 {
		t.Helper()
		got := resolveSankeyCoverage(ctx, client, orgID, req, 60, true, filters)
		if got == nil {
			t.Fatalf("%s: expected populated SankeyCoverage, got nil", label)
		}
		if got.RepoFanoutReposPerUnit == nil {
			t.Fatalf("%s: RepoFanoutReposPerUnit is nil on the investment path", label)
		}
		return *got.RepoFanoutReposPerUnit
	}

	unfiltered := fanoutFor("unfiltered", nil)
	filtered := fanoutFor("work-category filter", &model.FilterInput{
		Why: &model.WhyFilterInput{WorkCategory: []string{"feature_delivery"}},
	})

	// The unfiltered value is asserted absolutely as well, so a regression
	// that breaks BOTH calls identically cannot pass the equality check
	// alone: one unit fanned across three repos is a width of 3.
	const tol = 1e-9
	if math.Abs(unfiltered-3.0) > tol {
		t.Errorf("unfiltered RepoFanoutReposPerUnit = %v, want 3 (one unit, three repo allocations)", unfiltered)
	}
	if math.Abs(filtered-unfiltered) > tol {
		t.Errorf("RepoFanoutReposPerUnit moved under a work-category filter: unfiltered=%v filtered=%v -- the subcategory_kv ARRAY JOIN is multiplying the numerator, so the width is reported per filter rather than per repo", unfiltered, filtered)
	}
}

// seedFilterReweightOrg seeds one org for the work-category re-weighting
// characterization below: two units of equal effort, one carrying ONE
// subcategory and one carrying TWO, both under feature_delivery so a
// feature_delivery filter keeps every ARRAY JOIN row. assigned decides whether
// the second unit gets a repo-effort row at all.
func seedFilterReweightOrg(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, orgID string, secondUnitAssigned bool) {
	t.Helper()
	type unit struct {
		id      string
		subcats string
		repoSrc string // "" => seed no work_unit_repo_effort row at all
	}
	units := []unit{
		{"wu-one-subcat", "map('feature_delivery.build', 1.0)", "own_edges"},
		{"wu-two-subcat", "map('feature_delivery.build', 0.5, 'feature_delivery.ship', 0.5)", ""},
	}
	if secondUnitAssigned {
		units[1].repoSrc = "team:ALPHA"
	}
	for _, u := range units {
		if err := conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, work_unit_type, work_unit_name, org_id) VALUES "+
				"('%[1]s', toDateTime64('%[2]s', 3, 'UTC'), toDateTime64('2026-01-03 00:00:00.000', 3, 'UTC'), NULL, 'github', 'churn_loc', 100, map('feature_delivery', 1.0), %[3]s, '{\"issues\":[\"linear:ALPHA-1\"],\"prs\":[]}', 0.5, 'moderate', 'ok', '', 'v1', 'h', 'run-1', toDateTime64('%[2]s', 3, 'UTC'), 'pr', 'seeded', '%[4]s')",
			u.id, seededCoverageTS, u.subcats, orgID)); err != nil {
			t.Fatalf("seed work_unit_investments %s: %v", u.id, err)
		}
		if u.repoSrc == "" {
			continue
		}
		if err := conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO work_unit_repo_effort (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, repo_source, categorization_run_id, computed_at, org_id) SETTINGS optimize_on_insert = 0 VALUES ('%[1]s', toUUID('%[2]s'), 'churn_loc', 100, 1.0, 'seeded', '%[3]s', 'run-1', toDateTime64('%[4]s', 3, 'UTC'), '%[5]s')",
			u.id, seededCoverageRepo1, u.repoSrc, seededCoverageTS, orgID)); err != nil {
			t.Fatalf("seed work_unit_repo_effort %s: %v", u.id, err)
		}
	}
	if err := conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO repos (id, repo, ref, created_at, settings, tags, last_synced, org_id, provider, source_id) VALUES (toUUID('%[1]s'), 'acme/one', NULL, toDateTime64('%[2]s', 3, 'UTC'), NULL, NULL, toDateTime64('%[2]s', 3, 'UTC'), '%[3]s', 'github', NULL)",
		seededCoverageRepo1, seededCoverageTS, orgID)); err != nil {
		t.Fatalf("seed repos: %v", err)
	}
}

// TestResolveSankeyCoverage_SeededRealClickHouse_WorkCategoryFilterReweightsUnits_CHAOS5498
// is a CHARACTERIZATION test for the defect tracked as CHAOS-5498. It pins behaviour that is WRONG and that this
// change does not fix, so that the defect is visible in the suite instead of
// latent, and so whoever fixes it has a red-first anchor. Do not read a green
// here as an endorsement of these numbers.
//
// THE DEFECT, and it is NOT introduced by CHAOS-5483: a work-category filter
// appends `ARRAY JOIN CAST(subcategory_distribution_json ...)` (see
// hasWorkCategoryFilter in sankeycoverage.go). That multiplies each unit's
// joined rows by ITS OWN surviving subcategory count -- so units are re-weighted
// RELATIVE TO EACH OTHER, and every effort-weighted column in this query
// inherits it, including the pre-existing teamCoverage and repoCoverage. The
// same ARRAY JOIN is in the Python original at
// src/dev_health_ops/api/graphql/resolvers/analytics.py:833, so both planes
// share the behaviour. Filed as CHAOS-5498; the fix is a semantics decision
// (aggregate at unit grain, or weight by subcategory_kv.2 so a filtered view
// means "coverage among work in this category"), which is chris's call, not a
// silent correction inside a split PR.
//
// The reviewer that found this (round chaos-5483-pr1-r1, F1 P1) reported it
// against the SPLIT columns. Reproducing it showed the opposite: the split is
// an exact partition of the headline in every case measured, filtered and not
// -- it inherits the distortion faithfully rather than adding one. Fixing the
// split alone would BREAK the partition and make it disagree with the coverage
// card beside it, which is strictly worse than a documented shared distortion.
//
// Two scenarios, because the first one alone is misleading:
//
//	assigned: both units resolve a repo. repoCoverage stays 1.0 across the
//	  filter and looks immune -- it is merely SATURATED. The tell is the
//	  denominator: repo_total moves 2 -> 3.
//	unassigned: the two-subcategory unit resolves NO repo, so repoCoverage
//	  becomes sensitive and moves 0.5 -> 0.333 on identical data, with the
//	  split columns constant. This is the scenario that proves the defect is
//	  the headline's, not the split's.
func TestResolveSankeyCoverage_SeededRealClickHouse_WorkCategoryFilterReweightsUnits_CHAOS5498(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, stmt := range splitSQLStatements(seededQualitySchemaDDL + seededCoverageExtraDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	const (
		orgAssigned   = "seeded-coverage-reweight-assigned"
		orgUnassigned = "seeded-coverage-reweight-unassigned"
	)
	seedFilterReweightOrg(t, ctx, conn, orgAssigned, true)
	seedFilterReweightOrg(t, ctx, conn, orgUnassigned, false)

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	req, err := SankeyRequestFromInput(model.SankeyRequestInput{
		Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme},
		Measure: model.MeasureInputCount,
		DateRange: &model.DateRangeInput{
			StartDate: mustGraphQLDate("2026-01-01"),
			EndDate:   mustGraphQLDate("2026-01-08"),
		},
		MaxNodes: 16,
		MaxEdges: 100,
	})
	if err != nil {
		t.Fatalf("SankeyRequestFromInput: %v", err)
	}

	// The RAW compiled columns, so the headline is measured in the same call
	// as the split rather than inferred from the resolver's ratios.
	read := func(orgID, label string, filters *model.FilterInput) (repoTotal, assignedRepo, direct, fallback float64) {
		t.Helper()
		compiled, err := compileSankeyCoverage(req, orgID, 60, true, filters)
		if err != nil {
			t.Fatalf("%s: compileSankeyCoverage: %v", label, err)
		}
		rows, err := client.Query(ctx, compiled.sql, compiled.bindings)
		if err != nil {
			t.Fatalf("%s: execute compiled SQL: %v", label, err)
		}
		defer func() { _ = rows.Close() }()
		if !rows.Next() {
			t.Fatalf("%s: compiled SQL returned no rows", label)
		}
		var total, assignedTeam float64
		var dir, fb, fan *float64
		if err := rows.Scan(&total, &assignedTeam, &repoTotal, &assignedRepo, &dir, &fb, &fan); err != nil {
			t.Fatalf("%s: scan: %v", label, err)
		}
		if dir == nil || fb == nil {
			t.Fatalf("%s: split columns nil on the investment path", label)
		}
		return repoTotal, assignedRepo, *dir, *fb
	}

	categoryFilter := &model.FilterInput{
		Why: &model.WhyFilterInput{WorkCategory: []string{"feature_delivery"}},
	}
	const tol = 1e-9

	// Scenario 1 -- both units assigned. The shares look stable; the
	// DENOMINATOR is what gives the re-weighting away.
	for _, c := range []struct {
		label                                                 string
		filters                                               *model.FilterInput
		wantRepoTotal, wantAssigned, wantDirect, wantFallback float64
	}{
		{"assigned/unfiltered", nil, 2, 2, 1, 1},
		{"assigned/filtered", categoryFilter, 3, 3, 1, 2},
	} {
		repoTotal, assigned, direct, fallback := read(orgAssigned, c.label, c.filters)
		for _, got := range []struct {
			name string
			v    float64
			want float64
		}{
			{"repo_total", repoTotal, c.wantRepoTotal},
			{"assigned_repo", assigned, c.wantAssigned},
			{"direct_repo", direct, c.wantDirect},
			{"team_fallback_repo", fallback, c.wantFallback},
		} {
			if math.Abs(got.v-got.want) > tol {
				t.Errorf("%s: %s = %v, want %v", c.label, got.name, got.v, got.want)
			}
		}
		// The property this change actually owns: whatever the filter does to
		// the weights, the split still decomposes the headline exactly.
		if math.Abs((direct+fallback)-assigned) > tol {
			t.Errorf("%s: direct + fallback = %v, want assigned_repo = %v -- the partition must hold regardless of the filter re-weighting", c.label, direct+fallback, assigned)
		}
	}

	// Scenario 2 -- the two-subcategory unit is UNASSIGNED, so repoCoverage is
	// sensitive. This is the proof the defect belongs to the headline: the
	// split columns are identical across the filter while repoCoverage is not.
	unfilteredTotal, unfilteredAssigned, unfilteredDirect, unfilteredFallback := read(orgUnassigned, "unassigned/unfiltered", nil)
	filteredTotal, filteredAssigned, filteredDirect, filteredFallback := read(orgUnassigned, "unassigned/filtered", categoryFilter)

	if math.Abs(unfilteredAssigned/unfilteredTotal-0.5) > tol {
		t.Errorf("unassigned/unfiltered repoCoverage = %v, want 0.5", unfilteredAssigned/unfilteredTotal)
	}
	if math.Abs(filteredAssigned/filteredTotal-1.0/3.0) > tol {
		t.Errorf("unassigned/filtered repoCoverage = %v, want 0.333... -- if this now equals 0.5, the headline re-weighting has been FIXED and this characterization test should be replaced by a real assertion", filteredAssigned/filteredTotal)
	}
	if math.Abs(unfilteredDirect-filteredDirect) > tol || math.Abs(unfilteredFallback-filteredFallback) > tol {
		t.Errorf("split columns moved across the filter (direct %v->%v, fallback %v->%v) while the headline moved -- the split is supposed to be constant here",
			unfilteredDirect, filteredDirect, unfilteredFallback, filteredFallback)
	}
}

// TestResolveSankeyCoverage_SeededRealClickHouse_EmptyWindowIsNilNotZero
// pins the degradation boundary: a window with no rows returns zero rows,
// which Python leaves as coverage=None (`if c_rows:`) rather than 0/0.
// Asserting this is what keeps "no data" distinguishable from "0% covered"
// -- exactly the confusion the Allocation tiles suffered from while this
// field was hardcoded nil.
func TestResolveSankeyCoverage_SeededRealClickHouse_EmptyWindowIsNilNotZero(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for _, stmt := range splitSQLStatements(seededQualitySchemaDDL + seededCoverageExtraDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	req, err := SankeyRequestFromInput(model.SankeyRequestInput{
		Path:    []model.DimensionInput{model.DimensionInputTeam, model.DimensionInputTheme},
		Measure: model.MeasureInputCount,
		DateRange: &model.DateRangeInput{
			StartDate: mustGraphQLDate("2026-01-01"),
			EndDate:   mustGraphQLDate("2026-01-08"),
		},
		MaxNodes: 16,
		MaxEdges: 100,
	})
	if err != nil {
		t.Fatalf("SankeyRequestFromInput: %v", err)
	}

	// An aggregate over zero rows still returns ONE row in ClickHouse
	// (sum() of nothing is 0), so this asserts the total>0 guard, not an
	// empty result set: coverage is a real object whose shares are 0.
	got := resolveSankeyCoverage(ctx, client, "org-with-no-rows", req, 60, true, nil)
	if got == nil {
		t.Fatal("expected a SankeyCoverage object (ClickHouse returns one aggregate row even over zero input rows), got nil")
	}
	if got.TeamCoverage != 0 || got.RepoCoverage != 0 {
		t.Errorf("empty window: TeamCoverage=%v RepoCoverage=%v, want 0/0 (the total>0 guard, analytics.py:878-881)", got.TeamCoverage, got.RepoCoverage)
	}
}
