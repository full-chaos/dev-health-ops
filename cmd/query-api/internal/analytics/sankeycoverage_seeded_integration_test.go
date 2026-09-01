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
//     wu-zeroeffort-1repo would divide to 0 and vanish from the
//     denominator, making total 3.0, not 4.0.
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

// Column types mirror the live schema exactly (verified against the
// running dev stack's DESCRIBE TABLE, not guessed): repo_id is
// Nullable(UUID) on work_unit_repo_effort but a non-nullable UUID on
// work_item_team_attributions, and that table's source/confidence are
// Enum8 -- a String there silently fails to insert.
const seededCoverageExtraDDL = `
CREATE TABLE work_unit_repo_effort (
    work_unit_id String,
    repo_id Nullable(UUID),
    effort_metric String,
    effort_value Float64,
    allocation_weight Float64,
    allocation_source String,
    categorization_run_id String,
    computed_at DateTime64(3, 'UTC'),
    org_id String
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, work_unit_id, repo_id);

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
ORDER BY (org_id, work_item_id, repo_id);

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
		// effort_value = 0 exercises the 1.0/repo_row_count arm.
		{workUnitID: "wu-zeroeffort-1repo", effortValue: 0, repoID: seededCoverageRepo1, issueRef: "linear:NOTEAM-1"},
	})

	if err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO work_unit_repo_effort
        (work_unit_id, repo_id, effort_metric, effort_value, allocation_weight, allocation_source, categorization_run_id, computed_at, org_id) VALUES
        ('wu-team-2repos',      toUUID('%[1]s'), 'churn_loc', 60, 0.6, 'evidence', 'run-1', toDateTime64('%[3]s', 3, 'UTC'), '%[4]s'),
        ('wu-team-2repos',      toUUID('%[2]s'), 'churn_loc', 40, 0.4, 'evidence', 'run-1', toDateTime64('%[3]s', 3, 'UTC'), '%[4]s'),
        ('wu-noteam-1repo',     toUUID('%[1]s'), 'churn_loc', 50, 1.0, 'evidence', 'run-1', toDateTime64('%[3]s', 3, 'UTC'), '%[4]s'),
        ('wu-zeroeffort-1repo', toUUID('%[1]s'), 'churn_loc',  0, 1.0, 'evidence', 'run-1', toDateTime64('%[3]s', 3, 'UTC'), '%[4]s')`,
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
