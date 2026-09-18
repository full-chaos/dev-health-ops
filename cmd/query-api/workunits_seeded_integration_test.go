//go:build integration

// GET+POST /api/v1/work-units' own seeded-real-ClickHouse recurrence
// guard: every table (*Reader).BuildWorkUnitInvestments reads, created
// with the CURRENT sorting key confirmed against PROD's own
// system.tables (full-chaos/dev-health/_records/5885-5886-prod-engines.tsv)
// -- work_unit_membership in particular carries `run_id` as the LAST
// sorting-key column on prod (049_work_unit_membership_run_id_dedup_key.py
// rebuilt it there; the SIBLING seeded test in
// cmd/query-api/internal/investmentexplain predates that migration's
// rebuild and still declares the table without it).
//
// Three mechanisms this file proves against a REAL merge-eligible engine,
// each via a genuine physical DUPLICATE row (same ReplacingMergeTree sort
// key, two different `computed_at`/version values, unmerged since no
// OPTIMIZE FINAL runs here) rather than a fixture double that can only
// ever answer whatever its own author declared:
//
//  1. work_unit_investments: two physical versions of the same
//     (org_id, work_unit_id) key. LatestWorkUnitInvestmentsSource's own
//     argMax(tuple(col), computed_at).1 must return the LATER version's
//     values, never the earlier one and never both.
//  2. work_unit_supersessions: a work_unit_id present in
//     work_unit_investments AND recorded as superseded. The shared
//     source's unconditional NOT IN filter must exclude it from the
//     result list entirely.
//  3. work_unit_investment_quotes: two physical versions of the same
//     (org_id, work_unit_id, source_id, quote) key, differing only in
//     source_type. FetchWorkUnitInvestmentQuotes' own GROUP BY + argMax
//     (the Python plane this ports reads this table raw) must collapse
//     them to exactly ONE textual
//     evidence entry carrying the LATER version's source_type.
package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/analytics"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/investmentexplain"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/teamscope"
)

func splitWorkUnitsSeededDDL(sql string) []string {
	var out []string
	for _, stmt := range strings.Split(sql, ";") {
		if trimmed := strings.TrimSpace(stmt); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// workUnitsSeededSchemaDDL declares every table
// BuildWorkUnitInvestments' own call graph reads, with the sorting key
// PROD's system.tables carries today for each -- see this file's own
// package doc comment for the one table (work_unit_membership) where
// that differs from what the migration chain alone would suggest.
// work_unit_membership_runs is created but left EMPTY: with no
// completed-run marker, investmentMembershipScopeStateSource's own
// run id is empty and the membership-scope OR short-circuits
// (investment.go/investmentmembershipscope.go), so every seeded
// investment row passes that gate regardless -- the same posture
// investmentexplain's own sibling seeded test already takes, and
// orthogonal to the three mechanisms this file exists to prove.
const workUnitsSeededSchemaDDL = `
CREATE TABLE work_unit_investments (
    work_unit_id String,
    work_unit_type Nullable(String),
    work_unit_name Nullable(String),
    from_ts DateTime64(3, 'UTC'),
    to_ts DateTime64(3, 'UTC'),
    repo_id Nullable(UUID),
    provider Nullable(String),
    effort_metric String,
    effort_value Float64,
    theme_distribution_json Map(String, Float64),
    subcategory_distribution_json Map(String, Float64),
    structural_evidence_json String,
    evidence_quality Float64,
    evidence_quality_band String,
    categorization_status String,
    categorization_errors_json String,
    categorization_model_version String,
    categorization_input_hash String,
    categorization_run_id String,
    computed_at DateTime64(3, 'UTC'),
    org_id String
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, work_unit_id);

CREATE TABLE work_unit_investment_quotes (
    work_unit_id String,
    quote String,
    source_type String,
    source_id String,
    computed_at DateTime64(3, 'UTC'),
    categorization_run_id String,
    org_id String
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, work_unit_id, source_id, quote);

CREATE TABLE work_unit_supersessions (
    org_id String,
    superseded_work_unit_id String,
    superseded_by_run_id String,
    superseded_at DateTime64(9, 'UTC')
) ENGINE = ReplacingMergeTree(superseded_at)
ORDER BY (org_id, superseded_work_unit_id);

CREATE TABLE work_unit_membership_runs (
    org_id       String,
    run_id       String,
    completed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(completed_at)
ORDER BY (org_id, run_id);

CREATE TABLE work_unit_membership (
    org_id String,
    node_type String,
    node_id String,
    work_unit_id String,
    category_kind String,
    category String,
    weight Float64,
    is_dominant UInt8,
    categorization_status String,
    computed_at DateTime64(3, 'UTC'),
    run_id String DEFAULT ''
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, node_type, node_id, category_kind, category, run_id);

CREATE TABLE repos (
    id UUID,
    repo String,
    ref Nullable(String),
    created_at DateTime64(3, 'UTC'),
    settings Nullable(String),
    tags Nullable(String),
    last_synced DateTime64(3, 'UTC'),
    org_id String DEFAULT 'default',
    provider String DEFAULT 'unknown'
) ENGINE = ReplacingMergeTree(last_synced)
ORDER BY (org_id, id);

CREATE TABLE team_repo_ownership (
    org_id String,
    provider String,
    team_id String,
    repo_id Nullable(UUID),
    repo_full_name String,
    match_type Enum8('exact' = 1, 'pattern' = 2),
    source Enum8('native' = 1, 'jira_legacy' = 2, 'provider_access' = 3, 'manual' = 4, 'inferred' = 5),
    is_primary UInt8 DEFAULT 0,
    specificity UInt16 DEFAULT 0,
    priority Int32 DEFAULT 0,
    valid_from DateTime64(3, 'UTC'),
    valid_to Nullable(DateTime64(3, 'UTC')),
    updated_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (org_id, provider, repo_full_name, team_id, source, valid_from);

CREATE TABLE work_item_team_attributions (
    org_id String,
    repo_id UUID,
    work_item_id String,
    provider String,
    team_id Nullable(String),
    team_name Nullable(String),
    source Enum8('native_team' = 1, 'linked_issue' = 2, 'project_ownership' = 3, 'repo_ownership' = 4, 'assignee_membership' = 5, 'unassigned' = 6, 'issue_project' = 7, 'manual_fallback' = 8, 'author_membership' = 9),
    is_primary UInt8,
    confidence Enum8('high' = 1, 'medium' = 2, 'low' = 3),
    evidence String,
    computed_at DateTime64(3, 'UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, repo_id, work_item_id, ifNull(team_id, ''), source);

CREATE TABLE user_metrics_daily (
    repo_id UUID,
    day Date,
    author_email String,
    commits_count UInt32,
    loc_added UInt32,
    loc_deleted UInt32,
    files_changed UInt32,
    large_commits_count UInt32,
    avg_commit_size_loc Float64,
    prs_authored UInt32,
    prs_merged UInt32,
    avg_pr_cycle_hours Float64,
    median_pr_cycle_hours Float64,
    pr_cycle_p75_hours Float64,
    pr_cycle_p90_hours Float64,
    prs_with_first_review UInt32,
    reviews_given UInt32,
    changes_requested_given UInt32,
    reviews_received UInt32,
    review_reciprocity Float64,
    team_id Nullable(String),
    team_name Nullable(String),
    computed_at DateTime('UTC'),
    org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (org_id, repo_id, author_email, day);
`

// startSeededWorkUnitsClickHouse starts a real ClickHouse test container,
// applies workUnitsSeededSchemaDDL, and returns both a raw connection (for
// seeding) and an investmentexplain.Reader (for exercising the real
// production read path) against it.
func startSeededWorkUnitsClickHouse(t *testing.T) (chdriver.Conn, *investmentexplain.Reader, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		cancel()
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		cancel()
		_ = inst.Close(context.Background())
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		cancel()
		_ = inst.Close(context.Background())
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}

	for _, stmt := range splitWorkUnitsSeededDDL(workUnitsSeededSchemaDDL) {
		if err := conn.Exec(ctx, stmt); err != nil {
			_ = conn.Close()
			cancel()
			_ = inst.Close(context.Background())
			t.Fatalf("exec DDL %q: %v", stmt, err)
		}
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		_ = conn.Close()
		cancel()
		_ = inst.Close(context.Background())
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	// Same wiring as the production route: the reader gets a pinned client.
	reader, err := investmentexplain.NewReader(analytics.PinInvestmentMembershipScope(client))
	if err != nil {
		_ = client.Close()
		_ = conn.Close()
		cancel()
		_ = inst.Close(context.Background())
		t.Fatalf("NewReader: %v", err)
	}

	cleanup := func() {
		_ = client.Close()
		_ = conn.Close()
		cancel()
		_ = inst.Close(context.Background())
	}
	return conn, reader, cleanup
}

const workUnitsSeededOrgID = "chaos-5886-workunits-recurrence-guard"

// insertWorkUnitInvestmentVersion inserts one physical version of a
// work_unit_investments row -- called twice with the same
// (org_id, work_unit_id) key and different computedAt to create an
// unmerged ReplacingMergeTree duplicate.
func insertWorkUnitInvestmentVersion(t *testing.T, ctx context.Context, conn chdriver.Conn, workUnitID, workUnitName, effortMetric string, effortValue float64, computedAt string) {
	t.Helper()
	insertWorkUnitInvestmentVersionWithRunID(t, ctx, conn, workUnitID, workUnitName, effortMetric, effortValue, computedAt, "")
}

// insertWorkUnitInvestmentVersionWithRunID is insertWorkUnitInvestmentVersion
// plus an explicit categorization_run_id -- needed wherever a test also
// seeds work_unit_investment_quotes, since BuildWorkUnitInvestments only
// looks up quotes for a unit whose OWN categorization_run_id is non-empty
// (workunitassembly.go's unitRuns construction). Setting it at INSERT time
// rather than a later `ALTER TABLE ... UPDATE`: ClickHouse mutations are
// asynchronous by default and are not guaranteed visible to an immediately
// following SELECT, which is not the dedup mechanism this file exists to
// prove.
func insertWorkUnitInvestmentVersionWithRunID(t *testing.T, ctx context.Context, conn chdriver.Conn, workUnitID, workUnitName, effortMetric string, effortValue float64, computedAt, categorizationRunID string) {
	t.Helper()
	stmt := fmt.Sprintf(
		`INSERT INTO work_unit_investments
			(work_unit_id, work_unit_type, work_unit_name, from_ts, to_ts, repo_id, provider,
			 effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json,
			 structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status,
			 categorization_errors_json, categorization_model_version, categorization_input_hash,
			 categorization_run_id, computed_at, org_id)
		VALUES
			('%s', 'issue', '%s', toDateTime64('2026-01-01 00:00:00',3), toDateTime64('2026-01-05 00:00:00',3),
			 NULL, NULL, '%s', %v, map('feature_delivery', 0.8), map(), '{}',
			 0.5, 'moderate', 'ok', '', 'v1', 'hash', '%s', toDateTime64('%s',3), '%s')`,
		workUnitID, workUnitName, effortMetric, effortValue, categorizationRunID, computedAt, workUnitsSeededOrgID,
	)
	if err := conn.Exec(ctx, stmt); err != nil {
		t.Fatalf("seed work_unit_investments version (work_unit_id=%s computed_at=%s): %v", workUnitID, computedAt, err)
	}
}

// TestWorkUnitsSeededRealClickHouse_ArgMaxDedupReturnsLatestVersion is
// mechanism 1 from this file's own package doc comment: two unmerged
// physical versions of the SAME (org_id, work_unit_id) key must resolve
// to the LATER version's own values, never the earlier one.
func TestWorkUnitsSeededRealClickHouse_ArgMaxDedupReturnsLatestVersion(t *testing.T) {
	conn, reader, cleanup := startSeededWorkUnitsClickHouse(t)
	defer cleanup()
	ctx := context.Background()

	insertWorkUnitInvestmentVersion(t, ctx, conn, "wu-dedup", "Stale name", "churn_loc", 5.0, "2026-01-01 00:00:00")
	insertWorkUnitInvestmentVersion(t, ctx, conn, "wu-dedup", "Latest name", "churn_loc", 9.0, "2026-01-06 00:00:00")

	investments, err := reader.BuildWorkUnitInvestments(ctx, investmentexplain.BuildWorkUnitInvestmentsOptions{
		OrgID:   workUnitsSeededOrgID,
		StartTS: time.Date(2025, 12, 25, 0, 0, 0, 0, time.UTC),
		EndTS:   time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC),
		Limit:   200,
	})
	if err != nil {
		t.Fatalf("BuildWorkUnitInvestments: %v", err)
	}
	if len(investments) != 1 {
		t.Fatalf("len(investments) = %d, want exactly 1 (the two rows share one dedup key) -- got %+v", len(investments), investments)
	}
	got := investments[0]
	if got.WorkUnitName == nil || *got.WorkUnitName != "Latest name" {
		t.Errorf("work_unit_name = %v, want \"Latest name\" (the later computed_at version) -- a stale/blended answer here is the argMax-dedup defect this test guards", got.WorkUnitName)
	}
	if got.Effort.Value != 9.0 {
		t.Errorf("effort.value = %v, want 9.0 (the later computed_at version)", got.Effort.Value)
	}
}

// TestWorkUnitsSeededRealClickHouse_SupersessionExcludesTheUnit is
// mechanism 2: a work_unit_id recorded in work_unit_supersessions must
// never appear in the result, independent of the membership-scope gate
// (investmentsupersessions.go's own binding condition).
func TestWorkUnitsSeededRealClickHouse_SupersessionExcludesTheUnit(t *testing.T) {
	conn, reader, cleanup := startSeededWorkUnitsClickHouse(t)
	defer cleanup()
	ctx := context.Background()

	insertWorkUnitInvestmentVersion(t, ctx, conn, "wu-kept", "Kept unit", "churn_loc", 3.0, "2026-01-06 00:00:00")
	insertWorkUnitInvestmentVersion(t, ctx, conn, "wu-superseded", "Superseded unit", "churn_loc", 7.0, "2026-01-06 00:00:00")

	supersessionInsert := fmt.Sprintf(
		`INSERT INTO work_unit_supersessions (org_id, superseded_work_unit_id, superseded_by_run_id, superseded_at)
		VALUES ('%s', 'wu-superseded', 'run-regroup-1', toDateTime64('2026-01-07 00:00:00', 9))`,
		workUnitsSeededOrgID,
	)
	if err := conn.Exec(ctx, supersessionInsert); err != nil {
		t.Fatalf("seed work_unit_supersessions: %v", err)
	}

	investments, err := reader.BuildWorkUnitInvestments(ctx, investmentexplain.BuildWorkUnitInvestmentsOptions{
		OrgID:   workUnitsSeededOrgID,
		StartTS: time.Date(2025, 12, 25, 0, 0, 0, 0, time.UTC),
		EndTS:   time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC),
		Limit:   200,
	})
	if err != nil {
		t.Fatalf("BuildWorkUnitInvestments: %v", err)
	}
	if len(investments) != 1 {
		t.Fatalf("len(investments) = %d, want exactly 1 (wu-superseded must be excluded) -- got %+v", len(investments), investments)
	}
	if investments[0].WorkUnitID != "wu-kept" {
		t.Errorf("work_unit_id = %q, want \"wu-kept\" -- wu-superseded leaked through the supersession filter", investments[0].WorkUnitID)
	}
}

// TestWorkUnitsSeededRealClickHouse_QuotesDedupReturnsOneEntry is
// mechanism 3: two unmerged physical versions of the SAME
// (org_id, work_unit_id, source_id, quote) key in
// work_unit_investment_quotes must collapse to exactly one textual
// evidence entry, carrying the LATER version's source_type -- the
// dedup fix (FetchWorkUnitInvestmentQuotes' own GROUP BY + argMax,
// workunitreader.go).
func TestWorkUnitsSeededRealClickHouse_QuotesDedupReturnsOneEntry(t *testing.T) {
	conn, reader, cleanup := startSeededWorkUnitsClickHouse(t)
	defer cleanup()
	ctx := context.Background()

	// This unit's own categorization_run_id ("run-1") must match the quotes
	// rows' own categorization_run_id below: FetchWorkUnitInvestmentQuotes'
	// pairs filter is (work_unit_id, categorization_run_id), and
	// BuildWorkUnitInvestments only calls it when a row's own
	// CategorizationRunID is non-empty (workunitassembly.go's unitRuns
	// construction).
	insertWorkUnitInvestmentVersionWithRunID(t, ctx, conn, "wu-quotes", "Has quotes", "churn_loc", 4.0, "2026-01-06 00:00:00", "run-1")
	// Same (org_id, work_unit_id, source_id, quote) key on both inserts --
	// only source_type and computed_at differ, an unmerged physical
	// duplicate the ReplacingMergeTree engine has not yet collapsed.
	const quote = "Shipped the thing this sprint."
	for _, seed := range []struct {
		sourceType string
		computedAt string
	}{
		{"pr_body", "2026-01-06 00:00:00"},
		{"issue_desc", "2026-01-06 00:00:05"},
	} {
		stmt := fmt.Sprintf(
			`INSERT INTO work_unit_investment_quotes
				(work_unit_id, quote, source_type, source_id, computed_at, categorization_run_id, org_id)
			VALUES
				('wu-quotes', '%s', '%s', 'src-1', toDateTime64('%s',3), 'run-1', '%s')`,
			quote, seed.sourceType, seed.computedAt, workUnitsSeededOrgID,
		)
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed work_unit_investment_quotes (%s): %v", seed.sourceType, err)
		}
	}

	investments, err := reader.BuildWorkUnitInvestments(ctx, investmentexplain.BuildWorkUnitInvestmentsOptions{
		OrgID:       workUnitsSeededOrgID,
		StartTS:     time.Date(2025, 12, 25, 0, 0, 0, 0, time.UTC),
		EndTS:       time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC),
		Limit:       200,
		IncludeText: true,
	})
	if err != nil {
		t.Fatalf("BuildWorkUnitInvestments: %v", err)
	}
	if len(investments) != 1 {
		t.Fatalf("len(investments) = %d, want exactly 1 -- got %+v", len(investments), investments)
	}
	textual := investments[0].Evidence.Textual
	if len(textual) != 1 {
		t.Fatalf("len(evidence.textual) = %d, want exactly 1 (the duplicate physical quote row must collapse) -- got %+v", len(textual), textual)
	}
	if got := textual[0]["source"]; got != "issue_desc" {
		t.Errorf("evidence.textual[0].source = %v, want \"issue_desc\" (the later computed_at version) -- a stale/blended answer here is the raw-RMT-read defect this route declares against the Python plane", got)
	}
}

// TestWorkUnitsSeededRealClickHouse_TeamScopeFiltersByOwnedRepoID is
// mechanism 4: the team-scope branch this route's own "team_scoped" corpus
// request exercises, run for real against team_repo_ownership AND repos --
// teamscope.RepoCondition's own membership test, evaluated as part of
// FetchWorkUnitInvestments' own statement, never a separate round trip of
// its own.
func TestWorkUnitsSeededRealClickHouse_TeamScopeFiltersByOwnedRepoID(t *testing.T) {
	conn, reader, cleanup := startSeededWorkUnitsClickHouse(t)
	defer cleanup()
	ctx := context.Background()

	const memberRepoID = "11111111-1111-1111-1111-111111111111"
	const otherRepoID = "22222222-2222-2222-2222-222222222222"
	computedAt := "2026-01-06 00:00:00"

	if err := conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
		VALUES ('%s', 'acme/member-repo', toDateTime64('%s',3), toDateTime64('%s',3), '%s', 'github')`,
		memberRepoID, computedAt, computedAt, workUnitsSeededOrgID,
	)); err != nil {
		t.Fatalf("seed repos: %v", err)
	}
	if err := conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type,
			source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES ('%s', 'github', 'team-1', toUUID('%s'), 'acme/member-repo', 'exact', 'inferred',
			0, 0, 0, toDateTime64('%s',3), NULL, toDateTime64('%s',3))`,
		workUnitsSeededOrgID, memberRepoID, computedAt, computedAt,
	)); err != nil {
		t.Fatalf("seed team_repo_ownership: %v", err)
	}

	fromTS := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	toTS := time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC)
	for _, seed := range []struct {
		unitID string
		repoID string
	}{
		{"wu-member-repo", memberRepoID},
		{"wu-other-repo", otherRepoID},
	} {
		stmt := fmt.Sprintf(
			`INSERT INTO work_unit_investments
				(work_unit_id, work_unit_type, work_unit_name, from_ts, to_ts, repo_id, provider,
				 effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json,
				 structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status,
				 categorization_errors_json, categorization_model_version, categorization_input_hash,
				 categorization_run_id, computed_at, org_id)
			VALUES
				('%s', 'issue', 'Team scope fixture', toDateTime64('%s',3), toDateTime64('%s',3),
				 '%s', 'github', 'churn_loc', 1.0, map(), map(), '{}',
				 0.5, 'moderate', 'ok', '', 'v1', 'hash', '', toDateTime64('%s',3), '%s')`,
			seed.unitID, fromTS.Format("2006-01-02 15:04:05"), toTS.Format("2006-01-02 15:04:05"),
			seed.repoID, computedAt, workUnitsSeededOrgID,
		)
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed work_unit_investments (%s): %v", seed.unitID, err)
		}
	}

	repoIDs, err := reader.ResolveRepoFilterIDs(ctx, "team", []string{"team-1"}, nil, workUnitsSeededOrgID)
	if err != nil {
		t.Fatalf("ResolveRepoFilterIDs: %v", err)
	}
	if len(repoIDs) != 0 {
		t.Fatalf("ResolveRepoFilterIDs(team=team-1) = %v, want none -- this function never materializes team-scope membership; teamscope.RepoCondition carries it as a pushed-down condition instead", repoIDs)
	}
	teamCondition, teamBindings := teamscope.RepoCondition(workUnitsSeededOrgID, "work_unit_investments.repo_id", []string{"team-1"}, workUnitsTeamScopeAsOf)

	investments, err := reader.BuildWorkUnitInvestments(ctx, investmentexplain.BuildWorkUnitInvestmentsOptions{
		OrgID:              workUnitsSeededOrgID,
		StartTS:            fromTS,
		EndTS:              toTS,
		RepoIDs:            repoIDs,
		TeamScopeCondition: teamCondition,
		TeamScopeBindings:  teamBindings,
		Limit:              200,
	})
	if err != nil {
		t.Fatalf("BuildWorkUnitInvestments: %v", err)
	}
	if len(investments) != 1 || investments[0].WorkUnitID != "wu-member-repo" {
		t.Fatalf("investments = %+v, want exactly [wu-member-repo] (team scope must resolve via team_repo_ownership+repos and filter FetchWorkUnitInvestments' own repo_id predicate)", investments)
	}
}

// TestWorkUnitsSeededRealClickHouse_RepoScopeFiltersByRepoID is mechanism
// 5: scope.level="repo" resolves directly through repos FINAL
// (resolveRepoID), never previously executed against a real repos table
// by any seeded test in this package.
func TestWorkUnitsSeededRealClickHouse_RepoScopeFiltersByRepoID(t *testing.T) {
	conn, reader, cleanup := startSeededWorkUnitsClickHouse(t)
	defer cleanup()
	ctx := context.Background()

	const memberRepoID = "33333333-3333-3333-3333-333333333333"
	const otherRepoID = "44444444-4444-4444-4444-444444444444"
	computedAt := "2026-01-06 00:00:00"

	if err := conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
		VALUES ('%s', 'acme/scoped-repo', toDateTime64('%s',3), toDateTime64('%s',3), '%s', 'github')`,
		memberRepoID, computedAt, computedAt, workUnitsSeededOrgID,
	)); err != nil {
		t.Fatalf("seed repos: %v", err)
	}

	fromTS := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	toTS := time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC)
	for _, seed := range []struct {
		unitID string
		repoID string
	}{
		{"wu-repo-scoped", memberRepoID},
		{"wu-repo-other", otherRepoID},
	} {
		stmt := fmt.Sprintf(
			`INSERT INTO work_unit_investments
				(work_unit_id, work_unit_type, work_unit_name, from_ts, to_ts, repo_id, provider,
				 effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json,
				 structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status,
				 categorization_errors_json, categorization_model_version, categorization_input_hash,
				 categorization_run_id, computed_at, org_id)
			VALUES
				('%s', 'issue', 'Repo scope fixture', toDateTime64('%s',3), toDateTime64('%s',3),
				 '%s', 'github', 'churn_loc', 1.0, map(), map(), '{}',
				 0.5, 'moderate', 'ok', '', 'v1', 'hash', '', toDateTime64('%s',3), '%s')`,
			seed.unitID, fromTS.Format("2006-01-02 15:04:05"), toTS.Format("2006-01-02 15:04:05"),
			seed.repoID, computedAt, workUnitsSeededOrgID,
		)
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed work_unit_investments (%s): %v", seed.unitID, err)
		}
	}

	repoIDs, err := reader.ResolveRepoFilterIDs(ctx, "repo", []string{memberRepoID}, nil, workUnitsSeededOrgID)
	if err != nil {
		t.Fatalf("ResolveRepoFilterIDs: %v", err)
	}
	if len(repoIDs) != 1 || repoIDs[0] != memberRepoID {
		t.Fatalf("ResolveRepoFilterIDs(repo=%s) = %v, want exactly [%s]", memberRepoID, repoIDs, memberRepoID)
	}

	investments, err := reader.BuildWorkUnitInvestments(ctx, investmentexplain.BuildWorkUnitInvestmentsOptions{
		OrgID:   workUnitsSeededOrgID,
		StartTS: fromTS,
		EndTS:   toTS,
		RepoIDs: repoIDs,
		Limit:   200,
	})
	if err != nil {
		t.Fatalf("BuildWorkUnitInvestments: %v", err)
	}
	if len(investments) != 1 || investments[0].WorkUnitID != "wu-repo-scoped" {
		t.Fatalf("investments = %+v, want exactly [wu-repo-scoped]", investments)
	}
	// FetchRepoScopes' own repos FINAL lookup, reached because this
	// result row carries a non-null repo_id -- unexercised by every other
	// seeded test in this file (their fixture rows all leave repo_id
	// NULL).
	if got := investments[0].Evidence.Contextual; len(got) < 2 {
		t.Fatalf("evidence.contextual = %+v, want at least [time_range, repo_scope]", got)
	} else if repoScope, _ := got[1]["repo_ids"].([]string); len(repoScope) != 1 || repoScope[0] != "acme/scoped-repo" {
		t.Errorf("evidence.contextual[1] (repo_scope) = %+v, want repo_ids=[\"acme/scoped-repo\"] (FetchRepoScopes' own repos FINAL lookup)", got[1])
	}
}

// TestWorkUnitsSeededRealClickHouse_WorkCategoryFilterNarrowsRealRows is
// mechanism 6: the why.work_category filter, run against TWO real rows
// fetched from ClickHouse (not a hand-built fixture) to prove the
// post-fetch narrowing (_matches_category_filter/matchesCategoryFilter)
// actually excludes the non-matching row's real Map(String,Float64)
// column value.
func TestWorkUnitsSeededRealClickHouse_WorkCategoryFilterNarrowsRealRows(t *testing.T) {
	conn, reader, cleanup := startSeededWorkUnitsClickHouse(t)
	defer cleanup()
	ctx := context.Background()

	fromTS := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	toTS := time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC)
	computedAt := "2026-01-06 00:00:00"
	for _, seed := range []struct {
		unitID string
		themes string
	}{
		{"wu-matches", "map('feature_delivery', 1.0)"},
		{"wu-no-match", "map('maintenance', 1.0)"},
	} {
		stmt := fmt.Sprintf(
			`INSERT INTO work_unit_investments
				(work_unit_id, work_unit_type, work_unit_name, from_ts, to_ts, repo_id, provider,
				 effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json,
				 structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status,
				 categorization_errors_json, categorization_model_version, categorization_input_hash,
				 categorization_run_id, computed_at, org_id)
			VALUES
				('%s', 'issue', 'Category filter fixture', toDateTime64('%s',3), toDateTime64('%s',3),
				 NULL, NULL, 'churn_loc', 1.0, %s, map(), '{}',
				 0.5, 'moderate', 'ok', '', 'v1', 'hash', '', toDateTime64('%s',3), '%s')`,
			seed.unitID, fromTS.Format("2006-01-02 15:04:05"), toTS.Format("2006-01-02 15:04:05"),
			seed.themes, computedAt, workUnitsSeededOrgID,
		)
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed work_unit_investments (%s): %v", seed.unitID, err)
		}
	}

	themeFilters, subcategoryFilters := investmentexplain.SplitCategoryFilters([]string{"feature_delivery"})

	investments, err := reader.BuildWorkUnitInvestments(ctx, investmentexplain.BuildWorkUnitInvestmentsOptions{
		OrgID:              workUnitsSeededOrgID,
		StartTS:            fromTS,
		EndTS:              toTS,
		Limit:              200,
		ThemeFilters:       themeFilters,
		SubcategoryFilters: subcategoryFilters,
	})
	if err != nil {
		t.Fatalf("BuildWorkUnitInvestments: %v", err)
	}
	if len(investments) != 1 || investments[0].WorkUnitID != "wu-matches" {
		t.Fatalf("investments = %+v, want exactly [wu-matches] -- the real Map column's own content must drive the filter, not a fixture double", investments)
	}
}

// TestWorkUnitsSeededRealClickHouse_MembershipScopeEnabledFiltersNonMembers
// is mechanism 7: every other seeded test in this file leaves
// work_unit_membership_runs EMPTY, so the membership gate reads every work
// unit (no complete run). This test publishes one completed run and one
// membership row for a single work unit, with a SECOND work unit's own
// investment row left out of that run's membership set entirely, and
// proves the second is excluded. It then writes a newer investment row for
// the second unit (the materializer running ahead of the next marker) and
// proves the read stays on the published run.
func TestWorkUnitsSeededRealClickHouse_MembershipScopeEnabledFiltersNonMembers(t *testing.T) {
	conn, reader, cleanup := startSeededWorkUnitsClickHouse(t)
	defer cleanup()
	ctx := context.Background()

	const runID = "run-scope-enabled-1"
	const investmentComputedAt = "2026-01-05 00:00:00" // strictly before the run's own completed_at
	const runCompletedAt = "2026-01-06 00:00:00"

	fromTS := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	toTS := time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC)
	for _, unitID := range []string{"wu-in-scope", "wu-not-in-scope"} {
		stmt := fmt.Sprintf(
			`INSERT INTO work_unit_investments
				(work_unit_id, work_unit_type, work_unit_name, from_ts, to_ts, repo_id, provider,
				 effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json,
				 structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status,
				 categorization_errors_json, categorization_model_version, categorization_input_hash,
				 categorization_run_id, computed_at, org_id)
			VALUES
				('%s', 'issue', 'Membership scope fixture', toDateTime64('%s',3), toDateTime64('%s',3),
				 NULL, NULL, 'churn_loc', 1.0, map(), map(), '{}',
				 0.5, 'moderate', 'ok', '', 'v1', 'hash', '', toDateTime64('%s',3), '%s')`,
			unitID, fromTS.Format("2006-01-02 15:04:05"), toTS.Format("2006-01-02 15:04:05"),
			investmentComputedAt, workUnitsSeededOrgID,
		)
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed work_unit_investments (%s): %v", unitID, err)
		}
	}

	if err := conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO work_unit_membership_runs (org_id, run_id, completed_at)
		VALUES ('%s', '%s', toDateTime64('%s',3))`,
		workUnitsSeededOrgID, runID, runCompletedAt,
	)); err != nil {
		t.Fatalf("seed work_unit_membership_runs: %v", err)
	}
	if err := conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO work_unit_membership
			(org_id, node_type, node_id, work_unit_id, category_kind, category, weight, is_dominant,
			 categorization_status, computed_at, run_id)
		VALUES ('%s', 'issue', 'node-1', 'wu-in-scope', 'theme', 'feature_delivery', 1.0, 1, 'ok',
			toDateTime64('%s',3), '%s')`,
		workUnitsSeededOrgID, runCompletedAt, runID,
	)); err != nil {
		t.Fatalf("seed work_unit_membership: %v", err)
	}

	investments, err := reader.BuildWorkUnitInvestments(ctx, investmentexplain.BuildWorkUnitInvestmentsOptions{
		OrgID:   workUnitsSeededOrgID,
		StartTS: fromTS,
		EndTS:   toTS,
		Limit:   200,
	})
	if err != nil {
		t.Fatalf("BuildWorkUnitInvestments: %v", err)
	}
	if len(investments) != 1 || investments[0].WorkUnitID != "wu-in-scope" {
		t.Fatalf("investments = %+v, want exactly [wu-in-scope] -- with a completed membership run published, the gate must exclude wu-not-in-scope from membership_scoped_work_unit_ids", investments)
	}

	// Projection lag: a newer investment row lands before the next marker.
	if err := conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO work_unit_investments
			(work_unit_id, work_unit_type, work_unit_name, from_ts, to_ts, repo_id, provider,
			 effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json,
			 structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status,
			 categorization_errors_json, categorization_model_version, categorization_input_hash,
			 categorization_run_id, computed_at, org_id)
		VALUES
			('wu-not-in-scope', 'issue', 'Membership scope fixture', toDateTime64('%s',3), toDateTime64('%s',3),
			 NULL, NULL, 'churn_loc', 2.0, map(), map(), '{}',
			 0.5, 'moderate', 'ok', '', 'v1', 'hash', '', toDateTime64('2026-01-06 00:00:07',3), '%s')`,
		fromTS.Format("2006-01-02 15:04:05"), toTS.Format("2006-01-02 15:04:05"), workUnitsSeededOrgID,
	)); err != nil {
		t.Fatalf("seed lagging work_unit_investments row: %v", err)
	}
	lagging, err := reader.BuildWorkUnitInvestments(analytics.WithInvestmentMembershipScopeRequest(ctx), investmentexplain.BuildWorkUnitInvestmentsOptions{
		OrgID:   workUnitsSeededOrgID,
		StartTS: fromTS,
		EndTS:   toTS,
		Limit:   200,
	})
	if err != nil {
		t.Fatalf("BuildWorkUnitInvestments (projection lag): %v", err)
	}
	if len(lagging) != 1 || lagging[0].WorkUnitID != "wu-in-scope" {
		t.Fatalf("investments during projection lag = %+v, want exactly [wu-in-scope] -- reads stay on the latest complete run", lagging)
	}
}
