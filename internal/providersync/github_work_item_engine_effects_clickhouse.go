package providersync

import (
	"context"
	"math"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// These three tables are plain MergeTree tables, so FINAL does not deduplicate
// them. Issue-type metrics and classifications have no production latest-row
// reader contract and therefore accept only one unambiguous full row (identical
// retries may collapse). Investment metrics follows its production argMax
// contract but rejects divergent rows tied at the newest computed_at.

type GitHubIssueTypeMetricsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type GitHubInvestmentClassificationsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type GitHubInvestmentMetricsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func githubWorkItemEngineNullableUUID(value *uuid.UUID) string {
	if value == nil {
		return ""
	}
	return value.String()
}

func githubIssueTypeMetricsSortingKey(row githubIssueTypeMetricsDailyRow) string {
	return strings.Join([]string{
		row.OrgID, githubWorkItemEngineNullableUUID(row.RepoID), string(row.Day),
		row.Provider, row.TeamID, row.IssueTypeNorm,
	}, "\x00")
}

func githubInvestmentClassificationSortingKey(
	row githubInvestmentClassificationDailyRow,
) string {
	return strings.Join([]string{
		row.OrgID, githubWorkItemEngineNullableUUID(row.RepoID), string(row.Day),
		row.Provider, row.ArtifactType,
		githubWorkItemDerivedNullableString(row.InvestmentArea), row.ProjectStream,
		row.ArtifactID,
	}, "\x00")
}

func githubInvestmentMetricsSortingKey(row githubInvestmentMetricsDailyRow) string {
	return strings.Join([]string{
		row.OrgID, githubWorkItemEngineNullableUUID(row.RepoID), string(row.Day), row.TeamID,
		githubWorkItemDerivedNullableString(row.InvestmentArea), row.ProjectStream,
	}, "\x00")
}

func githubWorkItemEngineUInt32(value int) bool {
	return value >= 0 && uint64(value) <= math.MaxUint32
}

func githubWorkItemEngineUInt64(value int) bool { return value >= 0 }

func validGitHubIssueTypeMetricsRow(
	identity GitHubWorkItemEffectIdentity,
	row githubIssueTypeMetricsDailyRow,
) bool {
	_, dayErr := row.Day.time()
	return row.OrgID == identity.OrgID && row.Provider == identity.Provider &&
		dayErr == nil && row.TeamID != "" && row.IssueTypeNorm != "" &&
		!row.ComputedAt.IsZero() &&
		githubWorkItemEngineUInt32(row.CreatedCount) &&
		githubWorkItemEngineUInt32(row.CompletedCount) &&
		githubWorkItemEngineUInt32(row.ActiveCount)
}

func validGitHubInvestmentClassificationRow(
	identity GitHubWorkItemEffectIdentity,
	row githubInvestmentClassificationDailyRow,
) bool {
	_, dayErr := row.Day.time()
	return row.OrgID == identity.OrgID && row.Provider == identity.Provider &&
		dayErr == nil && row.ArtifactType == githubWorkItemEngineArtifactType &&
		row.ArtifactID != "" && row.InvestmentArea != nil && row.RuleID != nil &&
		!row.ComputedAt.IsZero()
}

func validGitHubInvestmentMetricsRow(
	identity GitHubWorkItemEffectIdentity,
	row githubInvestmentMetricsDailyRow,
) bool {
	_, dayErr := row.Day.time()
	return row.OrgID == identity.OrgID && dayErr == nil &&
		row.InvestmentArea != nil && !row.ComputedAt.IsZero() &&
		githubWorkItemEngineUInt32(row.DeliveryUnits) &&
		githubWorkItemEngineUInt32(row.WorkItemsCompleted) &&
		githubWorkItemEngineUInt32(row.PRsMerged) &&
		githubWorkItemEngineUInt64(row.ChurnLOC)
}

func (sink GitHubIssueTypeMetricsClickHouseEffects) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	rows, err := validateGitHubWorkItemDerivedEffect[githubIssueTypeMetricsDailyRow](
		identity, effect, githubIssueTypeMetricsDestination,
	)
	if err != nil || ctx == nil || sink.Conn == nil || sink.Lease == nil {
		return ErrInvalidConfiguration
	}
	for _, row := range rows {
		if !validGitHubIssueTypeMetricsRow(identity, row) {
			return ErrInvalidConfiguration
		}
	}
	if !githubWorkItemDerivedUniqueSortingKeys(rows, githubIssueTypeMetricsSortingKey) {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO issue_type_metrics_daily
(repo_id, day, provider, team_id, issue_type_norm, created_count,
completed_count, active_count, cycle_p50_hours, cycle_p90_hours,
lead_p50_hours, computed_at, org_id)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		day, _ := row.Day.time()
		if err := batch.Append(
			row.RepoID, day, row.Provider, row.TeamID, row.IssueTypeNorm,
			uint32(row.CreatedCount), uint32(row.CompletedCount), uint32(row.ActiveCount),
			row.CycleP50Hours, row.CycleP90Hours, row.LeadP50Hours,
			githubWorkItemDerivedSeconds(row.ComputedAt), identity.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubIssueTypeMetricsClickHouseEffects) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	rows, err := validateGitHubWorkItemDerivedEffect[githubIssueTypeMetricsDailyRow](
		identity, effect, githubIssueTypeMetricsDestination,
	)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	for _, row := range rows {
		if !validGitHubIssueTypeMetricsRow(identity, row) {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	if !githubWorkItemDerivedUniqueSortingKeys(rows, githubIssueTypeMetricsSortingKey) {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return inspectGitHubWorkItemDerivedRows(rows, func(row githubIssueTypeMetricsDailyRow) (EffectInspection, error) {
		return sink.inspect(ctx, identity, row)
	})
}

func (sink GitHubIssueTypeMetricsClickHouseEffects) inspect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	expected githubIssueTypeMetricsDailyRow,
) (EffectInspection, error) {
	day, _ := expected.Day.time()
	query := `SELECT repo_id, created_count, completed_count, active_count,
cycle_p50_hours, cycle_p90_hours, lead_p50_hours, computed_at
FROM issue_type_metrics_daily
WHERE org_id = ? AND day = ? AND provider = ? AND team_id = ?
	AND issue_type_norm = ?`
	arguments := []any{
		identity.OrgID, day, expected.Provider, expected.TeamID, expected.IssueTypeNorm,
	}
	if expected.RepoID == nil {
		query += ` AND repo_id IS NULL`
	} else {
		query += ` AND repo_id = ?`
		arguments = append(arguments, *expected.RepoID)
	}
	// There is no production latest-row reader contract for this table. Read
	// every DISTINCT full row for the logical identity. GROUP BY collapses
	// byte-identical replay duplicates while retaining ANY historical
	// divergence; without a reader contract, even an older competing row is
	// ambiguous and must fail closed.
	query += ` GROUP BY repo_id, created_count, completed_count, active_count,
cycle_p50_hours, cycle_p90_hours, lead_p50_hours, computed_at`
	scan, err := sink.Conn.Query(ctx, query, arguments...)
	if err != nil {
		return EffectConflict, err
	}
	defer scan.Close()
	var actual githubIssueTypeMetricsDailyRow
	distinctRows := 0
	for scan.Next() {
		candidate := githubIssueTypeMetricsDailyRow{
			Day: expected.Day, Provider: expected.Provider, TeamID: expected.TeamID,
			IssueTypeNorm: expected.IssueTypeNorm, OrgID: identity.OrgID,
		}
		var created, completed, active uint32
		if err := scan.Scan(
			&candidate.RepoID, &created, &completed, &active,
			&candidate.CycleP50Hours, &candidate.CycleP90Hours, &candidate.LeadP50Hours,
			&candidate.ComputedAt,
		); err != nil {
			return EffectConflict, err
		}
		candidate.CreatedCount = int(created)
		candidate.CompletedCount = int(completed)
		candidate.ActiveCount = int(active)
		if distinctRows == 0 {
			actual = candidate
		}
		distinctRows++
	}
	if err := scan.Err(); err != nil {
		return EffectConflict, err
	}
	return compareGitHubIssueTypeMetricsVersion(
		expected, actual, distinctRows, identity.OrgID,
	), nil
}

func (sink GitHubInvestmentClassificationsClickHouseEffects) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	rows, err := validateGitHubWorkItemDerivedEffect[githubInvestmentClassificationDailyRow](
		identity, effect, githubInvestmentClassificationsDestination,
	)
	if err != nil || ctx == nil || sink.Conn == nil || sink.Lease == nil {
		return ErrInvalidConfiguration
	}
	for _, row := range rows {
		if !validGitHubInvestmentClassificationRow(identity, row) {
			return ErrInvalidConfiguration
		}
	}
	if !githubWorkItemDerivedUniqueSortingKeys(rows, githubInvestmentClassificationSortingKey) {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO investment_classifications_daily
(repo_id, day, artifact_type, artifact_id, provider, investment_area,
project_stream, confidence, rule_id, computed_at, org_id)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		day, _ := row.Day.time()
		if err := batch.Append(
			row.RepoID, day, row.ArtifactType, row.ArtifactID, row.Provider,
			*row.InvestmentArea, row.ProjectStream, row.Confidence, *row.RuleID,
			githubWorkItemDerivedSeconds(row.ComputedAt), identity.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubInvestmentClassificationsClickHouseEffects) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	rows, err := validateGitHubWorkItemDerivedEffect[githubInvestmentClassificationDailyRow](
		identity, effect, githubInvestmentClassificationsDestination,
	)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	for _, row := range rows {
		if !validGitHubInvestmentClassificationRow(identity, row) {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	if !githubWorkItemDerivedUniqueSortingKeys(rows, githubInvestmentClassificationSortingKey) {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return inspectGitHubWorkItemDerivedRows(rows, func(row githubInvestmentClassificationDailyRow) (EffectInspection, error) {
		return sink.inspect(ctx, identity, row)
	})
}

func (sink GitHubInvestmentClassificationsClickHouseEffects) inspect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	expected githubInvestmentClassificationDailyRow,
) (EffectInspection, error) {
	day, _ := expected.Day.time()
	query := `SELECT repo_id, confidence, rule_id, computed_at
FROM investment_classifications_daily
WHERE org_id = ? AND day = ? AND provider = ? AND artifact_type = ?
	AND investment_area = ? AND project_stream = ? AND artifact_id = ?`
	arguments := []any{
		identity.OrgID, day, expected.Provider, expected.ArtifactType,
		*expected.InvestmentArea, expected.ProjectStream, expected.ArtifactID,
	}
	if expected.RepoID == nil {
		query += ` AND repo_id IS NULL`
	} else {
		query += ` AND repo_id = ?`
		arguments = append(arguments, *expected.RepoID)
	}
	// No production reader defines either a latest-row rule or a tie-break for
	// this table. Collapse only identical full-row replays; any different row at
	// the same logical identity remains a conflict, even at an older version.
	query += ` GROUP BY repo_id, confidence, rule_id, computed_at`
	scan, err := sink.Conn.Query(ctx, query, arguments...)
	if err != nil {
		return EffectConflict, err
	}
	defer scan.Close()
	var actual githubInvestmentClassificationDailyRow
	distinctRows := 0
	for scan.Next() {
		candidate := githubInvestmentClassificationDailyRow{
			Day: expected.Day, ArtifactType: expected.ArtifactType,
			ArtifactID: expected.ArtifactID, Provider: expected.Provider,
			InvestmentArea: expected.InvestmentArea, ProjectStream: expected.ProjectStream,
			OrgID: identity.OrgID,
		}
		if err := scan.Scan(
			&candidate.RepoID, &candidate.Confidence, &candidate.RuleID, &candidate.ComputedAt,
		); err != nil {
			return EffectConflict, err
		}
		if distinctRows == 0 {
			actual = candidate
		}
		distinctRows++
	}
	if err := scan.Err(); err != nil {
		return EffectConflict, err
	}
	return compareGitHubInvestmentClassificationVersion(
		expected, actual, distinctRows, identity.OrgID,
	), nil
}

func (sink GitHubInvestmentMetricsClickHouseEffects) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	rows, err := validateGitHubWorkItemDerivedEffect[githubInvestmentMetricsDailyRow](
		identity, effect, githubInvestmentMetricsDestination,
	)
	if err != nil || ctx == nil || sink.Conn == nil || sink.Lease == nil {
		return ErrInvalidConfiguration
	}
	for _, row := range rows {
		if !validGitHubInvestmentMetricsRow(identity, row) {
			return ErrInvalidConfiguration
		}
	}
	if !githubWorkItemDerivedUniqueSortingKeys(rows, githubInvestmentMetricsSortingKey) {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO investment_metrics_daily
(repo_id, day, team_id, investment_area, project_stream, delivery_units,
work_items_completed, prs_merged, churn_loc, cycle_p50_hours, computed_at, org_id)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		day, _ := row.Day.time()
		if err := batch.Append(
			row.RepoID, day, row.TeamID, *row.InvestmentArea, row.ProjectStream,
			uint32(row.DeliveryUnits), uint32(row.WorkItemsCompleted), uint32(row.PRsMerged),
			uint64(row.ChurnLOC), row.CycleP50Hours,
			githubWorkItemDerivedSeconds(row.ComputedAt), identity.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubInvestmentMetricsClickHouseEffects) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	rows, err := validateGitHubWorkItemDerivedEffect[githubInvestmentMetricsDailyRow](
		identity, effect, githubInvestmentMetricsDestination,
	)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	for _, row := range rows {
		if !validGitHubInvestmentMetricsRow(identity, row) {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	if !githubWorkItemDerivedUniqueSortingKeys(rows, githubInvestmentMetricsSortingKey) {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return inspectGitHubWorkItemDerivedRows(rows, func(row githubInvestmentMetricsDailyRow) (EffectInspection, error) {
		return sink.inspect(ctx, identity, row)
	})
}

func (sink GitHubInvestmentMetricsClickHouseEffects) inspect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	expected githubInvestmentMetricsDailyRow,
) (EffectInspection, error) {
	day, _ := expected.Day.time()
	query := `SELECT repo_id, delivery_units, work_items_completed, prs_merged,
churn_loc, cycle_p50_hours, computed_at
FROM investment_metrics_daily
WHERE org_id = ? AND day = ? AND team_id = ?
	AND investment_area = ? AND project_stream = ?`
	arguments := []any{
		identity.OrgID, day, expected.TeamID, *expected.InvestmentArea,
		expected.ProjectStream,
	}
	if expected.RepoID == nil {
		query += ` AND repo_id IS NULL`
	} else {
		query += ` AND repo_id = ?`
		arguments = append(arguments, *expected.RepoID)
	}
	// Production readers use argMax for this rollup. That is deterministic only
	// when the newest full value tuple is unique, so readback makes that premise
	// explicit: identical retries collapse, but equal-time divergence remains
	// multiple groups and fails closed instead of accepting argMax's arbitrary
	// winner.
	query += ` GROUP BY repo_id, delivery_units, work_items_completed, prs_merged,
churn_loc, cycle_p50_hours, computed_at`
	scan, err := sink.Conn.Query(ctx, query, arguments...)
	if err != nil {
		return EffectConflict, err
	}
	defer scan.Close()
	var actual githubInvestmentMetricsDailyRow
	latestDistinct := 0
	for scan.Next() {
		candidate := githubInvestmentMetricsDailyRow{
			Day: expected.Day, TeamID: expected.TeamID,
			InvestmentArea: expected.InvestmentArea, ProjectStream: expected.ProjectStream,
			OrgID: identity.OrgID,
		}
		var delivery, completed, merged uint32
		var churn uint64
		if err := scan.Scan(
			&candidate.RepoID, &delivery, &completed, &merged, &churn,
			&candidate.CycleP50Hours, &candidate.ComputedAt,
		); err != nil {
			return EffectConflict, err
		}
		candidate.DeliveryUnits = int(delivery)
		candidate.WorkItemsCompleted = int(completed)
		candidate.PRsMerged = int(merged)
		if churn > math.MaxInt64 {
			return EffectConflict, ErrInvalidConfiguration
		}
		candidate.ChurnLOC = int(churn)
		switch {
		case latestDistinct == 0 || candidate.ComputedAt.After(actual.ComputedAt):
			actual = candidate
			latestDistinct = 1
		case candidate.ComputedAt.Equal(actual.ComputedAt):
			latestDistinct++
		}
	}
	if err := scan.Err(); err != nil {
		return EffectConflict, err
	}
	return compareGitHubInvestmentMetricsVersion(
		expected, actual, latestDistinct, identity.OrgID,
	), nil
}

func compareGitHubIssueTypeMetricsVersion(
	expected, actual githubIssueTypeMetricsDailyRow,
	distinctRows int,
	orgID string,
) EffectInspection {
	if distinctRows != 1 {
		if distinctRows == 0 {
			return EffectAbsent
		}
		return EffectConflict
	}
	if actual.OrgID != orgID {
		return EffectConflict
	}
	if !actual.ComputedAt.Equal(githubWorkItemDerivedSeconds(expected.ComputedAt)) {
		return EffectConflict
	}
	if actual.Day != expected.Day || actual.Provider != expected.Provider ||
		actual.TeamID != expected.TeamID || actual.IssueTypeNorm != expected.IssueTypeNorm ||
		!githubWorkItemEngineUUIDPointerEqual(actual.RepoID, expected.RepoID) ||
		actual.CreatedCount != expected.CreatedCount ||
		actual.CompletedCount != expected.CompletedCount ||
		actual.ActiveCount != expected.ActiveCount ||
		actual.CycleP50Hours != expected.CycleP50Hours ||
		actual.CycleP90Hours != expected.CycleP90Hours ||
		actual.LeadP50Hours != expected.LeadP50Hours {
		return EffectConflict
	}
	return EffectExact
}

func compareGitHubInvestmentClassificationVersion(
	expected, actual githubInvestmentClassificationDailyRow,
	distinctRows int,
	orgID string,
) EffectInspection {
	if distinctRows != 1 {
		if distinctRows == 0 {
			return EffectAbsent
		}
		return EffectConflict
	}
	if actual.OrgID != orgID {
		return EffectConflict
	}
	if !actual.ComputedAt.Equal(githubWorkItemDerivedSeconds(expected.ComputedAt)) {
		return EffectConflict
	}
	if actual.Day != expected.Day || actual.Provider != expected.Provider ||
		actual.ArtifactType != expected.ArtifactType ||
		actual.ArtifactID != expected.ArtifactID ||
		!githubWorkItemDerivedStringPointerEqual(
			actual.InvestmentArea, expected.InvestmentArea,
		) || actual.ProjectStream != expected.ProjectStream ||
		!githubWorkItemEngineUUIDPointerEqual(actual.RepoID, expected.RepoID) ||
		actual.Confidence != expected.Confidence ||
		!githubWorkItemDerivedStringPointerEqual(actual.RuleID, expected.RuleID) {
		return EffectConflict
	}
	return EffectExact
}

func compareGitHubInvestmentMetricsVersion(
	expected, actual githubInvestmentMetricsDailyRow,
	latestDistinct int,
	orgID string,
) EffectInspection {
	if latestDistinct != 1 {
		if latestDistinct == 0 {
			return EffectAbsent
		}
		return EffectConflict
	}
	if actual.OrgID != orgID {
		return EffectConflict
	}
	if verdict, decided := githubWorkItemDerivedVersionOrder(
		actual.ComputedAt, githubWorkItemDerivedSeconds(expected.ComputedAt),
	); decided {
		return verdict
	}
	if actual.Day != expected.Day || actual.TeamID != expected.TeamID ||
		!githubWorkItemDerivedStringPointerEqual(
			actual.InvestmentArea, expected.InvestmentArea,
		) || actual.ProjectStream != expected.ProjectStream ||
		!githubWorkItemEngineUUIDPointerEqual(actual.RepoID, expected.RepoID) ||
		actual.DeliveryUnits != expected.DeliveryUnits ||
		actual.WorkItemsCompleted != expected.WorkItemsCompleted ||
		actual.PRsMerged != expected.PRsMerged || actual.ChurnLOC != expected.ChurnLOC ||
		actual.CycleP50Hours != expected.CycleP50Hours {
		return EffectConflict
	}
	return EffectExact
}

func githubWorkItemEngineUUIDPointerEqual(left, right *uuid.UUID) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

var (
	_ GitHubWorkItemEffectAdapter = GitHubIssueTypeMetricsClickHouseEffects{}
	_ GitHubWorkItemEffectAdapter = GitHubInvestmentClassificationsClickHouseEffects{}
	_ GitHubWorkItemEffectAdapter = GitHubInvestmentMetricsClickHouseEffects{}
)
