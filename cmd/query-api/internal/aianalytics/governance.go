package aianalytics

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

const coverageStatement = `SELECT
    team_id,
    toString(repo_id) AS repo_id_str,
    day,
    argMax(ai_artifacts, coverage.computed_at) AS ai_artifacts,
    argMax(declared_artifacts, coverage.computed_at) AS declared_artifacts,
    argMax(human_reviewed_prs, coverage.computed_at) AS human_reviewed_prs,
    argMax(security_scanned_prs, coverage.computed_at) AS security_scanned_prs,
    argMax(in_policy_artifacts, coverage.computed_at) AS in_policy_artifacts
FROM ai_governance_coverage_daily AS coverage
WHERE org_id = {org_id:String}
  AND day >= {start_day:Date}
  AND day <= {end_day:Date}
  AND ({team_id:String} = '' OR team_id = {team_id:String})
  AND ({repo_id:String} = '' OR toString(repo_id) = {repo_id:String})
GROUP BY org_id, team_id, repo_id, day
ORDER BY day, team_id, repo_id`

const violationsStatement = `SELECT
    team_id,
    toString(repo_id) AS repo_id_str,
    rule_id,
    severity,
    subject_type,
    subject_id,
    observed_at,
    evidence
FROM ai_policy_events FINAL
WHERE org_id = {org_id:String}
  AND toDate(observed_at) >= {start_day:Date}
  AND toDate(observed_at) <= {end_day:Date}
  AND ({team_id:String} = '' OR team_id = {team_id:String})
  AND ({repo_id:String} = '' OR toString(repo_id) = {repo_id:String})
ORDER BY observed_at DESC
LIMIT {limit:UInt32}`

func governanceBindings(orgID string, dr model.AIDateRangeInput, sc scope) []clickhouse.Binding {
	return []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "start_day", Value: dateValue(dr.StartDate.Time())},
		{Name: "end_day", Value: dateValue(dr.EndDate.Time())},
		{Name: "team_id", Value: sc.teamID},
		{Name: "repo_id", Value: sc.repoID},
	}
}

// optionalText is a nullable text column read as absent when null or empty.
func optionalText(v *string) *string {
	if v == nil || *v == "" {
		return nil
	}
	return v
}

// coverageRatio is numerator/denominator, correctly rounded for any pair of
// counts (integer true division does not round its operands first); a zero
// denominator means nothing was expected, which counts as fully covered.
func coverageRatio(numerator, denominator uint64) float64 {
	if denominator == 0 {
		return 1.0
	}
	quotient := new(big.Rat).SetFrac(new(big.Int).SetUint64(numerator), new(big.Int).SetUint64(denominator))
	out, _ := quotient.Float64()
	return out
}

func loadCoverage(ctx context.Context, client QueryClient, orgID string, dr model.AIDateRangeInput, sc scope) ([]model.AIGovernanceCoverageRow, error) {
	rs, err := client.Query(ctx, coverageStatement, governanceBindings(orgID, dr, sc))
	if err != nil {
		return nil, fmt.Errorf("aianalytics: coverage query: %w", err)
	}
	defer rs.Close()
	out := []model.AIGovernanceCoverageRow{}
	for rs.Next() {
		var (
			team, repo                      *string
			day                             time.Time
			ai, declared, reviewed, scanned uint64
			inPolicy                        uint64
		)
		if err := rs.Scan(&team, &repo, &day, &ai, &declared, &reviewed, &scanned, &inPolicy); err != nil {
			return nil, fmt.Errorf("aianalytics: coverage scan: %w", err)
		}
		out = append(out, model.AIGovernanceCoverageRow{
			Day:                  graphqldate.New(day),
			TeamID:               optionalText(team),
			RepoID:               optionalText(repo),
			AiArtifacts:          int(ai),
			DeclaredArtifacts:    int(declared),
			HumanReviewedPrs:     int(reviewed),
			SecurityScannedPrs:   int(scanned),
			InPolicyArtifacts:    int(inPolicy),
			DeclarationCoverage:  coverageRatio(declared, ai),
			HumanReviewCoverage:  coverageRatio(reviewed, ai),
			SecurityScanCoverage: coverageRatio(scanned, ai),
			InPolicyCoverage:     coverageRatio(inPolicy, ai),
		})
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: coverage rows: %w", err)
	}
	return out, nil
}

func loadViolations(ctx context.Context, client QueryClient, orgID string, dr model.AIDateRangeInput, sc scope, limit int) ([]model.AIGovernanceViolationRow, error) {
	if limit < 0 {
		limit = 0
	}
	bindings := append(governanceBindings(orgID, dr, sc), clickhouse.Binding{Name: "limit", Value: uint32(limit)})
	rs, err := client.Query(ctx, violationsStatement, bindings)
	if err != nil {
		return nil, fmt.Errorf("aianalytics: violations query: %w", err)
	}
	defer rs.Close()
	out := []model.AIGovernanceViolationRow{}
	for rs.Next() {
		var (
			team, repo                             *string
			rule, severity, subjectType, subjectID string
			observed                               time.Time
			evidence                               string
		)
		if err := rs.Scan(&team, &repo, &rule, &severity, &subjectType, &subjectID, &observed, &evidence); err != nil {
			return nil, fmt.Errorf("aianalytics: violations scan: %w", err)
		}
		if evidence == "" {
			evidence = "{}"
		}
		out = append(out, model.AIGovernanceViolationRow{
			RuleID: rule, Severity: severity, SubjectType: subjectType, SubjectID: subjectID,
			TeamID: optionalText(team), RepoID: optionalText(repo),
			ObservedAt: observed.UTC(), Evidence: evidence,
		})
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("aianalytics: violations rows: %w", err)
	}
	return out, nil
}

// GovernanceSummary answers aiGovernanceSummary.
func GovernanceSummary(ctx context.Context, client QueryClient, orgID string, dr model.AIDateRangeInput, in *model.AIScopeInput, violationLimit int) (*model.AIGovernanceSummary, error) {
	if client == nil {
		return nil, fmt.Errorf("aianalytics: clickhouse client is required")
	}
	if err := validateRange(dr); err != nil {
		return nil, err
	}
	sc, err := normalizeScope(ctx, client, orgID, in)
	if err != nil {
		return nil, err
	}
	if sc.unresolved {
		return &model.AIGovernanceSummary{
			OrgID: orgID, StartDate: dr.StartDate, EndDate: dr.EndDate,
			Coverage: []model.AIGovernanceCoverageRow{}, RecentViolations: []model.AIGovernanceViolationRow{},
		}, nil
	}
	coverage, err := loadCoverage(ctx, client, orgID, dr, sc)
	if err != nil {
		return nil, err
	}
	violations, err := loadViolations(ctx, client, orgID, dr, sc, violationLimit)
	if err != nil {
		return nil, err
	}
	return &model.AIGovernanceSummary{
		OrgID: orgID, StartDate: dr.StartDate, EndDate: dr.EndDate,
		Coverage: coverage, RecentViolations: violations,
		DataAvailable: len(coverage) > 0 || len(violations) > 0,
	}, nil
}
