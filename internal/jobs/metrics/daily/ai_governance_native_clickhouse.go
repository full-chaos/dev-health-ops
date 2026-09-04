package daily

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aigovernance"
)

// LoadGovernanceArtifacts ports _ARTIFACTS_SQL plus _artifact_from_row
// (audit/ai_governance/loaders.py:214 and :126) -- the input half of
// build_governance_rows_for_day.
//
// # Window
//
// Python bounds the scan with datetime.combine(day, time.min) and
// time.MAX -- i.e. an INCLUSIVE upper bound at 23:59:59.999999, not an
// exclusive next-midnight bound (loaders.py:41-42, `observed_at <= {end}`).
// Reproduced exactly rather than "cleaned up" to a half-open window: the
// column is DateTime64(3) so the two agree in practice, but a port that
// silently changed a boundary operator would be a behavioural assumption
// rather than a proven equivalence.
//
// # FINAL -> argMax, GROUPED BY THE CURRENT SORTING KEY (team-lead ruling, design.md Q4)
//
// Python's scan/finding subqueries read `... FINAL ... GROUP BY repo_id`, which
// is FINAL underneath an aggregation -- the shape the standing rule forbids,
// because it makes the answer depend on background-merge timing. Both are
// replaced with `argMax(<col>, last_synced) GROUP BY <the table's CURRENT
// sorting key>`, which is the FINAL winner merge-independently.
//
// # READ THE CURRENT KEY FROM THE REKEY MIGRATIONS, NOT FROM CREATE TABLE
//
// Every sorting key here starts with org_id:
//
//	git_pull_requests         (org_id, repo_id, number)
//	ci_pipeline_runs          (org_id, repo_id, run_id)
//	security_alerts           (org_id, repo_id, alert_id)
//
// from 027_add_org_id_to_sorting_keys.py:63-65 and
// 042_rmt_org_id_dedup_keys.py:93, which REBUILT these tables. The CREATE
// statements in 000_raw_tables.sql / 032_security_alerts.sql are stale for
// every one of them, and migration 024 -- which says org_id was added as a
// plain column NOT in any sorting key -- was true when written and is now
// false.
//
// This file's FIRST version grouped by (repo_id, number) / (repo_id, run_id) /
// (repo_id, alert_id) on the strength of that 024 note, and cited 024 in this
// comment, which made a stale claim look verified. The consequence was not
// cosmetic: two orgs legitimately share a repo_id, so an argMax that omits
// org_id selects the newest row ACROSS TENANTS. Org A's reviewed PR could be
// suppressed by org B's newer row, and Go would emit a spurious
// MISSING_HUMAN_REVIEW for A. Found by codex round 1 on #2229.
//
// Because org_id is IN the key, the org filter now sits BEFORE the dedup:
// rows of other tenants form different groups, so filtering early is the same
// answer and prunes the scan instead of reading every tenant's history. The
// earlier "carried through the dedup and filtered AFTER it" design was a
// correct consequence of a false premise.
//
// RULE for anyone adding a reader here: derive the GROUP BY from 027/042's key
// maps, never from a CREATE TABLE or a migration's prose. A migration comment
// is a snapshot of the day it was written.
//
// # ONE argMax OVER A TUPLE WHEN MORE THAN ONE NON-KEY COLUMN
//
// Two aggregates over the same GROUP BY resolve ties INDEPENDENTLY: on a shared
// last_synced, one argMax can take its value from row A while another takes its
// value from row B, emitting a row that never existed. Where a dedup projects
// more than one non-key column, take a SINGLE argMax over a tuple and unwrap it
// with tupleElement (the shape discover_repos adopted for CHAOS-2787,
// job_daily.py:176-189). The tuple also preserves NULLs, which a bare argMax
// over a Nullable column silently skips.
//
// After the rekey above, each dedup in THIS file projects exactly one non-key
// column (org_id having moved into the group), so a single-column argMax is
// correct and no tuple is needed -- the defect requires two aggregates. The
// same is true of the two ai_tool_allowlist subqueries further down. Do not
// "consistency-fix" any of them into tuples; do reach for the tuple the moment
// a second non-key column is projected.
//
// Pinned by TestGovernanceDedupPicksOneWholeRowOnALastSyncedTie (tuple
// coherence) and TestGovernanceDedupIsTenantScoped (the org_id key).
//
// # git_pull_requests dedup: A DELIBERATE DIVERGENCE (Finding A)
//
// Python LEFT JOINs git_pull_requests with NEITHER FINAL NOR any dedup
// (loaders.py:241-245). That table is ReplacingMergeTree(last_synced) ORDER
// BY (repo_id, number) -- and, again, org_id is not in the sorting key -- so
// every un-merged duplicate PR row MULTIPLIES the artifact it joins to. The
// duplicates inflate ai_artifacts/declared_artifacts/human_reviewed_prs/
// security_scanned_prs/in_policy_artifacts and emit duplicate policy events,
// by an amount that depends on when ClickHouse last merged the part. Python's
// output for a fixed input is therefore NOT deterministic.
//
// This loader dedups the join to one row per (repo_id, number). The Go path
// becomes deterministic; Python's does not, so the two disagree exactly when
// duplicates are present and agree otherwise. Same port-with-fix shape as
// LoadIncidentsStarted's CHAOS-4269 guard. The live-Python oracle runs on a
// duplicate-free fixture (where the comparison is a real bit-exact parity
// proof); the integration test carries the duplicate fixture and asserts this
// path's determinism. See this PR's RISK-NOTES.
func LoadGovernanceArtifacts(
	ctx context.Context, conn repositoryRows, organizationID string,
	windowStart, windowEndInclusive time.Time,
) ([]aigovernance.Artifact, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || windowEndInclusive.Before(windowStart) {
		return nil, ErrInvalidState
	}

	rows, err := conn.Query(ctx, `
SELECT
    toString(a.org_id) AS org_id,
    a.repo_id AS repo_id,
    a.subject_type AS subject_type,
    a.subject_id AS subject_id,
    a.observed_at AS observed_at,
    a.source IN ('manual', 'pr_label', 'commit_trailer') AS declared_ai,
    if(a.subject_type = 'pull_request', pr.reviews_count > 0, NULL) AS human_reviewed,
    if(a.subject_type = 'pull_request', scan.scan_count > 0, NULL) AS security_scanned,
    finding.finding_count > 0 AS license_or_dependency_finding,
    JSONExtractString(a.evidence, 'tool_name') AS tool_name,
    JSONExtractString(a.evidence, 'model_name') AS model_name,
    multiIf(
        allow_exact.status != '', allow_exact.status,
        allow_wild.status != '', allow_wild.status,
        'unknown'
    ) AS tool_allowlist_status,
    a.source AS source,
    a.kind AS kind,
    a.confidence AS confidence
FROM ai_attribution_resolved AS a
-- Deduped to the ReplacingMergeTree ORDER BY key (repo_id, number). Python
-- omits this entirely; see the doc comment's "Finding A" section.
LEFT JOIN (
    SELECT
        repo_id,
        number,
        argMax(reviews_count, last_synced) AS reviews_count
    FROM git_pull_requests
    WHERE org_id = ?
    GROUP BY org_id, repo_id, number
) AS pr
    ON a.repo_id = pr.repo_id
    AND a.subject_type = 'pull_request'
    AND a.subject_id = toString(pr.number)
LEFT JOIN (
    SELECT repo_id, count() AS scan_count
    FROM (
        SELECT
            repo_id,
            argMax(status, last_synced) AS status
        FROM ci_pipeline_runs
        WHERE org_id = ?
        GROUP BY org_id, repo_id, run_id
    )
    WHERE lower(coalesce(status, '')) IN ('success', 'passed', 'completed')
    GROUP BY repo_id
) AS scan ON a.repo_id = scan.repo_id
LEFT JOIN (
    SELECT repo_id, count() AS finding_count
    FROM (
        SELECT
            repo_id,
            argMax(source, last_synced) AS source
        FROM security_alerts
        WHERE org_id = ?
        GROUP BY org_id, repo_id, alert_id
    )
    WHERE lower(coalesce(source, '')) IN ('dependabot', 'gitlab_dependency', 'dependency_scanning')
    GROUP BY repo_id
) AS finding ON a.repo_id = finding.repo_id
-- Allowlist precedence (CHAOS-2209), ported verbatim: an exact tool+model row
-- beats a wildcard row, each side already deduped by argMax over computed_at.
-- Wildcard means nullIf(model_name, '') IS NULL -- legacy '' rows are
-- wildcard, never exact, because JSONExtractString yields '' for missing model
-- evidence and a '' "exact" key would phantom-match every artifact lacking it.
LEFT JOIN (
    SELECT
        org_id,
        tool_name,
        model_name AS model_key,
        argMax(status, computed_at) AS status
    FROM ai_tool_allowlist
    WHERE nullIf(model_name, '') IS NOT NULL
    GROUP BY org_id, tool_name, model_key
) AS allow_exact
    ON toString(a.org_id) = allow_exact.org_id
    AND JSONExtractString(a.evidence, 'tool_name') = allow_exact.tool_name
    AND JSONExtractString(a.evidence, 'model_name') = allow_exact.model_key
LEFT JOIN (
    SELECT
        org_id,
        tool_name,
        argMax(status, computed_at) AS status
    FROM ai_tool_allowlist
    WHERE nullIf(model_name, '') IS NULL
    GROUP BY org_id, tool_name
) AS allow_wild
    ON toString(a.org_id) = allow_wild.org_id
    AND JSONExtractString(a.evidence, 'tool_name') = allow_wild.tool_name
WHERE toString(a.org_id) = ?
  AND a.observed_at >= ?
  AND a.observed_at <= ?
ORDER BY a.subject_type, a.subject_id, a.source`,
		organizationID, organizationID, organizationID,
		organizationID, windowStart.UTC(), windowEndInclusive.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("load ai governance artifacts: %w", err)
	}
	defer rows.Close()

	var artifacts []aigovernance.Artifact
	for rows.Next() {
		var (
			orgID       string
			repoID      *uuid.UUID
			subjectType string
			subjectID   string
			observedAt  time.Time
			// BOOLEAN COLUMNS ARE UInt8 ON THE WIRE, NOT Bool.
			//
			// ClickHouse comparison and IN operators yield UInt8, so
			// `a.source IN (...)` and `finding.finding_count > 0` arrive as
			// UInt8, and `if(cond, x, NULL)` arrives as Nullable(UInt8). The
			// driver matches the DECLARED column type, so a *bool target (and
			// therefore a **bool argument to Scan) is rejected outright --
			// which is what CI's go-storage-integration shard caught on the
			// first version of this file, exactly the driver-type class this
			// package's integration test exists to find and that no unit test
			// with a fake scanner can.
			//
			// Scanned at the wire type and converted below rather than asking
			// the driver to coerce: the two `if(...)` projections are genuinely
			// nullable (a non-pull_request subject yields NULL, which is
			// Optional[bool] in Python and must stay distinct from false), and
			// the two bare comparisons are not.
			declaredAI          uint8
			humanReviewed       *uint8
			securityScanned     *uint8
			licenseFinding      uint8
			toolName            string
			modelName           string
			toolAllowlistStatus string
			source              string
			kind                string
			// confidence is Float32 on the wire (035_ai_attribution). Scanned
			// as float32 and widened to float64 exactly once, which is what
			// clickhouse-connect hands Python before json.dumps renders it --
			// so the persisted evidence bytes match. Widening a DIFFERENT
			// value (e.g. re-parsing a formatted string) would not.
			confidence *float32
		)
		if err := rows.Scan(
			&orgID, &repoID, &subjectType, &subjectID, &observedAt,
			&declaredAI, &humanReviewed, &securityScanned, &licenseFinding,
			&toolName, &modelName, &toolAllowlistStatus,
			&source, &kind, &confidence,
		); err != nil {
			return nil, fmt.Errorf("scan ai governance artifact row: %w", err)
		}

		artifact := aigovernance.Artifact{
			OrgID:       orgID,
			TeamID:      nil, // _ARTIFACTS_SQL selects a literal NULL team_id.
			RepoID:      repoID,
			SubjectType: subjectType,
			SubjectID:   subjectID,
			ObservedAt:  observedAt,
			// The SQL projects literal 1/0/1 for these three, so
			// _artifact_from_row's row.get(..., default) fallbacks never fire.
			AIDetected:                 true,
			SensitiveRepo:              false,
			RepoAllowsAI:               true,
			DeclaredAI:                 declaredAI != 0,
			HumanReviewed:              optionalGovernanceBool(humanReviewed),
			SecurityScanned:            optionalGovernanceBool(securityScanned),
			LicenseOrDependencyFinding: licenseFinding != 0,
			// _optional_str maps BOTH None and "" to None (loaders.py:179).
			ToolName:            optionalGovernanceString(toolName),
			ModelName:           optionalGovernanceString(modelName),
			ToolAllowlistStatus: aigovernance.ParseToolAllowlistStatus(toolAllowlistStatus),
			Evidence: aigovernance.ArtifactEvidence{
				Source: optionalGovernanceString(source),
				Kind:   optionalGovernanceString(kind),
				// evidence["confidence"] is row.get("confidence") RAW -- it is
				// NOT passed through _optional_str, so a real 0.0 stays 0.0
				// rather than collapsing to None the way an empty string would.
				Confidence: widenConfidence(confidence),
				// _ARTIFACTS_SQL projects a literal '' for artifact_url, and
				// _optional_str turns '' into None -- so this is ALWAYS null in
				// the persisted evidence. Written explicitly so a later reader
				// does not "fix" it into a real column read.
				ArtifactURL: nil,
			},
		}
		artifacts = append(artifacts, artifact)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ai governance artifact rows: %w", err)
	}
	return artifacts, nil
}

// optionalGovernanceBool converts a Nullable(UInt8) scan target into the
// Optional[bool] the compute kernel expects. NULL stays nil, which is
// _artifact_from_row's `_optional_bool(None) -> None` -- and that distinction
// is load-bearing: evaluate_artifact tests `human_reviewed is not True`, so
// UNKNOWN and False both violate, but only False is a stated answer.
func optionalGovernanceBool(value *uint8) *bool {
	if value == nil {
		return nil
	}
	converted := *value != 0
	return &converted
}

func optionalGovernanceString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func widenConfidence(value *float32) *float64 {
	if value == nil {
		return nil
	}
	widened := float64(*value)
	return &widened
}

// aiGovernanceBatchConn is the narrow write capability the two governance
// writers need.
type aiGovernanceBatchConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// WriteAIPolicyEvents ports write_ai_policy_events
// (sinks/clickhouse/ai_governance.py:111) and _policy_event_row (:64) --
// same table, same column order as POLICY_EVENT_COLUMNS (:27).
func WriteAIPolicyEvents(
	ctx context.Context, conn aiGovernanceBatchConn,
	violations []aigovernance.Violation, computedAt time.Time,
) (int, error) {
	if len(violations) == 0 {
		return 0, nil
	}
	if conn == nil {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO ai_policy_events (
		event_id, org_id, team_id, repo_id, rule_id, severity,
		subject_type, subject_id, observed_at, evidence, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare ai_policy_events batch: %w", err)
	}
	computedAtUTC := computedAt.UTC()
	for _, violation := range violations {
		evidence, err := violation.EvidenceJSON()
		if err != nil {
			return 0, fmt.Errorf("encode ai_policy_events evidence: %w", err)
		}
		if err := batch.Append(
			violation.EventID, violation.OrgID, violation.TeamID, violation.RepoID,
			string(violation.RuleID), string(violation.Severity),
			violation.SubjectType, violation.SubjectID,
			violation.ObservedAt.UTC(), evidence, computedAtUTC,
		); err != nil {
			return 0, fmt.Errorf("append ai_policy_events row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send ai_policy_events batch: %w", err)
	}
	return len(violations), nil
}

// WriteAIGovernanceCoverageDaily ports write_ai_governance_coverage_daily
// (sinks/clickhouse/ai_governance.py:125) and _coverage_row (:90) -- same
// table, same column order as COVERAGE_COLUMNS (:50).
func WriteAIGovernanceCoverageDaily(
	ctx context.Context, conn aiGovernanceBatchConn,
	rows []aigovernance.CoverageDaily, computedAt time.Time,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if conn == nil {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO ai_governance_coverage_daily (
		org_id, team_id, repo_id, day, ai_artifacts, declared_artifacts,
		human_reviewed_prs, security_scanned_prs, in_policy_artifacts, computed_at)`)
	if err != nil {
		return 0, fmt.Errorf("prepare ai_governance_coverage_daily batch: %w", err)
	}
	computedAtUTC := computedAt.UTC()
	for _, row := range rows {
		if err := batch.Append(
			row.OrgID, row.TeamID, row.RepoID, row.Day,
			row.AIArtifacts, row.DeclaredArtifacts, row.HumanReviewedPRs,
			row.SecurityScannedPRs, row.InPolicyArtifacts, computedAtUTC,
		); err != nil {
			return 0, fmt.Errorf("append ai_governance_coverage_daily row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send ai_governance_coverage_daily batch: %w", err)
	}
	return len(rows), nil
}
