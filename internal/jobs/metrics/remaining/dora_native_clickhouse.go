package remaining

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
)

// OperationalOrderingContract selects how a "current" row is resolved out of
// the canonical operational tables.
//
// This mirrors src/dev_health_ops/storage/operational_current.py, which emits
// TWO different SQL shapes depending on an environment-configured contract.
// Go had no equivalent of this concept at all before R1, and the distinction
// is not cosmetic: the branches differ ONLY when a row has more than one
// version, so a port that picks the wrong one returns a real but WRONG
// "current" row -- and a fixture that writes each row once cannot tell the
// difference. That is why the parity fixture for this kind deliberately writes
// a superseded incident version (see the multi-version case in the R1 parity
// fixture): without it, a whole-table comparison would report EQUAL over a
// dataset incapable of expressing the defect.
type OperationalOrderingContract int

const (
	// OperationalOrderingLegacy resolves via ReplacingMergeTree FINAL.
	OperationalOrderingLegacy OperationalOrderingContract = 1
	// OperationalOrderingRevision resolves by explicit revision ordering.
	OperationalOrderingRevision OperationalOrderingContract = 2
)

// operationalOrderingContractEnv is the same variable Python reads
// (OPERATIONAL_ORDERING_CONTRACT). Read from the environment rather than
// plumbed through config so the two runtimes cannot disagree about which
// contract is in force for the same deployment.
const operationalOrderingContractEnv = "OPERATIONAL_ORDERING_CONTRACT"

// configuredOperationalOrderingContract mirrors
// configured_operational_ordering_contract (operational_ordering_guard.py:72).
func configuredOperationalOrderingContract() OperationalOrderingContract {
	if strings.TrimSpace(os.Getenv(operationalOrderingContractEnv)) == "2" {
		return OperationalOrderingRevision
	}
	return OperationalOrderingLegacy
}

// currentOperationalRowsSQL ports current_operational_rows_sql
// (operational_current.py:25). The parenthesised sub-select is spliced into a
// FROM clause exactly as Python splices it.
func currentOperationalRowsSQL(
	table string, postSelectionFilters []string, contract OperationalOrderingContract,
) string {
	outer := ""
	if len(postSelectionFilters) > 0 {
		outer = "WHERE " + strings.Join(postSelectionFilters, " AND ")
	}
	if contract == OperationalOrderingRevision {
		return fmt.Sprintf(`(
        SELECT *
        FROM (
            SELECT *
            FROM %s
            WHERE org_id = {org_id:String}
            ORDER BY org_id, id, source_revision DESC, source_conflict_key DESC, ingest_revision DESC
            LIMIT 1 BY org_id, id
        )
        %s
    )`, table, outer)
	}
	return fmt.Sprintf(`(
        SELECT *
        FROM (
            SELECT *
            FROM %s FINAL
            WHERE org_id = {org_id:String}
        )
        %s
    )`, table, outer)
}

// resolvedIncidentsQuery ports active_incidents_query(IncidentWindow.RESOLVED)
// (metrics/active_incidents.py:22).
//
// Only the RESOLVED window is ported: it is the only one DORA uses, and a
// general-purpose builder would be a larger new Go surface than the kind being
// ported needs. The shape is otherwise character-faithful, because a
// paraphrase is exactly how a copied query drifts from the one it mirrors --
// the divergence class internal/providersync's readback pairs exist to catch.
// A live-Python oracle test pins this against the real builder.
func resolvedIncidentsQuery(repoFilter string, contract OperationalOrderingContract) string {
	currentIncidents := currentOperationalRowsSQL(
		"operational_incidents",
		[]string{
			"is_deleted = 0",
			"resolved_at IS NOT NULL " +
				"AND resolved_at >= {start:DateTime64(3, 'UTC')} " +
				"AND resolved_at < {end:DateTime64(3, 'UTC')}",
		},
		contract,
	)
	currentMappings := currentOperationalRowsSQL(
		"operational_service_repository_mappings",
		[]string{
			"repo_id IS NOT NULL",
			"is_active = 1",
			"valid_from <= {as_of:DateTime64(6, 'UTC')}",
			"(valid_to IS NULL OR valid_to > {as_of:DateTime64(6, 'UTC')})",
		},
		contract,
	)
	return fmt.Sprintf(`
        SELECT repo_id, incident_id, status, started_at, resolved_at, last_synced
        FROM (
            SELECT
                mapping.repo_id AS repo_id,
                incident.id AS incident_id,
                incident.normalized_status AS status,
                incident.started_at,
                incident.resolved_at,
                incident.last_synced AS last_synced
            FROM %s AS incident
            INNER JOIN %s AS mapping
                ON incident.org_id = mapping.org_id
               AND incident.service_id = mapping.service_id
            INNER JOIN repos AS repo FINAL
                ON mapping.org_id = repo.org_id
               AND mapping.repo_id = repo.id
            WHERE mapping.repo_id IS NOT NULL%s
            ORDER BY mapping.repo_id, incident.id, incident.last_synced DESC
            LIMIT 1 BY mapping.repo_id, incident.id
        )
        ORDER BY repo_id, incident_id
    `, currentIncidents, currentMappings, repoFilter)
}

// repoFilterClause ports _repo_filter (job_dora.py:60). repo_id wins over
// repo_name when both are set, and the name lookup stays org-scoped so a name
// collision across tenants cannot pull in another org's repo.
func repoFilterClause(scope doraScope, arguments map[string]any) string {
	if scope.RepoID != nil {
		arguments["repo_id"] = *scope.RepoID
		return " AND repo_id = {repo_id:UUID}"
	}
	if scope.RepoName != nil {
		arguments["repo_name"] = *scope.RepoName
		return " AND repo_id IN (" +
			"SELECT id FROM repos" +
			" WHERE repo = {repo_name:String}" +
			" AND org_id = {org_id:String})"
	}
	return ""
}

// deploymentWindowQuery is the _load_deployments SELECT (job_dora.py:84),
// extracted so the four-way coalesce is directly assertable in a unit test --
// it is the clause most likely to be "simplified" into a divergence.
func deploymentWindowQuery(repoFilter string) string {
	return fmt.Sprintf(`
        SELECT repo_id, status, started_at, deployed_at, merged_at
        FROM deployments FINAL
        WHERE org_id = {org_id:String}
          AND coalesce(deployed_at, finished_at, started_at, last_synced)
              >= {start:DateTime64(3, 'UTC')}
          AND coalesce(deployed_at, finished_at, started_at, last_synced)
              < {end:DateTime64(3, 'UTC')}%s`, repoFilter)
}

// loadDeployments ports _load_deployments (job_dora.py:84).
//
// The window predicate is the FOUR-way coalesce, not the two-value fallback
// the kernel counts on. See the DORAExecutor doc comment: these are
// deliberately different filters, and collapsing them changes results.
func (executor *DORAExecutor) loadDeployments(
	ctx context.Context, organizationID string, day time.Time, scope doraScope,
) ([]numerical.Deployment, int, error) {
	start, end := utcDayWindow(day)
	arguments := map[string]any{
		"org_id": organizationID,
		"start":  start,
		"end":    end,
	}
	filter := repoFilterClause(scope, arguments)
	rows, err := executor.conn.Query(
		ctx, deploymentWindowQuery(filter), namedArguments(arguments)...,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("load deployments: %w", err)
	}
	defer rows.Close()

	var deployments []numerical.Deployment
	skipped := 0
	for rows.Next() {
		var (
			repoID     *uuid.UUID
			status     *string
			startedAt  *time.Time
			deployedAt *time.Time
			mergedAt   *time.Time
		)
		if err := rows.Scan(&repoID, &status, &startedAt, &deployedAt, &mergedAt); err != nil {
			return nil, 0, fmt.Errorf("scan deployment: %w", err)
		}
		// _has_valid_repo: a row without a parseable repo id is SKIPPED, not
		// fatal. Python tolerates it; converting a tolerated partial into an
		// error is how a port turns into data loss.
		if repoID == nil {
			skipped++
			continue
		}
		deployments = append(deployments, numerical.Deployment{
			RepoID:     repoID.String(),
			Status:     derefString(status),
			StartedAt:  derefTime(startedAt),
			DeployedAt: derefTime(deployedAt),
			MergedAt:   derefTime(mergedAt),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate deployments: %w", err)
	}
	return deployments, skipped, nil
}

// loadIncidents ports _load_incidents (job_dora.py:123) plus
// deduplicate_active_incidents (active_incidents.py:89).
func (executor *DORAExecutor) loadIncidents(
	ctx context.Context, organizationID string, day time.Time, scope doraScope,
) ([]numerical.Incident, int, error) {
	start, end := utcDayWindow(day)
	arguments := map[string]any{
		"org_id": organizationID,
		"start":  start,
		"end":    end,
		// Python binds now() here. CHAOS-4111: mappings whose valid_from is
		// NULL match nothing, because NULL <= as_of is NULL -- a producer gap,
		// not something this reader may paper over. Reproduced exactly so the
		// two runtimes agree on which mappings are live.
		"as_of": executor.nowUTC(),
	}
	filter := repoFilterClause(scope, arguments)
	query := resolvedIncidentsQuery(filter, configuredOperationalOrderingContract())

	rows, err := executor.conn.Query(ctx, query, namedArguments(arguments)...)
	if err != nil {
		return nil, 0, fmt.Errorf("load incidents: %w", err)
	}
	defer rows.Close()

	var incidents []numerical.Incident
	seen := make(map[string]struct{})
	skipped := 0
	for rows.Next() {
		var (
			repoID     *uuid.UUID
			incidentID string
			status     *string
			startedAt  *time.Time
			resolvedAt *time.Time
			lastSynced time.Time
		)
		if err := rows.Scan(
			&repoID, &incidentID, &status, &startedAt, &resolvedAt, &lastSynced,
		); err != nil {
			return nil, 0, fmt.Errorf("scan incident: %w", err)
		}
		if repoID == nil {
			skipped++
			continue
		}
		// deduplicate_active_incidents keeps the FIRST row per
		// (repo_id, incident_id); the query's ORDER BY already decides which
		// that is.
		key := repoID.String() + "\x1f" + incidentID
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		incidents = append(incidents, numerical.Incident{
			RepoID:     repoID.String(),
			StartedAt:  derefTime(startedAt),
			ResolvedAt: derefTime(resolvedAt),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate incidents: %w", err)
	}
	return incidents, skipped, nil
}

// writeMetrics ports write_dora_metrics (sinks/clickhouse/dora.py:40) --
// the same table and the same column order, with org_id supplied explicitly
// rather than injected from bound sink context.
func (executor *DORAExecutor) writeMetrics(
	ctx context.Context, rows []doraRow,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := executor.conn.PrepareBatch(ctx,
		"INSERT INTO dora_metrics_daily (repo_id, day, metric_name, value, computed_at, org_id)")
	if err != nil {
		return 0, fmt.Errorf("prepare dora batch: %w", err)
	}
	for _, row := range rows {
		repoID, parseErr := uuid.Parse(row.RepoID)
		if parseErr != nil {
			return 0, fmt.Errorf("dora row has an unparseable repo id: %w", parseErr)
		}
		if err := batch.Append(
			repoID, row.Day, row.MetricName, row.Value, row.ComputedAt, row.OrgID,
		); err != nil {
			return 0, fmt.Errorf("append dora row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send dora batch: %w", err)
	}
	return len(rows), nil
}

// namedArguments binds ClickHouse named parameters, matching the {name:Type}
// placeholders the ported Python queries use verbatim.
func namedArguments(arguments map[string]any) []any {
	named := make([]any, 0, len(arguments))
	for name, value := range arguments {
		named = append(named, clickhouse.Named(name, value))
	}
	return named
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func derefTime(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return *value
}

// ErrOrderingContractMismatch reports that the configured operational ordering
// contract disagrees with the schema actually deployed.
var ErrOrderingContractMismatch = errors.New(
	"operational ordering contract does not match the deployed schema")

// schemaOrderingContract reads the contract the TABLE was built for.
//
// The sorting key is the authoritative signal: migration 067 moves it to
// (org_id, id, source_revision, source_conflict_key), and it is the sorting
// key -- not the presence of the columns -- that decides whether FINAL still
// collapses two versions of one row.
func schemaOrderingContract(
	ctx context.Context, conn driver.Conn,
) (OperationalOrderingContract, error) {
	var sortingKey string
	if err := conn.QueryRow(ctx, `
        SELECT sorting_key
        FROM system.tables
        WHERE database = currentDatabase() AND name = 'operational_incidents'
    `).Scan(&sortingKey); err != nil {
		return 0, fmt.Errorf("read operational_incidents sorting key: %w", err)
	}
	if strings.Contains(sortingKey, "source_revision") {
		return OperationalOrderingRevision, nil
	}
	return OperationalOrderingLegacy, nil
}

// verifyOrderingContract fails closed when the configured contract and the
// deployed schema disagree.
func verifyOrderingContract(ctx context.Context, conn driver.Conn) error {
	configured := configuredOperationalOrderingContract()
	deployed, err := schemaOrderingContract(ctx, conn)
	if err != nil {
		return err
	}
	if configured != deployed {
		return fmt.Errorf(
			"%w: %s says contract %d, operational_incidents is built for contract %d "+
				"-- reading it under the wrong contract does not return a different "+
				"row, it returns a different NUMBER of rows",
			ErrOrderingContractMismatch, operationalOrderingContractEnv, configured, deployed,
		)
	}
	return nil
}
