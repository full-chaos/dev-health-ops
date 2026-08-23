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

// ErrOrderingContractUnparseable reports a value that names no contract.
var ErrOrderingContractUnparseable = errors.New(
	"OPERATIONAL_ORDERING_CONTRACT must be unset, \"1\" or \"2\"")

// parseOperationalOrderingContract mirrors
// parse_operational_ordering_contract (operational_ordering_guard.py:62), and
// mirrors its STRICTNESS, which is the part that matters:
//
//	if raw is None: return LEGACY
//	if raw not in {"1", "2"}: raise OperationalOrderingConfigurationError(raw)
//
// Two details are deliberate because an earlier version got both wrong:
//
//  1. UNSET and EMPTY are different. Python sees None for an unset variable and
//     returns LEGACY, but sees "" for one exported as empty and RAISES. So this
//     takes the value through LookupEnv rather than Getenv, which flattens the
//     two into "".
//
//  2. NO TRIMMING. Python compares the raw string, so "2 " raises. Trimming
//     first looks harmless and is the opposite of harmless: it silently ACCEPTS
//     a value the Python runtime refuses to start on, so the two runtimes would
//     disagree about whether the same deployment is even configured -- and the
//     Go side would be the one quietly proceeding.
//
// Anything else falling back to legacy would be worse still: a typo would not
// merely be ignored, it would select the branch that counts one incident as
// several, and nothing would say so.
func parseOperationalOrderingContract(
	raw string, present bool,
) (OperationalOrderingContract, error) {
	if !present {
		return OperationalOrderingLegacy, nil
	}
	switch raw {
	case "1":
		return OperationalOrderingLegacy, nil
	case "2":
		return OperationalOrderingRevision, nil
	default:
		return 0, fmt.Errorf("%w: got %q", ErrOrderingContractUnparseable, raw)
	}
}

// configuredOperationalOrderingContract reads the contract from the
// environment. It is resolved ONCE, at construction, and stored on the
// executor: re-reading per query would let a mid-flight environment change
// split a single partition across two contracts, and would reintroduce the
// possibility of an unparseable value after the guard had already passed.
func configuredOperationalOrderingContract() (OperationalOrderingContract, error) {
	raw, present := os.LookupEnv(operationalOrderingContractEnv)
	return parseOperationalOrderingContract(raw, present)
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
		"start":  dateTime64Argument(start, millisecondPrecision),
		"end":    dateTime64Argument(end, millisecondPrecision),
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
		"start":  dateTime64Argument(start, millisecondPrecision),
		"end":    dateTime64Argument(end, millisecondPrecision),
		// Python binds now() here. CHAOS-4111: mappings whose valid_from is
		// NULL match nothing, because NULL <= as_of is NULL -- a producer gap,
		// not something this reader may paper over. Reproduced exactly so the
		// two runtimes agree on which mappings are live.
		"as_of": dateTime64Argument(executor.nowUTC(), microsecondPrecision),
	}
	filter := repoFilterClause(scope, arguments)
	query := resolvedIncidentsQuery(filter, executor.contract)

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

// doraContractTables are the tables the native projection reads THROUGH the
// ordering contract -- both of them.
//
// resolvedIncidentsQuery wraps each in currentOperationalRowsSQL
// (operational_incidents and operational_service_repository_mappings), so both
// carry the contract and both must agree with it. Guarding only the first was
// a real gap: migration 067 rebuilds each operational table in a LOOP using
// non-transactional ClickHouse DDL, so an interrupted migration can genuinely
// leave incidents on v2 while mappings is still legacy. Construction would
// have succeeded and the query would then have referenced v2 columns that do
// not exist on mappings, or resolved mapping versions under the wrong
// semantics -- wrong repository attribution rather than a clean failure.
//
// This is a narrower set than Python's guard_operational_writer_tables, which
// walks every OPERATIONAL_ENTITY_TABLE. That is not a divergence but a
// difference of role: Python's is a WRITER admission check for a store that
// may write any of them, while this is a READER check for an executor that
// touches exactly these two. Refusing the family over a table this code never
// queries would be over-refusal, and would make an unrelated migration state
// take DORA down.
var doraContractTables = []string{
	"operational_incidents",
	"operational_service_repository_mappings",
}

// The canonical sorting keys, as migration 067 leaves them.
//
// These are compared EXACTLY rather than probed for a substring. An earlier
// version asked whether the key contained "source_revision" and treated every
// other shape as legacy, which is fail-OPEN in the one direction that matters:
// a reordered, truncated or future key would have been classified legacy and
// read with FINAL, producing valid-LOOKING numbers instead of a refusal. An
// unknown schema is precisely the case where this code has no basis for a
// guess.
//
// Python does not guess either. operational_table_contract
// (operational_ordering_guard.py:83) requires an exact match to the legacy
// shape or to the complete v2 marker set and raises
// OperationalOrderingStaleStateError for anything else.
const (
	legacySortingKey   = "org_id, id"
	revisionSortingKey = "org_id, id, source_revision, source_conflict_key"
)

// ErrOrderingContractUnknownSchema reports a sorting key that matches neither
// canonical shape.
var ErrOrderingContractUnknownSchema = errors.New(
	"operational table sorting key matches no known ordering contract")

// schemaOrderingContract reads the contract the TABLE was built for.
//
// The sorting key is the authoritative signal: migration 067 moves it to
// (org_id, id, source_revision, source_conflict_key), and it is the sorting
// key -- not the presence of the columns -- that decides whether FINAL still
// collapses two versions of one row.
func schemaOrderingContract(
	ctx context.Context, conn driver.Conn, table string,
) (OperationalOrderingContract, error) {
	var sortingKey string
	if err := conn.QueryRow(ctx, `
        SELECT sorting_key
        FROM system.tables
        WHERE database = currentDatabase() AND name = {table:String}
    `, clickhouse.Named("table", table)).Scan(&sortingKey); err != nil {
		return 0, fmt.Errorf("read %s sorting key: %w", table, err)
	}
	return classifySortingKey(table, sortingKey)
}

// classifySortingKey maps a sorting key onto a contract, or refuses.
//
// Whitespace is normalised because ClickHouse's own rendering of the key is not
// something this code should depend on; nothing else is. Order is significant
// and is NOT sorted away: (org_id, id, source_conflict_key, source_revision)
// is a different table from the canonical one, and treating them as equal is
// the same guess this function exists to stop making.
func classifySortingKey(
	table string, sortingKey string,
) (OperationalOrderingContract, error) {
	normalized := strings.Join(strings.Fields(
		strings.ReplaceAll(sortingKey, "`", " ")), " ")
	normalized = strings.ReplaceAll(normalized, " ,", ",")
	switch normalized {
	case legacySortingKey:
		return OperationalOrderingLegacy, nil
	case revisionSortingKey:
		return OperationalOrderingRevision, nil
	default:
		return 0, fmt.Errorf(
			"%w: %s is ordered by (%s), which is neither the legacy key (%s) "+
				"nor the revision key (%s) -- refusing rather than guessing, "+
				"because guessing legacy here reads the table with FINAL and "+
				"produces valid-looking wrong numbers",
			ErrOrderingContractUnknownSchema, table, normalized,
			legacySortingKey, revisionSortingKey,
		)
	}
}

// verifyOrderingContract fails closed when the configured contract and the
// deployed schema disagree, for ANY table the projection reads.
func verifyOrderingContract(
	ctx context.Context, conn driver.Conn, configured OperationalOrderingContract,
) error {
	for _, table := range doraContractTables {
		deployed, err := schemaOrderingContract(ctx, conn, table)
		if err != nil {
			return err
		}
		if configured != deployed {
			return fmt.Errorf(
				"%w: %s says contract %d, %s is built for contract %d "+
					"-- reading it under the wrong contract does not return a "+
					"different row, it returns a different NUMBER of rows",
				ErrOrderingContractMismatch, operationalOrderingContractEnv,
				configured, table, deployed,
			)
		}
	}
	return nil
}

// ClickHouse query parameters are LITERALS, not expressions.
//
// clickhouse-go renders a time.Time bound to a {name:DateTime64(...)}
// placeholder as `toDateTime('2026-08-09 00:00:00')`, and the server rejects
// it: "Value toDateTime(...) cannot be parsed as DateTime64(3, 'UTC') ...
// because it isn't parsed completely: only 10 of 33 bytes was parsed"
// (BAD_QUERY_PARAMETER). The parameter mechanism does not evaluate the value,
// it PARSES it, so a function call is text the parser gives up on partway
// through.
//
// Formatting the value here is also the closer match to Python, whose driver
// sends these parameters as plain literals; the timestamps therefore cross the
// wire in the same shape on both sides rather than in two driver-specific
// encodings that have to be trusted to agree.
const (
	millisecondPrecision = "2006-01-02 15:04:05.000"
	microsecondPrecision = "2006-01-02 15:04:05.000000"
)

// dateTime64Argument renders a timestamp at the precision its column declares.
// Passing more digits than the column holds is silently truncated by the
// server, which would make a window boundary land a fraction early or late
// without any error to notice.
func dateTime64Argument(value time.Time, precision string) string {
	return value.UTC().Format(precision)
}
