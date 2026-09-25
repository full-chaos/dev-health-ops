package operationalbackfill

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Contract is the operational ordering contract of a deployment
// (OPERATIONAL_ORDERING_CONTRACT): 1 is the legacy table shape, 2 the shape
// with the four ordering columns.
type Contract uint8

const (
	ContractLegacy  Contract = 1
	ContractCurrent Contract = 2

	// ContractEnv names the variable that selects the contract.
	ContractEnv = "OPERATIONAL_ORDERING_CONTRACT"
)

// orderingColumns are the four columns a legacy table does not have.
var orderingColumns = map[string]bool{
	"source_revision": true, "source_conflict_key": true, "ingest_revision": true, "ordering_contract": true,
}

// entityTables is OPERATIONAL_ENTITY_TABLES, in its order: the tables the
// writer guard inspects.
var entityTables = []string{
	"operational_services", "operational_incidents", "operational_alerts",
	"operational_incident_timeline_events", "operational_incident_notes",
	"operational_incident_responders", "operational_escalation_policies",
	"operational_on_call_schedules", "operational_on_call_assignments",
	"operational_teams", "operational_users", "operational_service_repository_mappings",
}

// ParseContract is parse_operational_ordering_contract: unset is legacy, only
// "1" or "2" is valid.
func ParseContract(raw string, present bool) (Contract, error) {
	if !present {
		return ContractLegacy, nil
	}
	switch raw {
	case "1":
		return ContractLegacy, nil
	case "2":
		return ContractCurrent, nil
	}
	return 0, fmt.Errorf("%s must be exactly '1' or '2', got %s", ContractEnv, pythonparity.StrRepr(raw))
}

var (
	whitespace = regexp.MustCompile(`\s+`)
	commaSpace = regexp.MustCompile(`\s*,\s*`)
)

// TableContract is operational_table_contract: which contract a table's DDL is.
func TableContract(ddl, table string) (Contract, error) {
	normalized := whitespace.ReplaceAllString(strings.ReplaceAll(ddl, "`", " "), " ")
	normalized = commaSpace.ReplaceAllString(strings.TrimSpace(normalized), ", ")
	present := 0
	for name := range orderingColumns {
		if strings.Contains(normalized, name) {
			present++
		}
	}
	if present == 0 && strings.Contains(normalized, "ReplacingMergeTree(source_version_at)") &&
		strings.Contains(normalized, "ORDER BY (org_id, id)") {
		return ContractLegacy, nil
	}
	if present == len(orderingColumns) {
		current := true
		for _, marker := range []string{
			"source_revision UInt128", "source_conflict_key String", "ingest_revision UInt128",
			"ordering_contract UInt8", "CONSTRAINT ordering_contract_v2 CHECK ordering_contract = 2",
			"ReplacingMergeTree(ingest_revision)", "PRIMARY KEY (org_id, id)",
			"ORDER BY (org_id, id, source_revision, source_conflict_key)",
		} {
			current = current && strings.Contains(normalized, marker)
		}
		if current {
			return ContractCurrent, nil
		}
	}
	return 0, staleState(table, 0, nil)
}

// StaleStateError is OperationalOrderingStaleStateError.
type StaleStateError struct {
	Table      string
	Configured Contract
	Stored     *Contract
}

func (err StaleStateError) Error() string {
	stored := "None"
	if err.Stored != nil {
		stored = fmt.Sprint(uint8(*err.Stored))
	}
	return fmt.Sprintf("operational ordering stale_state table=%s configured=%d stored=%s",
		err.Table, err.Configured, stored)
}

func staleState(table string, configured Contract, stored *Contract) error {
	return StaleStateError{Table: table, Configured: configured, Stored: stored}
}

// OldWriterRejectedError is OperationalOldWriterRejectedError: a legacy writer
// against a current-shape table.
type OldWriterRejectedError struct{ Table, Service, Version string }

func (err OldWriterRejectedError) Error() string {
	return fmt.Sprintf("legacy operational writer rejected for %s service=%s version=%s",
		err.Table, err.Service, err.Version)
}

// GuardTables is guard_operational_writer_tables: every operational table is
// in the shape the configured contract expects.
func GuardTables(ctx context.Context, conn driver.Conn, configured Contract, service, version string) error {
	for _, table := range entityTables {
		rows, err := conn.Query(ctx, "SHOW CREATE TABLE `"+table+"`")
		if err != nil {
			return err
		}
		var ddl string
		found := false
		for rows.Next() {
			if err := rows.Scan(&ddl); err != nil {
				_ = rows.Close()
				return err
			}
			found = true
			break
		}
		if err := closeRows(rows); err != nil {
			return err
		}
		if !found {
			return staleState(table, configured, nil)
		}
		stored, err := TableContract(ddl, table)
		if err != nil {
			return err
		}
		if stored == configured {
			continue
		}
		if configured == ContractLegacy && stored == ContractCurrent {
			return OldWriterRejectedError{Table: table, Service: service, Version: version}
		}
		return staleState(table, configured, &stored)
	}
	return nil
}

// columns is the insert column list of a row under a contract: the dataclass
// declaration order, without the ordering columns for the legacy shape.
func (row Row) columns(contract Contract) []string {
	names := make([]string, 0, len(row.Fields)+5)
	for _, item := range row.Fields[:6] {
		names = append(names, item.name)
	}
	if contract == ContractCurrent {
		names = append(names, "source_revision", "source_conflict_key", "ingest_revision", "ordering_contract")
	}
	names = append(names, "id")
	for _, item := range row.Fields[6:] {
		names = append(names, item.name)
	}
	return names
}

func (row Row) values(contract Contract) []any {
	columns := row.columns(contract)
	values := make([]any, len(columns))
	for index, name := range columns {
		if derived, ok := row.derived(name); ok {
			values[index] = derived
			continue
		}
		value, _ := row.value(name)
		values[index] = value
	}
	return values
}

// insertRows writes the rows of one entity family in one batch (none: nothing).
func insertRows(ctx context.Context, conn driver.Conn, rows []Row, contract Contract) error {
	if len(rows) == 0 {
		return nil
	}
	columns := rows[0].columns(contract)
	statement, err := insertPrefix(rows[0].Table)
	if err != nil {
		return err
	}
	batch, err := conn.PrepareBatch(ctx, statement+strings.Join(columns, ", ")+")")
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := batch.Append(row.values(contract)...); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

// insertPrefix is the start of the table's INSERT statement, each written out
// in full: the module's writer inventory (internal/storedversion) reads the
// statements, and a table name built at run time would be unresolvable there.
func insertPrefix(table string) (string, error) {
	switch table {
	case tableIncidents:
		return "INSERT INTO operational_incidents (", nil
	case tableAlerts:
		return "INSERT INTO operational_alerts (", nil
	case tableSchedules:
		return "INSERT INTO operational_on_call_schedules (", nil
	}
	return "", fmt.Errorf("no canonical writer for table %q", table)
}

// Write persists a batch as write_operational_batch does: incidents, then
// alerts, then schedules (this migration has no services or mappings). The
// three inserts are separate statements with no transaction across them: a
// failure names the failed stage, the stages before it stay written, and a
// re-run writes the same identities again (the tables are ReplacingMergeTree).
func Write(ctx context.Context, conn driver.Conn, batch Batch, contract Contract) error {
	for _, rows := range [][]Row{batch.Incidents, batch.Alerts, batch.Schedules} {
		if err := insertRows(ctx, conn, rows, contract); err != nil {
			table := "operational rows"
			if len(rows) > 0 {
				table = rows[0].Table
			}
			return fmt.Errorf("write stage %s failed (stages before it are written; re-running writes the same identities again): %w", table, err)
		}
	}
	return nil
}

// currentRowsSQL is current_operational_rows_sql for one table, with an id
// filter applied after the latest version is chosen.
func currentRowsSQL(table string, contract Contract) string {
	if contract == ContractLegacy {
		return "(SELECT * FROM (SELECT * FROM " + table + " FINAL WHERE org_id = {org_id:String}) WHERE id IN {ids:Array(String)})"
	}
	return "(SELECT * FROM (SELECT * FROM " + table + " WHERE org_id = {org_id:String} " +
		"ORDER BY org_id, id, source_revision DESC, source_conflict_key DESC, ingest_revision DESC " +
		"LIMIT 1 BY org_id, id) WHERE id IN {ids:Array(String)})"
}

func currentIdentityIDs(ctx context.Context, conn driver.Conn, table, orgID string, expected map[string]bool, contract Contract) (map[string]bool, error) {
	found := map[string]bool{}
	if len(expected) == 0 {
		return found, nil
	}
	ids := make([]string, 0, len(expected))
	for id := range expected {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	rows, err := conn.Query(ctx, "SELECT id FROM "+currentRowsSQL(table, contract),
		clickhouse.Named("org_id", orgID), clickhouse.Named("ids", ids))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		found[id] = true
	}
	if err := closeRows(rows); err != nil {
		return nil, err
	}
	return found, nil
}

// ParityError is OperationalBackfillParityError.
type ParityError struct{ MissingIncidents, MissingMappings int }

func (err ParityError) Error() string {
	return fmt.Sprintf("canonical operational backfill parity verification failed: missing incidents=%d, "+
		"missing service_repository_mappings=%d; canonical backfill is incomplete",
		err.MissingIncidents, err.MissingMappings)
}

// Result is OperationalBackfillResult.
type Result struct {
	Services, Incidents, Alerts, Schedules, ServiceRepositoryMappings int
	ExpectedIncidents, VerifiedIncidents                              int
	ExpectedMappings, VerifiedMappings                                int
}

// ParityVerified is the property of the same name.
func (result Result) ParityVerified() bool {
	return result.VerifiedIncidents == result.ExpectedIncidents && result.VerifiedMappings == result.ExpectedMappings
}

// Summary is the line the verb prints.
func (result Result) Summary() string {
	return fmt.Sprintf("Migrated canonical operational rows: services=%d, incidents=%d, alerts=%d, "+
		"schedules=%d, service_repository_mappings=%d; parity_verified=%t, incidents=%d/%d, "+
		"service_repository_mappings=%d/%d",
		result.Services, result.Incidents, result.Alerts, result.Schedules, result.ServiceRepositoryMappings,
		result.ParityVerified(), result.VerifiedIncidents, result.ExpectedIncidents,
		result.VerifiedMappings, result.ExpectedMappings)
}

// Verify is _verify_expected_canonical_identities: every incident id of the
// batch is a current row.
func Verify(ctx context.Context, conn driver.Conn, orgID string, batch Batch, contract Contract) (Result, error) {
	expected := map[string]bool{}
	for _, row := range batch.Incidents {
		expected[row.ID] = true
	}
	verified, err := currentIdentityIDs(ctx, conn, tableIncidents, orgID, expected, contract)
	if err != nil {
		return Result{}, err
	}
	// The mapping table is never written by this migration; its expected set
	// is empty (as in Python, where no batch carries mappings).
	result := Result{
		Incidents: len(batch.Incidents), Alerts: len(batch.Alerts), Schedules: len(batch.Schedules),
		ExpectedIncidents: len(expected), VerifiedIncidents: len(verified),
	}
	if missing := len(expected) - len(verified); missing != 0 {
		return result, ParityError{MissingIncidents: missing}
	}
	return result, nil
}

// Run is run_canonical_operational_backfill.
func Run(ctx context.Context, conn driver.Conn, orgID, instance string, contract Contract, now func() time.Time) (Result, error) {
	legacy, err := LoadLegacy(ctx, conn, orgID)
	if err != nil {
		return Result{}, fmt.Errorf("read stage (nothing written): %w", err)
	}
	batch, err := Map(orgID, instance, legacy, now())
	if err != nil {
		return Result{}, fmt.Errorf("map stage (nothing written): %w", err)
	}
	if err := Write(ctx, conn, batch, contract); err != nil {
		return Result{}, err
	}
	result, err := Verify(ctx, conn, orgID, batch, contract)
	if err != nil {
		return result, fmt.Errorf("verify stage (all rows written): %w", err)
	}
	return result, nil
}
