package providersync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"strings"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// An operational_* table exists in one of two column shapes. Contract 1 is the
// legacy source_version_at shape. Contract 2 (migration 067) adds
// source_revision, source_conflict_key, ingest_revision and ordering_contract
// directly after source_version_at, with CHECK ordering_contract = 2. A
// contract-2 table that receives the legacy column list stores the type
// default 0 in ordering_contract and refuses the whole batch, so the column
// shape a sink sends is taken from the table itself. OPERATIONAL_ORDERING_CONTRACT
// states what the deployment expects; a disagreement with the table is logged
// and the table's shape is used.
type operationalStorageContract uint8

const (
	operationalLegacyContract  operationalStorageContract = 1
	operationalCurrentContract operationalStorageContract = 2

	operationalOrderingContractEnv = "OPERATIONAL_ORDERING_CONTRACT"
	// operationalLegacyLeadColumns is the column prefix every legacy
	// operational column list starts with; the four ordering columns follow
	// it in the contract-2 shape.
	operationalLegacyLeadColumns = "org_id,provider,provider_instance_id,source_entity_type,external_id,source_version_at"
	operationalOrderingColumns   = "source_revision,source_conflict_key,ingest_revision,ordering_contract"
	operationalLegacyLeadCount   = 6
)

// ErrOperationalTableContractUnknown is returned when a table's column shape
// cannot be read or matches neither contract. The sink writes nothing.
var ErrOperationalTableContractUnknown = errors.New("operational table ordering contract unknown")

var operationalOrderingColumnNames = strings.Split(operationalOrderingColumns, ",")

// configuredOperationalStorageContract reads the deployment's expected
// contract: unset or "1" is legacy, "2" is current, anything else is a
// configuration error.
func configuredOperationalStorageContract() (operationalStorageContract, string, error) {
	raw, present := os.LookupEnv(operationalOrderingContractEnv)
	if !present {
		return operationalLegacyContract, "unset", nil
	}
	switch raw {
	case "1":
		return operationalLegacyContract, raw, nil
	case "2":
		return operationalCurrentContract, raw, nil
	}
	return 0, "invalid", ErrInvalidConfiguration
}

// classifyOperationalTableColumns maps a table's column names to its
// contract: none of the four ordering columns is legacy, all four is current,
// anything else (no columns at all, or a partial set) is unknown.
func classifyOperationalTableColumns(columns []string) (operationalStorageContract, error) {
	if len(columns) == 0 {
		return 0, fmt.Errorf("%w: table has no columns", ErrOperationalTableContractUnknown)
	}
	present := make(map[string]bool, len(columns))
	for _, column := range columns {
		present[column] = true
	}
	found := 0
	for _, column := range operationalOrderingColumnNames {
		if present[column] {
			found++
		}
	}
	switch found {
	case 0:
		return operationalLegacyContract, nil
	case len(operationalOrderingColumnNames):
		return operationalCurrentContract, nil
	}
	return 0, fmt.Errorf("%w: %d of %d ordering columns present",
		ErrOperationalTableContractUnknown, found, len(operationalOrderingColumnNames))
}

// operationalTableContracts caches each table's resolved contract for ONE
// WriteEffect or InspectEffect call: every PagerDuty sink entry point starts
// with a new cache, so a table migrated between calls is read again and the
// next call sends its new shape. A nil cache reads the table on every
// resolve. A failed read is never cached.
type operationalTableContracts struct {
	mu      sync.Mutex
	byTable map[string]operationalStorageContract
}

func newOperationalTableContracts() *operationalTableContracts {
	return &operationalTableContracts{byTable: map[string]operationalStorageContract{}}
}

func (cache *operationalTableContracts) cached(table string) (operationalStorageContract, bool) {
	if cache == nil {
		return 0, false
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	contract, ok := cache.byTable[table]
	return contract, ok
}

func (cache *operationalTableContracts) store(table string, contract operationalStorageContract) {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.byTable == nil {
		cache.byTable = map[string]operationalStorageContract{}
	}
	cache.byTable[table] = contract
}

// resolve returns the column shape of table. The env value is validated on
// every call so an invalid setting always refuses the write.
func (cache *operationalTableContracts) resolve(
	ctx context.Context, conn driver.Conn, table string,
) (operationalStorageContract, error) {
	configured, configuredRaw, err := configuredOperationalStorageContract()
	if err != nil {
		slog.Default().ErrorContext(ctx, "operational_ordering_contract_env_invalid",
			slog.String("table", table), slog.String("env", operationalOrderingContractEnv))
		return 0, err
	}
	if contract, ok := cache.cached(table); ok {
		return contract, nil
	}
	if conn == nil {
		return 0, ErrInvalidConfiguration
	}
	contract, err := readOperationalTableContract(ctx, conn, table)
	if err != nil {
		return 0, err
	}
	if contract != configured {
		slog.Default().WarnContext(ctx, "operational_ordering_contract_mismatch",
			slog.String("table", table),
			slog.String("env_value", configuredRaw),
			slog.Int("env_contract", int(configured)),
			slog.Int("table_contract", int(contract)),
			slog.String("used", "table"))
	}
	cache.store(table, contract)
	return contract, nil
}

// readOperationalTableContract reads and classifies table's shape now,
// without the cache.
func readOperationalTableContract(
	ctx context.Context, conn driver.Conn, table string,
) (operationalStorageContract, error) {
	columns, err := readOperationalTableColumns(ctx, conn, table)
	if err != nil {
		slog.Default().ErrorContext(ctx, "operational_ordering_contract_unresolved",
			slog.String("table", table), slog.String("error", err.Error()))
		return 0, fmt.Errorf("%w: %s: %v", ErrOperationalTableContractUnknown, table, err)
	}
	contract, err := classifyOperationalTableColumns(columns)
	if err != nil {
		slog.Default().ErrorContext(ctx, "operational_ordering_contract_unresolved",
			slog.String("table", table), slog.String("error", err.Error()))
		return 0, fmt.Errorf("%w: %s", err, table)
	}
	return contract, nil
}

// confirmInspection returns a readback's result only when every table the
// call resolved still has the shape the call read. A legacy-shape SELECT on
// a contract-2 table is valid SQL, so a table migrated between the call's
// shape read and its SELECT could otherwise be read as absent or exact; the
// second read turns that into an error, and the unit's retry reads again.
func (cache *operationalTableContracts) confirmInspection(
	ctx context.Context, conn driver.Conn, inspection EffectInspection, inspectErr error,
) (EffectInspection, error) {
	if inspectErr != nil || cache == nil {
		return inspection, inspectErr
	}
	cache.mu.Lock()
	resolved := make(map[string]operationalStorageContract, len(cache.byTable))
	if len(cache.byTable) == 0 {
		cache.mu.Unlock()
		return inspection, nil
	}
	for table, contract := range cache.byTable {
		resolved[table] = contract
	}
	cache.mu.Unlock()
	if len(resolved) == 0 {
		return inspection, nil
	}
	for table, contract := range resolved {
		current, err := readOperationalTableContract(ctx, conn, table)
		if err != nil {
			return EffectConflict, err
		}
		if current != contract {
			slog.Default().ErrorContext(ctx, "operational_ordering_contract_changed_during_readback",
				slog.String("table", table),
				slog.Int("read_contract", int(contract)),
				slog.Int("table_contract", int(current)))
			return EffectConflict, fmt.Errorf("%w: %s changed from contract %d to %d during the readback",
				ErrOperationalTableContractUnknown, table, contract, current)
		}
	}
	return inspection, nil
}

func readOperationalTableColumns(ctx context.Context, conn driver.Conn, table string) ([]string, error) {
	rows, err := conn.Query(ctx,
		"SELECT name FROM system.columns WHERE database = currentDatabase() AND table = ?", table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

// columns returns the column list for this contract from a legacy list that
// starts with operationalLegacyLeadColumns.
func (contract operationalStorageContract) columns(legacy string) string {
	if contract == operationalLegacyContract {
		return legacy
	}
	return operationalLegacyLeadColumns + "," + operationalOrderingColumns +
		strings.TrimPrefix(legacy, operationalLegacyLeadColumns)
}

// insertValues returns the INSERT values for this contract from the legacy
// values, placing the four ordering values after source_version_at.
func (contract operationalStorageContract) insertValues(
	legacy []any, sourceRevision *big.Int, conflictKey string,
	ingestRevision *big.Int, ordering uint8,
) []any {
	if contract == operationalLegacyContract {
		return legacy
	}
	return operationalWithOrdering(legacy, sourceRevision, conflictKey, ingestRevision, ordering)
}

func operationalWithOrdering(legacy []any, a, b, c, d any) []any {
	values := make([]any, 0, len(legacy)+4)
	values = append(values, legacy[:operationalLegacyLeadCount]...)
	values = append(values, a, b, c, d)
	return append(values, legacy[operationalLegacyLeadCount:]...)
}

// operationalOrderingTarget names the ordering fields of one decoded row.
type operationalOrderingTarget struct {
	SourceRevision    **big.Int
	SourceConflictKey *string
	IngestRevision    **big.Int
	OrderingContract  *uint8
}

// scan reads one row in this contract's shape. Contract 2 takes the stored
// ordering values; contract 1 derives them with fillLegacy, as the legacy
// table stores none.
func (contract operationalStorageContract) scan(
	rows driver.Rows, legacyTargets []any, target operationalOrderingTarget,
	fillLegacy func() error,
) error {
	if contract == operationalLegacyContract {
		if err := rows.Scan(legacyTargets...); err != nil {
			return err
		}
		return fillLegacy()
	}
	var sourceRevision, ingestRevision big.Int
	if err := rows.Scan(operationalWithOrdering(
		legacyTargets, &sourceRevision, target.SourceConflictKey, &ingestRevision, target.OrderingContract,
	)...); err != nil {
		return err
	}
	*target.SourceRevision = new(big.Int).Set(&sourceRevision)
	*target.IngestRevision = new(big.Int).Set(&ingestRevision)
	return nil
}

// latestQuery selects the newest stored version matching where.
func (contract operationalStorageContract) latestQuery(legacyColumns, table, where string) string {
	columns := contract.columns(legacyColumns)
	if contract == operationalLegacyContract {
		return "SELECT " + columns + " FROM " + table + " FINAL WHERE " + where + " LIMIT 1"
	}
	return "SELECT " + columns + " FROM " + table + " WHERE " + where +
		" ORDER BY source_revision DESC, source_conflict_key DESC, ingest_revision DESC LIMIT 1"
}

// activeQuery selects the newest stored version of every id matching where,
// then keeps those that pass active.
func (contract operationalStorageContract) activeQuery(legacyColumns, table, where, active string) string {
	columns := contract.columns(legacyColumns)
	if contract == operationalLegacyContract {
		return "SELECT " + columns + " FROM (SELECT " + columns + " FROM " + table +
			" FINAL WHERE " + where + ") WHERE " + active
	}
	return "SELECT " + columns + " FROM (SELECT " + columns + " FROM " + table + " WHERE " + where +
		" ORDER BY org_id, id, source_revision DESC, source_conflict_key DESC, ingest_revision DESC LIMIT 1 BY org_id, id) WHERE " +
		active
}

// fromCurrentValues reduces a value or scan-target list written in the
// contract-2 order to this contract's shape.
func (contract operationalStorageContract) fromCurrentValues(values []any) []any {
	if contract != operationalLegacyContract {
		return values
	}
	legacy := make([]any, 0, len(values)-4)
	legacy = append(legacy, values[:operationalLegacyLeadCount]...)
	return append(legacy, values[operationalLegacyLeadCount+4:]...)
}
