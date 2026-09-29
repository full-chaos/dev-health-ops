package fixturescli

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/operationalbackfill"
)

// The frozen worlds were written by the Python generator against the legacy
// operational tables (no ordering columns). The migrated head is contract 2:
// operational_* tables carry source_revision, source_conflict_key,
// ingest_revision and ordering_contract, and a CHECK requires
// ordering_contract = 2 (chmigrate baseline; operationalbackfill.TableContract).
// A frozen row inserted without those columns takes the column default 0 and
// the server refuses it (code 469). stampOrdering gives such a table the four
// columns, derived the way the producer derives them.

// operationalFamilies maps an operational table of a frozen world to its entity
// family (models/operational.py entity_family).
var operationalFamilies = map[string]string{
	"operational_services":                    "operational_service",
	"operational_incidents":                   "operational_incident",
	"operational_alerts":                      "operational_alert",
	"operational_service_repository_mappings": "operational_service_repository_mapping",
}

var orderingColumnDefs = []FrozenColumn{
	{Name: "source_revision", Type: "UInt128"},
	{Name: "source_conflict_key", Type: "String"},
	{Name: "ingest_revision", Type: "UInt128"},
	{Name: "ordering_contract", Type: "UInt8"},
}

// liveHasOrderingColumns reports whether the connected database's table has the
// contract-2 shape.
func liveHasOrderingColumns(ctx context.Context, conn driver.Conn, table string) (bool, error) {
	var count uint64
	row := conn.QueryRow(ctx, "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = ? AND name = 'ordering_contract'", table)
	if err := row.Scan(&count); err != nil {
		return false, fmt.Errorf("read the columns of %s: %w", table, err)
	}
	return count > 0, nil
}

// stampOrdering returns the table with the four contract-2 columns appended to
// its columns and to every one of the (already transformed) rows. A table that
// is not an operational entity table, or already holds the columns, is returned
// as it is.
func stampOrdering(table WorldTable, rows [][]any) (WorldTable, [][]any, error) {
	family, ok := operationalFamilies[table.Name]
	if !ok {
		return table, rows, nil
	}
	index := map[string]int{}
	for position, column := range table.Columns {
		index[column.Name] = position
	}
	if _, has := index["ordering_contract"]; has {
		return table, rows, nil
	}
	for _, name := range []string{"source_version_at", "last_synced", "observed_at"} {
		if _, has := index[name]; !has {
			return table, nil, fmt.Errorf("table %s has no %s column to derive the ordering from", table.Name, name)
		}
	}
	out := make([][]any, len(rows))
	for number, row := range rows {
		fields := make([]operationalbackfill.OrderingField, 0, len(row))
		times := map[string]time.Time{}
		tombstone := false
		for position, column := range table.Columns {
			value, err := orderingValue(column, row[position])
			if err != nil {
				return table, nil, fmt.Errorf("table %s row %d column %s: %w", table.Name, number, column.Name, err)
			}
			if at, isTime := value.(time.Time); isTime {
				times[column.Name] = at
			}
			switch column.Name {
			case "is_deleted":
				tombstone = tombstone || value == true
			case "is_active":
				tombstone = tombstone || value == false
			}
			fields = append(fields, operationalbackfill.OrderingField{Name: column.Name, Value: value})
		}
		ordering, err := operationalbackfill.DeriveOrdering(family, fields, times["source_version_at"], times["last_synced"], times["observed_at"], tombstone)
		if err != nil {
			return table, nil, fmt.Errorf("table %s row %d: %w", table.Name, number, err)
		}
		stamped := make([]any, 0, len(row)+len(orderingColumnDefs))
		stamped = append(stamped, row...)
		// UInt128 crosses JSON as a quoted decimal, like ClickHouse writes it.
		stamped = append(stamped, ordering.SourceRevision.String(), ordering.ConflictKey, ordering.IngestRevision.String(), 2)
		out[number] = stamped
	}
	table.Columns = append(append([]FrozenColumn{}, table.Columns...), orderingColumnDefs...)
	return table, out, nil
}

// orderingValue types a frozen JSON value the way the producer's dataclass
// field holds it, which is what the conflict key encodes.
func orderingValue(column FrozenColumn, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	base := columnBase(column.Type)
	switch {
	case column.Name == "is_deleted" || column.Name == "is_active":
		switch typed := value.(type) {
		case bool:
			return typed, nil
		case json.Number:
			return typed.String() != "0", nil
		case float64:
			return typed != 0, nil
		}
		return nil, fmt.Errorf("%v is not a flag", value)
	case base == "Date" || base == "Date32" || strings.HasPrefix(base, "DateTime"):
		text, isText := value.(string)
		if !isText {
			return nil, fmt.Errorf("%v is not a time string", value)
		}
		for _, layout := range []string{"2006-01-02 15:04:05.999999999", "2006-01-02"} {
			if parsed, err := time.ParseInLocation(layout, text, time.UTC); err == nil {
				return parsed.UTC(), nil
			}
		}
		return nil, fmt.Errorf("%q is not a time", text)
	case column.Name == "source_id" || column.Name == "repo_id":
		// The producer's dataclass holds these two as UUID, whatever the column type.
		text, isText := value.(string)
		if !isText {
			return nil, fmt.Errorf("%v is not a UUID string", value)
		}
		return operationalbackfill.UUIDText(text), nil
	case strings.HasPrefix(base, "Float"):
		number, err := strconv.ParseFloat(fmt.Sprint(value), 64)
		if err != nil {
			return nil, err
		}
		return number, nil
	case strings.HasPrefix(base, "Int") || strings.HasPrefix(base, "UInt"):
		number, err := strconv.ParseInt(fmt.Sprint(value), 10, 64)
		if err != nil {
			return nil, err
		}
		return number, nil
	}
	text, isText := value.(string)
	if !isText {
		return nil, fmt.Errorf("%v (%T) is not a string", value, value)
	}
	return text, nil
}
