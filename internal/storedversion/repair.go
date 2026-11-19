package storedversion

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Group is a set of columns a writer states together, led by its first column, a terminal one: the group is stated when
// the lead is non-null. State names its column in the kept table.
type Group struct {
	State   string
	Columns []string
}

// Kept is the kept-group store of one base table (migration 163). A materialized view feeds it from every block inserted
// into the base table, before a merge removes an older version, with argMaxIf(group, version, lead IS NOT NULL): the group
// of the newest version that states the lead, even when a newer version carried an older read over it.
type Kept struct {
	Base, Table, Version string
	// insert is "INSERT INTO <Base>", spelled out so a census reads the table the repair writes.
	insert string
	// Keys are the base key columns after org_id.
	Keys   []string
	Groups []Group
}

// PullRequestKept and DeploymentKept are the kept groups of the two tables in scope.
var (
	PullRequestKept = Kept{
		Base: "git_pull_requests", Table: "git_pull_request_kept", Version: "last_synced",
		insert: "INSERT INTO git_pull_requests",
		Keys:   []string{"repo_id", "number"},
		Groups: []Group{
			{State: "merged", Columns: []string{"merged_at"}},
			{State: "review", Columns: []string{"first_review_at", "changes_requested_count", "reviews_count"}},
		},
	}
	DeploymentKept = Kept{
		Base: "deployments", Table: "deployment_kept", Version: "last_synced",
		insert: "INSERT INTO deployments",
		Keys:   []string{"repo_id", "deployment_id"},
		Groups: []Group{{State: "merged", Columns: []string{"merged_at", "pull_request_number", "pull_request_tier"}}},
	}
)

// RepairRounds bounds the repair loop of one write.
const RepairRounds = 3

// RepairUnconvergedEvent and RepairUnconvergedMetric name a write whose keys still hold a NULL lead over a stated group
// after RepairRounds rounds. The write still succeeds: the next write of the key, or the migration's repair, heals it.
const (
	RepairUnconvergedEvent  = "storedversion.repair_unconverged"
	RepairUnconvergedMetric = "dev_health_storedversion_repair_unconverged_total"
)

// KeptTableMissingEvent and KeptTableMissingMetric name a write whose repair found no kept table (a schema without
// migration 163): the write succeeds and repairs nothing, once per write, loudly.
const (
	KeptTableMissingEvent  = "storedversion.kept_table_missing"
	KeptTableMissingMetric = "dev_health_storedversion_kept_table_missing_total"
)

func counter(name, description string) metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/storedversion").Int64Counter(
		name, metric.WithDescription(description))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}

var (
	repairUnconverged = counter(RepairUnconvergedMetric,
		"Keys whose kept group was still not repaired after the bounded repair rounds of one write.")
	keptTableMissing = counter(KeptTableMissingMetric,
		"Writes whose kept-group repair found no kept table (a schema without migration 163) and repaired nothing.")
)

// Repairer is the ClickHouse connection the repair needs: the reads and one INSERT..SELECT per round.
type Repairer interface {
	Querier
	Exec(ctx context.Context, query string, args ...any) error
}

// SyncContext makes an insert visible to the next read: a server or user profile with async inserts on would otherwise
// queue it, and the repair read after it would not see it. It replaces the context's ClickHouse settings, so it also
// drops an insert_deduplication_token: a repair row must never be dropped as a duplicate of the write before it.
func SyncContext(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"async_insert":          0,
		"wait_for_async_insert": 1,
	}))
}

// unknownTable is ClickHouse error 60: a schema without migration 163 has no kept table.
const unknownTable = 60

// Repair heals, for the keys of one written batch, each kept group whose lead FINAL holds as NULL while the kept table
// holds it stated: it inserts the FINAL row with that group from the kept table, at FINAL's version + 1 ms. A non-null
// lead is never replaced, so a repair never writes over a value. It runs at most RepairRounds rounds, each a count and,
// when the count is not 0, one INSERT..SELECT; guard runs before each insert. When keys still hold a NULL lead over a
// stated group after the last round, one WARN and the counter name the count, and Repair returns nil.
//
// The key filter is one IN list per key column, so a key outside the batch that shares each column value may be
// repaired too; a repair is correct for any key. A schema without the kept table (ClickHouse error 60 on the first
// read) repairs nothing, the behaviour before migration 163, and each such write logs one WARN that names the kept table
// and the org and adds 1 to the counter.
func (k Kept) Repair(
	ctx context.Context, conn Repairer, writer, orgID string, keys [][]any, guard func(context.Context) error,
) error {
	if len(keys) == 0 {
		return nil
	}
	ctx = SyncContext(ctx)
	filter, args := k.filter(orgID, keys)
	count := func() (uint64, error) {
		rows, err := conn.Query(ctx, "SELECT count() FROM ("+k.selectToRepair(filter)+")", append(args, args...)...)
		if err != nil {
			return 0, err
		}
		defer rows.Close()
		var n uint64
		for rows.Next() {
			if err := rows.Scan(&n); err != nil {
				return 0, err
			}
		}
		return n, rows.Err()
	}
	for round := 0; round < RepairRounds; round++ {
		pending, err := count()
		if err != nil {
			var exception *clickhouse.Exception
			if round == 0 && errors.As(err, &exception) && exception.Code == unknownTable {
				slog.Default().LogAttrs(ctx, slog.LevelWarn, KeptTableMissingEvent,
					slog.String("writer", writer), slog.String("table", k.Base), slog.String("kept_table", k.Table),
					slog.String("org_id", orgID), slog.Int("keys", len(keys)))
				keptTableMissing.Add(ctx, 1, metric.WithAttributes(
					attribute.String("writer", writer), attribute.String("table", k.Base)))
				return nil
			}
			return fmt.Errorf("read %s kept groups: %w", k.Base, err)
		}
		if pending == 0 {
			return nil
		}
		if guard != nil {
			if err := guard(ctx); err != nil {
				return err
			}
		}
		if err := conn.Exec(ctx, k.RepairInsert(filter), append(args, args...)...); err != nil {
			return fmt.Errorf("repair %s kept groups: %w", k.Base, err)
		}
	}
	pending, err := count()
	if err != nil {
		return fmt.Errorf("read %s kept groups: %w", k.Base, err)
	}
	if pending > 0 {
		slog.Default().LogAttrs(ctx, slog.LevelWarn, RepairUnconvergedEvent,
			slog.String("writer", writer), slog.String("table", k.Base), slog.Uint64("keys", pending))
		repairUnconverged.Add(ctx, int64(pending), metric.WithAttributes(
			attribute.String("writer", writer), attribute.String("table", k.Base)))
	}
	return nil
}

// RepairVersionWindow is how far past a writer's own version its repair rows can move the key: one millisecond per round.
const RepairVersionWindow = RepairRounds * time.Millisecond

// filter is the org and key filter of the batch, applied to the FINAL read and to the kept table.
func (k Kept) filter(orgID string, keys [][]any) (string, []any) {
	args := []any{orgID}
	clauses := []string{"org_id = ?"}
	for position, column := range k.Keys {
		seen := make(map[string]bool, len(keys))
		values := make([]any, 0, len(keys))
		for _, key := range keys {
			value := key[position]
			if id, ok := value.(uuid.UUID); ok {
				value = id.String()
			}
			text := fmt.Sprint(value)
			if seen[text] {
				continue
			}
			seen[text] = true
			values = append(values, value)
		}
		clauses = append(clauses, column+" IN (?)")
		args = append(args, values)
	}
	return strings.Join(clauses, " AND "), args
}

func fill(group Group) string  { return "kept_fill_" + group.State }
func state(group Group) string { return "kept_" + group.State }

// selectToRepair is the FINAL rows of the filter joined to their kept groups, with one fill flag per group, for the rows
// where at least one group is to be filled. An empty filter reads every key.
func (k Kept) selectToRepair(filter string) string {
	where := ""
	if filter != "" {
		where = " WHERE " + filter
	}
	keys := append([]string{"org_id"}, k.Keys...)
	var picked, flags, merges, on, either []string
	for _, group := range k.Groups {
		picked = append(picked, "kept."+state(group))
		flags = append(flags, fmt.Sprintf("base.%s IS NULL AND kept.%s.1 IS NOT NULL AS %s", group.Columns[0], state(group), fill(group)))
		merges = append(merges, fmt.Sprintf("argMaxIfMerge(%s) AS %s", group.State, state(group)))
		either = append(either, fill(group))
	}
	for _, key := range keys {
		on = append(on, "base."+key+" = kept."+key)
	}
	return fmt.Sprintf("SELECT base.*, %s, %s FROM (SELECT * FROM %s FINAL%s) AS base INNER JOIN (SELECT %s, %s FROM %s%s GROUP BY %s) AS kept ON %s WHERE %s",
		strings.Join(picked, ", "), strings.Join(flags, ", "), k.Base, where,
		strings.Join(keys, ", "), strings.Join(merges, ", "), k.Table, where, strings.Join(keys, ", "),
		strings.Join(on, " AND "), strings.Join(either, " OR "))
}

// RepairInsert is the repair statement over the filter (args: the filter's twice). With an empty filter it is the
// repair of every key that migration 163 runs once.
func (k Kept) RepairInsert(filter string) string {
	var except, replace []string
	for _, group := range k.Groups {
		except = append(except, fill(group), state(group))
		for position, column := range group.Columns {
			replace = append(replace, fmt.Sprintf("if(%s, %s.%d, %s) AS %s", fill(group), state(group), position+1, column, column))
		}
	}
	replace = append(replace, fmt.Sprintf("%s + toIntervalMillisecond(1) AS %s", k.Version, k.Version))
	return k.insert + fmt.Sprintf(" SELECT * EXCEPT (%s) REPLACE (%s) FROM (%s)",
		strings.Join(except, ", "), strings.Join(replace, ", "), k.selectToRepair(filter))
}

// BatchKeys returns the key of each row of an insert in Keys order.
func (k Kept) BatchKeys(insert string, rows [][]any) ([][]any, error) {
	positions, err := Positions(insert)
	if err != nil {
		return nil, err
	}
	keys := make([][]any, len(rows))
	for i, row := range rows {
		key := make([]any, len(k.Keys))
		for j, column := range k.Keys {
			position, ok := positions[column]
			if !ok || position >= len(row) {
				return nil, fmt.Errorf("kept groups of %s: key column %s is not written by %q", k.Base, column, insert)
			}
			key[j] = row[position]
		}
		keys[i] = key
	}
	return keys, nil
}
