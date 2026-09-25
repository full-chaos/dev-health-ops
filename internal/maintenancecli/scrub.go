package maintenancecli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The scrub of the legacy error-text columns (CHAOS-2780). Rows written before
// the write paths sanitized their error text keep raw credential material at
// rest; the scrub applies the same sanitizer to them, in place, per column.

const (
	defaultMaxErrorTextLength = 4000
	outboxMaxErrorLength      = 2000
)

type columnSpec struct {
	name string
	max  int
	// jsonErrorKey: the column is JSON and only its "error" string value is
	// scrubbed, the sibling keys kept.
	jsonErrorKey bool
}

type tableSpec struct {
	name    string
	columns []columnSpec
	// viaJob: job_runs has no org_id and joins scheduled_jobs to scope by org.
	viaJob bool
	// onUpdate is the SQL the model's onupdate column takes on every UPDATE the
	// scrub issues ("" for none); the ORM moved updated_at, and so do we.
	onUpdate string
}

const wallClock = "$wallclock"

// registry mirrors REGISTRY of maintenance/scrub_error_text.py, in its order;
// the Python labels are the model table names.
var registry = []tableSpec{
	{name: "sync_run_units", columns: []columnSpec{{name: "error", max: defaultMaxErrorTextLength}}, onUpdate: wallClock},
	{name: "sync_runs", columns: []columnSpec{{name: "error", max: defaultMaxErrorTextLength}}},
	{name: "sync_run_reference_discoveries", columns: []columnSpec{{name: "error", max: defaultMaxErrorTextLength}}, onUpdate: wallClock},
	{name: "sync_dispatch_outbox", columns: []columnSpec{{name: "last_error", max: outboxMaxErrorLength}}, onUpdate: wallClock},
	{name: "job_runs", columns: []columnSpec{{name: "error", max: defaultMaxErrorTextLength}, {name: "error_traceback", max: defaultMaxErrorTextLength}}, viaJob: true},
	{name: "backfill_jobs", columns: []columnSpec{{name: "error_message", max: defaultMaxErrorTextLength}}, onUpdate: "now()"},
	{name: "sync_configurations", columns: []columnSpec{{name: "last_sync_error", max: defaultMaxErrorTextLength}, {name: "last_sync_stats", max: defaultMaxErrorTextLength, jsonErrorKey: true}}, onUpdate: wallClock},
	{name: "integration_credentials", columns: []columnSpec{{name: "last_test_error", max: defaultMaxErrorTextLength}}, onUpdate: wallClock},
}

// Counters are one column's tallies.
type Counters struct {
	Scanned, Redact, TruncateOnly, SkippedConcurrent int
}

type key struct{ table, column string }

// classify says whether a change was a redaction or only the length cap:
// _classify_change.
func classify(old string) string {
	if pythonparity.SanitizeErrorText(old, 0) != old {
		return "redact"
	}
	return "truncate_only"
}

func (c *Counters) bump(kind string) {
	if kind == "redact" {
		c.Redact++
	} else {
		c.TruncateOnly++
	}
}

// ScrubParams are the run's inputs.
type ScrubParams struct {
	Apply     bool
	Org       string // "" scans every organization
	BatchSize int
}

// updateSet is the SET tail that carries the model's on-update column.
func (t tableSpec) updateSet(now any, args []any) (string, []any) {
	switch t.onUpdate {
	case wallClock:
		args = append(args, now)
		return fmt.Sprintf(", updated_at = $%d", len(args)), args
	case "now()":
		return ", updated_at = now()", args
	}
	return "", args
}

// processTable scans (and, when applying, scrubs) one table batch by batch.
func processTable(ctx context.Context, conn *pgx.Conn, table tableSpec, params ScrubParams, counters map[key]*Counters, now func() any) error {
	for _, column := range table.columns {
		counters[key{table.name, column.name}] = &Counters{}
	}
	var notNull []string
	var selects []string
	for _, column := range table.columns {
		notNull = append(notNull, "t."+column.name+" IS NOT NULL")
		if column.jsonErrorKey {
			selects = append(selects, "t."+column.name+"::text")
		} else {
			selects = append(selects, "t."+column.name)
		}
	}
	lastID := ""
	for {
		var args []any
		sql := "SELECT t.id::text, " + strings.Join(selects, ", ") + " FROM public." + table.name + " t"
		if params.Org != "" && table.viaJob {
			sql += " JOIN public.scheduled_jobs sj ON sj.id = t.job_id"
		}
		sql += " WHERE (" + strings.Join(notNull, " OR ") + ")"
		if lastID != "" {
			args = append(args, lastID)
			sql += fmt.Sprintf(" AND t.id > $%d::uuid", len(args))
		}
		if params.Org != "" {
			args = append(args, params.Org)
			if table.viaJob {
				sql += fmt.Sprintf(" AND sj.org_id = $%d", len(args))
			} else {
				sql += fmt.Sprintf(" AND t.org_id = $%d", len(args))
			}
		}
		args = append(args, params.BatchSize)
		sql += fmt.Sprintf(" ORDER BY t.id LIMIT $%d", len(args))

		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, sql, args...)
		if err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		type scanned struct {
			id     string
			values []*string
		}
		var batch []scanned
		for rows.Next() {
			row := scanned{values: make([]*string, len(table.columns))}
			destinations := []any{&row.id}
			for index := range row.values {
				destinations = append(destinations, &row.values[index])
			}
			if err := rows.Scan(destinations...); err != nil {
				rows.Close()
				_ = tx.Rollback(ctx)
				return err
			}
			batch = append(batch, row)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if len(batch) == 0 {
			_ = tx.Rollback(ctx)
			return nil
		}
		lastID = batch[len(batch)-1].id
		for _, row := range batch {
			for index, column := range table.columns {
				counter := counters[key{table.name, column.name}]
				var err error
				if column.jsonErrorKey {
					err = processJSONColumn(ctx, tx, table, column, row.id, row.values[index], params.Apply, counter, now)
				} else {
					err = processTextColumn(ctx, tx, table, column, row.id, row.values[index], params.Apply, counter, now)
				}
				if err != nil {
					_ = tx.Rollback(ctx)
					return err
				}
			}
		}
		if params.Apply {
			if err := tx.Commit(ctx); err != nil {
				return err
			}
		} else {
			_ = tx.Rollback(ctx)
		}
		if len(batch) < params.BatchSize {
			return nil
		}
	}
}

func processTextColumn(ctx context.Context, tx pgx.Tx, table tableSpec, column columnSpec, id string, old *string, apply bool, counter *Counters, now func() any) error {
	if old == nil {
		return nil
	}
	counter.Scanned++
	updated := pythonparity.SanitizeErrorText(*old, column.max)
	if updated == *old {
		return nil
	}
	kind := classify(*old)
	if !apply {
		counter.bump(kind)
		return nil
	}
	args := []any{updated, id, *old}
	set, args := table.updateSet(now(), args)
	tag, err := tx.Exec(ctx, fmt.Sprintf("UPDATE public.%s SET %s = $1%s WHERE id = $2::uuid AND %s = $3", table.name, column.name, set, column.name), args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		counter.bump(kind)
	} else {
		counter.SkippedConcurrent++
	}
	return nil
}

func processJSONColumn(ctx context.Context, tx pgx.Tx, table tableSpec, column columnSpec, id string, rawText *string, apply bool, counter *Counters, now func() any) error {
	if rawText == nil {
		return nil
	}
	document, err := pyjson.DecodeString(*rawText)
	if err != nil {
		return fmt.Errorf("decode %s.%s of %s: %w", table.name, column.name, id, err)
	}
	if document == nil {
		return nil
	}
	counter.Scanned++
	object, ok := document.(*pyjson.Object)
	if !ok {
		return nil
	}
	errorValue, present := object.Get("error")
	text, isString := errorValue.(string)
	if !present || !isString {
		return nil
	}
	sanitized := pythonparity.SanitizeErrorText(text, column.max)
	if sanitized == text {
		return nil
	}
	kind := classify(text)
	if !apply {
		counter.bump(kind)
		return nil
	}
	object.Set("error", sanitized)
	replacement, err := pyjson.Dumps(object)
	if err != nil {
		return err
	}
	args := []any{replacement, id, *rawText}
	set, args := table.updateSet(now(), args)
	tag, err := tx.Exec(ctx, fmt.Sprintf("UPDATE public.%s SET %s = $1::json%s WHERE id = $2::uuid AND %s::text = $3", table.name, column.name, set, column.name), args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		counter.bump(kind)
	} else {
		counter.SkippedConcurrent++
	}
	return nil
}

// Scrub runs every registry table; a table that fails is reported through
// onFailure and does not stop the others. It returns the per-column counters.
func Scrub(ctx context.Context, conn *pgx.Conn, params ScrubParams, now func() any, onFailure func(table string, err error)) map[key]*Counters {
	counters := map[key]*Counters{}
	for _, table := range registry {
		if err := processTable(ctx, conn, table, params, counters, now); err != nil {
			onFailure(table.name, err)
			// A failed batch leaves its transaction rolled back; the connection
			// itself is fine for the next table.
		}
	}
	return counters
}

// WriteReport prints the report the Python verb prints.
func WriteReport(out io.Writer, counters map[key]*Counters, apply bool, org string) {
	scope := "ALL organizations"
	if org != "" {
		scope = "org=" + org
	}
	fmt.Fprintf(out, "Org scope: %s\n", scope)
	redactLabel, truncateLabel := "would_redact", "would_truncate_only"
	if apply {
		redactLabel, truncateLabel = "redacted", "truncated_only"
	}
	fmt.Fprintf(out, "%-48s %10s %14s %18s %19s\n", "column", "scanned", redactLabel, truncateLabel, "skipped_concurrent")
	var total Counters
	for _, table := range registry {
		for _, column := range table.columns {
			counter := counters[key{table.name, column.name}]
			if counter == nil {
				counter = &Counters{}
			}
			fmt.Fprintf(out, "%-48s %10d %14d %18d %19d\n", table.name+"."+column.name, counter.Scanned, counter.Redact, counter.TruncateOnly, counter.SkippedConcurrent)
			total.Scanned += counter.Scanned
			total.Redact += counter.Redact
			total.TruncateOnly += counter.TruncateOnly
			total.SkippedConcurrent += counter.SkippedConcurrent
		}
	}
	fmt.Fprintf(out, "%-48s %10d %14d %18d %19d\n", "TOTAL", total.Scanned, total.Redact, total.TruncateOnly, total.SkippedConcurrent)
	if !apply {
		fmt.Fprintln(out)
		fmt.Fprintln(out, "Dry-run: pass --apply to write these changes.")
	}
}

func runScrub(ctx context.Context, env cli.Env) int {
	var apply *bool
	var org *string
	var batch *int
	s, flags, exit := open(ctx, "scrub-error-text", env, func(flags *flag.FlagSet) {
		apply = flags.Bool("apply", false, "apply the scrub (without it, only would-change counts are reported)")
		org = flags.String("org", "", "scan only this organization (ORG_ID never scopes the scrub)")
		batch = flags.Int("batch-size", 1000, "rows to scan per keyset-paginated batch")
	})
	if exit != nil {
		return *exit
	}
	defer s.close()
	_ = flags
	// Python takes --batch-size as given, and 0 or less as the default.
	batchSize := *batch
	if batchSize <= 0 {
		batchSize = 1000
	}
	params := ScrubParams{Apply: *apply, Org: *org, BatchSize: batchSize}
	failed := false
	counters := Scrub(ctx, s.conn, params, func() any { return clock() }, func(table string, err error) {
		failed = true
		s.logger.Error("scrub-error-text failed while processing a table", "table", table, "error", s.boundary.Redact(err).Error())
	})
	WriteReport(s.env.Stdout, counters, params.Apply, params.Org)
	if failed {
		return cli.ExitFailure
	}
	return cli.ExitOK
}
