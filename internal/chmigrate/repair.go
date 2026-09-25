package chmigrate

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// The repair of stale duplicate `repos` rows (dev-hops migrate clickhouse
// repair). ClickHouse keys `repos` by (org_id, id): a repository that moved
// between organizations leaves its old (id, org_id) row behind, and the newest
// row (by last_synced) is the active one. The verb lists the older rows and,
// with --apply, deletes them.

// repairDetectQuery is the Python query, character for character, with the
// optional org filter placed as it places it.
const repairDetectQuery = `
WITH latest AS (
    SELECT
        id,
        argMax(org_id, last_synced) AS active_org_id,
        max(last_synced) AS active_last_synced
    FROM repos
    GROUP BY id
    HAVING uniqExact(org_id) > 1
)
SELECT
    toString(r.id) AS id,
    r.repo AS repo,
    r.org_id AS stale_org_id,
    l.active_org_id AS active_org_id,
    r.last_synced AS stale_last_synced,
    l.active_last_synced AS active_last_synced
FROM repos r
INNER JOIN latest l ON r.id = l.id
WHERE r.org_id != l.active_org_id
%s
ORDER BY r.repo, r.org_id
`

// staleRow is one stale duplicate row.
type staleRow struct {
	ID, Repo, StaleOrg, ActiveOrg string
	StaleLastSynced               time.Time
}

// pyDatetimeText is Python's str(datetime) for the naive datetime clickhouse-connect
// returns for a DateTime64 column: microseconds only when they are not zero.
func pyDatetimeText(at time.Time) string {
	at = at.UTC()
	text := at.Format("2006-01-02 15:04:05")
	if micros := at.Nanosecond() / 1000; micros != 0 {
		text += fmt.Sprintf(".%06d", micros)
	}
	return text
}

// padRight is Python's f"{text:<width}": padded to width code points, never cut.
func padRight(text string, width int) string {
	if missing := width - utf8.RuneCountInString(text); missing > 0 {
		return text + strings.Repeat(" ", missing)
	}
	return text
}

// Repair finds the stale duplicate rows and, when apply is set, deletes them, and
// writes the report Python wrote. org, when not empty, keeps only the groups
// whose newest row belongs to it.
func Repair(ctx context.Context, conn driver.Conn, org string, apply bool, out io.Writer) error {
	filter := ""
	var args []any
	if org != "" {
		filter = "AND l.active_org_id = {active_org:String}"
		args = append(args, clickhouse.Named("active_org", org))
	}
	rows, err := conn.Query(ctx, fmt.Sprintf(repairDetectQuery, filter), args...)
	if err != nil {
		return err
	}
	var stale []staleRow
	for rows.Next() {
		var row staleRow
		var activeLastSynced time.Time
		if err := rows.Scan(&row.ID, &row.Repo, &row.StaleOrg, &row.ActiveOrg, &row.StaleLastSynced, &activeLastSynced); err != nil {
			_ = rows.Close()
			return err
		}
		stale = append(stale, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}

	if len(stale) == 0 {
		if org != "" {
			fmt.Fprintf(out, "No stale duplicate rows found where the newest row belongs to org %s.\n", org)
		} else {
			fmt.Fprintln(out, "No stale duplicate rows found in repos.")
		}
		return nil
	}
	fmt.Fprintf(out, "Found %d stale duplicate row(s) in repos:\n\n", len(stale))
	fmt.Fprintf(out, "  %s  %s  %s  %s\n", padRight("repo", 40), padRight("stale_org_id", 40), padRight("active_org_id", 40), "stale_last_synced")
	for _, row := range stale {
		fmt.Fprintf(out, "  %s  %s  %s  %s\n", padRight(row.Repo, 40), padRight(row.StaleOrg, 40), padRight(row.ActiveOrg, 40), pyDatetimeText(row.StaleLastSynced))
	}
	fmt.Fprintln(out)
	if !apply {
		fmt.Fprintln(out, "Dry-run: pass --apply to delete these stale duplicate rows.")
		return nil
	}
	fmt.Fprintln(out, "Applying ALTER TABLE repos DELETE for each stale duplicate row...")
	deleted := 0
	for _, row := range stale {
		if err := conn.Exec(ctx, "ALTER TABLE repos DELETE WHERE id = {id:UUID} AND org_id = {org:String} SETTINGS mutations_sync=2",
			clickhouse.Named("id", row.ID), clickhouse.Named("org", row.StaleOrg)); err != nil {
			return fmt.Errorf("delete %s (%s): %w", row.Repo, row.StaleOrg, err)
		}
		deleted++
	}
	fmt.Fprintf(out, "Deleted %d stale duplicate row(s) from repos.\n", deleted)
	return nil
}
