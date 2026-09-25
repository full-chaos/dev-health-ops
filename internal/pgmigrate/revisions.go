package pgmigrate

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/jackc/pgx/v5"
)

// The `current` and `heads` verbs print revisions the way `alembic current` and
// `alembic heads` print them (dev-hops migrate postgres current|heads): one
// line per revision, its branch label in parentheses when the script has one,
// and "(head)" for a head. The venue oracle compares the text with the real
// Alembic while the Python chain exists.

// headLabel is the branch label `alembic heads` shows on a head. The cutover
// head carries river_cutover (0066 declares it). The application head is the
// head of the branch 0067 labels application_schema, whatever its number: the
// baseline holds exactly the cutover head and the application head
// (TestBaselineHoldsOnlyTheCutoverAndApplicationHeads pins it), so the
// application head is the baseline head that is not the cutover head. A chain
// revision after the baseline has none: it is not an Alembic script.
func headLabel(baseline Baseline, revision string) string {
	switch {
	case revision == cutoverRevision:
		return "river_cutover"
	case revision == applicationHead(baseline):
		return "application_schema"
	}
	return ""
}

// Heads is the set of head revisions: the baseline's heads, with the application
// head replaced by the last chain revision when there is a chain. Sorted.
func Heads(baseline Baseline, chain []ChainFile) []string {
	application := applicationHead(baseline)
	heads := make([]string, 0, len(baseline.Heads))
	for _, head := range baseline.Heads {
		if head == application && len(chain) > 0 {
			head = chain[len(chain)-1].Revision
		}
		heads = append(heads, head)
	}
	sort.Strings(heads)
	return heads
}

// FormatRevision is one line of `alembic heads` (its branch label, when it
// has one) or `alembic current` (label "").
func FormatRevision(revision string, isHead bool, label string) string {
	line := revision
	if label != "" {
		line += " (" + label + ")"
	}
	if isHead {
		line += " (head)"
	}
	return line
}

// WriteHeads prints the heads.
func WriteHeads(out io.Writer, baseline Baseline, chain []ChainFile) error {
	for _, head := range Heads(baseline, chain) {
		if _, err := fmt.Fprintln(out, FormatRevision(head, true, headLabel(baseline, head))); err != nil {
			return err
		}
	}
	return nil
}

// Recorded reads the revisions alembic_version records (none when the table
// does not exist), sorted: Alembic's own order for two heads is not stable
// across runs, so dho prints a deterministic one.
func Recorded(ctx context.Context, conn *pgx.Conn) ([]string, error) {
	var recorded []string
	err := readOnlyTransaction(ctx, conn, func(tx pgx.Tx) error {
		var hasTable bool
		if err := tx.QueryRow(ctx, "SELECT to_regclass('public.alembic_version') IS NOT NULL").Scan(&hasTable); err != nil {
			return fmt.Errorf("look for alembic_version: %w", err)
		}
		if !hasTable {
			return nil
		}
		rows, err := tx.Query(ctx, "SELECT version_num FROM public.alembic_version")
		if err != nil {
			return fmt.Errorf("read alembic_version: %w", err)
		}
		versions, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("read alembic_version: %w", err)
		}
		sort.Strings(versions)
		recorded = versions
		return nil
	})
	return recorded, err
}

// WriteCurrent prints the recorded revisions, each marked as a head when it is
// one. The revisions are printed as recorded: Alembic refuses one its script
// graph does not contain, but the Go migrator has only the baseline and the
// chain after it, not the graph of older revisions, so it cannot tell an old
// revision from a wrong one.
func WriteCurrent(out io.Writer, recorded []string, baseline Baseline, chain []ChainFile) error {
	isHead := map[string]bool{}
	for _, head := range Heads(baseline, chain) {
		isHead[head] = true
	}
	for _, revision := range recorded {
		if _, err := fmt.Fprintln(out, FormatRevision(revision, isHead[revision], "")); err != nil {
			return err
		}
	}
	return nil
}
