package chschema

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// Execer runs one statement. driver.Conn satisfies it.
type Execer interface {
	Exec(ctx context.Context, query string, args ...any) error
}

// CreateTables creates each named table as the live migration chain leaves it: the baseline DDL, then every chain
// statement on that table, in chain order (TableStatements). A test that needs a few real tables, not the whole
// schema Apply builds, uses it instead of hand-typed DDL: hand-typed DDL keeps the shape of the day it was written.
func CreateTables(ctx context.Context, t testing.TB, conn Execer, names ...string) {
	t.Helper()
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatalf("chschema: %v", err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatalf("chschema: %v", err)
	}
	for _, name := range names {
		statements, err := TableStatements(baseline, chain, name)
		if err != nil {
			t.Fatalf("chschema: %v", err)
		}
		for _, statement := range statements {
			if err := conn.Exec(ctx, statement); err != nil {
				t.Fatalf("chschema: create %s: %v\n%s", name, err, statement)
			}
		}
	}
}

var (
	alterTableRE  = regexp.MustCompile(`(?i)^ALTER TABLE (?:[A-Za-z0-9_]+\.)?([A-Za-z0-9_]+)(?: |$)`)
	createTableRE = regexp.MustCompile(`(?i)^CREATE (?:OR REPLACE )?TABLE (?:IF NOT EXISTS )?(?:[A-Za-z0-9_]+\.)?([A-Za-z0-9_]+)(?: |\(|$)`)
	viewRE        = regexp.MustCompile(`(?i)^(?:CREATE (?:OR REPLACE )?(?:MATERIALIZED |LIVE )?VIEW|DROP VIEW) (?:IF (?:NOT )?EXISTS )?(?:[A-Za-z0-9_]+\.)?([A-Za-z0-9_]+)(?: |\(|$)`)
	insertRE      = regexp.MustCompile(`(?i)^INSERT INTO (?:TABLE )?(?:[A-Za-z0-9_]+\.)?([A-Za-z0-9_]+)(?: |\(|$)`)
	dataOnlyRE    = regexp.MustCompile(`(?i)^(?:OPTIMIZE TABLE|TRUNCATE(?: TABLE)?(?: IF EXISTS)?) (?:[A-Za-z0-9_]+\.)?([A-Za-z0-9_]+)(?: |$)`)
	dropTableRE   = regexp.MustCompile(`(?i)^(?:DROP|DETACH|ATTACH|UNDROP) TABLE (?:IF (?:NOT )?EXISTS )?(?:[A-Za-z0-9_]+\.)?([A-Za-z0-9_]+)(?: |$)`)
	renameRE      = regexp.MustCompile(`(?i)^(?:RENAME TABLE|EXCHANGE TABLES) (.*)$`)
	tableWordRE   = regexp.MustCompile(`(?:[A-Za-z0-9_]+\.)?([A-Za-z0-9_]+)`)
)

// normalize collapses whitespace and drops backquotes, so the statement forms match one way.
func normalize(statement string) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(statement, "`", "")), " ")
}

func mentions(statement, name string) bool {
	return regexp.MustCompile(`(^|[^A-Za-z0-9_])` + regexp.QuoteMeta(name) + `($|[^A-Za-z0-9_])`).MatchString(statement)
}

// TableStatements returns the statements that build name as the chain leaves it. A chain statement on name runs when
// it is ALTER TABLE name or CREATE TABLE name; it is skipped when it only writes or reads rows of name (INSERT,
// OPTIMIZE, TRUNCATE, a view, another table's statement). Any other form that names the table (DROP, DETACH, RENAME,
// EXCHANGE, a view of that name) is an error: replaying the rest would build a shape the chain does not, so such a
// table needs Apply.
func TableStatements(baseline chmigrate.Baseline, chain []chmigrate.ChainFile, name string) ([]string, error) {
	var statements []string
	created := false
	for _, object := range baseline.Objects {
		if object.Name == name {
			if object.IsView() {
				return nil, fmt.Errorf("%s is a view in the baseline, not a table", name)
			}
			statements, created = append(statements, object.Create), true
		}
	}
	for _, file := range chain {
		for index, statement := range chmigrate.SplitStatements(file.SQL) {
			form := normalize(statement)
			if !mentions(form, name) {
				continue
			}
			where := fmt.Sprintf("chain file %s statement %d", file.Version, index+1)
			switch {
			case targets(alterTableRE, form, name):
				if !created {
					return nil, fmt.Errorf("%s alters %s before any statement creates it", where, name)
				}
				statements = append(statements, statement)
			case targets(createTableRE, form, name):
				statements, created = append(statements, statement), true
			case targets(viewRE, form, name):
				return nil, fmt.Errorf("%s makes %s a view; use Apply", where, name)
			case insertRE.MatchString(form), dataOnlyRE.MatchString(form), viewRE.MatchString(form):
			case alterTableRE.MatchString(form), createTableRE.MatchString(form):
				// Another table's statement that reads name (a partition copy, a CREATE ... AS): name keeps its shape.
			default:
				return nil, fmt.Errorf("%s changes %s in a form CreateTables does not replay; use Apply: %s", where, name, form)
			}
		}
	}
	if !created {
		return nil, fmt.Errorf("%s is not in the baseline and no chain file creates it", name)
	}
	return statements, nil
}

func targets(form *regexp.Regexp, statement, name string) bool {
	match := form.FindStringSubmatch(statement)
	return match != nil && match[1] == name
}

// ChainTables returns every table a chain statement creates, alters, drops, detaches, renames or exchanges, sorted.
// A table whose rows are the only thing a chain statement touches (INSERT, OPTIMIZE, TRUNCATE, a view) is not in it.
func ChainTables(chain []chmigrate.ChainFile) []string {
	seen := map[string]bool{}
	for _, file := range chain {
		for _, statement := range chmigrate.SplitStatements(file.SQL) {
			form := normalize(statement)
			for _, re := range []*regexp.Regexp{alterTableRE, createTableRE, dropTableRE} {
				if match := re.FindStringSubmatch(form); match != nil {
					seen[match[1]] = true
				}
			}
			if match := renameRE.FindStringSubmatch(form); match != nil {
				for _, word := range strings.FieldsFunc(match[1], func(r rune) bool { return r == ',' || r == ' ' }) {
					if strings.EqualFold(word, "TO") || strings.EqualFold(word, "AND") {
						continue
					}
					if table := tableWordRE.FindStringSubmatch(word); table != nil {
						seen[table[1]] = true
					}
				}
			}
		}
	}
	tables := make([]string, 0, len(seen))
	for table := range seen {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}
