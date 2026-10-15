package chmigrate_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// The operator rollback files of rollback/ are not part of the chain, so the
// migrator never sees them run. A file that undoes its migration's schema change
// and leaves the migration's row in schema_migrations makes the next
// `dho migrate clickhouse upgrade` of a build that has the migration read it as
// applied and skip it: the schema stays rolled back under a build that expects
// it. Every rollback file therefore ends by deleting its own version row, last,
// so a statement that fails part-way leaves the row and the file can be run
// again.

// rollbackVersionRowProblems reports what is wrong with one rollback file's text:
// name is the file name, body its text.
func rollbackVersionRowProblems(name, body string) []string {
	var problems []string
	if !strings.HasSuffix(name, ".down.sql") {
		return []string{name + ": not a .down.sql file"}
	}
	version := strings.TrimSuffix(name, ".down.sql") + ".sql"
	want := "ALTER TABLE schema_migrations DELETE WHERE version = '" + version + "' SETTINGS mutations_sync = 2"
	statements := chmigrate.SplitStatements(body)
	if len(statements) == 0 {
		return []string{name + ": no statement"}
	}
	last := strings.Join(strings.Fields(statements[len(statements)-1]), " ")
	if last != want {
		problems = append(problems, name+": the last statement is not `"+want+"`")
	}
	for index, statement := range statements {
		if index == len(statements)-1 {
			continue
		}
		if strings.Contains(statement, chmigrate.SchemaMigrationsTable) {
			problems = append(problems, name+": a statement other than the last touches schema_migrations")
		}
	}
	return problems
}

func TestEveryRollbackFileDeletesItsOwnVersionRowLast(t *testing.T) {
	files, err := filepath.Glob("rollback/*.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no rollback file found: the census checks nothing")
	}
	for _, file := range files {
		name := filepath.Base(file)
		chainFile := filepath.Join("sql", strings.TrimSuffix(name, ".down.sql")+".sql")
		if _, err := os.Stat(chainFile); err != nil {
			t.Errorf("%s names no chain file (%s): %v", name, chainFile, err)
		}
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, problem := range rollbackVersionRowProblems(name, string(body)) {
			t.Error(problem)
		}
	}
}

// The checker itself refuses the shapes it exists for.
func TestRollbackVersionRowCheckerRefusesAFileThatKeepsTheRow(t *testing.T) {
	const name = "199_example.down.sql"
	good := "-- comment\nALTER TABLE t DROP COLUMN IF EXISTS c;\nALTER TABLE schema_migrations DELETE WHERE version = '199_example.sql' SETTINGS mutations_sync = 2;\n"
	if problems := rollbackVersionRowProblems(name, good); len(problems) != 0 {
		t.Fatalf("a correct file was refused: %v", problems)
	}
	for label, body := range map[string]string{
		"no version delete":       "ALTER TABLE t DROP COLUMN IF EXISTS c;\n",
		"another migration's row": "ALTER TABLE t DROP COLUMN IF EXISTS c;\nALTER TABLE schema_migrations DELETE WHERE version = '198_example.sql' SETTINGS mutations_sync = 2;\n",
		"not the last statement":  "ALTER TABLE schema_migrations DELETE WHERE version = '199_example.sql' SETTINGS mutations_sync = 2;\nALTER TABLE t DROP COLUMN IF EXISTS c;\n",
		"no synchronous mutation": "ALTER TABLE t DROP COLUMN IF EXISTS c;\nALTER TABLE schema_migrations DELETE WHERE version = '199_example.sql';\n",
		"empty file":              "-- nothing\n",
	} {
		if len(rollbackVersionRowProblems(name, body)) == 0 {
			t.Errorf("%s: the checker accepted the file", label)
		}
	}
}
