package pgmigrate

import (
	"regexp"
	"strings"
	"testing"
)

// The upgrade walk is ONE transaction (CHAOS-7291, as Python's Alembic walk): every chain
// revision must be able to run inside a transaction block and must not end the transaction
// itself. A statement that cannot (CREATE INDEX CONCURRENTLY, VACUUM, CREATE DATABASE...) fails
// the whole walk on the server; one that commits (COMMIT, an explicit transaction control
// statement) would commit the revisions before it and break the atomicity. These are refused at
// the chain's source, in a unit test, not on a server at upgrade time.
var nonTransactionalStatements = []struct {
	class   string
	pattern *regexp.Regexp
}{
	{"CREATE/DROP INDEX CONCURRENTLY", regexp.MustCompile(`(?i)\b(?:CREATE|DROP)\s+(?:UNIQUE\s+)?INDEX\s+CONCURRENTLY\b`)},
	{"REINDEX (CONCURRENTLY, DATABASE, SYSTEM)", regexp.MustCompile(`(?i)\bREINDEX\s+(?:\(.*?\)\s*)?(?:CONCURRENTLY|DATABASE|SYSTEM)\b`)},
	{"VACUUM", regexp.MustCompile(`(?i)(?:^|;)\s*VACUUM\b`)},
	{"ALTER TYPE ... ADD VALUE", regexp.MustCompile(`(?i)\bALTER\s+TYPE\b[^;]*\bADD\s+VALUE\b`)},
	{"ALTER SYSTEM", regexp.MustCompile(`(?i)\bALTER\s+SYSTEM\b`)},
	{"CREATE/DROP DATABASE", regexp.MustCompile(`(?i)\b(?:CREATE|DROP)\s+DATABASE\b`)},
	{"CREATE/DROP TABLESPACE", regexp.MustCompile(`(?i)\b(?:CREATE|DROP)\s+TABLESPACE\b`)},
	{"CLUSTER", regexp.MustCompile(`(?i)(?:^|;)\s*CLUSTER\b`)},
	{"transaction control (COMMIT, BEGIN, START TRANSACTION, END, ROLLBACK, SAVEPOINT, PREPARE TRANSACTION)",
		regexp.MustCompile(`(?i)(?:^|;)\s*(?:COMMIT|BEGIN|START\s+TRANSACTION|END|ROLLBACK|ABORT|SAVEPOINT|RELEASE\s+SAVEPOINT|PREPARE\s+TRANSACTION|COMMIT\s+PREPARED)\b`)},
	{"SET TRANSACTION / SET SESSION CHARACTERISTICS", regexp.MustCompile(`(?i)(?:^|;)\s*SET\s+(?:SESSION\s+CHARACTERISTICS|TRANSACTION)\b`)},
	{"LISTEN/NOTIFY across the walk is fine, but COPY ... PROGRAM and server file access are not", regexp.MustCompile(`(?i)\bCOPY\b[^;]*\bPROGRAM\b`)},
}

var (
	sqlLineComment  = regexp.MustCompile(`--[^\n]*`)
	sqlBlockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)
	sqlLiteral      = regexp.MustCompile(`(?s)'(?:[^']|'')*'`)
	sqlDollarTag    = regexp.MustCompile(`\$[A-Za-z_]*\$`)
)

// stripDollarQuoted replaces every $tag$ ... $tag$ body with an empty literal (RE2 has no
// back-reference, so the closing tag is found by search).
func stripDollarQuoted(text string) string {
	var out strings.Builder
	for {
		open := sqlDollarTag.FindStringIndex(text)
		if open == nil {
			out.WriteString(text)
			return out.String()
		}
		tag := text[open[0]:open[1]]
		end := strings.Index(text[open[1]:], tag)
		if end < 0 {
			out.WriteString(text)
			return out.String()
		}
		out.WriteString(text[:open[0]])
		out.WriteString("''")
		text = text[open[1]+end+len(tag):]
	}
}

// statementsThatCannotShareATransaction names the classes of statement in sql that cannot run in
// the walk's single transaction block. Comments, string literals and dollar-quoted bodies are
// removed first, so a word in a comment or a literal is not a statement.
func statementsThatCannotShareATransaction(sql string) []string {
	text := sqlBlockComment.ReplaceAllString(sql, " ")
	text = sqlLineComment.ReplaceAllString(text, " ")
	text = stripDollarQuoted(text)
	text = sqlLiteral.ReplaceAllString(text, "''")
	var found []string
	for _, statement := range nonTransactionalStatements {
		if statement.pattern.MatchString(text) {
			found = append(found, statement.class)
		}
	}
	return found
}

func TestEveryChainRevisionCanRunInTheWalksOneTransaction(t *testing.T) {
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if len(chain) < 7 {
		t.Fatalf("the chain holds %d revisions: the guard measures nothing", len(chain))
	}
	for _, file := range chain {
		if found := statementsThatCannotShareATransaction(file.SQL); len(found) > 0 {
			t.Errorf("%s holds statements that cannot run in the walk's one transaction: %s", file.Name, strings.Join(found, "; "))
		}
	}
}

// A plant for every class: the statement is reported, and the same word in a comment, a string
// literal or a dollar-quoted body is not.
func TestTheTransactionGuardReportsEveryClassAndOnlyStatements(t *testing.T) {
	plants := map[string]string{
		"CREATE/DROP INDEX CONCURRENTLY":           "CREATE INDEX CONCURRENTLY ix ON t (a);",
		"REINDEX (CONCURRENTLY, DATABASE, SYSTEM)": "REINDEX DATABASE app;",
		"VACUUM":                   "ALTER TABLE t ADD COLUMN a int; VACUUM t;",
		"ALTER TYPE ... ADD VALUE": "ALTER TYPE status ADD VALUE 'x';",
		"ALTER SYSTEM":             "ALTER SYSTEM SET work_mem = '1GB';",
		"CREATE/DROP DATABASE":     "CREATE DATABASE other;",
		"CREATE/DROP TABLESPACE":   "CREATE TABLESPACE ts LOCATION '/x';",
		"CLUSTER":                  "CLUSTER t;",
		"transaction control (COMMIT, BEGIN, START TRANSACTION, END, ROLLBACK, SAVEPOINT, PREPARE TRANSACTION)": "ALTER TABLE t ADD COLUMN a int; COMMIT;",
		"SET TRANSACTION / SET SESSION CHARACTERISTICS":                                                         "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;",
		"LISTEN/NOTIFY across the walk is fine, but COPY ... PROGRAM and server file access are not":            "COPY t FROM PROGRAM 'cat /x';",
	}
	if len(plants) != len(nonTransactionalStatements) {
		t.Fatalf("%d plants for %d classes: every class needs one", len(plants), len(nonTransactionalStatements))
	}
	for class, sql := range plants {
		found := statementsThatCannotShareATransaction(sql)
		if len(found) == 0 || found[0] != class && !contains(found, class) {
			t.Errorf("the plant for %q was reported as %v", class, found)
		}
	}
	for _, benign := range []string{
		"-- COMMIT; VACUUM t; CREATE INDEX CONCURRENTLY\nALTER TABLE t ADD COLUMN a int;",
		"/* BEGIN; COMMIT; */ CREATE TABLE t (a text DEFAULT 'COMMIT; VACUUM');",
		"CREATE FUNCTION f() RETURNS void AS $body$ BEGIN PERFORM 1; END; $body$ LANGUAGE plpgsql;",
		"CREATE INDEX ix ON t (a); CREATE UNIQUE INDEX uq ON t (b) WHERE b IS NOT NULL;",
		"ALTER TABLE t ADD COLUMN vacuum_count int; CREATE TABLE commits (id int);",
	} {
		if found := statementsThatCannotShareATransaction(benign); len(found) != 0 {
			t.Errorf("a benign statement was reported as %v: %q", found, benign)
		}
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
