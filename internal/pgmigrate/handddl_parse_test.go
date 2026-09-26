package pgmigrate_test

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Hand-written DDL in Go tests, parsed statically. The guard in
// handddl_guard_integration_test.go compares what this finds against the
// migrated schema (CHAOS-6769, Trap #412). The parser reads SQL text out of
// the Go source; DDL assembled at run time (fmt.Sprintf, concatenation) is not
// seen, which the guard states rather than hides.

// handTable is one CREATE TABLE found in a test file: the columns it declares
// plus those a later ALTER TABLE ... ADD COLUMN in the same file adds.
type handTable struct {
	File    string
	Schema  string
	Table   string
	Columns []string
	// Unreadable holds column definitions whose name is not a plain
	// identifier. The guard fails on one only for a table that exists in the
	// real schema: pseudo-SQL in a test about something else is not drift.
	Unreadable []string
}

var (
	createTableRE = regexp.MustCompile(`(?is)CREATE\s+(?:(TEMP|TEMPORARY)\s+)?(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?((?:"?[A-Za-z_][A-Za-z0-9_]*"?\.)?"?[A-Za-z_][A-Za-z0-9_]*"?)\s*\(`)
	addColumnRE   = regexp.MustCompile(`(?is)ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?((?:"?[A-Za-z_][A-Za-z0-9_]*"?\.)?"?[A-Za-z_][A-Za-z0-9_]*"?)\s+(ADD\s+COLUMN[^;` + "`" + `"]*)`)
	addOneRE      = regexp.MustCompile(`(?is)ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?"?([A-Za-z_][A-Za-z0-9_]*)"?`)
	identRE       = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
	constraintRE  = regexp.MustCompile(`(?i)^(PRIMARY|UNIQUE|FOREIGN|CONSTRAINT|CHECK|EXCLUDE|LIKE)\b`)
	// concatRE joins "a" + "b" and `a` + `b` string pieces into one literal.
	concatRE      = regexp.MustCompile("(?s)(?:\"\\s*\\+\\s*\\\")|(?:`\\s*\\+\\s*`)")
	lineCommentRE = regexp.MustCompile(`--[^\n]*`)
)

// normalizeGoSQL turns the escapes of an interpreted Go string into their
// characters so line comments end where the author ended them.
func normalizeGoSQL(src string) string {
	src = concatRE.ReplaceAllString(src, "")
	r := strings.NewReplacer(`\n`, "\n", `\t`, " ", `\"`, `"`, `\r`, " ")
	return r.Replace(src)
}

func splitTopLevel(body string) []string {
	var parts []string
	depth, start := 0, 0
	for i, ch := range body {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, body[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, body[start:])
}

// matchingParen returns the index just past the ")" closing the "(" at open-1.
func matchingParen(s string, from int) int {
	depth := 1
	for i := from; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func splitQualified(name string) (schema, table string) {
	name = strings.ReplaceAll(name, `"`, "")
	if i := strings.LastIndex(name, "."); i >= 0 {
		return strings.ToLower(name[:i]), strings.ToLower(name[i+1:])
	}
	return "public", strings.ToLower(name)
}

// parseHandTables returns every non-temporary CREATE TABLE in src. A column
// token that is not a plain identifier is kept in Unreadable, never dropped:
// a measurement that did not happen must fail, not shrink the set silently.
func parseHandTables(file, src string) ([]handTable, error) {
	text := lineCommentRE.ReplaceAllString(normalizeGoSQL(src), "")
	var out []handTable
	for _, m := range createTableRE.FindAllStringSubmatchIndex(text, -1) {
		if m[2] >= 0 { // TEMP / TEMPORARY
			continue
		}
		open := m[1]
		end := matchingParen(text, open)
		if end < 0 {
			return nil, fmt.Errorf("%s: unbalanced CREATE TABLE at offset %d", file, m[0])
		}
		schema, table := splitQualified(text[m[4]:m[5]])
		var cols, unreadable []string
		for _, part := range splitTopLevel(text[open:end]) {
			fields := strings.Fields(part)
			if len(fields) == 0 || constraintRE.MatchString(fields[0]) {
				continue
			}
			name := strings.ToLower(strings.Trim(fields[0], `"`))
			if !identRE.MatchString(name) {
				unreadable = append(unreadable, strings.TrimSpace(part))
				continue
			}
			cols = append(cols, name)
		}
		if len(cols) == 0 && len(unreadable) == 0 {
			continue // CREATE TABLE t () -- no column to compare
		}
		out = append(out, handTable{File: file, Schema: schema, Table: table, Columns: cols, Unreadable: unreadable})
	}
	for _, m := range addColumnRE.FindAllStringSubmatch(text, -1) {
		schema, table := splitQualified(m[1])
		for i := range out {
			if out[i].Schema == schema && out[i].Table == table {
				for _, one := range addOneRE.FindAllStringSubmatch(m[2], -1) {
					out[i].Columns = append(out[i].Columns, strings.ToLower(one[1]))
				}
			}
		}
	}
	for i := range out {
		sort.Strings(out[i].Columns)
	}
	return out, nil
}

// probeMaxColumns bounds a PROBE shell: `id` plus a probe column or two.
const probeMaxColumns = 3

// probeVenues are the privilege venues whose hand tables are deliberate shells:
// each test builds a scratch database with hand-made roles and creates a table
// under a production name only so the posture manifest has something to GRANT
// on (the manifest is table-level; no query reads a column). A table here that
// carries a column production lacks is recorded as PROBE, not INVENTED. A file
// is added only with the reason for it.
var probeVenues = map[string]string{
	"internal/storage/postgres/runtime_authorization_integration_test.go":            "runtime posture: GRANT targets for the domain/queue/elevated roles",
	"internal/storage/postgres/domain_authorization_integration_test.go":             "domain posture: GRANT targets, DDL-forbidden probes",
	"internal/storage/postgres/domain_grant_reconciliation_integration_test.go":      "grant reconciliation over shell tables",
	"internal/storage/postgres/coordinator_statement_privileges_integration_test.go": "coordinator statement privileges over shell tables",
	"internal/storage/river/migrate_integration_test.go":                             "river migration role grants over shell tables",
	"internal/syncreconciler/kernel_integration_test.go":                             "reconciler roles over shell tables",
	"internal/syncdispatchruntime/publisher_integration_test.go":                     "publisher roles over shell outbox tables",
	"internal/joboperator/postgres_integration_test.go":                              "operator role posture over shell tables",
	"internal/api/policy/store_integration_test.go":                                  "API posture: readiness requires every declared table",
}

// classifyHandTable names a hand table that differs from the real one (invented and missing are
// the columns it has that production lacks and the reverse; either non-empty).
//
//	INVENTED  the hand table declares a column production lacks, and is not a probe shell.
//	PROBE     a shell of at most probeMaxColumns columns in a privilege venue (probeVenues): a GRANT
//	          target for the posture manifest, whether it carries a probe column production lacks or
//	          only omits columns production has. No query in a venue reads a column of a shell, so
//	          "missing columns" is the point of a shell, not drift.
//	SUBSET    any other table that lacks columns production has: a query that reads one fails.
//
// A larger table in a probe venue is NOT a shell: it keeps INVENTED / SUBSET so it stays on the
// backlog, and a venue that grows a shell past probeMaxColumns leaves PROBE.
func classifyHandTable(file string, columns, invented []string) string {
	_, venue := probeVenues[file]
	if venue && len(columns) <= probeMaxColumns {
		return "PROBE"
	}
	if len(invented) > 0 {
		return "INVENTED"
	}
	return "SUBSET"
}
