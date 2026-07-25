// Package domaingrants derives, from the actual Go query surface reachable
// through the Postgres domain connection pool, which (table, privilege)
// pairs the domain role needs -- and cross-checks that derived set against
// the two hand-maintained artefacts that are supposed to describe it:
//
//   - runtimeGrantStatements in internal/storage/river/migrate.go
//   - required_table_privileges in internal/storage/postgres/domain_authorization.go
//
// Those two lists were each written by restating the other, so on their own
// they always agree; the drift that actually matters is against what the Go
// code executes. See docs/architecture (grant-surface-deriver handoff) for
// design rationale and known limitations, and
// /Users/chris/projects/full-chaos/dev-health/.remember/grant-surface-derivation.md
// for the findings this package produced against origin/main.
package domaingrants

import (
	"regexp"
	"strings"
)

// Privilege is one of the four DML privilege kinds this package reasons
// about. TRUNCATE/REFERENCES/TRIGGER/MAINTAIN are out of scope: no domain
// statement surface exercises them today, and domainAuthorizationQuery
// already unconditionally forbids them for every required table.
type Privilege int

const (
	PrivSelect Privilege = iota
	PrivInsert
	PrivUpdate
	PrivDelete
	numPrivileges
)

func (p Privilege) String() string {
	switch p {
	case PrivSelect:
		return "SELECT"
	case PrivInsert:
		return "INSERT"
	case PrivUpdate:
		return "UPDATE"
	case PrivDelete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// PrivilegeSet is a small bitset over the four privileges above.
type PrivilegeSet [numPrivileges]bool

func (s *PrivilegeSet) add(p Privilege) { s[p] = true }

// Has reports whether p is set.
func (s PrivilegeSet) Has(p Privilege) bool { return s[p] }

// Empty reports whether no privilege is set.
func (s PrivilegeSet) Empty() bool {
	for _, v := range s {
		if v {
			return false
		}
	}
	return true
}

// Union returns the OR of s and other.
func (s PrivilegeSet) Union(other PrivilegeSet) PrivilegeSet {
	var out PrivilegeSet
	for i := range s {
		out[i] = s[i] || other[i]
	}
	return out
}

// StatementResult is what parsing one SQL statement produces.
type StatementResult struct {
	// Tables maps a bare public-schema table name to the privileges this one
	// statement requires on it.
	Tables map[string]PrivilegeSet
	// NonPublicTables records schema-qualified references this parse chose
	// NOT to attribute to a public-schema table (e.g. the River schema) --
	// kept for diagnostics, never compared against required_table_privileges.
	NonPublicTables map[string]bool
	// RequiresAnyWriteLock lists tables LOCK TABLE'd in a mode stricter than
	// ROW EXCLUSIVE (e.g. SHARE ROW EXCLUSIVE), which per Postgres docs
	// requires at least one of INSERT/UPDATE/DELETE/TRUNCATE -- NOT
	// specifically UPDATE. This is intentionally NOT folded into
	// PrivilegeSet.PrivUpdate: doing so produced a real false positive
	// (internal/jobroute/control.go's `LOCK TABLE worker_job_outbox IN
	// SHARE ROW EXCLUSIVE MODE` is already satisfied by the existing
	// SELECT+INSERT grant; Postgres never requires UPDATE specifically
	// here). The comparator checks this against the granted set directly
	// (any of I/U/D satisfies it) instead of comparing a single privilege.
	RequiresAnyWriteLock map[string]bool
}

var (
	commentLineRE  = regexp.MustCompile(`--[^\n]*`)
	commentBlockRE = regexp.MustCompile(`(?s)/\*.*?\*/`)

	// Matches both `name AS (` and `name(col1, col2) AS (` -- a CTE may
	// declare an explicit output column list between its name and AS,
	// e.g. domain_authorization.go's own
	// `required_table_privileges(table_name, allow_insert, allow_update) AS (...)`.
	// Without the optional column-list group, that CTE's name is missed and
	// its own self-reference later in the same query
	// (`FROM required_table_privileges AS required`) gets mis-parsed as a
	// real external table read.
	cteDefRE = regexp.MustCompile(`(?i)\b([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:\([^()]*\)\s*)?AS\s*\(`)

	insertIntoRE = regexp.MustCompile(`(?i)\bINSERT\s+INTO\s+((?:[a-zA-Z_][a-zA-Z0-9_]*\.)?[a-zA-Z_][a-zA-Z0-9_]*)`)
	updateSetRE  = regexp.MustCompile(`(?i)\bUPDATE\s+((?:[a-zA-Z_][a-zA-Z0-9_]*\.)?[a-zA-Z_][a-zA-Z0-9_]*)\s+(?:AS\s+[a-zA-Z_][a-zA-Z0-9_]*\s+)?SET\b`)
	deleteFromRE = regexp.MustCompile(`(?i)\bDELETE\s+FROM\s+(?:ONLY\s+)?((?:[a-zA-Z_][a-zA-Z0-9_]*\.)?[a-zA-Z_][a-zA-Z0-9_]*)`)
	fromJoinRE   = regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+((?:[a-zA-Z_][a-zA-Z0-9_]*\.)?[a-zA-Z_][a-zA-Z0-9_]*)`)
	lockTableRE  = regexp.MustCompile(`(?i)\bLOCK\s+TABLE\s+(?:ONLY\s+)?((?:[a-zA-Z_][a-zA-Z0-9_]*\.)?[a-zA-Z_][a-zA-Z0-9_]*)\s+IN\s+([A-Z ]+?)\s+MODE\b`)

	doUpdateRE = regexp.MustCompile(`(?i)\bON\s+CONFLICT\b[\s\S]*?\bDO\s+UPDATE\b`)
	forWriteRE = regexp.MustCompile(`(?i)\bFOR\s+(?:NO\s+KEY\s+UPDATE|UPDATE|KEY\s+SHARE|SHARE)\b`)
	// forUpdateOfRE captures the OF <table[, table...]> qualifier on a
	// row-locking clause, when present, so escalation can be scoped to
	// exactly the named table(s) instead of every table the statement
	// reads. Captures to end-of-line/string since Postgres puts
	// NOWAIT/SKIP LOCKED (if any) right after the table list with no other
	// delimiter; resolveForUpdateOf strips those tokens off afterward.
	forUpdateOfRE = regexp.MustCompile(`(?i)\bFOR\s+(?:NO\s+KEY\s+UPDATE|UPDATE|KEY\s+SHARE|SHARE)\s+OF\s+([^\n;]+)`)
	aliasRE       = regexp.MustCompile(`(?i)^\s*(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\b`)
)

// sqlClauseKeyword catches words that can legally follow a FROM/JOIN table
// reference without being an alias for it (join conditions, the next
// clause, etc). If aliasRE's capture is one of these, it is NOT recorded as
// an alias.
var sqlClauseKeyword = map[string]bool{
	"on": true, "where": true, "join": true, "left": true, "right": true,
	"inner": true, "full": true, "cross": true, "group": true, "order": true,
	"for": true, "using": true, "set": true, "values": true, "returning": true,
	"limit": true, "offset": true, "and": true, "or": true, "lateral": true,
	"union": true, "intersect": true, "except": true,
}

// sqlKeywordNotATable catches identifiers that legitimately follow FROM/JOIN
// but never name a table: the LATERAL modifier, and (defensively) UNNEST /
// JSON_TABLE-style constructs if they ever appear without an immediate '('.
var sqlKeywordNotATable = map[string]bool{
	"lateral": true,
	"only":    true,
}

// stripComments removes -- line comments and /* */ block comments so they
// cannot smuggle a fake FROM/JOIN/INSERT token into the parse.
func stripComments(sql string) string {
	sql = commentBlockRE.ReplaceAllString(sql, " ")
	sql = commentLineRE.ReplaceAllString(sql, " ")
	return sql
}

// cteNames returns every identifier this statement defines as a CTE (WITH
// name AS ( ... ) or , name AS ( ... )). Over-collecting is safe: it only
// ever suppresses treating a name as a real table, never the reverse, and
// "<ident> AS (" is not a shape real table/subquery aliasing uses elsewhere
// in Postgres SQL.
func cteNames(sql string) map[string]bool {
	names := map[string]bool{}
	for _, m := range cteDefRE.FindAllStringSubmatch(sql, -1) {
		names[strings.ToLower(m[1])] = true
	}
	return names
}

// splitSchemaQualified returns (table, isPublicOrBare). A schema-qualified
// reference to anything other than "public" (most commonly the River
// schema) is out of scope for required_table_privileges, which only
// governs relations in the public schema.
func splitSchemaQualified(raw string) (table string, inScope bool) {
	if idx := strings.IndexByte(raw, '.'); idx >= 0 {
		schema, rest := raw[:idx], raw[idx+1:]
		if !strings.EqualFold(schema, "public") {
			return rest, false
		}
		return rest, true
	}
	return raw, true
}

// findNames runs re over sql and returns the in-scope, non-keyword,
// non-CTE table names it captures in group 1. When skipCall is true, a
// match is treated as a function call (not a table) and discarded when the
// next non-space character after the match is '(' -- Go's RE2 engine has no
// lookahead, so this is done by inspecting the trailing text directly. Only
// FROM/JOIN references need skipCall: a set-returning function
// (generate_series(...)) can appear there, whereas "INSERT INTO t (col,
// ...)" has a legitimate '(' column list immediately after the table name
// that must NOT be mistaken for a function call.
func findNames(re *regexp.Regexp, sql string, ctes map[string]bool, nonPublic map[string]bool, skipCall bool) []string {
	var out []string
	for _, loc := range re.FindAllStringSubmatchIndex(sql, -1) {
		raw := sql[loc[2]:loc[3]]
		if skipCall {
			tail := strings.TrimLeft(sql[loc[3]:], " \t\r\n")
			if strings.HasPrefix(tail, "(") {
				continue // set-returning function call, not a table
			}
		}
		table, inScope := splitSchemaQualified(raw)
		lower := strings.ToLower(table)
		if sqlKeywordNotATable[lower] {
			continue
		}
		if !inScope {
			nonPublic[lower] = true
			continue
		}
		if ctes[lower] {
			continue
		}
		out = append(out, lower)
	}
	return out
}

// findFromJoinWithAliases is findNames specialized for FROM/JOIN: it also
// resolves each table reference's alias (`FROM public.sync_run_units AS
// unit` / bare `FROM public.sync_run_units unit`), returning an
// alias-or-bare-name -> table map. This powers resolveForUpdateOf, which
// needs to know that a `FOR UPDATE OF unit` clause locks
// sync_run_units specifically, not every table the statement joins.
func findFromJoinWithAliases(sql string, ctes map[string]bool, nonPublic map[string]bool) (tables []string, aliasToTable map[string]string) {
	aliasToTable = map[string]string{}
	for _, loc := range fromJoinRE.FindAllStringSubmatchIndex(sql, -1) {
		raw := sql[loc[2]:loc[3]]
		tail := strings.TrimLeft(sql[loc[3]:], " \t\r\n")
		if strings.HasPrefix(tail, "(") {
			continue // set-returning function call, not a table
		}
		table, inScope := splitSchemaQualified(raw)
		lower := strings.ToLower(table)
		if sqlKeywordNotATable[lower] {
			continue
		}
		if !inScope {
			nonPublic[lower] = true
			continue
		}
		if ctes[lower] {
			continue
		}
		tables = append(tables, lower)
		aliasToTable[lower] = lower // the bare/schema-qualified name always resolves to itself
		if m := aliasRE.FindStringSubmatch(tail); m != nil {
			alias := strings.ToLower(m[1])
			if !sqlClauseKeyword[alias] {
				aliasToTable[alias] = lower
			}
		}
	}
	return tables, aliasToTable
}

// resolveForUpdateOf scopes a row-locking clause's UPDATE requirement to
// exactly the table(s) named in its `OF <alias[, alias...]>` qualifier, when
// present (falling back to escalating every table the statement reads, the
// old conservative behavior, when the clause has no OF qualifier). Without
// this, a single multi-join statement that locks ONE table
// (`FOR UPDATE OF unit`) while reading several others purely for enrichment
// would wrongly demand UPDATE on all of them -- confirmed against
// internal/providersync/repository_postgres.go's claimUnitSQL, which joins
// integrations/integration_sources/integration_datasets read-only alongside
// a `FOR UPDATE OF unit` lock on sync_run_units.
func resolveForUpdateOf(clean string, selectTables []string, aliasToTable map[string]string, add func(string, Privilege)) {
	m := forUpdateOfRE.FindStringSubmatch(clean)
	if m == nil {
		if forWriteRE.MatchString(clean) {
			for _, t := range selectTables {
				add(t, PrivUpdate)
			}
		}
		return
	}
	for _, part := range strings.Split(m[1], ",") {
		fields := strings.Fields(part)
		if len(fields) == 0 {
			continue
		}
		ref := strings.ToLower(strings.Trim(fields[0], "\"'"))
		if ref == "nowait" || ref == "skip" {
			continue // trailing NOWAIT/SKIP LOCKED with no preceding comma
		}
		if table, ok := aliasToTable[ref]; ok {
			add(table, PrivUpdate)
			continue
		}
		// Not a known alias from this statement's FROM/JOIN set (e.g. a CTE
		// name, or an alias findFromJoinWithAliases didn't capture) --
		// record it directly rather than silently dropping the lock target;
		// worst case this adds a table that turns out to be a CTE, which a
		// human reviewing the finding will immediately recognize.
		add(ref, PrivUpdate)
	}
}

// ParseStatement extracts, per public-schema table, which of
// SELECT/INSERT/UPDATE/DELETE the given SQL statement text requires.
//
// This is a best-effort regex/tokenizer, not a SQL parser -- see the
// grant-surface-deriver handoff README for the enumerated classes of SQL it
// cannot see (deeply nested CTEs that shadow a real table name, dynamic
// table names, stored-procedure-internal access, etc).
func ParseStatement(sql string) StatementResult {
	clean := stripComments(sql)
	ctes := cteNames(clean)
	result := StatementResult{
		Tables:               map[string]PrivilegeSet{},
		NonPublicTables:      map[string]bool{},
		RequiresAnyWriteLock: map[string]bool{},
	}

	add := func(table string, p Privilege) {
		set := result.Tables[table]
		set.add(p)
		result.Tables[table] = set
	}

	insertTables := findNames(insertIntoRE, clean, ctes, result.NonPublicTables, false)
	for _, t := range insertTables {
		add(t, PrivInsert)
	}
	// Upsert: INSERT ... ON CONFLICT ... DO UPDATE also needs UPDATE on the
	// same insert target(s).
	if doUpdateRE.MatchString(clean) {
		for _, t := range insertTables {
			add(t, PrivUpdate)
		}
	}

	for _, t := range findNames(updateSetRE, clean, ctes, result.NonPublicTables, false) {
		add(t, PrivUpdate)
	}

	for _, t := range findNames(deleteFromRE, clean, ctes, result.NonPublicTables, false) {
		add(t, PrivDelete)
	}

	selectTables, aliasToTable := findFromJoinWithAliases(clean, ctes, result.NonPublicTables)
	for _, t := range selectTables {
		add(t, PrivSelect)
	}

	// SELECT ... FOR UPDATE / FOR NO KEY UPDATE / FOR SHARE / FOR KEY SHARE:
	// Postgres requires UPDATE-class privilege for every row-locking
	// variant, not just SELECT (confirmed against a live server by the
	// sibling lane/domain-grant-reconciliation integration test). When the
	// clause names its target(s) explicitly (`FOR UPDATE OF unit`),
	// resolveForUpdateOf scopes the escalation to exactly those tables;
	// otherwise (no OF qualifier) it conservatively escalates every table
	// the statement reads, because under-attribution is the exact defect
	// class this tool exists to catch and a false negative is worse than a
	// false positive here.
	resolveForUpdateOf(clean, selectTables, aliasToTable, add)

	// LOCK TABLE ... IN <mode> MODE: SELECT is always required; a mode
	// stricter than ROW EXCLUSIVE additionally requires at least one of
	// INSERT/UPDATE/DELETE/TRUNCATE (see RequiresAnyWriteLock's doc
	// comment for why this is not simply added as PrivUpdate).
	for _, m := range lockTableRE.FindAllStringSubmatch(clean, -1) {
		table, inScope := splitSchemaQualified(m[1])
		table = strings.ToLower(table)
		if !inScope || ctes[table] {
			continue
		}
		add(table, PrivSelect)
		if strings.Contains(strings.ToUpper(m[2]), "EXCLUSIVE") {
			result.RequiresAnyWriteLock[table] = true
		}
	}

	return result
}
