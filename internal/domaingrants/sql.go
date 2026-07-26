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
	// LockRequirements maps a table to the privilege demand its strictest
	// explicit LOCK TABLE places on it. Absent means no LOCK, or only ACCESS
	// SHARE (which SELECT alone satisfies). See LockRequirement.
	LockRequirements map[string]LockRequirement
	// UnparsedLocks is the verbatim text of every LOCK statement this parser
	// could not fully understand, including any with an unrecognised mode. These
	// are REPORTED and FAIL the gate: an unparsed LOCK may demand a privilege on
	// a table that never appears in the derived surface at all, so swallowing it
	// is a silent hole rather than a known limitation.
	UnparsedLocks []string
}

// LockRequirement is the privilege demand one LOCK mode places on its target:
// any ONE privilege in Satisfying permits the lock.
//
// It is a DISJUNCTION, and that is the whole shape. It is not "SELECT and a
// write privilege", and it is not foldable into PrivilegeSet, where every member
// is an independent requirement.
type LockRequirement struct {
	// Mode is the normalized lock mode, e.g. "SHARE ROW EXCLUSIVE".
	Mode string
	// Satisfying holds the privileges any one of which permits this mode.
	Satisfying PrivilegeSet
	// rank orders modes by strictness so the strictest LOCK on a table wins when
	// a table is locked more than once.
	rank int
}

// The lock model, DERIVED FROM THE BACKEND'S OWN ACL CHECK rather than
// reconstructed from prose. Three previous attempts each got a different part
// wrong (a substring test that failed open on bare SHARE; a boolean satisfied by
// any write privilege, which passed SELECT+INSERT against SHARE ROW EXCLUSIVE;
// and a conjunction with SELECT that over-granted every lock-only path), so this
// stops paraphrasing the documentation and encodes the structure of
// LockTableAclCheck (src/backend/commands/lockcmds.c) directly:
//
//	aclmask =  (mode == AccessShareLock)      ? ACL_SELECT
//	        : ((mode <= RowExclusiveLock)     ? ACL_INSERT : 0)
//	aclmask |= ACL_UPDATE | ACL_DELETE | ACL_TRUNCATE | ACL_MAINTAIN
//	pg_class_aclcheck(rel, user, aclmask)     // ANY bit suffices
//
// One OR-mask per mode, tested with pg_class_aclcheck, which succeeds if the
// role holds ANY bit in it. Measured on PostgreSQL 18.4 (server_version_num
// 180004) with each privilege granted ALONE and the LOCK executed as the only
// statement in its transaction:
//
//	MODE (lock only)        NONE    SELECT  INSERT  UPDATE  DELETE  TRUNCATE  MAINTAIN  REFERENCES  TRIGGER
//	ACCESS SHARE            DENIED  ok      ok      ok      ok      ok        ok        DENIED      DENIED
//	ROW SHARE               DENIED  DENIED  ok      ok      ok      ok        ok        DENIED      DENIED
//	ROW EXCLUSIVE           DENIED  DENIED  ok      ok      ok      ok        ok        DENIED      DENIED
//	SHARE UPDATE EXCLUSIVE  DENIED  DENIED  DENIED  ok      ok      ok        ok        DENIED      DENIED
//	SHARE                   DENIED  DENIED  DENIED  ok      ok      ok        ok        DENIED      DENIED
//	SHARE ROW EXCLUSIVE     DENIED  DENIED  DENIED  ok      ok      ok        ok        DENIED      DENIED
//	EXCLUSIVE               DENIED  DENIED  DENIED  ok      ok      ok        ok        DENIED      DENIED
//	ACCESS EXCLUSIVE        DENIED  DENIED  DENIED  ok      ok      ok        ok        DENIED      DENIED
//	<mode omitted>          DENIED  DENIED  DENIED  ok      ok      ok        ok        DENIED      DENIED
//
// Two confounds in the PREVIOUS measurement, both of which made the model
// unfalsifiable in exactly the direction it was wrong, and both avoided here:
//
//   - every grant set began with SELECT, so a conjunction and a disjunction
//     containing SELECT are indistinguishable. The tiers came out right and the
//     OPERATOR came out wrong: a lock-only path authorized by UPDATE alone was
//     reported as also needing SELECT, which over-grants -- the exact failure the
//     two-role split exists to prevent.
//   - the probe appended a read after the LOCK, so no grant set without SELECT
//     could ever reach the lock's own check.
//
// Correction to a claim previously recorded here: the PostgreSQL documentation is
// NOT wrong about ROW SHARE. It says the INSERT-class applies to "ROW EXCLUSIVE or
// a less-conflicting mode", which INCLUDES ROW SHARE, matching the measurement
// above. The earlier note asserting otherwise was a misreading, and a wrong "the
// docs are wrong" note is worse than the original confusion because it teaches the
// next reader to distrust the correct source.
//
// TRUNCATE and MAINTAIN are in the real mask but omitted from Satisfying:
// rolePostureQuery asserts both ABSENT for every runtime role, so neither can ever
// be the privilege that satisfies a lock here. A role holding one would already
// have failed its posture check for a louder reason.
const (
	lockRankSelectOrAny    = iota // ACCESS SHARE
	lockRankInsertOrWrite         // ROW SHARE, ROW EXCLUSIVE
	lockRankUpdateOrDelete        // everything stricter, and an omitted mode
)

// lockDefaultMode is what PostgreSQL uses when the mode is omitted entirely
// (`LOCK TABLE t;`). Measured above: it behaves as ACCESS EXCLUSIVE.
const lockDefaultMode = "ACCESS EXCLUSIVE"

func lockSatisfyingSet(rank int) PrivilegeSet {
	var set PrivilegeSet
	// The UPDATE/DELETE bits are unconditional in the backend's mask.
	set.add(PrivUpdate)
	set.add(PrivDelete)
	if rank <= lockRankInsertOrWrite {
		set.add(PrivInsert)
	}
	if rank == lockRankSelectOrAny {
		set.add(PrivSelect)
	}
	return set
}

// lockRequirementForMode maps a lock mode to its privilege disjunction. The
// second result is FALSE for an unrecognized mode -- the caller must then record
// the statement as unparsed and fail the gate rather than guess.
//
// An earlier version returned a guessed strict set for unknown modes and called
// that fail-closed. It was not: the comparator only reported the mode when the
// GUESSED set was unsatisfied, so a role already holding UPDATE passed silently
// with no manual-verification finding. A guess that happens to be satisfied is
// indistinguishable from knowledge. Unknown input is now a reported fact.
func lockRequirementForMode(mode string) (LockRequirement, bool) {
	normalized := normalizeLockMode(mode)
	switch normalized {
	case "ACCESS SHARE":
		return LockRequirement{Mode: normalized, Satisfying: lockSatisfyingSet(lockRankSelectOrAny), rank: lockRankSelectOrAny}, true
	case "ROW SHARE", "ROW EXCLUSIVE":
		return LockRequirement{Mode: normalized, Satisfying: lockSatisfyingSet(lockRankInsertOrWrite), rank: lockRankInsertOrWrite}, true
	case "SHARE UPDATE EXCLUSIVE", "SHARE", "SHARE ROW EXCLUSIVE", "EXCLUSIVE", "ACCESS EXCLUSIVE":
		return LockRequirement{Mode: normalized, Satisfying: lockSatisfyingSet(lockRankUpdateOrDelete), rank: lockRankUpdateOrDelete}, true
	default:
		return LockRequirement{}, false
	}
}

func normalizeLockMode(mode string) string {
	normalized := strings.TrimSpace(strings.ToUpper(mode))
	return lockModeWhitespaceRE.ReplaceAllString(normalized, " ")
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
	// lockModeWhitespaceRE normalizes the captured mode so "SHARE   ROW
	// EXCLUSIVE" compares equal to "SHARE ROW EXCLUSIVE".
	lockModeWhitespaceRE = regexp.MustCompile(`\s+`)

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
		Tables:           map[string]PrivilegeSet{},
		NonPublicTables:  map[string]bool{},
		LockRequirements: map[string]LockRequirement{},
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

	// LOCK: a DISJUNCTION over a mode-specific set, and nothing else.
	//
	// Note what is NOT here: SELECT is not added. The backend's check is a single
	// OR-mask (see the model above), so a lock-only path authorized by UPDATE
	// alone does not need SELECT, and asserting one over-grants -- precisely the
	// failure the two-role split exists to prevent. An earlier version added
	// PrivSelect for every LOCK, which no mode test could catch because every
	// fixture already granted SELECT.
	//
	// When a table is locked more than once, the STRICTEST mode wins.
	statements, unparsed := parseLockStatements(clean)
	result.UnparsedLocks = append(result.UnparsedLocks, unparsed...)
	for _, statement := range statements {
		requirement, known := lockRequirementForMode(statement.Mode)
		if !known {
			// Unrecognised mode: record the fact, never a guessed requirement.
			result.UnparsedLocks = append(result.UnparsedLocks,
				"unrecognised lock mode "+statement.Mode+" on "+strings.Join(statement.Targets, ", "))
			continue
		}
		for _, target := range statement.Targets {
			table, inScope := splitSchemaQualified(target)
			table = strings.ToLower(table)
			if !inScope || ctes[table] {
				continue
			}
			if existing, ok := result.LockRequirements[table]; ok && existing.rank >= requirement.rank {
				continue
			}
			result.LockRequirements[table] = requirement
		}
	}

	return result
}
