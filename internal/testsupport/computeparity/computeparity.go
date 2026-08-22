// Package computeparity compares a compute kind's whole OUTPUT TABLES between
// two implementations (CHAOS-3092 P0).
//
// internal/testsupport/oraclecompare compares one row against one row. That is
// the right boundary for "does this Go row builder produce what Python's does",
// and internal/providersync's readback pairs extend the same machinery to
// "does the row a caller wrote survive ReplacingMergeTree resolution"
// (oracle_readback_integration_test.go, the precedent this package follows for
// its ClickHouse side).
//
// A compute port needs a third boundary neither covers: Python ran a whole job
// against one store, Go ran the same job against another, and the question is
// whether the two stores now hold the same TABLE. That adds set semantics on
// top of row semantics -- a row present on one side only, a differing row
// count, a duplicate key -- and it adds replay: what a second execution over
// the same input does to the table.
//
// It adds NO new comparison vocabulary. Encoding, leaf type tagging, value
// equality, field diffing and exclusion integrity are all oraclecompare's,
// unchanged, so a divergence means the same thing here as it does in every
// oracle pair. What this package owns is only the set and replay layer.
//
// Field completeness comes from the row TYPE. The SELECT list is derived from
// the struct by reflection (oraclecompare.EncodedFieldNames), so a column
// cannot be quietly dropped from the comparison by being forgotten in a
// hand-written column list -- adding a field to the row type adds it to the
// query and to the diff in the same edit.
package computeparity

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/oraclecompare"
)

// RepeatPolicy is what a SECOND execution over the same input is expected to
// do to a table. Declaring it is mandatory because every value here is a real
// producer behaviour somewhere in this platform, and a port that silently
// swaps one for another is broken even when its first run matches perfectly.
type RepeatPolicy string

const (
	// Idempotent: the table is byte-identical after a replay.
	Idempotent RepeatPolicy = "idempotent"
	// AppendDuplicates: the key set is unchanged but rows accumulate. This is
	// what a plain MergeTree producer that never deletes actually does.
	AppendDuplicates RepeatPolicy = "append_duplicates"
	// ReplaceWindow: the key set and row count are unchanged; values may move.
	ReplaceWindow RepeatPolicy = "replace_window"
)

// Table declares one output table under comparison.
//
// Note what is NOT here: a column list. Columns come from the row type.
type Table struct {
	// Name is the physical table, used to build the query.
	Name string
	// OrderBy is appended to the derived SELECT for stable reads. It does not
	// affect any digest -- comparison is set-based -- but a stable order makes
	// sampled failure output reproducible.
	OrderBy string
	// SemanticKey names the encoded fields that identify a row's meaning.
	SemanticKey []string
	// Exclusions are fields deliberately not compared, each with a written
	// reason. oraclecompare.CheckExclusionIntegrity additionally fails any
	// exclusion that never matches a field actually present, so an exclusion
	// cannot outlive the thing it excused.
	Exclusions map[string]string
	// Repeat is the declared replay behaviour, checked by EvaluateRepeat.
	Repeat RepeatPolicy
	// AllowEmpty permits a table that is empty on BOTH sides to count as a
	// match. It defaults to false because two empty tables agree at every
	// level while proving nothing -- usually a fixture that produced nothing,
	// or a filter that matched nothing on both sides.
	AllowEmpty bool
}

// Query is the SELECT this table is read with, derived from the row type so it
// cannot drift from what gets compared.
func Query[T any](table Table) string {
	columns := oraclecompare.EncodedFieldNames(reflect.TypeOf((*T)(nil)).Elem())
	query := "SELECT " + strings.Join(columns, ", ") + " FROM " + table.Name
	if table.OrderBy != "" {
		query += " ORDER BY " + table.OrderBy
	}
	return query
}

// Snapshot is one side's whole-table read, already encoded through
// oraclecompare so both sides are held to one encoding rule.
type Snapshot struct {
	Table string
	Side  string
	Rows  []map[string]any
}

// Encode turns concrete, production-typed rows into a Snapshot.
//
// Rows must be the real row struct the producer persists, never a hand-built
// map: TypedEncode reflects every field the type declares, so the type system
// -- not a second hand-maintained list -- is what guarantees the comparison
// saw every column.
func Encode[T any](t *testing.T, table Table, side string, rows []T) Snapshot {
	t.Helper()
	encoded := make([]map[string]any, 0, len(rows))
	for index, row := range rows {
		asMap, ok := oraclecompare.TypedEncode(t, reflect.ValueOf(row)).(map[string]any)
		if !ok {
			t.Fatalf("%s row %d of %s did not encode to a map", side, index, table.Name)
		}
		encoded = append(encoded, asMap)
	}
	return Snapshot{Table: table.Name, Side: side, Rows: encoded}
}

// KeyOf renders a row's semantic key. It fails closed on a key field that is
// missing from the encoded row: a key that silently reads as empty would
// collapse distinct rows into one bucket and hide a missing row behind an
// extra one.
func KeyOf(t *testing.T, table Table, row map[string]any) string {
	t.Helper()
	if len(table.SemanticKey) == 0 {
		t.Fatalf("table %s declares no semantic key", table.Name)
	}
	parts := make([]string, 0, len(table.SemanticKey))
	for _, column := range table.SemanticKey {
		value, present := row[column]
		if !present {
			t.Fatalf(
				"table %s: semantic key field %q is absent from an encoded row -- "+
					"the key would silently collapse distinct rows",
				table.Name, column,
			)
		}
		// Length-prefixed: a delimiter inside a value must not be able to
		// forge a field boundary and make two different keys collide.
		rendered := fmt.Sprintf("%v", value)
		parts = append(parts, fmt.Sprintf("%d:%s", len(rendered), rendered))
	}
	return strings.Join(parts, "\x1f")
}

// Compare diffs two whole-table snapshots by semantic key and returns every
// divergence found, WITHOUT failing t for the divergences themselves (setup
// problems still fail immediately). Returning messages is what lets a negative
// control assert that a specific injected defect IS reported, without the
// assertion itself turning the enclosing test red.
//
// Levels, none short-circuiting the others, because a lane needs to see WHICH
// rows moved and not merely that the totals disagree:
//
//  1. absolute row count
//  2. keys present on one side only
//  3. key multiplicity, for producers that can write a key twice
//  4. field-by-field diff per shared key, via oraclecompare.DiffRows
//  5. batch-level exclusion integrity, via oraclecompare.CheckExclusionIntegrity
func Compare(t *testing.T, table Table, left, right Snapshot) []string {
	t.Helper()
	if len(left.Rows) == 0 && len(right.Rows) == 0 && !table.AllowEmpty {
		return []string{fmt.Sprintf(
			"table %s: empty on BOTH sides -- identical counts and identical rows, "+
				"and nothing compared. That is an absence of evidence, not parity; "+
				"set AllowEmpty only if this table is legitimately allowed to be empty",
			table.Name,
		)}
	}

	leftByKey := groupByKey(t, table, left)
	rightByKey := groupByKey(t, table, right)

	var messages []string
	if len(left.Rows) != len(right.Rows) {
		messages = append(messages, fmt.Sprintf(
			"table %s: row count %s=%d %s=%d",
			table.Name, left.Side, len(left.Rows), right.Side, len(right.Rows),
		))
	}

	for _, key := range sortedKeys(leftByKey) {
		if _, present := rightByKey[key]; !present {
			messages = append(messages, fmt.Sprintf(
				"table %s, key %s: present in %s but absent from %s",
				table.Name, key, left.Side, right.Side,
			))
		}
	}
	for _, key := range sortedKeys(rightByKey) {
		if _, present := leftByKey[key]; !present {
			messages = append(messages, fmt.Sprintf(
				"table %s, key %s: present in %s but absent from %s",
				table.Name, key, right.Side, left.Side,
			))
		}
	}

	leftRowsByCase := map[string]map[string]any{}
	rightRowsByCase := map[string]map[string]any{}
	for _, key := range sortedKeys(leftByKey) {
		leftGroup, rightGroup := leftByKey[key], rightByKey[key]
		if len(rightGroup) == 0 {
			continue
		}
		if len(leftGroup) != len(rightGroup) {
			messages = append(messages, fmt.Sprintf(
				"table %s, key %s: %s has %d rows for this key, %s has %d",
				table.Name, key, left.Side, len(leftGroup), right.Side, len(rightGroup),
			))
			continue
		}
		for index := range leftGroup {
			caseID := fmt.Sprintf("%s key %s", table.Name, key)
			if len(leftGroup) > 1 {
				caseID = fmt.Sprintf("%s key %s copy %d", table.Name, key, index)
			}
			leftRowsByCase[caseID] = leftGroup[index]
			rightRowsByCase[caseID] = rightGroup[index]
			messages = append(messages, oraclecompare.DiffRows(
				caseID, leftGroup[index], rightGroup[index], table.Exclusions, nil,
			)...)
		}
	}

	// Exclusion integrity asks "did each declared exclusion match a field
	// present in at least one row across this batch". Over a batch with no
	// rows at all that question has no answer: every exclusion would be
	// reported stale, which is a false finding rather than a discovery --
	// staleness is UNKNOWABLE over zero rows, not proven. This only arises
	// for a table that declared AllowEmpty, since an empty pair is otherwise
	// already refused above.
	if len(left.Rows) > 0 || len(right.Rows) > 0 {
		messages = append(messages, oraclecompare.CheckExclusionIntegrity(
			leftRowsByCase, rightRowsByCase, table.Exclusions, nil,
		)...)
	}
	return messages
}

// EvaluateRepeat checks one side's replay against its declared policy.
//
// This is a single-implementation behavioural claim, evaluated per side and per
// replay: a producer can honour its policy on the first replay and drift on the
// third, and a port that is row-equal on run one but replays differently is
// still not a correct port.
func EvaluateRepeat(table Table, before, after Snapshot) []string {
	observed := observeRepeat(table, before, after)
	if observed == table.Repeat {
		return nil
	}
	return []string{fmt.Sprintf(
		"table %s, side %s: declared repeat policy %q but a replay behaved as %q "+
			"(rows %d -> %d)",
		table.Name, before.Side, table.Repeat, observed, len(before.Rows), len(after.Rows),
	)}
}

func observeRepeat(table Table, before, after Snapshot) RepeatPolicy {
	beforeKeys := distinctKeys(table, before)
	afterKeys := distinctKeys(table, after)
	keysStable := reflect.DeepEqual(beforeKeys, afterKeys)
	switch {
	case !keysStable:
		return "changed_key_set"
	case len(after.Rows) == len(before.Rows) && sameRows(before, after):
		return Idempotent
	case len(after.Rows) > len(before.Rows):
		return AppendDuplicates
	case len(after.Rows) == len(before.Rows):
		return ReplaceWindow
	default:
		return "rows_disappeared"
	}
}

func sameRows(before, after Snapshot) bool {
	if len(before.Rows) != len(after.Rows) {
		return false
	}
	beforeRendered := renderedRows(before)
	afterRendered := renderedRows(after)
	return reflect.DeepEqual(beforeRendered, afterRendered)
}

func renderedRows(snapshot Snapshot) []string {
	rendered := make([]string, 0, len(snapshot.Rows))
	for _, row := range snapshot.Rows {
		rendered = append(rendered, fmt.Sprintf("%v", sortedRow(row)))
	}
	sort.Strings(rendered)
	return rendered
}

func sortedRow(row map[string]any) []string {
	parts := make([]string, 0, len(row))
	for key, value := range row {
		parts = append(parts, fmt.Sprintf("%s=%v", key, value))
	}
	sort.Strings(parts)
	return parts
}

// distinctKeys is deliberately a SET: multiplicity is reported by the count
// level and by the per-key multiplicity check, not folded in here. Without
// that, an append-on-replay producer would read as a changed key set and hide
// its actual behaviour.
func distinctKeys(table Table, snapshot Snapshot) []string {
	seen := map[string]struct{}{}
	for _, row := range snapshot.Rows {
		seen[keyOfQuiet(table, row)] = struct{}{}
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func keyOfQuiet(table Table, row map[string]any) string {
	parts := make([]string, 0, len(table.SemanticKey))
	for _, column := range table.SemanticKey {
		rendered := fmt.Sprintf("%v", row[column])
		parts = append(parts, fmt.Sprintf("%d:%s", len(rendered), rendered))
	}
	return strings.Join(parts, "\x1f")
}

func groupByKey(t *testing.T, table Table, snapshot Snapshot) map[string][]map[string]any {
	t.Helper()
	grouped := map[string][]map[string]any{}
	for _, row := range snapshot.Rows {
		key := KeyOf(t, table, row)
		grouped[key] = append(grouped[key], row)
	}
	return grouped
}

func sortedKeys(grouped map[string][]map[string]any) []string {
	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Execution records what a harness OBSERVED running for one side of a
// comparison. It is not a label the caller chose.
type Execution struct {
	// Side is the label used in divergence messages. Cosmetic only.
	Side string
	// Program is the resolved executable the harness actually invoked.
	Program string
	// EntryPoint is the script or subcommand it ran, resolved to an absolute
	// path where one exists.
	EntryPoint string
}

// Identity is what distinguishes one implementation from another: the resolved
// binary plus the entry point inside it. Two sides that ran the same script
// through the same interpreter have the same identity no matter what the test
// called them.
func (e Execution) Identity() string {
	return sha256Hex(e.Program + "\x1f" + e.EntryPoint)
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// RunProducer executes one side's producer and returns what actually ran.
//
// The harness resolves the executable itself and records it, so producer
// identity is OBSERVED rather than declared. An earlier version of this guard
// took a caller-supplied implementation string, which meant a port test could
// keep invoking the Python reference on both sides, label one of them "go",
// and satisfy the guard while proving nothing -- the exact degradation the
// guard exists to prevent, re-entering through its own input.
func RunProducer(
	t *testing.T, side, workingDir string, environment []string, argv ...string,
) Execution {
	t.Helper()
	if len(argv) < 2 {
		t.Fatalf("%s: a producer invocation needs an executable and an entry point", side)
	}
	program, err := exec.LookPath(argv[0])
	if err != nil {
		t.Fatalf("%s: resolve %q: %v", side, argv[0], err)
	}
	// EXECUTE the path as given, IDENTIFY by its canonical form. These must
	// stay separate: a virtualenv interpreter is a symlink to a system one and
	// works only when invoked through its OWN path -- running the symlink
	// target directly gets the system interpreter and none of the venv's
	// packages. Canonicalizing before exec silently did that, and every
	// producer failed with ModuleNotFoundError.
	entryPoint := argv[1]
	identityEntryPoint := entryPoint
	if resolved, err := filepath.Abs(entryPoint); err == nil {
		if _, statErr := os.Stat(resolved); statErr == nil {
			identityEntryPoint = canonicalPath(resolved)
		}
	}
	command := exec.Command(program, argv[1:]...)
	command.Dir = workingDir
	command.Env = environment
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("%s: %s failed: %v\n%s", side, strings.Join(argv, " "), err, output)
	}
	return Execution{
		Side:       side,
		Program:    canonicalPath(program),
		EntryPoint: identityEntryPoint,
	}
}

// canonicalPath resolves symlinks so two names for the same file are one
// identity. It is used ONLY to compute identity, never to choose what to
// execute -- see RunProducer for why that distinction is load-bearing. Without it, a symlink or a copied wrapper pointing at the
// reference producer would read as a different implementation and could
// satisfy the port-proof guard while running the reference on both sides.
//
// This closes the aliasing hole, and it is where the guard's ambition stops.
// It does NOT hash executable contents, nor detect a wrapper that re-execs the
// reference producer underneath a different name -- that needs execution
// provenance this repo has no plane for, and it is a decoy someone would have
// to build on purpose rather than the realistic mistake (forgetting to point
// one side at the native executor), which this does catch.
func canonicalPath(path string) string {
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	return path
}

// RequirePortProof fails unless the two sides were produced by DIFFERENT
// implementations, judged by what the harness observed running.
//
// A port proof that runs the reference producer on both sides proves only that
// the comparator detects injected mutations and that the reference is
// reproducible. It says nothing about the port -- and it stays green when the
// port is broken, missing, or wired to the wrong entry point, which is the
// worst possible failure mode for a release gate. Every kind's port test must
// call this; a comparator self-test deliberately must not, and should say so
// in its name.
func RequirePortProof(t *testing.T, left, right Execution) {
	t.Helper()
	if violation := PortProofViolation(left, right); violation != "" {
		t.Fatal(violation)
	}
}

// PortProofViolation returns why this pair does not constitute a port proof,
// or "" if it does.
//
// Pure, for the same reason oraclecompare's DiffRows is pure: a guard's own
// logic is exactly the code that must not be trusted on the strength of the
// runs it reports passing, and it cannot be unit-tested through a t.Fatal.
func PortProofViolation(left, right Execution) string {
	if left.Program == "" || right.Program == "" {
		return "a port proof must record what actually ran on both sides"
	}
	if left.Identity() == right.Identity() {
		return fmt.Sprintf(
			"both sides ran the same implementation (%s %s), so this proves nothing "+
				"about a port: it shows the comparator detects injected differences "+
				"and that the reference is reproducible. Point one side at the native "+
				"executor, or name this test a comparator self-test",
			left.Program, left.EntryPoint,
		)
	}
	return ""
}
