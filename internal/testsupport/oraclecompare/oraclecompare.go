// Package oraclecompare is the single Python<->Go row-comparison vocabulary
// this repository has (CHAOS-3162, extracted under CHAOS-3092 P0).
//
// It was lifted verbatim out of internal/providersync/oracle_compare_test.go,
// where it had been unexported inside a _test.go file and therefore importable
// by nothing. Every rule it encodes -- exhaustive reflection over the concrete
// Go row type, {"t","v"} type-tagged leaves, failure on ANY undeclared
// divergence, exclusions that must carry a written reason AND must actually
// match something, float/datetime canonicalization -- was hard-won against
// several rounds of adversarial review, and the point of extracting it is that
// the next comparator built in this repo REUSES it instead of re-deriving a
// second, subtly different vocabulary alongside it.
//
// The orchestration around a comparison deliberately does NOT live here:
// choosing a Python interpreter, shelling out to a pair's build_row, recording
// live-oracle proofs, and embedding the pair sources all stay with the package
// that owns the pairs, because those are bound to that package's own testdata.
// What lives here is only the part every crossing shares -- Python<->Go,
// write<->readback, or Python-store<->Go-store alike: how two rows are encoded
// and how they are compared.
//
// This package imports "testing" and is intended for use from _test.go files
// only, matching the existing internal/testsupport convention (see
// internal/testsupport/chschema).
package oraclecompare

import (
	"bytes"
	"fmt"
	"io/fs"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// DateValued is how a Go type declares that it persists as a calendar DAY
// rather than as a string or an instant, so TypedEncode tags it "date" to
// match what Python's _encode emits for datetime.date.
//
// The method is EXPORTED deliberately. It was `oracleDate()` while this code
// lived inside package providersync, and an interface whose method name is
// unexported can only ever be satisfied from the package that declares it --
// so the moment the encoder moved here, every providersync day type silently
// stopped matching and got tagged "str" instead. Nothing failed to compile;
// the live-python-oracle lane caught it as ~22 failing pairs reporting
// python={"t":"date"} against go={"t":"str"}. An exported method is what makes
// this contract implementable from any package, which is the whole point of
// the extraction.
type DateValued interface {
	OracleDate() string
}

// Case is one input case for a generic Python<->Go oracle comparison. Input
// must be JSON-serializable exactly as the target pair's Python
// build_row(case) expects it.
type Case struct {
	ID    string
	Input map[string]any
}

// AssertSourcesUnchangedSinceBuild re-reads every file in an embedded source
// set from disk and fails if it no longer matches what go:embed baked into the
// test binary.
//
// The embed directive itself stays with the package that owns the sources --
// go:embed paths are package-relative, and the files it names are that
// package's testdata. Its purpose is to make the compiled test binary's content
// hash sensitive to Python-only edits, so `go test`'s result cache cannot serve
// a stale PASS after a Python file changes with no Go file change. This half is
// the run-time check that the embedded set and the on-disk set still agree,
// which would only diverge if the directive's file list drifted from what the
// comparison actually executes. It takes the source set and the base directory
// as parameters precisely so it is not bound to any one package's testdata.
func AssertSourcesUnchangedSinceBuild(t *testing.T, sources fs.FS, baseDir string) {
	t.Helper()
	err := fs.WalkDir(sources, ".", func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		embedded, readErr := fs.ReadFile(sources, path)
		if readErr != nil {
			return readErr
		}
		onDisk, readErr := os.ReadFile(filepath.Join(baseDir, path))
		if readErr != nil {
			return readErr
		}
		if !bytes.Equal(embedded, onDisk) {
			t.Fatalf("embedded copy of %s does not match the on-disk file -- this "+
				"should be impossible outside of a build-cache bug, since go:embed "+
				"reads the same file go test just compiled from; if you see this, "+
				"rebuild with -count=1 and report it", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk embedded oracle sources: %v", err)
	}
}

// CaseDivergencePrefix is the exact prefix DiffRows's per-field messages
// start with -- used to attribute a flat message list back to the case it
// concerns for per-case subtests. %q's own closing quote plus the trailing
// comma keeps "foo" from matching a message about "foobar".
func CaseDivergencePrefix(caseID string) string {
	return fmt.Sprintf("case %q,", caseID)
}

// CheckExclusionIntegrity is CHAOS-3162's third-review fix: a declared
// exclusion is a CLAIM, and nothing previously checked either claim
// against what the batch's rows actually contained.
//
//  1. goOnlyFields[key] asserts "the Python side structurally cannot have
//     this field". If key shows up in ANY case's Python row anyway, that
//     assertion is false: the field IS present and comparable on the
//     Python side, and excluding it hides a real, comparable value --
//     exactly the undeclared-divergence risk this whole framework exists
//     to rule out, smuggled in through the exclusion mechanism meant to
//     prevent it.
//  2. A declared exclusion (either map) that never matches a key present
//     in ANY case's row, across the whole batch, is stale: a typo, a
//     field renamed/removed without updating the pair, or one that was
//     never real. Silent either way, and indistinguishable from a
//     genuine, currently-effective exclusion without this check.
//
// Both are necessarily BATCH-level properties (checkable only once every
// case's rows are known), not per-case ones, which is why this takes the
// whole case-keyed map rather than being folded into DiffRows.
//
// This is a pure function precisely so it can be unit-tested with
// synthetic data (TestCheckExclusionIntegrityClauseCoverage) without a
// Python subprocess -- the same reason DiffRows is pure and
// TestDiffRowsClauseCoverage exists.
func CheckExclusionIntegrity(
	pythonRowsByCase, goRowsByCase map[string]map[string]any,
	excludedFields, goOnlyFields map[string]string,
) []string {
	// usedExclusions tracks, across the WHOLE batch, which declared
	// exclusions actually matched a key present in at least one case's
	// Python or Go row. A "go:" prefix disambiguates a goOnlyFields key
	// from a same-named excludedFields key, since the two maps are
	// independent declarations that happen to share a namespace.
	usedExclusions := map[string]bool{}
	caseIDs := make([]string, 0, len(pythonRowsByCase))
	for caseID := range pythonRowsByCase {
		caseIDs = append(caseIDs, caseID)
	}
	sort.Strings(caseIDs)

	var messages []string
	for _, caseID := range caseIDs {
		pythonRow, goRow := pythonRowsByCase[caseID], goRowsByCase[caseID]
		for key := range pythonRow {
			if reason, claimedGoOnly := goOnlyFields[key]; claimedGoOnly {
				messages = append(messages, fmt.Sprintf(
					"goOnlyFields[%q] (declared: %q) claims this field never appears on "+
						"the Python side, but case %q's Python row has it -- this field is "+
						"NOT actually Go-only, and excluding it hides a real, comparable value",
					key, reason, caseID))
			}
			if _, declared := excludedFields[key]; declared {
				usedExclusions[key] = true
			}
		}
		for key := range goRow {
			if _, declared := excludedFields[key]; declared {
				usedExclusions[key] = true
			}
			if _, declared := goOnlyFields[key]; declared {
				usedExclusions["go:"+key] = true
			}
		}
	}

	for key, reason := range excludedFields {
		if !usedExclusions[key] {
			messages = append(messages, fmt.Sprintf(
				"excluded_fields[%q] (declared: %q) never matched a key present in any "+
					"case's Python or Go row across this batch -- a stale exclusion, not "+
					"a currently-effective one", key, reason))
		}
	}
	for key, reason := range goOnlyFields {
		if !usedExclusions["go:"+key] {
			messages = append(messages, fmt.Sprintf(
				"goOnlyFields[%q] (declared: %q) never matched a key present in any "+
					"case's Go row across this batch -- a stale exclusion, not a "+
					"currently-effective one", key, reason))
		}
	}
	sort.Strings(messages)
	return messages
}

// TypedEncode walks v exhaustively via reflection and produces the SAME
// type-tagged wire shape python_generic_row_oracle.py's _encode produces
// (codex finding #2): every leaf becomes {"t": "<tag>", "v": "<string>"},
// nil pointers/interfaces become bare JSON null (unambiguous, no tag
// needed), and structs/maps/slices recurse. Struct fields are named by
// their `json:"..."` tag (falling back to the Go field name, matching
// encoding/json's own default) so the wire field names match what the row
// actually persists as.
//
// Because this walks reflect.Type's fields EXHAUSTIVELY -- there is no
// mechanism to skip one -- a caller cannot construct a "narrowed" struct
// type that quietly hides a field the way a hand-built map[string]any
// could: the completeness guarantee this closes (codex finding #1) comes
// from the Go type system itself, not from a second, parallel, hand
// maintained field list that could drift from the struct it describes.
func TypedEncode(t *testing.T, v reflect.Value) any {
	t.Helper()
	for v.IsValid() && (v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return nil
	}
	if timeValue, ok := v.Interface().(time.Time); ok {
		return map[string]any{"t": "datetime", "v": timeValue.UTC().Format(time.RFC3339Nano)}
	}
	if integer, ok := v.Interface().(big.Int); ok {
		return map[string]any{"t": "int", "v": integer.String()}
	}
	if identifier, ok := v.Interface().(uuid.UUID); ok {
		return map[string]any{"t": "uuid", "v": strings.ToLower(identifier.String())}
	}
	// A calendar day is not a string and not an instant. Python's _encode tags
	// datetime.date as "date", so a Go day type whose underlying kind is string
	// would otherwise be tagged "str" and compare unequal to EVERY Python date
	// -- or, worse, compare equal to a genuinely-string field that happens to
	// hold the same text. Types that persist as a ClickHouse Date implement
	// DateValued to opt into the matching tag.
	if dated, ok := v.Interface().(DateValued); ok {
		return map[string]any{"t": "date", "v": dated.OracleDate()}
	}
	switch v.Kind() {
	case reflect.String:
		return map[string]any{"t": "str", "v": v.String()}
	case reflect.Bool:
		return map[string]any{"t": "bool", "v": strconv.FormatBool(v.Bool())}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return map[string]any{"t": "int", "v": strconv.FormatInt(v.Int(), 10)}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"t": "int", "v": strconv.FormatUint(v.Uint(), 10)}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"t": "float", "v": strconv.FormatFloat(v.Float(), 'g', -1, 64)}
	case reflect.Struct:
		result := make(map[string]any, v.NumField())
		structType := v.Type()
		for i := 0; i < v.NumField(); i++ {
			field := structType.Field(i)
			if field.PkgPath != "" {
				continue // unexported: not part of the persisted row.
			}
			name := jsonFieldName(field)
			if name == "-" {
				continue
			}
			result[name] = TypedEncode(t, v.Field(i))
		}
		return result
	case reflect.Map:
		result := make(map[string]any, v.Len())
		for _, key := range v.MapKeys() {
			keyString, ok := key.Interface().(string)
			if !ok {
				t.Fatalf("TypedEncode: unsupported non-string map key type %s", key.Type())
			}
			result[keyString] = TypedEncode(t, v.MapIndex(key))
		}
		return result
	case reflect.Slice, reflect.Array:
		result := make([]any, v.Len())
		for i := 0; i < v.Len(); i++ {
			result[i] = TypedEncode(t, v.Index(i))
		}
		return result
	default:
		t.Fatalf("TypedEncode: unsupported kind %s for type %s -- every leaf type "+
			"this comparator can see must have an explicit, type-tagged encoding; "+
			"falling through untagged would silently reopen codex finding #2", v.Kind(), v.Type())
		return nil
	}
}

// EncodedFieldNames returns, in declaration order, the wire field names
// TypedEncode will produce for a struct type -- the same names, by the same
// rule, so a caller that needs to know a row type's fields ahead of encoding
// it cannot drift from what the encoder actually emits.
//
// This is what lets a whole-table comparison derive its SELECT list FROM the
// row type instead of carrying a hand-written column list beside it. A hand
// list is a second, parallel declaration of the same thing, and the moment it
// falls behind the struct, the column it forgot is simply never compared and
// the comparison still reports a pass.
func EncodedFieldNames(rowType reflect.Type) []string {
	for rowType.Kind() == reflect.Ptr {
		rowType = rowType.Elem()
	}
	if rowType.Kind() != reflect.Struct {
		return nil
	}
	names := make([]string, 0, rowType.NumField())
	for i := 0; i < rowType.NumField(); i++ {
		field := rowType.Field(i)
		if field.PkgPath != "" {
			continue // unexported: not part of the persisted row.
		}
		name := jsonFieldName(field)
		if name == "-" {
			continue
		}
		names = append(names, name)
	}
	return names
}

func jsonFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag == "" {
		return field.Name
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "" {
		return field.Name
	}
	return name
}

// DiffRows is CHAOS-3162's actual comparison logic: every key present on
// EITHER side must either match on both, or be declared excluded
// (Python-declared via excluded_fields, or Go-only via goOnlyFields) with a
// written reason. A key present on one side and absent on the other is
// exactly as much a divergence as a key with different values on each side
// -- neither is treated as "probably fine". Returns one message per
// divergence found (nil/empty = the rows matched under the declared
// exclusions).
//
// codex finding #6 (second review): if every key in the union is excluded,
// this returns zero divergences even though nothing was actually compared
// -- indistinguishable, from the caller's side, from a real match. Fail
// loudly instead: a case where every field is excluded is a caller
// configuration error (probably an over-broad goOnlyFields), not a pass.
func DiffRows(
	caseID string,
	pythonRow, goRow map[string]any,
	pythonExcluded, goOnlyFields map[string]string,
) []string {
	allKeys := map[string]struct{}{}
	for key := range pythonRow {
		allKeys[key] = struct{}{}
	}
	for key := range goRow {
		allKeys[key] = struct{}{}
	}
	if len(allKeys) == 0 {
		return []string{fmt.Sprintf(
			"case %q: both rows are empty -- nothing was compared, which is a "+
				"setup error, not a pass", caseID)}
	}
	sortedKeys := make([]string, 0, len(allKeys))
	for key := range allKeys {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	var messages []string
	comparedFields := 0
	for _, key := range sortedKeys {
		if _, excluded := pythonExcluded[key]; excluded {
			continue
		}
		if _, excluded := goOnlyFields[key]; excluded {
			continue
		}
		comparedFields++
		pythonValue, pythonHas := pythonRow[key]
		goValue, goHas := goRow[key]
		switch {
		case pythonHas && !goHas:
			messages = append(messages, fmt.Sprintf(
				"case %q, field %q: present in Python's row (%v) but absent from "+
					"Go's -- declare an exclusion with a reason if this is intentional, "+
					"don't leave it silently missing", caseID, key, pythonValue))
		case !pythonHas && goHas:
			messages = append(messages, fmt.Sprintf(
				"case %q, field %q: present in Go's row (%v) but absent from "+
					"Python's -- declare an exclusion with a reason if this is intentional, "+
					"don't leave it silently missing", caseID, key, goValue))
		case !TypedValuesEqual(pythonValue, goValue):
			messages = append(messages, fmt.Sprintf(
				"case %q, field %q: python=%#v go=%#v", caseID, key, pythonValue, goValue))
		}
	}
	if comparedFields == 0 {
		return []string{fmt.Sprintf(
			"case %q: every field in both rows is declared excluded -- nothing was "+
				"actually compared, which is a caller configuration error (an "+
				"over-broad exclusion list), not a pass", caseID)}
	}
	return messages
}

// TypedValuesEqual compares two type-tagged leaf values (codex adversarial
// review, CHAOS-3162 fourth round). Plain reflect.DeepEqual on the {"t":
// ..., "v": "<string>"} envelope is exactly right for str/int/bool -- those
// three types each have exactly one canonical string form on both sides
// (an int's digit sequence, a bool's "true"/"false", a string verbatim) --
// but it is WRONG for float and datetime, which do not:
//   - Python's `repr(5.0)` is `"5.0"`; Go's `strconv.FormatFloat(5.0, 'g',
//     -1, 64)` is `"5"`. Same IEEE754 double, different text.
//   - Python's `datetime.isoformat()` shows microseconds as a fixed
//     6-digit field when non-zero (`.123000Z`); Go's `time.RFC3339Nano`
//     strips trailing zero fractional digits (`.123Z`). Same instant,
//     different text.
//
// Both gaps were latent, not active: every case in this package so far
// uses whole-second or millisecond-truncated timestamps and no float
// fields, so the mismatch never fired -- exactly why it needed fixing at
// the comparator level rather than by adding a case that happens to dodge
// it. Values are parsed back and compared numerically/temporally instead
// of as text; everything else still falls through to reflect.DeepEqual
// unchanged.
// A pair whose row is COLUMN-ORIENTED (one key per production field, holding
// the ordered list of that field's values across every record the case
// produced) puts its leaves one level down, inside a []any. A top-level-only
// canonicalization would then compare those lists with reflect.DeepEqual on
// their raw text and report `[5.0]` vs `[5]` as a divergence -- reintroducing
// the exact false positive the scalar path exists to remove, for every pair
// that compares a list of records rather than a single row. So recurse:
// containers are compared element-wise with this same function, and only
// genuine leaves reach the text/DeepEqual comparison.
func TypedValuesEqual(pythonValue, goValue any) bool {
	pythonTagged, pythonOK := asTaggedValue(pythonValue)
	goTagged, goOK := asTaggedValue(goValue)
	if pythonOK && goOK && pythonTagged.tag == goTagged.tag {
		switch pythonTagged.tag {
		case "float":
			// Fail closed on both halves of "this is not a comparable
			// number". A parse failure used to fall through to
			// reflect.DeepEqual on the envelope, so two sides carrying the
			// SAME malformed text compared equal -- a corrupted producer or
			// fixture reporting as a clean match. And an infinity parses
			// fine, so +Inf on both sides also compared equal, which hides
			// the broken computation that produced it rather than surfacing
			// it. A value arithmetic cannot be trusted on is never a match.
			pythonFloat, pythonErr := strconv.ParseFloat(pythonTagged.value, 64)
			goFloat, goErr := strconv.ParseFloat(goTagged.value, 64)
			if pythonErr != nil || goErr != nil {
				return false
			}
			if math.IsNaN(pythonFloat) || math.IsNaN(goFloat) ||
				math.IsInf(pythonFloat, 0) || math.IsInf(goFloat, 0) {
				return false
			}
			return pythonFloat == goFloat
		case "datetime":
			pythonTime, pythonErr := time.Parse(time.RFC3339Nano, pythonTagged.value)
			goTime, goErr := time.Parse(time.RFC3339Nano, goTagged.value)
			if pythonErr == nil && goErr == nil {
				return pythonTime.Equal(goTime)
			}
		}
		return reflect.DeepEqual(pythonValue, goValue)
	}
	switch python := pythonValue.(type) {
	case []any:
		other, ok := goValue.([]any)
		if !ok || len(other) != len(python) {
			return false
		}
		for index := range python {
			if !TypedValuesEqual(python[index], other[index]) {
				return false
			}
		}
		return true
	case map[string]any:
		other, ok := goValue.(map[string]any)
		if !ok || len(other) != len(python) {
			return false
		}
		for key, value := range python {
			otherValue, exists := other[key]
			if !exists || !TypedValuesEqual(value, otherValue) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(pythonValue, goValue)
}

type taggedValue struct {
	tag   string
	value string
}

// asTaggedValue recognizes the {"t": "<tag>", "v": "<string>"} shape
// TypedEncode/_encode produce, without assuming every map[string]any this
// package ever compares is one (a nested struct/dict legitimately encodes
// to a map with arbitrary other keys, which must keep falling through to
// reflect.DeepEqual).
func asTaggedValue(value any) (taggedValue, bool) {
	asMap, ok := value.(map[string]any)
	if !ok || len(asMap) != 2 {
		return taggedValue{}, false
	}
	tag, tagOK := asMap["t"].(string)
	leaf, leafOK := asMap["v"].(string)
	if !tagOK || !leafOK {
		return taggedValue{}, false
	}
	return taggedValue{tag: tag, value: leaf}, true
}
