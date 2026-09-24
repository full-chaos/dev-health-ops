package pyunicodedata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// tablesProgram dumps, from the live interpreter, every unicodedata answer
// this package freezes. Runs are [start, value] pairs over 0..0x10FFFF.
const tablesProgram = `
import json, re, sys, unicodedata

def runs(fn):
    out, prev = [], object()
    for cp in range(0x110000):
        value = fn(chr(cp))
        if value != prev:
            out.append([cp, value])
            prev = value
    return out

def ranges(pred):
    out, start = [], None
    for cp in range(0x110001):
        hit = cp < 0x110000 and pred(chr(cp))
        if hit and start is None:
            start = cp
        elif not hit and start is not None:
            out.append([start, cp - 1])
            start = None
    return out

word = re.compile(r"\w")
u32 = unicodedata.ucd_3_2_0

def overrides32():
    out = []
    for cp in range(0x110000):
        c = chr(cp)
        if 0xD800 <= cp <= 0xDFFF or u32.category(c) == "Cn":
            continue
        old = u32.normalize("NFKD", c)
        if old != unicodedata.normalize("NFKD", c):
            out.append([cp, [ord(x) for x in old]])
    return out

def decompositions(module, compat):
    out = []
    for cp in range(0x110000):
        d = module.decomposition(chr(cp))
        if not d:
            continue
        tagged = d.startswith("<")
        if tagged != compat:
            continue
        parts = d.split()[1:] if tagged else d.split()
        out.append([cp, [int(x, 16) for x in parts]])
    return out

print(json.dumps({
    "version": unicodedata.unidata_version,
    "python": sys.version.split()[0],
    "category": runs(unicodedata.category),
    "bidirectional": runs(unicodedata.bidirectional),
    "combining": runs(unicodedata.combining),
    "no_name": ranges(lambda c: not unicodedata.name(c, "")),
    "names": [[ord(c), unicodedata.name(c)] for c in map(chr, range(0x110000))
              if unicodedata.category(c)[0] in "MZC" and unicodedata.name(c, "")],
    "full_stop": [cp for cp in range(0x110000) if "002E" in unicodedata.decomposition(chr(cp)).split(" ")],
    "word": ranges(lambda c: word.match(c) is not None),
    "canonical": [[cp, [int(x, 16) for x in d.split()]]
                  for cp, d in ((cp, unicodedata.decomposition(chr(cp))) for cp in range(0x110000))
                  if d and not d.startswith("<")],
    "composites": [[int(d.split()[0], 16), int(d.split()[1], 16), cp]
                   for cp, d in ((cp, unicodedata.decomposition(chr(cp))) for cp in range(0x110000))
                   if d and not d.startswith("<") and len(d.split()) == 2
                   and unicodedata.normalize("NFC", chr(int(d.split()[0], 16)) + chr(int(d.split()[1], 16))) == chr(cp)],
    "compat": decompositions(unicodedata, True),
    "unassigned32": ranges(lambda c: u32.category(c) == "Cn"),
    "decomp32": overrides32(),
}))
`

type tablesDump struct {
	Version       string               `json:"version"`
	Python        string               `json:"python"`
	Category      [][2]any             `json:"category"`
	Bidirectional [][2]any             `json:"bidirectional"`
	Combining     [][2]any             `json:"combining"`
	NoName        [][2]rune            `json:"no_name"`
	Names         [][2]any             `json:"names"`
	FullStop      []rune               `json:"full_stop"`
	Word          [][2]rune            `json:"word"`
	CanonicalRaw  [][2]json.RawMessage `json:"canonical"`
	Composites    [][3]rune            `json:"composites"`
	Compat        [][2]json.RawMessage `json:"compat"`
	Unassigned32  [][2]rune            `json:"unassigned32"`
	Decomp32      [][2]json.RawMessage `json:"decomp32"`
}

// TestUnicodeDataTablesMatchLivePython regenerates tables.go from the live
// interpreter and fails when the committed file differs. With
// DEV_HEALTH_REGENERATE_TABLES=1 it rewrites the file instead.
func TestUnicodeDataTablesMatchLivePython(t *testing.T) {
	regenerate := os.Getenv("DEV_HEALTH_REGENERATE_TABLES") == "1"
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" && !regenerate {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	directory := filepath.Dir(file)
	root := filepath.Clean(filepath.Join(directory, "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, "-c", tablesProgram)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, stderr.Bytes()))
	}
	var dump tablesDump
	if err := json.Unmarshal(output, &dump); err != nil {
		t.Fatalf("decode: %v", err)
	}
	rendered := renderTables(t, dump)
	target := filepath.Join(directory, "tables.go")
	if regenerate {
		if err := os.WriteFile(target, rendered, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	committed, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(committed, rendered) {
		t.Fatalf("tables.go differs from the live interpreter (%s, unicodedata %s); regenerate with DEV_HEALTH_REGENERATE_TABLES=1",
			dump.Python, dump.Version)
	}
	checkLookups(t, dump)
	writeProof(t, "pythonparity-pyunicodedata")
}

// checkLookups asks every function about every code point and compares it
// with the dump, so a lookup bug cannot hide behind a correct table.
func checkLookups(t *testing.T, dump tablesDump) {
	t.Helper()
	names := map[rune]string{}
	for _, entry := range dump.Names {
		names[rune(entry[0].(float64))] = entry[1].(string)
	}
	fullStop := map[rune]bool{}
	for _, r := range dump.FullStop {
		fullStop[r] = true
	}
	// in is a plain membership test over the dump's own ranges, walked
	// with a cursor per table so the pass stays linear.
	cursor := map[*[][2]rune]int{}
	in := func(ranges *[][2]rune, r rune) bool {
		i := cursor[ranges]
		for i < len(*ranges) && (*ranges)[i][1] < r {
			i++
		}
		cursor[ranges] = i
		return i < len(*ranges) && (*ranges)[i][0] <= r
	}
	failures := 0
	fail := func(format string, args ...any) {
		failures++
		if failures <= 20 {
			t.Errorf(format, args...)
		}
	}
	// Walking the runs in step keeps this linear.
	ci, bi, ki := 0, 0, 0
	for r := rune(0); r <= 0x10ffff; r++ {
		for ci+1 < len(dump.Category) && rune(dump.Category[ci+1][0].(float64)) <= r {
			ci++
		}
		for bi+1 < len(dump.Bidirectional) && rune(dump.Bidirectional[bi+1][0].(float64)) <= r {
			bi++
		}
		for ki+1 < len(dump.Combining) && rune(dump.Combining[ki+1][0].(float64)) <= r {
			ki++
		}
		if got, want := Category(r), dump.Category[ci][1].(string); got != want {
			fail("Category(U+%04X) = %q, python %q", r, got, want)
		}
		if got, want := Bidirectional(r), dump.Bidirectional[bi][1].(string); got != want {
			fail("Bidirectional(U+%04X) = %q, python %q", r, got, want)
		}
		if got, want := Combining(r), int(dump.Combining[ki][1].(float64)); got != want {
			fail("Combining(U+%04X) = %d, python %d", r, got, want)
		}
		if got, want := DecompositionHasFullStop(r), fullStop[r]; got != want {
			fail("DecompositionHasFullStop(U+%04X) = %v, python %v", r, got, want)
		}
		name, ok := Name(r)
		if want, wantOK := names[r]; ok != wantOK || name != want {
			fail("Name(U+%04X) = %q %v, python %q %v", r, name, ok, want, wantOK)
		}
		if got, want := HasName(r), !in(&dump.NoName, r); got != want {
			fail("HasName(U+%04X) = %v, python %v", r, got, want)
		}
		if got, want := IsWord(r), in(&dump.Word, r); got != want {
			fail("IsWord(U+%04X) = %v, python %v", r, got, want)
		}
	}
	if failures > 0 {
		t.Fatalf("%d lookup differences", failures)
	}
}

func writeProof(t *testing.T, name string) {
	t.Helper()
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, name), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func renderTables(t *testing.T, dump tablesDump) []byte {
	t.Helper()
	var out strings.Builder
	fmt.Fprintf(&out, "// Code generated from CPython %s (unicodedata %s) by TestUnicodeDataTablesMatchLivePython; DO NOT EDIT.\n\n", dump.Python, dump.Version)
	out.WriteString("package pyunicodedata\n\n")
	fmt.Fprintf(&out, "const unidataVersion = %q\n\n", dump.Version)
	renderRuns(t, &out, "category", dump.Category)
	renderRuns(t, &out, "bidi", dump.Bidirectional)
	// Combining classes are numbers already; the runs hold them directly.
	out.WriteString("var combiningRuns = [...]run{\n")
	for _, entry := range dump.Combining {
		fmt.Fprintf(&out, "\t{0x%x, %d},\n", int(entry[0].(float64)), int(entry[1].(float64)))
	}
	out.WriteString("}\n\n")
	renderRanges(&out, "noNameRanges", dump.NoName)
	out.WriteString("var markSeparatorOtherNames = [...]namedRune{\n")
	for _, entry := range dump.Names {
		fmt.Fprintf(&out, "\t{0x%x, %q},\n", int(entry[0].(float64)), entry[1].(string))
	}
	out.WriteString("}\n\n")
	out.WriteString("var fullStopDecompositions = [...]rune{\n")
	for _, r := range dump.FullStop {
		fmt.Fprintf(&out, "\t0x%x,\n", r)
	}
	out.WriteString("}\n\n")
	renderRanges(&out, "wordRanges", dump.Word)
	renderDecompositions(t, &out, "canonicalDecompositions", dump.CanonicalRaw)
	renderDecompositions(t, &out, "compatDecompositions", dump.Compat)
	renderRanges(&out, "unassigned32Ranges", dump.Unassigned32)
	renderDecompositions(t, &out, "decompositionOverrides32", dump.Decomp32)
	out.WriteString("var primaryComposites = [...][3]rune{\n")
	for _, entry := range dump.Composites {
		fmt.Fprintf(&out, "\t{0x%x, 0x%x, 0x%x},\n", entry[0], entry[1], entry[2])
	}
	out.WriteString("}\n")
	formatted, err := format.Source([]byte(out.String()))
	if err != nil {
		t.Fatalf("format tables: %v", err)
	}
	return formatted
}

// renderRuns writes <prefix>Names (the distinct values in first-seen
// order) and <prefix>Runs (start, index into the names).
func renderRuns(t *testing.T, out *strings.Builder, prefix string, runs [][2]any) {
	t.Helper()
	var names []string
	index := map[string]int{}
	for _, entry := range runs {
		value := entry[1].(string)
		if _, seen := index[value]; !seen {
			index[value] = len(names)
			names = append(names, value)
		}
	}
	if len(names) > 256 {
		t.Fatalf("%s: %d distinct values do not fit a uint8", prefix, len(names))
	}
	fmt.Fprintf(out, "var %sNames = [...]string{", prefix)
	for i, name := range names {
		if i > 0 {
			out.WriteString(", ")
		}
		fmt.Fprintf(out, "%q", name)
	}
	out.WriteString("}\n\n")
	fmt.Fprintf(out, "var %sRuns = [...]run{\n", prefix)
	for _, entry := range runs {
		fmt.Fprintf(out, "\t{0x%x, %d},\n", int(entry[0].(float64)), index[entry[1].(string)])
	}
	out.WriteString("}\n\n")
}

func renderRanges(out *strings.Builder, name string, ranges [][2]rune) {
	fmt.Fprintf(out, "var %s = [...][2]rune{\n", name)
	for _, entry := range ranges {
		fmt.Fprintf(out, "\t{0x%x, 0x%x},\n", entry[0], entry[1])
	}
	out.WriteString("}\n\n")
}

// renderDecompositions writes one decomposition table: entries of a code
// point and the code points its mapping holds.
func renderDecompositions(t *testing.T, out *strings.Builder, name string, entries [][2]json.RawMessage) {
	t.Helper()
	fmt.Fprintf(out, "var %s = [...]decomposition{\n", name)
	for _, entry := range entries {
		var r rune
		var mapping []rune
		if err := json.Unmarshal(entry[0], &r); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(entry[1], &mapping); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(out, "\t{0x%x, []rune{", r)
		for i, part := range mapping {
			if i > 0 {
				out.WriteString(", ")
			}
			fmt.Fprintf(out, "0x%x", part)
		}
		out.WriteString("}},\n")
	}
	out.WriteString("}\n\n")
}
