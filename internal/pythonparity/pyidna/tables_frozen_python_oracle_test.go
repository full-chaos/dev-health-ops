package pyidna_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// tablesProgram dumps the idna package's data. Intranges become
// half-open [start, end) pairs.
const tablesProgram = `
import json, idna, idna.idnadata as d, idna.uts46data as u

def pairs(ranges):
    return [[r >> 32, r & 0xFFFFFFFF] for r in ranges]

print(json.dumps({
    "version": idna.__version__,
    "data_version": d.__version__,
    "starts": list(u.uts46_starts),
    "statuses": "".join(chr(s) for s in u.uts46_statuses),
    "replacements": list(u.uts46_replacements),
    "classes": {k: pairs(v) for k, v in d.codepoint_classes.items()},
    "scripts": {k: pairs(d.scripts[k]) for k in ("Greek", "Hebrew", "Hiragana", "Katakana", "Han")},
    "joining": [[k, pairs(v)] for k, v in d.joining_types.items()],
}, ensure_ascii=True))
`

type tablesDump struct {
	Version      string               `json:"version"`
	DataVersion  string               `json:"data_version"`
	Starts       []rune               `json:"starts"`
	Statuses     string               `json:"statuses"`
	Replacements []*string            `json:"replacements"`
	Classes      map[string][][2]rune `json:"classes"`
	Scripts      map[string][][2]rune `json:"scripts"`
	Joining      [][2]json.RawMessage `json:"joining"`
}

// TestIDNATablesMatchFrozenPython renders tables.go from the frozen dump of
// the idna package and fails when the committed file differs. With
// DEV_HEALTH_REGENERATE_TABLES=1 it rewrites the file instead.
func TestIDNATablesMatchFrozenPython(t *testing.T) {
	regenerate := os.Getenv("DEV_HEALTH_REGENERATE_TABLES") == "1"
	_, file, _, _ := runtime.Caller(0)
	directory := filepath.Dir(file)
	output := frozenPython(t, "tables.golden.json", programoracle.Program{Name: "tables", Text: tablesProgram})[0]
	var dump tablesDump
	if err := json.Unmarshal([]byte(output), &dump); err != nil {
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
		t.Fatalf("tables.go differs from the frozen dump of idna %s; regenerate with DEV_HEALTH_REGENERATE_TABLES=1", dump.Version)
	}
}

func renderTables(t *testing.T, dump tablesDump) []byte {
	t.Helper()
	if len(dump.Starts) != len(dump.Statuses) || len(dump.Starts) != len(dump.Replacements) {
		t.Fatalf("uts46 table lengths differ: %d %d %d", len(dump.Starts), len(dump.Statuses), len(dump.Replacements))
	}
	var out strings.Builder
	fmt.Fprintf(&out, "// Code generated from idna %s (data %s) by TestIDNATablesMatchFrozenPython; DO NOT EDIT.\n\n", dump.Version, dump.DataVersion)
	out.WriteString("package pyidna\n\n")
	fmt.Fprintf(&out, "const idnaVersion = %q\n\n", dump.Version)
	out.WriteString("var uts46Starts = [...]rune{\n")
	for _, r := range dump.Starts {
		fmt.Fprintf(&out, "\t0x%x,\n", r)
	}
	out.WriteString("}\n\n")
	fmt.Fprintf(&out, "var uts46Statuses = %q\n\n", dump.Statuses)
	out.WriteString("var uts46Replacements = [...]string{\n")
	for _, replacement := range dump.Replacements {
		if replacement == nil {
			out.WriteString("\t\"\",\n")
		} else {
			fmt.Fprintf(&out, "\t%+q,\n", *replacement)
		}
	}
	out.WriteString("}\n\n")
	renderRangeMap(&out, "codepointClasses", dump.Classes, []string{"PVALID", "CONTEXTJ", "CONTEXTO"})
	renderRangeMap(&out, "scripts", dump.Scripts, []string{"Greek", "Han", "Hebrew", "Hiragana", "Katakana"})
	out.WriteString("var joiningTypes = [...]struct {\n\tname   string\n\tranges [][2]rune\n}{\n")
	for _, entry := range dump.Joining {
		var name string
		var ranges [][2]rune
		if err := json.Unmarshal(entry[0], &name); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(entry[1], &ranges); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&out, "\t{%q, [][2]rune{\n", name)
		for _, pair := range ranges {
			fmt.Fprintf(&out, "\t\t{0x%x, 0x%x},\n", pair[0], pair[1])
		}
		out.WriteString("\t}},\n")
	}
	out.WriteString("}\n")
	formatted, err := format.Source([]byte(out.String()))
	if err != nil {
		t.Fatalf("format tables: %v", err)
	}
	return formatted
}

func renderRangeMap(out *strings.Builder, name string, values map[string][][2]rune, keys []string) {
	if len(values) != len(keys) {
		panic(fmt.Sprintf("%s: %d keys in the dump, %d expected", name, len(values), len(keys)))
	}
	fmt.Fprintf(out, "var %s = map[string][][2]rune{\n", name)
	for _, key := range keys {
		ranges, ok := values[key]
		if !ok {
			panic(fmt.Sprintf("%s: key %q missing from the dump", name, key))
		}
		fmt.Fprintf(out, "\t%q: {\n", key)
		for _, pair := range ranges {
			fmt.Fprintf(out, "\t\t{0x%x, 0x%x},\n", pair[0], pair[1])
		}
		out.WriteString("\t},\n")
	}
	out.WriteString("}\n\n")
}
