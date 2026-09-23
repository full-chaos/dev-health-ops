package pyidna

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

// tablesProgram dumps the pinned idna package's data. Intranges become
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

// TestIDNATablesMatchLivePython regenerates tables.go from the live idna
// package and fails when the committed file differs. With
// DEV_HEALTH_REGENERATE_TABLES=1 it rewrites the file instead.
func TestIDNATablesMatchLivePython(t *testing.T) {
	regenerate := os.Getenv("DEV_HEALTH_REGENERATE_TABLES") == "1"
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" && !regenerate {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	directory := filepath.Dir(file)
	root := filepath.Clean(filepath.Join(directory, "..", "..", ".."))
	output := runPython(t, root, tablesProgram, nil)
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
		t.Fatalf("tables.go differs from the live idna %s; regenerate with DEV_HEALTH_REGENERATE_TABLES=1", dump.Version)
	}
	writeProof(t, "pythonparity-pyidna-tables")
}

func runPython(t *testing.T, root, program string, stdin any) []byte {
	t.Helper()
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, "-c", program)
	if stdin != nil {
		payload, err := json.Marshal(stdin)
		if err != nil {
			t.Fatal(err)
		}
		command.Stdin = bytes.NewReader(payload)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, stderr.Bytes()))
	}
	return output
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
	if len(dump.Starts) != len(dump.Statuses) || len(dump.Starts) != len(dump.Replacements) {
		t.Fatalf("uts46 table lengths differ: %d %d %d", len(dump.Starts), len(dump.Statuses), len(dump.Replacements))
	}
	var out strings.Builder
	fmt.Fprintf(&out, "// Code generated from idna %s (data %s) by TestIDNATablesMatchLivePython; DO NOT EDIT.\n\n", dump.Version, dump.DataVersion)
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
	out.WriteString("var uts46HasReplacement = [...]bool{\n")
	for _, replacement := range dump.Replacements {
		fmt.Fprintf(&out, "\t%v,\n", replacement != nil)
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
