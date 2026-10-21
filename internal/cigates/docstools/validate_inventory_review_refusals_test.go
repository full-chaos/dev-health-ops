package docstools_test

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The refusal inventory of validate-inventory-review: every line of the frozen Python script that can refuse (a
// raise, an open/read/decode, a subscript, a .strip() on a csv value, a glob, argparse) is a row of
// testdata/refusals/validate-inventory-review.tsv naming the parity cases or Go tests that answer it.
var ivRefusalSite = regexp.MustCompile(`raise |SystemExit|\.open\(|DictReader\(|for row in rows:|dict\(row\)|" \| "\.join\(|Counter\(|\brow\[|\brow\.get\([^)]*\)\.strip\(|\.glob\(|sorted\((classifications|artifact_types)|\.read_bytes\(|\.read_text\(|json\.loads\(|factual\[|committed\[|len\((committed|factual_rows)|with_suffix\(|with_name\(|\.write_text\(|parse_args\(`)

func TestEveryPythonRefusalOfValidateInventoryHasAGoRow(t *testing.T) {
	checkRefusalInventory(t, refusalInventory(t, "validate-inventory-review.tsv"),
		pythonRefusalSites(t, "validate_docs_inventory_review.py.frozen", ivRefusalSite), "#unreadable", "#stricter", "#argparse")
}

// runCigate runs the built cigate binary in the current directory.
func runCigate(t *testing.T) func(args ...string) (int, string, string) {
	bin := liveCigate(t)
	return func(args ...string) (int, string, string) {
		cmd := exec.Command(bin, append([]string{"docs"}, args...)...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		var exit *exec.ExitError
		if err != nil && !errors.As(err, &exit) {
			t.Fatal(err)
		}
		return cmd.ProcessState.ExitCode(), stdout.String(), stderr.String()
	}
}

// Every case row replays through the binary exactly as the in-process parity replay does (inputs, padding, exit
// code, stdout, stderr, written files); every go row names a test that exists.
func TestValidateInventoryRefusalRowsRunLiveThroughTheCLI(t *testing.T) {
	inventory := refusalInventory(t, "validate-inventory-review.tsv")
	run := runCigate(t)
	tests, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	var testText strings.Builder
	for _, file := range tests {
		testText.WriteString(mustRead(t, file))
	}
	ran := map[string]bool{}
	for site, row := range inventory {
		for _, ref := range strings.Split(row.refs, ";") {
			switch row.kind {
			case "case":
				if !strings.HasPrefix(ref, "val-") {
					t.Errorf("row %q names %s, not a validate-inventory-review case", site, ref)
				} else if !ran[ref] {
					ran[ref] = true
					dir, err := filepath.Abs(filepath.Join("testdata", "parity", ref))
					if err != nil {
						t.Fatal(err)
					}
					t.Run(ref, func(t *testing.T) { replayCaseWith(t, dir, run) })
				}
			case "go":
				if !strings.Contains(testText.String(), "func "+ref+"(") {
					t.Errorf("row %q names Go test %s, which does not exist", site, ref)
				}
			default:
				t.Errorf("row %q has unknown kind %q", site, row.kind)
			}
		}
	}
}

const ivGenerated = ".build/documentation-inventory.json"

// ivLiveTree is val-ok's inputs, padded to the reviewed sizes, in a fresh work directory (the current directory).
func ivLiveTree(t *testing.T) string {
	t.Helper()
	base, err := filepath.Abs(filepath.Join("testdata", "parity", "val-ok"))
	if err != nil {
		t.Fatal(err)
	}
	work := t.TempDir()
	in := filepath.Join(base, "in")
	_ = filepath.Walk(in, func(path string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			rel, _ := filepath.Rel(in, path)
			writeTree(t, filepath.Join(work, rel), mustRead(t, path), 0o644)
		}
		return nil
	})
	ivPadCase(t, base, work)
	t.Chdir(work)
	return work
}

func ivSetJSON(t *testing.T, text string) {
	t.Helper()
	for _, rel := range []string{ivGenerated, "docs-data/inventory/documentation-inventory.json"} {
		writeTree(t, rel, text, 0o644)
	}
}

// What Python 3.12.3 printed for each of these is in the comment of the row (recorded once by hand; a mode 000 file
// cannot be a recorded case).
func TestValidateInventoryUnreadableInputs(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode 000 file")
	}
	for _, c := range []struct{ name, path, want string }{
		{"generated json", ivGenerated, "PermissionError: [Errno 13] Permission denied: '.build/documentation-inventory.json'\n"},
		{"disposition", "docs-data/inventory/disposition-matrix.tsv", "PermissionError: [Errno 13] Permission denied: 'docs-data/inventory/disposition-matrix.tsv'\n"},
		{"split file", "docs-data/inventory/ops-a.tsv", "PermissionError: [Errno 13] Permission denied: 'docs-data/inventory/ops-a.tsv'\n"},
		// Python's glob swallows the PermissionError of scandir: no IA URL is approved.
		{"ia dir", "docs-data/ia", "ValueError: Inventory review failed:\nfull-chaos/dev-health-ops:docs/index.md targets non-IA URL /\nfull-chaos/dev-health-web:app/page.tsx targets non-IA URL /guide/\n"},
	} {
		t.Run(c.name, func(t *testing.T) {
			work := ivLiveTree(t)
			path := filepath.Join(work, c.path)
			if err := os.Chmod(path, 0); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(path, 0o755) })
			code, stdout, stderr := runCigate(t)("validate-inventory-review")
			if code != 1 || stdout != "" || stderr != c.want {
				t.Errorf("rc %d stdout %q stderr %q; Python rc 1, no stdout, stderr %q", code, stdout, stderr, c.want)
			}
		})
	}
}

// Shapes the port refuses although Python went on (or refused with a different message); README "Stricter than
// Python". Each refuses with exit 1, no stdout and one stricter-refusal line on stderr.
func TestValidateInventoryStricterThanPython(t *testing.T) {
	base := func(t *testing.T) string { ivLiveTree(t); return mustRead(t, ivGenerated) }
	for _, c := range []struct {
		name, python string
		edit         func(t *testing.T, text string)
	}{
		{"lone surrogate in a written field", "rc 1, UnicodeEncodeError", func(t *testing.T, s string) {
			ivSetJSON(t, strings.Replace(s, "plain note", `plain \ud800 note`, 1))
		}},
		{"lone surrogate in an unwritten key", "rc 0", func(t *testing.T, s string) {
			ivSetJSON(t, strings.Replace(s, `"counts": {}`, `"counts": {"x": "\ud800"}`, 1))
		}},
		{"nesting deeper than 200", "rc 0", func(t *testing.T, s string) {
			ivSetJSON(t, strings.Replace(s, `"counts": {}`, `"counts": `+strings.Repeat("[", 300)+strings.Repeat("]", 300), 1))
		}},
		{"classification None next to str", "rc 1, TypeError: '<' not supported between instances of 'NoneType' and 'str'", func(t *testing.T, s string) {
			ivSetJSON(t, strings.Replace(s, `"publication_classification": "excluded-internal"`, `"publication_classification": null`, 1))
		}},
		{"numeric artifact types", "sorts them; rc 0 when the snapshots match", func(t *testing.T, s string) {
			for _, ty := range []string{`"markdown-page"`, `"tooling"`, `"external-gap"`} {
				s = strings.ReplaceAll(s, `"artifact_type": `+ty, `"artifact_type": 3`)
			}
			ivSetJSON(t, strings.Replace(s, `"artifact_type": 3`, `"artifact_type": 1`, 1))
		}},
		{"rows a str", "rc 1, ValueError: dictionary update sequence element #0 has length 1; 2 is required", func(t *testing.T, s string) {
			ivSetJSON(t, `{"row_count": 2, "rows": "ab"}`+"\n")
		}},
		{"rows an empty str", "rc 0 when the snapshots match", func(t *testing.T, s string) {
			ivSetJSON(t, `{"row_count": 0, "rows": ""}`+"\n")
		}},
		{"rows an object", "iterates its keys", func(t *testing.T, s string) {
			ivSetJSON(t, `{"row_count": 1, "rows": {"ab": 1}}`+"\n")
		}},
		{"row a list of pairs", "dict() of the pairs", func(t *testing.T, s string) {
			s = strings.Replace(s, `"row_count": 4`, `"row_count": 5`, 1)
			ivSetJSON(t, strings.Replace(s, `"rows": [`, `"rows": [[["source_repo", "x"]],`, 1))
		}},
		{"non-UTF-8 IA file name", "rc 0", func(t *testing.T, s string) {
			writeTree(t, "docs-data/ia/\xff.tsv", "url\n/x/\n", 0o644)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			c.edit(t, base(t))
			code, stdout, stderr := runCigate(t)("validate-inventory-review")
			if code != 1 || stdout != "" || strings.Count(stderr, "\n") != 1 || !strings.HasPrefix(stderr, "cigate: refused (stricter than the Python checker): ") {
				t.Errorf("rc %d stdout %q stderr %q; want rc 1, no stdout, one stricter-refusal line (Python: %s)", code, stdout, stderr, c.python)
			}
		})
	}
}
