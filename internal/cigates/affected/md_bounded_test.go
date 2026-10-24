package affected_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// A pull request that changes Markdown files only plans the bounded readers of those files (--md-bounded): the exact
// paths the scan finds the code naming, the walks that cover the file (their filter included) and the expressed edges.
// The unbounded readers (dynreads.tsv, unexpressed walks and edges) no longer select. Every other event, a .go file, a
// testdata segment or a go:embed target keeps the full exact plan.

const (
	mdNameOnly = `package x

import (
	"os"
	"path/filepath"
	"strings"
)

func repoRoot() string { return ".." }

func skipped(name string) bool {
	switch name {
	case "README.md":
		return true
	}
	return name == "README.md" || strings.HasSuffix(name, "README.md")
}

func readAgents() ([]byte, error) { return os.ReadFile(filepath.Join(repoRoot(), "AGENTS.md")) }

func readRunbook() ([]byte, error) {
	return os.ReadFile(filepath.Join(repoRoot(), "docs", "operate", "runbooks", "a.md"))
}
`
	mdContracts = `package a

import (
	"os"
	"path/filepath"
)

func repoRoot() string { return ".." }

var contractsDir = filepath.Join(repoRoot(), "contracts", "auth", "v1")

func readContractsReadme() ([]byte, error) { return os.ReadFile(filepath.Join(contractsDir, "README.md")) }
`
	mdGoWalk = `package d

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func repoRoot() string { return ".." }

func walk() error {
	return filepath.WalkDir(repoRoot(), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		_, err = os.ReadFile(path)
		return err
	})
}
`
	mdReads = "# package\tscope\tclass\nb\tcode\tcontracts\nx\ttest\tgithub\nx\ttest\tci\nb\tcode\tdeploy\nx\ttest\tagents\nx\ttest\tdocs\na\ttest\tcontracts\n"
)

func mdHead(extra map[string]string) map[string]string {
	h := map[string]string{
		"x/x_names_test.go":        mdNameOnly,
		"a/a_contracts_test.go":    mdContracts,
		"d/d_walk_test.go":         mdGoWalk,
		"b/b_dyn.go":               fmt.Sprintf(plantedEnvReader, "b"),
		tablesDir + "reads.tsv":    mdReads,
		tablesDir + "dynreads.tsv": "b\tb_dyn.go\n",
		tablesDir + "reach.tsv":    "b\tb_dyn.go\t*\n",
		tablesDir + "walks.tsv":    "d\td_walk_test.go\t.\ttree\t*.go\n",
	}
	for k, v := range extra {
		h[k] = v
	}
	return h
}

func TestPlanVerdictMarkdownOnlyPlansTheBoundedReaders(t *testing.T) {
	md := []string{"--go-files-full", "--md-bounded"}
	q := []string{"--go-files-full"}
	h := mdHead(nil)
	runRows(t, []verdictRow{
		// The fixture names, case labels and filters of README.md read nothing, the *.go walk skips it and the dynamic
		// reader b is unbounded: no bounded reader.
		{name: "root_readme_pr_is_none", head: h, extra: md, changed: []string{"README.md"}, want: "NONE"},
		{name: "unnamed_docs_file_is_none", head: h, extra: md, changed: []string{"docs/zz.md"}, want: "NONE"},
		{name: "same_readme_without_the_flag_keeps_the_unbounded_readers", head: h, extra: q, changed: []string{"README.md"},
			want: "AFFECTED 2", keys: []string{"b", "c"}},
		{name: "same_readme_on_merge_group_is_full", head: h, event: "merge_group", extra: md, changed: []string{"README.md"},
			want: "FULL event merge_group*"},
		{name: "same_readme_on_a_range_keeps_the_full_plan", head: h, event: "range", extra: md, changed: []string{"README.md"},
			want: "AFFECTED 2", keys: []string{"b", "c"}},
		{name: "md_and_go_is_full", head: h, extra: md, changed: []string{"README.md", "x/x.go"}, want: "FULL Go source*"},
		{name: "md_under_testdata_keeps_the_full_plan", head: h, extra: md, changed: []string{"docs/testdata/a.md"},
			want: "AFFECTED 3", keys: []string{"b", "c", "x"}},
		{name: "judge_error_is_full", head: mdHead(map[string]string{"d/d_docs_test.go": "package d\n\nvar doc = \"../docs/a.md\"\n"}),
			extra: md, changed: []string{"README.md"}, want: "FULL the read tables differ*"},
		{name: "contracts_readme_selects_its_reader", head: h, extra: md, changed: []string{"contracts/auth/v1/README.md"},
			want: "AFFECTED 3", keys: []string{"a", "b", "c"}},
		{name: "agents_selects_its_reader", head: h, extra: md, changed: []string{"AGENTS.md"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "named_runbook_selects_its_reader", head: h, extra: md, changed: []string{"docs/operate/runbooks/a.md"},
			want: "AFFECTED 1", keys: []string{"x"}},
		{name: "other_runbook_is_none", head: h, extra: md, changed: []string{"docs/operate/runbooks/b.md"}, want: "NONE"},
		// a holds a docs row but names no docs path the scan sees: the read shape is unknown, so it still selects.
		{name: "class_row_without_an_exact_path_still_selects", head: mdHead(map[string]string{tablesDir + "reads.tsv": mdReads + "a\ttest\tdocs\n"}),
			extra: md, changed: []string{"docs/zz.md"}, want: "AFFECTED 1", keys: []string{"a"}},
	})
}

// A Markdown file a package embeds is that package's input: the full exact plan.
func TestPlanVerdictMarkdownEmbedKeepsTheFullPlan(t *testing.T) {
	work := t.TempDir()
	f, err := newVerdictFixture(work, mdHead(map[string]string{"docs/gen/gen.go": "package gen\n"}))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(f.golist)
	if err != nil {
		t.Fatal(err)
	}
	gen, err := json.Marshal(map[string]any{"ImportPath": "m/docs/gen", "Dir": filepath.Join(f.root, "docs", "gen"),
		"Module": map[string]string{"Path": "m"}, "EmbedFiles": []string{"notes.md"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.golist, append(raw, append(gen, '\n')...), 0o644); err != nil {
		t.Fatal(err)
	}
	md := []string{"--go-files-full", "--md-bounded"}
	for _, c := range []struct {
		changed, want string
		keys          []string
	}{
		{"docs/gen/notes.md", "AFFECTED 3", []string{"b", "c", "x"}},
		{"docs/gen/other.md", "NONE", nil},
	} {
		first, keys, err := f.verdict(t, work, "pull_request", md, c.changed)
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) == 0 || (len(keys) == 1 && keys[0] == "") {
			keys = nil
		}
		if first != c.want || !reflect.DeepEqual(keys, c.keys) {
			t.Errorf("%s: %q %q, want %q %q", c.changed, first, keys, c.want, c.keys)
		}
	}
}

// The literal scan records a bare root name only where it can name a file: a name compared, matched or used as a case
// label opens nothing, a name joined to another directory is that directory's file, and a name a test reads directly is
// read in the test's own directory.
func TestPlanReadsBareNamesAreNotRootReads(t *testing.T) {
	classes := "docs\tdocs/**\ncontracts\tcontracts/**\nREADME.md\tREADME.md\n"
	src := func(body string) string { return "package p\n\n" + importsFor(body) + body }
	cases := []struct {
		name string
		body string
		code int
		want string // a line plan-reads must print; "" = none
	}{
		{"case_label", "func skip(n string) bool {\n\tswitch n {\n\tcase \"README.md\":\n\t\treturn true\n\t}\n\treturn false\n}\n", 0, ""},
		{"comparison", "func skip(n string) bool { return n == \"README.md\" || \"README.md\" != n }\n", 0, ""},
		{"name_test", "func skip(n string) bool { return strings.HasSuffix(n, \"README.md\") }\n", 0, ""},
		{"own_directory_read", "func read() ([]byte, error) { return os.ReadFile(\"README.md\") }\n", 0, ""},
		{"join_under_another_directory", "var dir = os.Getenv(\"DIR\")\n\nfunc read() ([]byte, error) { return os.ReadFile(filepath.Join(dir, \"README.md\")) }\n", 1, "p\tp_test.go"},
		{"join_under_the_root", "func repoRoot() string { return \"..\" }\n\nfunc read() ([]byte, error) { return os.ReadFile(filepath.Join(repoRoot(), \"README.md\")) }\n", 1, "p\ttest\tREADME.md"},
		{"fs_read_stays_a_root_read", "func read(fsys fs.FS) ([]byte, error) { return fs.ReadFile(fsys, \"README.md\") }\n", 1, "p\ttest\tREADME.md"},
		{"name_handed_to_a_helper_stays_a_read", "type guard struct{}\n\nfunc (guard) read(string) string { return \"\" }\n\nvar _ = guard{}.read(\"README.md\")\n", 1, "p\ttest\tREADME.md"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			res := planReads(t, readsFixture(t, map[string]string{"p/p_test.go": src(c.body), "README.md": "r\n",
				"internal/cigates/affected/classes.tsv": classes}))
			out := res.stdout + res.stderr
			if res.code != c.code {
				t.Fatalf("plan-reads exited %d, want %d:\n%s", res.code, c.code, out)
			}
			if c.want != "" && !strings.Contains(out, c.want) {
				t.Fatalf("plan-reads output does not hold %q:\n%s", c.want, out)
			}
			if c.want != "p\ttest\tREADME.md" && strings.Contains(out, "\tREADME.md") {
				t.Fatalf("plan-reads asks for a README.md row:\n%s", out)
			}
		})
	}
}

// importsFor is the import block of the packages src uses, so a planted file needs no use of a reader as a value.
func importsFor(src string) string {
	var b strings.Builder
	b.WriteString("import (\n")
	for _, p := range []string{"io/fs", "os", "path/filepath", "strings"} {
		if strings.Contains(src, filepath.Base(p)+".") {
			b.WriteString("\t\"" + p + "\"\n")
		}
	}
	b.WriteString(")\n\n")
	return b.String()
}

// A walk whose callback returns nil for every file without a suffix, before it does anything else with a file, acts on
// those files alone: walks.tsv records the filter, and a change to any other file is outside the walk.
func TestPlanReadsWalkFilterComesFromTheGate(t *testing.T) {
	src := func(params, body string) string {
		rest := "func repoRoot() string { return \"..\" }\n\nvar names []string\n\nfunc walk() error {\n\troot := repoRoot()\n\treturn filepath.WalkDir(root, func(" +
			params + ") error {\n" + body + "\n\t})\n}\n"
		return "package w\n\n" + importsFor(rest) + rest
	}
	walkDir := "path string, d fs.DirEntry, err error"
	read := "\t\t_, rerr := os.ReadFile(path)\n\t\treturn rerr"
	cases := []struct {
		name, params, body string
		want               []string // the filters of the walks.tsv rows plan-reads asks for
	}{
		{"negative_gate", walkDir, "\t\tif err != nil {\n\t\t\treturn err\n\t\t}\n\t\tif d.IsDir() {\n\t\t\tswitch d.Name() {\n\t\t\tcase \".git\":\n\t\t\t\treturn filepath.SkipDir\n\t\t\t}\n\t\t\treturn nil\n\t\t}\n\t\tif !strings.HasSuffix(path, \".go\") || strings.HasSuffix(path, \"_test.go\") {\n\t\t\treturn nil\n\t\t}\n" + read, []string{"*.go"}},
		{"entry_name_alias", walkDir, "\t\tname := d.Name()\n\t\tif !strings.HasSuffix(name, \"_test.go\") {\n\t\t\treturn nil\n\t\t}\n" + read, []string{"*_test.go"}},
		{"rel_alias_reassigned", walkDir, "\t\trel, _ := filepath.Rel(root, path)\n\t\trel = filepath.ToSlash(rel)\n\t\tif !strings.HasSuffix(rel, \".go\") {\n\t\t\treturn nil\n\t\t}\n" + read, []string{"*.go"}},
		{"positive_gate", walkDir, "\t\tif strings.HasSuffix(path, \".go\") {\n" + read + "\n\t\t}\n\t\treturn nil", []string{"*.go"}},
		{"is_dir_in_the_gate", "path string, info os.FileInfo, err error", "\t\tif err != nil || info.IsDir() || !strings.HasSuffix(path, \".go\") {\n\t\t\treturn nil\n\t\t}\n" + read, []string{"*.go"}},
		{"skip_dir_as_the_gate_skips_siblings", walkDir, "\t\tif !strings.HasSuffix(path, \".go\") {\n\t\t\treturn filepath.SkipDir\n\t\t}\n" + read, []string{"*"}},
		{"name_kept_before_the_gate", walkDir, "\t\tnames = append(names, path)\n\t\tif !strings.HasSuffix(path, \".go\") {\n\t\t\treturn nil\n\t\t}\n" + read, []string{"*"}},
		{"read_before_the_gate", walkDir, "\t\t_, _ = os.ReadFile(path)\n\t\tif !strings.HasSuffix(path, \".go\") {\n\t\t\treturn nil\n\t\t}\n" + read, []string{"*"}},
		{"file_skip_dir_before_the_gate", walkDir, "\t\tif strings.HasSuffix(path, \".md\") {\n\t\t\treturn filepath.SkipDir\n\t\t}\n\t\tif !strings.HasSuffix(path, \".go\") {\n\t\t\treturn nil\n\t\t}\n" + read, []string{"*"}},
		{"alias_changed_in_a_branch", walkDir, "\t\trel := path\n\t\tif d.IsDir() {\n\t\t\trel = \"x\"\n\t\t\treturn nil\n\t\t}\n\t\tif !strings.HasSuffix(rel, \".go\") {\n\t\t\treturn nil\n\t\t}\n" + read, []string{"*"}},
		{"sibling_of_the_gated_file", walkDir, "\t\tif !strings.HasSuffix(path, \".go\") {\n\t\t\treturn nil\n\t\t}\n\t\t_, rerr := os.ReadFile(filepath.Join(path, \"notes\"))\n\t\treturn rerr", []string{"*", "*.go"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			res := planReads(t, readsFixture(t, map[string]string{"w/w_walk_test.go": src(c.params, c.body)}))
			out := res.stdout + res.stderr
			var got []string
			for _, l := range strings.Split(out, "\n") {
				if f := strings.Split(l, "\t"); len(f) == 5 && f[0] == "w" && f[2] == "." && f[3] == "tree" {
					got = append(got, f[4])
				}
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("walks.tsv filters %q, want %q:\n%s", got, c.want, out)
			}
		})
	}
}
