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

func mdExecPattern(pattern string) string {
	return "package x\n\nimport \"os/exec\"\n\nfunc tracked() ([]byte, error) {\n\treturn exec.Command(\"git\", \"ls-files\", \"-z\", \"--\", \"" + pattern + "\").Output()\n}\n"
}

const mdTailRead = "package x\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n)\n\nfunc readme() ([]byte, error) {\n\treturn os.ReadFile(filepath.Join(os.Getenv(\"DOC_DIR\"), \"README.md\"))\n}\n"

const mdExtWalk = `package d

import (
	"io/fs"
	"os"
	"path/filepath"
)

var scanExtensions = map[string]bool{".go": true, ".md": true}

func scan() error {
	return filepath.WalkDir(os.Getenv("SCAN_ROOT"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		ext := filepath.Ext(path)
		if !scanExtensions[ext] {
			return nil
		}
		_, err = os.ReadFile(path)
		return err
	})
}
`

func mdHead(extra map[string]string) map[string]string {
	h := map[string]string{
		"x/x_names_test.go":     mdNameOnly,
		"a/a_contracts_test.go": mdContracts,
		"d/d_walk_test.go":      mdGoWalk,
		tablesDir + "reads.tsv": mdReads,
		tablesDir + "walks.tsv": "d\td_walk_test.go\t.\ttree\t*.go\n",
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
	// b reads a path from the environment: a dynreads.tsv row, which may name any file, a .md file too.
	hd := mdHead(map[string]string{"b/b_dyn.go": fmt.Sprintf(plantedEnvReader, "b"),
		tablesDir + "dynreads.tsv": "b\tb_dyn.go\n", tablesDir + "reach.tsv": "b\tb_dyn.go\t*\n"})
	runRows(t, []verdictRow{
		// The fixture names, case labels and filters of README.md read nothing and the *.go walk skips it: no reader.
		{name: "root_readme_pr_is_none", head: h, extra: md, changed: []string{"README.md"}, want: "NONE"},
		{name: "unnamed_docs_file_is_none", head: h, extra: md, changed: []string{"docs/zz.md"}, want: "NONE"},
		// An unbounded reader with no Markdown evidence is dropped on a Markdown-only pull request; the merge group runs it.
		{name: "plain_dynamic_reader_is_dropped_on_a_markdown_pr", head: hd, extra: md, changed: []string{"README.md"}, want: "NONE"},
		// An exec whose literal pattern names Markdown files reads them: git ls-files -- '*.md'.
		{name: "exec_md_pattern_selects_on_a_markdown_pr", head: mdHead(map[string]string{"x/x_md_test.go": mdExecPattern("*.md"),
			tablesDir + "edges.tsv": "x\tx_md_test.go\tunexpressed\n", tablesDir + "patterns.tsv": "x\tx_md_test.go\t*.md\n"}),
			extra: md, changed: []string{"README.md"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "exec_go_pattern_is_dropped_on_a_markdown_pr", head: mdHead(map[string]string{"x/x_md_test.go": mdExecPattern("*.go"),
			tablesDir + "edges.tsv": "x\tx_md_test.go\tunexpressed\n"}),
			extra: md, changed: []string{"README.md"}, want: "NONE"},
		// An unbounded directory joined to a constant .md name reads files of that name.
		{name: "unbounded_join_to_an_md_name_selects_that_name", head: mdHead(map[string]string{"x/x_tail_test.go": mdTailRead,
			tablesDir + "dynreads.tsv": "x\tx_tail_test.go\n", tablesDir + "patterns.tsv": "x\tx_tail_test.go\t**/README.md\n"}),
			extra: md, changed: []string{"README.md"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "unbounded_join_to_an_md_name_skips_another_name", head: mdHead(map[string]string{"x/x_tail_test.go": mdTailRead,
			tablesDir + "dynreads.tsv": "x\tx_tail_test.go\n", tablesDir + "patterns.tsv": "x\tx_tail_test.go\t**/README.md\n"}),
			extra: md, changed: []string{"docs/zz.md"}, want: "NONE"},
		// A walk of an unbounded root whose gate passes .md files reads them: an extension map with ".md".
		{name: "unbounded_walk_with_an_md_gate_selects_on_a_markdown_pr", head: mdHead(map[string]string{"d/d_ext_test.go": mdExtWalk,
			tablesDir + "walks.tsv": "d\td_walk_test.go\t.\ttree\t*.go\nd\td_ext_test.go\tunexpressed\t-\t*.go|*.md\n", tablesDir + "dynreads.tsv": "d\td_ext_test.go\n"}),
			extra: md, changed: []string{"docs/zz.md"}, want: "AFFECTED 1", keys: []string{"d"}},
		{name: "same_readme_without_the_flag", head: hd, extra: q, changed: []string{"README.md"},
			want: "AFFECTED 2", keys: []string{"b", "c"}},
		{name: "same_readme_on_merge_group_is_full", head: hd, event: "merge_group", extra: md, changed: []string{"README.md"},
			want: "FULL event merge_group*"},
		{name: "same_readme_on_a_range_keeps_the_full_plan", head: hd, event: "range", extra: md, changed: []string{"README.md"},
			want: "AFFECTED 2", keys: []string{"b", "c"}},
		{name: "md_and_go_is_full", head: h, extra: md, changed: []string{"README.md", "x/x.go"}, want: "FULL Go source*"},
		{name: "md_under_testdata_keeps_the_full_plan", head: h, extra: md, changed: []string{"docs/testdata/a.md"},
			want: "AFFECTED 1", keys: []string{"x"}},
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
		{"docs/gen/notes.md", "AFFECTED 1", []string{"x"}},
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
		// The value flow keeps every root name whose value reaches a read, or whose flow it cannot follow.
		{"keep_method_helper", "type guard struct{ root string }\n\nfunc (g *guard) path(rel string) string { return filepath.Join(g.root, rel) }\n\n" +
			"func (g *guard) read(rel string) string {\n\tb, _ := os.ReadFile(g.path(rel))\n\treturn string(b)\n}\n\nfunc use(g *guard) string { return g.read(\"README.md\") }\n", 1, "p\ttest\tREADME.md"},
		{"keep_variadic_helper", "func readRepoFile(rel ...string) string {\n\tb, _ := os.ReadFile(filepath.Join(append([]string{\"/r\"}, rel...)...))\n\treturn string(b)\n}\n\n" +
			"var text = readRepoFile(\"README.md\")\n", 1, "p\ttest\tREADME.md"},
		{"keep_closure_helper", "func scan() {\n\tone := func(name, rel string) { _, _ = os.ReadFile(rel) }\n\tone(\"x\", \"README.md\")\n}\n", 1, "p\ttest\tREADME.md"},
		{"keep_range_table", "func each() {\n\tfor _, file := range []string{\"README.md\"} {\n\t\t_, _ = os.ReadFile(filepath.Join(\"..\", file))\n\t}\n}\n", 1, "p\ttest\tREADME.md"},
		{"keep_struct_field_read_later", "func TestX(t *testing.T) {\n\tcases := []struct{ name, file string }{{\"a\", \"README.md\"}}\n\tfor _, c := range cases {\n" +
			"\t\tt.Run(c.name, func(t *testing.T) { _, _ = os.ReadFile(c.file) })\n\t}\n}\n", 1, "p\ttest\tREADME.md"},
		{"keep_cross_package_helper", "var text = othermod.Read(\"README.md\")\n", 1, "p\ttest\tREADME.md"},
		{"keep_max_result", "func repoRoot() string { return \"..\" }\n\nfunc read() ([]byte, error) {\n\tname := max(\"README.md\", \"A\")\n\treturn os.ReadFile(filepath.Join(repoRoot(), name))\n}\n", 1, "p\ttest\tREADME.md"},
		{"keep_trim_result", "func repoRoot() string { return \"..\" }\n\nfunc read() ([]byte, error) {\n\treturn os.ReadFile(filepath.Join(repoRoot(), strings.TrimSuffix(\"README.md\", \".txt\")))\n}\n", 1, "p\ttest\tREADME.md"},
		{"keep_imported_method_with_a_local_namesake", "type local struct{}\n\nfunc (local) Read(string) {}\n\nvar r othermod.Reader\n\nvar text = r.Read(\"README.md\")\n", 1, "p\ttest\tREADME.md"},
		{"keep_fprint_into_a_builder", "func repoRoot() string { return \"..\" }\n\nfunc read() ([]byte, error) {\n\tvar sb strings.Builder\n" +
			"\tfmt.Fprint(&sb, \"README.md\")\n\treturn os.ReadFile(filepath.Join(repoRoot(), sb.String()))\n}\n", 1, "p\ttest\tREADME.md"},
		{"keep_writestring_into_a_builder", "func repoRoot() string { return \"..\" }\n\nfunc read() ([]byte, error) {\n\tvar sb strings.Builder\n" +
			"\t_, _ = io.WriteString(&sb, \"README.md\")\n\treturn os.ReadFile(filepath.Join(repoRoot(), sb.String()))\n}\n", 1, "p\ttest\tREADME.md"},
		{"keep_closure_named_like_a_builtin", "func repoRoot() string { return \"..\" }\n\nfunc read() {\n" +
			"\tlen := func(s string) int {\n\t\t_, _ = os.ReadFile(filepath.Join(repoRoot(), s))\n\t\treturn 0\n\t}\n\t_ = len(\"README.md\")\n}\n", 1, "p\ttest\tREADME.md"},
		{"keep_closure_named_like_a_package_func", "func repoRoot() string { return \"..\" }\n\nfunc helper(string) {}\n\nfunc use() {\n" +
			"\thelper := func(s string) { _, _ = os.ReadFile(filepath.Join(repoRoot(), s)) }\n\thelper(\"README.md\")\n}\n", 1, "p\ttest\tREADME.md"},
		{"keep_method_with_no_declaration", "type guard struct{}\n\nvar _ = guard{}.read(\"README.md\")\n", 1, "p\ttest\tREADME.md"},
		// The fixture shapes of the README.md rows the flow drops: a changed-path list written to a file, a struct field
		// only compared, and a path list handed to a script by the name of the file that holds it.
		{"drop_changed_list_written_to_a_file", "func TestX(t *testing.T) {\n\tsel := func(changed ...string) error {\n" +
			"\t\tif err := os.WriteFile(\"/tmp/changed.txt\", []byte(strings.Join(changed, \"\\n\")), 0o644); err != nil {\n\t\t\treturn fmt.Errorf(\"write: %w\", err)\n\t\t}\n\t\treturn nil\n\t}\n" +
			"\tfor _, path := range []string{\"guide.txt\", \"README.md\"} {\n\t\tif err := sel(path); err != nil {\n\t\t\tt.Fatalf(\"%s: %v\", path, err)\n\t\t}\n\t}\n}\n", 0, ""},
		{"drop_struct_field_only_compared", "func TestX(t *testing.T) {\n\tcases := []struct {\n\t\tname    string\n\t\tchanged []string\n\t}{{\"x\", []string{\"README.md\"}}}\n" +
			"\tfor _, c := range cases {\n\t\tt.Run(c.name, func(t *testing.T) {\n\t\t\tif strings.Join(c.changed, \",\") == \"\" {\n\t\t\t\tt.Fatal(\"empty\")\n\t\t\t}\n\t\t})\n\t}\n}\n", 0, ""},
		{"drop_paths_handed_to_a_script_by_file", "func selected(paths ...string) error {\n\tfile := \"/tmp/changed.txt\"\n" +
			"\tif err := os.WriteFile(file, []byte(strings.Join(paths, \"\\n\")), 0o644); err != nil {\n\t\treturn err\n\t}\n\treturn exec.Command(\"bash\", \"x.sh\", file).Run()\n}\n\n" +
			"func use() error { return selected(\"README.md\") }\n", 1, "p\tp_test.go\tunexpressed"},
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
	for _, p := range []string{"fmt", "io", "io/fs", "os", "os/exec", "path/filepath", "strings", "testing", "example.com/othermod"} {
		if strings.Contains(src, filepath.Base(p)+".") {
			b.WriteString("\t\"" + p + "\"\n")
		}
	}
	b.WriteString(")\n\n")
	return b.String()
}

// A method named like an entry method but called on another value (here one that reads README.md) is no pure gate
// condition: the walk records no filter.
func TestPlanReadsWalkFilterTrustsOnlyTheEntrysMethods(t *testing.T) {
	for name, pre := range map[string]string{
		"another_value":                "\t\tif probe.IsDir() {\n\t\t\treturn nil\n\t\t}\n",
		"entry_name_redeclared":        "\t\td := probeT{}\n\t\tif d.IsDir() {\n\t\t\treturn nil\n\t\t}\n",
		"entry_name_redeclared_in_two": "\t\td, n := probeT{}, 0\n\t\tif d.IsDir() || n > 0 {\n\t\t\treturn nil\n\t\t}\n",
	} {
		src := "package w\n\nimport (\n\t\"io/fs\"\n\t\"os\"\n\t\"path/filepath\"\n\t\"strings\"\n)\n\n" +
			"type probeT struct{}\n\nfunc (probeT) IsDir() bool {\n\t_, err := os.ReadFile(\"../README.md\")\n\treturn err != nil\n}\n\nvar probe probeT\n\n" +
			"func repoRoot() string { return \"..\" }\n\nfunc walk() error {\n\treturn filepath.WalkDir(repoRoot(), func(path string, d fs.DirEntry, err error) error {\n" +
			pre + "\t\tif !strings.HasSuffix(path, \".go\") {\n\t\t\treturn nil\n\t\t}\n\t\t_, rerr := os.ReadFile(path)\n\t\treturn rerr\n\t})\n}\n"
		res := planReads(t, readsFixture(t, map[string]string{"w/w_walk_test.go": src}))
		out := res.stdout + res.stderr
		if !strings.Contains(out, "w\tw_walk_test.go\t.\ttree\t*\n") || strings.Contains(out, "w\tw_walk_test.go\t.\ttree\t*.go") {
			t.Errorf("%s: want the walk row with filter *, and none with *.go:\n%s", name, out)
		}
	}
}

// A method call resolves to a method of the directory only when the receiver's type is known: a name declared twice,
// a type declared twice (a function-local alias, the package and its external tests) leaves it unresolved, and the
// literal stays a read.
func TestPlanReadsBareNamesShadowedTypesStayReads(t *testing.T) {
	classes := "README.md\tREADME.md\n"
	cases := map[string]map[string]string{
		"package_var_and_a_local_in_a_sibling_block": {"p/p_test.go": "package p\n\nimport \"example.com/othermod\"\n\n" +
			"type local struct{}\n\nfunc (local) Read(string) {}\n\nvar g othermod.Reader\n\n" +
			"func use() {\n\t{\n\t\tg := local{}\n\t\tg.Read(\"x\")\n\t}\n\t_ = g.Read(\"README.md\")\n}\n"},
		"function_local_alias_shadows_the_type": {"p/p_test.go": "package p\n\nimport \"example.com/othermod\"\n\n" +
			"type helper struct{}\n\nfunc (helper) Read(string) {}\n\n" +
			"func use() {\n\ttype helper = othermod.Reader\n\tvar h helper\n\t_ = h.Read(\"README.md\")\n}\n"},
		"package_and_external_tests_declare_the_type": {
			"p/p.go":      "package p\n\ntype helper struct{}\n\nfunc (helper) Read(string) {}\n",
			"p/p_test.go": "package p_test\n\nimport \"example.com/othermod\"\n\ntype helper = othermod.Reader\n\nvar h helper\n\nvar _ = h.Read(\"README.md\")\n"},
	}
	for name, files := range cases {
		files["README.md"] = "r\n"
		files["internal/cigates/affected/classes.tsv"] = classes
		res := planReads(t, readsFixture(t, files))
		if out := res.stdout + res.stderr; !strings.Contains(out, "p\ttest\tREADME.md") {
			t.Errorf("%s: plan-reads does not ask for the README.md row:\n%s", name, out)
		}
	}
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

// A gate on a package map of constant extensions (`if !exts[filepath.Ext(path)] { return nil }`) records every
// extension of the map; a map some code writes records none.
func TestPlanReadsWalkFilterExtensionSet(t *testing.T) {
	for name, c := range map[string]struct{ extra, want string }{
		"constant_map":   {"", "w\tw_walk_test.go\t.\ttree\t*.go|*.md\n"},
		"map_written_to": {"func init() { exts[\".txt\"] = true }\n", "w\tw_walk_test.go\t.\ttree\t*\n"},
	} {
		src := "package w\n\nimport (\n\t\"io/fs\"\n\t\"os\"\n\t\"path/filepath\"\n)\n\n" +
			"var exts = map[string]bool{\".md\": true, \".go\": true}\n\n" + c.extra +
			"func repoRoot() string { return \"..\" }\n\nfunc walk() error {\n\treturn filepath.WalkDir(repoRoot(), func(path string, d fs.DirEntry, err error) error {\n" +
			"\t\tif err != nil || d.IsDir() {\n\t\t\treturn nil\n\t\t}\n\t\text := filepath.Ext(path)\n\t\tif !exts[ext] {\n\t\t\treturn nil\n\t\t}\n" +
			"\t\t_, rerr := os.ReadFile(path)\n\t\treturn rerr\n\t})\n}\n"
		res := planReads(t, readsFixture(t, map[string]string{"w/w_walk_test.go": src}))
		if out := res.stdout + res.stderr; !strings.Contains(out, c.want) {
			t.Errorf("%s: want the row %q:\n%s", name, c.want, out)
		}
	}
}
