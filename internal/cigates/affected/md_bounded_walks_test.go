package affected_test

import (
	"strings"
	"testing"
)

// The walk shapes plan-reads expresses: a gate behind an `if` with an init statement, an fs.WalkDir over a bound FS, a
// walk root named through filepath.Abs, a helper function or a range over a package variable, and a helper that
// returns a temp directory or the path under a root. A shape it cannot prove stays an unexpressed walk.

const walkImports = "package c\n\nimport (\n\t\"io/fs\"\n\t\"os\"\n\t\"path/filepath\"\n\t\"strings\"\n\t\"testing\"\n)\n\n" +
	"var _ = fs.SkipDir\nvar _ = strings.HasSuffix\nvar _ = testing.Short\nvar _ = os.Getenv\nvar _ = filepath.Join\n\n"

const mdWalkCallback = "func(path string, info os.FileInfo, err error) error {\n\t\tif err != nil || !info.Mode().IsRegular() || !strings.HasSuffix(path, \".md\") {\n\t\t\treturn nil\n\t\t}\n\t\t_, rerr := os.ReadFile(path)\n\t\treturn rerr\n\t}"

func planFixture(t *testing.T, file, src string) string {
	t.Helper()
	res := planReads(t, readsFixture(t, map[string]string{file: src}))
	return res.stdout + res.stderr
}

func wantRows(t *testing.T, name, out string, want, notWant []string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(out, w) {
			t.Errorf("%s: want the row %q:\n%s", name, w, out)
		}
	}
	for _, w := range notWant {
		if strings.Contains(out, w) {
			t.Errorf("%s: unwanted row %q:\n%s", name, w, out)
		}
	}
}

func TestPlanReadsWalkGateBehindAnInitIf(t *testing.T) {
	walk := func(init string) string {
		return "package w\n\nimport (\n\t\"io/fs\"\n\t\"os\"\n\t\"path/filepath\"\n\t\"strings\"\n)\n\nfunc repoRoot() string { return \"..\" }\n\n" +
			"func walk() error {\n\troot := repoRoot()\n\treturn filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {\n" +
			"\t\tif err != nil {\n\t\t\treturn err\n\t\t}\n\t\tif d.IsDir() {\n" + init + "\t\t\treturn nil\n\t\t}\n" +
			"\t\tif !strings.HasSuffix(path, \".go\") || strings.HasSuffix(path, \"_test.go\") {\n\t\t\treturn nil\n\t\t}\n" +
			"\t\t_, rerr := os.ReadFile(path)\n\t\treturn rerr\n\t})\n}\n"
	}
	for name, c := range map[string]struct {
		init            string
		want, notWanted []string
	}{
		"pure_init_skip": {"\t\t\tif name := d.Name(); path != root && strings.HasPrefix(name, \".\") {\n\t\t\t\treturn filepath.SkipDir\n\t\t\t}\n",
			[]string{"w\tw_walk_test.go\t.\ttree\t*.go\n"}, []string{"w\tw_walk_test.go\t.\ttree\t*\n"}},
		"init_that_reads": {"\t\t\tif _, serr := os.Stat(path); serr != nil {\n\t\t\t\treturn filepath.SkipDir\n\t\t\t}\n",
			[]string{"w\tw_walk_test.go\t.\ttree\t*\n"}, []string{"w\tw_walk_test.go\t.\ttree\t*.go\n"}},
	} {
		wantRows(t, name, planFixture(t, "w/w_walk_test.go", walk(c.init)), c.want, c.notWanted)
	}
}

func TestPlanReadsFSWalkOverABoundFS(t *testing.T) {
	census := func(read string) string {
		return "package w\n\nimport (\n\t\"io/fs\"\n\t\"os\"\n\t\"strings\"\n)\n\nfunc repoRoot() string { return \"..\" }\n\n" +
			"func census(fsys fs.FS) error {\n\treturn fs.WalkDir(fsys, \".\", func(name string, d fs.DirEntry, err error) error {\n" +
			"\t\tif err != nil {\n\t\t\treturn err\n\t\t}\n\t\tif d.IsDir() || !strings.HasSuffix(name, \".go\") {\n\t\t\treturn nil\n\t\t}\n" +
			"\t\t_, rerr := fs.ReadFile(fsys, " + read + ")\n\t\treturn rerr\n\t})\n}\n\n" +
			"func Check() error { return census(os.DirFS(repoRoot())) }\n"
	}
	for name, c := range map[string]struct {
		read            string
		want, notWanted []string
	}{
		"gated_name":     {"name", []string{"w\tw_walk_test.go\t.\ttree\t*.go\n"}, []string{"w\tw_walk_test.go\t.\ttree\t*\n"}},
		"another_name":   {"\"README.md\"", []string{"w\tw_walk_test.go\t.\ttree\t*\n"}, []string{"w\tw_walk_test.go\t.\ttree\t*.go\n"}},
		"fs_used_beside": {"name); _, _ = fs.ReadFile(fsys, \"README.md\"", []string{"w\tw_walk_test.go\t.\ttree\t*\n"}, []string{"w\tw_walk_test.go\t.\ttree\t*.go\n"}},
	} {
		wantRows(t, name, planFixture(t, "w/w_walk_test.go", census(c.read)), c.want, c.notWanted)
	}
}

func TestPlanReadsWalkRootExpressions(t *testing.T) {
	repo := "func guardsRoot() string { return filepath.Join(\"..\", \"..\", \"..\") }\n\n"
	for name, c := range map[string]struct {
		src             string
		want, notWanted []string
	}{
		"abs_of_a_relative_root": {walkImports + "func TestA(t *testing.T) {\n\troot, err := filepath.Abs(filepath.Join(\"..\", \"..\", \"..\"))\n\tif err != nil {\n\t\tt.Fatal(err)\n\t}\n\t_ = filepath.Walk(filepath.Join(root, \"docs\"), " + mdWalkCallback + ")\n}\n",
			[]string{"a/b/c\tc_test.go\tdocs\ttree\t*.md\n"}, []string{"unexpressed"}},
		"abs_leaving_the_repository": {walkImports + "func TestA(t *testing.T) {\n\troot, err := filepath.Abs(filepath.Join(\"..\", \"..\", \"..\", \"..\"))\n\tif err != nil {\n\t\tt.Fatal(err)\n\t}\n\t_ = filepath.Walk(filepath.Join(root, \"docs\"), " + mdWalkCallback + ")\n}\n",
			[]string{"a/b/c\tc_test.go\tunexpressed\t-\t*.md\n"}, nil},
		"root_through_a_helper_parameter": {walkImports + repo + "func docs(root string) {\n\t_ = filepath.Walk(filepath.Join(root, \"docs\"), " + mdWalkCallback + ")\n}\n\nfunc TestC(t *testing.T) { docs(guardsRoot()) }\n",
			[]string{"a/b/c\tc_test.go\tdocs\ttree\t*.md\n"}, []string{"unexpressed"}},
		"range_over_a_package_variable": {walkImports + repo + "var scanRoots = []string{\"cmd\", \"docs\"}\n\nfunc hits(root string) {\n\tfor _, top := range scanRoots {\n\t\t_ = filepath.Walk(filepath.Join(root, top), " + mdWalkCallback + ")\n\t}\n}\n\nfunc TestB(t *testing.T) { hits(guardsRoot()) }\n",
			[]string{"a/b/c\tc_test.go\tcmd\ttree\t*.md\n", "a/b/c\tc_test.go\tdocs\ttree\t*.md\n"}, []string{"unexpressed"}},
		"range_over_a_written_variable": {walkImports + repo + "var scanRoots = []string{\"cmd\", \"docs\"}\n\nfunc init() { scanRoots = append(scanRoots, os.Getenv(\"X\")) }\n\nfunc hits(root string) {\n\tfor _, top := range scanRoots {\n\t\t_ = filepath.Walk(filepath.Join(root, top), " + mdWalkCallback + ")\n\t}\n}\n\nfunc TestB(t *testing.T) { hits(guardsRoot()) }\n",
			[]string{"a/b/c\tc_test.go\tunexpressed\t-\t*.md\n"}, nil},
	} {
		wantRows(t, name, planFixture(t, "a/b/c/c_test.go", c.src), c.want, c.notWanted)
	}
}

func TestPlanReadsInlinesHelperResults(t *testing.T) {
	for name, c := range map[string]struct {
		src             string
		want, notWanted []string
	}{
		"single_return_helper_names_the_exact_file": {"package contracts\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n)\n\nconst subpath = \"contracts\"\n\nfunc repoRoot() string { return \"..\" }\n\n" +
			"func Dir(root string) string { return filepath.Join(root, subpath) }\n\nfunc read() ([]byte, error) {\n\treturn os.ReadFile(filepath.Join(Dir(repoRoot()), \"README.md\"))\n}\n",
			[]string{"contracts\ttest\tcontracts\n"}, []string{"**/README.md"}},
		"single_return_helper_of_an_unbound_root": {"package contracts\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n)\n\nfunc Dir(root string) string { return filepath.Join(root, \"contracts\") }\n\n" +
			"func read() ([]byte, error) {\n\treturn os.ReadFile(filepath.Join(Dir(os.Getenv(\"R\")), \"README.md\"))\n}\n",
			[]string{"contracts\tcontracts_test.go\t**/README.md\n"}, nil},
		"helper_returning_a_temp_dir": {walkImports + "func repoDir(t *testing.T) string {\n\troot := t.TempDir()\n\tif err := os.WriteFile(filepath.Join(root, \"go.mod\"), []byte(\"x\"), 0o644); err != nil {\n\t\tt.Fatal(err)\n\t}\n\treturn root\n}\n\n" +
			"func TestD(t *testing.T) {\n\troot := repoDir(t)\n\t_ = filepath.Walk(filepath.Join(root, \"docs\"), " + mdWalkCallback + ")\n}\n",
			nil, []string{"unexpressed"}},
		"helper_returning_an_environment_path": {walkImports + "func repoDir(t *testing.T) string {\n\treturn os.Getenv(\"R\")\n}\n\n" +
			"func TestD(t *testing.T) {\n\troot := repoDir(t)\n\t_ = filepath.Walk(filepath.Join(root, \"docs\"), " + mdWalkCallback + ")\n}\n",
			[]string{"a/b/c\tc_test.go\tunexpressed\t-\t*.md\n"}, nil},
	} {
		file := "a/b/c/c_test.go"
		if strings.HasPrefix(c.src, "package contracts") {
			file = "contracts/contracts_test.go"
		}
		wantRows(t, name, planFixture(t, file, c.src), c.want, c.notWanted)
	}
}
