package affected_test

import (
	"strings"
	"testing"
)

// A root file name in test data is a read of the repository file only when its value can reach a reader. These shapes
// are the ones the value flow follows to the end: an argument of a program run in a temp directory, and a value
// converted to an integer type. Every shape it cannot prove keeps the name a read.
func TestPlanReadsBareNameFlowShapes(t *testing.T) {
	classes := "docs\tdocs/**\nREADME.md\tREADME.md\n"
	head := "package p\n\nimport (\n\t\"crypto/sha256\"\n\t\"os\"\n\t\"os/exec\"\n\t\"path/filepath\"\n\t\"testing\"\n)\n\n" +
		"var _ = sha256.Sum256\nvar _ = os.Getenv\nvar _ = filepath.Join\nvar _ = exec.Command\nvar _ = testing.Short\n\n" +
		"func repoRoot() string { return \"..\" }\n\n"
	run := "func run(t *testing.T, dir string, args ...string) {\n\tcmd := exec.Command(\"git\", args...)\n\tcmd.Dir = dir\n\t_ = cmd.Run()\n}\n\n"
	cases := []struct {
		name string
		body string
		keep bool // the README.md name stays a read of the repository file
	}{
		{"exec_in_a_temp_dir", run + "func TestX(t *testing.T) { run(t, t.TempDir(), \"add\", \"README.md\") }\n", false},
		{"exec_in_a_temp_dir_made_by_a_helper", "func work(t *testing.T) string { d := t.TempDir(); return d }\n\n" + run + "func TestX(t *testing.T) { run(t, work(t), \"add\", \"README.md\") }\n", false},
		{"exec_in_the_repository", run + "func TestX(t *testing.T) { run(t, repoRoot(), \"add\", \"README.md\") }\n", true},
		{"exec_in_an_unknown_dir", run + "func TestX(t *testing.T) { run(t, os.Getenv(\"D\"), \"add\", \"README.md\") }\n", true},
		{"exec_in_a_temp_dir_and_in_the_repository", run + "func TestX(t *testing.T) {\n\trun(t, t.TempDir(), \"add\", \"README.md\")\n\trun(t, repoRoot(), \"add\", \"x\")\n}\n", true},
		{"exec_in_a_temp_dir_with_a_joined_name", run + "func TestX(t *testing.T) { run(t, t.TempDir(), \"add\", os.Getenv(\"ROOT\")+\"/\"+\"README.md\") }\n", true},
		{"exec_without_a_dir", "func TestX(t *testing.T) { _ = exec.Command(\"git\", \"add\", \"README.md\").Run() }\n", true},
		{"integer_conversion_of_a_hash", "func TestX(t *testing.T) {\n\tfor file := range map[string]int{\"README.md\": 0} {\n\t\tsum := sha256.Sum256([]byte(file))\n\t\t_ = 1 + int(sum[0])%3\n\t}\n}\n", false},
		{"integer_conversion_reaching_a_read", "func TestX(t *testing.T) {\n\tfor file := range map[string]int{\"README.md\": 0} {\n\t\tsum := sha256.Sum256([]byte(file))\n\t\t_, _ = os.ReadFile(filepath.Join(repoRoot(), string(sum[:])+file))\n\t}\n}\n", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			res := planReads(t, readsFixture(t, map[string]string{"p/p_test.go": head + c.body, "README.md": "r\n",
				"internal/cigates/affected/classes.tsv": classes}))
			out := res.stdout + res.stderr
			if got := strings.Contains(out, "p\ttest\tREADME.md"); got != c.keep {
				t.Fatalf("a README.md reader row is %v, want %v:\n%s", got, c.keep, out)
			}
		})
	}
}

// A name handed to a function of another package of the module is no read when that function only computes: its body
// resolves, it reads, runs and opens nothing, and every call in it is a builtin, a string or path function or another
// such function of the module. Its result is still followed. A callee that reads, calls an interface method or a
// function value, or calls another package keeps the name a read.
func TestPlanReadsBareNameIntoAPureModuleFunction(t *testing.T) {
	classes := "docs\tdocs/**\nREADME.md\tREADME.md\n"
	head := "package p\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n\t\"testing\"\n\n\t\"example.com/m/y\"\n)\n\nvar _ = os.Getenv\nvar _ = filepath.Join\nvar _ = testing.Short\n\n" +
		"func repoRoot() string { return \"..\" }\n\n"
	yhead := "package y\n\nimport (\n\t\"os\"\n\t\"strings\"\n\n\t\"example.com/othermod\"\n)\n\nvar _ = os.Getenv\nvar _ = strings.ToUpper\nvar _ = othermod.Skip\n\n"
	cases := []struct {
		name, lib, use string
		keep           bool
	}{
		{"pure_result_compared", "func Has(s string) bool { return strings.HasSuffix(s, \".md\") }\n", "var _ = y.Has(\"README.md\")\n", false},
		{"pure_result_read", "func Clean(s string) string { return strings.TrimSpace(s) }\n", "var _, _ = os.ReadFile(filepath.Join(repoRoot(), y.Clean(\"README.md\")))\n", true},
		{"pure_through_a_pure_helper", "func inner(s string) bool { return len(s) > 3 }\n\nfunc Has(s string) bool { return inner(strings.ToUpper(s)) }\n", "var _ = y.Has(\"README.md\")\n", false},
		{"reads", "func Has(s string) bool { _, err := os.Stat(s); return err == nil }\n", "var _ = y.Has(\"README.md\")\n", true},
		{"reads_through_a_helper", "func look(s string) bool { _, err := os.ReadFile(s); return err == nil }\n\nfunc Has(s string) bool { return look(s) }\n", "var _ = y.Has(\"README.md\")\n", true},
		{"calls_another_package", "func Has(s string) bool { return othermod.Skip(s) }\n", "var _ = y.Has(\"README.md\")\n", true},
		{"calls_an_interface_method", "type R interface{ Has(string) bool }\n\nfunc Has(r R, s string) bool { return r.Has(s) }\n", "var _ = y.Has(nil, \"README.md\")\n", true},
		{"calls_a_function_value", "var probe = func(string) bool { return true }\n\nfunc Has(s string) bool { return probe(s) }\n", "var _ = y.Has(\"README.md\")\n", true},
		{"starts_a_goroutine", "func Has(s string) bool {\n\tgo func() { _, _ = os.ReadFile(s) }()\n\treturn true\n}\n", "var _ = y.Has(\"README.md\")\n", true},
		{"no_body_in_the_module", "", "var _ = y.Missing(\"README.md\")\n", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			res := planReads(t, readsFixture(t, map[string]string{"p/p_test.go": head + c.use, "y/y.go": yhead + c.lib, "go.mod": "module example.com/m\n", "README.md": "r\n",
				"internal/cigates/affected/classes.tsv": classes}))
			out := res.stdout + res.stderr
			if got := strings.Contains(out, "p\ttest\tREADME.md"); got != c.keep {
				t.Fatalf("a README.md reader row is %v, want %v:\n%s", got, c.keep, out)
			}
		})
	}
}

// A name written into a strings.Builder or bytes.Buffer lives on in the builder: only its String or Bytes result carries
// it on. Any other method of the builder, or a builder handed on, keeps the name a read.
func TestPlanReadsBareNameThroughABuilder(t *testing.T) {
	classes := "docs\tdocs/**\nREADME.md\tREADME.md\n"
	head := "package p\n\nimport (\n\t\"bytes\"\n\t\"os\"\n\t\"path/filepath\"\n\t\"strings\"\n\t\"testing\"\n)\n\n" +
		"var _ = bytes.NewBuffer\nvar _ = os.Getenv\nvar _ = filepath.Join\nvar _ = strings.ToLower\nvar _ = testing.Short\n\n" +
		"func repoRoot() string { return \"..\" }\n\n"
	cases := []struct {
		name, body string
		keep       bool
	}{
		{"builder_string_compared", "func ident(name string) string {\n\tvar out strings.Builder\n\tfor _, r := range strings.ToLower(name) {\n\t\tout.WriteRune(r)\n\t}\n\tif out.Len() == 0 {\n\t\treturn \"x\"\n\t}\n\treturn out.String()\n}\n\nvar _ = ident(\"README.md\") == \"\"\n", false},
		{"builder_string_read", "func ident(name string) string {\n\tvar out strings.Builder\n\tout.WriteString(name)\n\treturn out.String()\n}\n\nvar _, _ = os.ReadFile(filepath.Join(repoRoot(), ident(\"README.md\")))\n", true},
		{"buffer_bytes_read", "func ident(name string) []byte {\n\tvar out bytes.Buffer\n\tout.WriteString(name)\n\treturn out.Bytes()\n}\n\nvar _, _ = os.ReadFile(filepath.Join(repoRoot(), string(ident(\"README.md\"))))\n", true},
		{"builder_read_from", "func ident(name string) string {\n\tvar out strings.Builder\n\tout.WriteString(name)\n\t_, _ = out.WriteTo(nil)\n\treturn \"\"\n}\n\nvar _ = ident(\"README.md\")\n", true},
		{"builder_handed_on", "func fill(b *strings.Builder) { _, _ = os.ReadFile(b.String()) }\n\nfunc ident(name string) {\n\tvar out strings.Builder\n\tout.WriteString(name)\n\tfill(&out)\n}\n\nvar _ = ident\n\nfunc use() { ident(\"README.md\") }\n", true},
		{"unknown_type_with_the_same_methods", "type w struct{}\n\nfunc (w) WriteString(string) {}\n\nfunc ident(out othermod2) {\n\tout.WriteString(\"README.md\")\n}\n", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			body := c.body
			if strings.Contains(body, "othermod2") {
				body = strings.ReplaceAll(body, "othermod2", "othermod.W")
				body = strings.Replace(head, "\t\"testing\"\n)", "\t\"testing\"\n\n\t\"example.com/othermod\"\n)", 1) + body
			} else {
				body = head + body
			}
			res := planReads(t, readsFixture(t, map[string]string{"p/p_test.go": body, "README.md": "r\n",
				"internal/cigates/affected/classes.tsv": classes}))
			out := res.stdout + res.stderr
			if got := strings.Contains(out, "p\ttest\tREADME.md"); got != c.keep {
				t.Fatalf("a README.md reader row is %v, want %v:\n%s", got, c.keep, out)
			}
		})
	}
}

// A program run in a directory kept in a field of a helper struct runs in a temp dir when every value the package
// stores in that field is under a temp dir: its literals, its assignments, and the literal that leaves it out.
func TestPlanReadsBareNameExecInAStructFieldDir(t *testing.T) {
	classes := "docs\tdocs/**\nREADME.md\tREADME.md\n"
	head := "package p\n\nimport (\n\t\"os\"\n\t\"os/exec\"\n\t\"testing\"\n)\n\nvar _ = os.Getenv\nvar _ = testing.Short\n\n" +
		"func repoRoot() string { return \"..\" }\n\n" +
		"type gitRepo struct {\n\tt   *testing.T\n\tdir string\n}\n\n" +
		"func (r *gitRepo) git(args ...string) {\n\tcmd := exec.Command(\"git\", args...)\n\tcmd.Dir = r.dir\n\t_ = cmd.Run()\n}\n\n"
	cases := []struct {
		name, body string
		keep       bool
	}{
		{"temp_literal", "func TestX(t *testing.T) {\n\tr := &gitRepo{t: t, dir: t.TempDir()}\n\tr.git(\"add\", \"README.md\")\n}\n", false},
		{"copy_of_a_temp_dir", "func TestX(t *testing.T) {\n\tr := &gitRepo{t: t, dir: t.TempDir()}\n\trr := &gitRepo{t: t, dir: r.dir}\n\trr.git(\"add\", \"README.md\")\n}\n", false},
		{"temp_assignment", "func TestX(t *testing.T) {\n\tvar r *gitRepo = &gitRepo{t: t, dir: t.TempDir()}\n\tr.dir = t.TempDir()\n\tr.git(\"add\", \"README.md\")\n}\n", false},
		{"another_struct_with_the_same_field_name", "type spec struct{ dir string }\n\nvar _ = spec{dir: repoRoot()}\n\nfunc TestX(t *testing.T) {\n\tr := &gitRepo{t: t, dir: t.TempDir()}\n\tr.git(\"add\", \"README.md\")\n}\n", false},
		{"assignment_through_an_unknown_receiver", "func set(x any) {}\n\nfunc TestX(t *testing.T) {\n\tr := &gitRepo{t: t, dir: t.TempDir()}\n\tm := map[string]*gitRepo{\"a\": r}\n\tm[\"a\"].dir = repoRoot()\n\tr.git(\"add\", \"README.md\")\n}\n", true},
		{"repository_literal", "func TestX(t *testing.T) {\n\tr := &gitRepo{t: t, dir: repoRoot()}\n\tr.git(\"add\", \"README.md\")\n}\n", true},
		{"temp_and_repository_literals", "func TestX(t *testing.T) {\n\tr := &gitRepo{t: t, dir: t.TempDir()}\n\tq := &gitRepo{t: t, dir: repoRoot()}\n\tr.git(\"add\", \"README.md\")\n\tq.git(\"x\")\n}\n", true},
		{"literal_without_the_dir", "func TestX(t *testing.T) {\n\tr := &gitRepo{t: t, dir: t.TempDir()}\n\tq := &gitRepo{t: t}\n\tr.git(\"add\", \"README.md\")\n\tq.git(\"x\")\n}\n", true},
		{"unkeyed_literal", "func TestX(t *testing.T) {\n\tr := &gitRepo{t, t.TempDir()}\n\tr.git(\"add\", \"README.md\")\n}\n", true},
		{"assignment_of_the_repository", "func TestX(t *testing.T) {\n\tr := &gitRepo{t: t, dir: t.TempDir()}\n\tr.dir = repoRoot()\n\tr.git(\"add\", \"README.md\")\n}\n", true},
		{"zero_value_struct", "func TestX(t *testing.T) {\n\tvar r gitRepo\n\tr.dir = t.TempDir()\n\tr.git(\"add\", \"README.md\")\n}\n", true},
		{"address_of_the_field", "func TestX(t *testing.T) {\n\tr := &gitRepo{t: t, dir: t.TempDir()}\n\tp := &r.dir\n\t*p = repoRoot()\n\tr.git(\"add\", \"README.md\")\n}\n", true},
		{"slice_of_structs", "func TestX(t *testing.T) {\n\trs := []gitRepo{{t: t, dir: repoRoot()}, {t: t, dir: t.TempDir()}}\n\trs[0].git(\"add\", \"README.md\")\n}\n", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			res := planReads(t, readsFixture(t, map[string]string{"p/p_test.go": head + c.body, "README.md": "r\n",
				"internal/cigates/affected/classes.tsv": classes}))
			out := res.stdout + res.stderr
			if got := strings.Contains(out, "p\ttest\tREADME.md"); got != c.keep {
				t.Fatalf("a README.md reader row is %v, want %v:\n%s", got, c.keep, out)
			}
		})
	}
}

// A path joined to a directory that is a temp dir names a file of that temp dir: it is no read of the repository path
// its parts spell. A directory the scan cannot place keeps the parts a read.
func TestPlanReadsJoinUnderATempDirIsNoRead(t *testing.T) {
	classes := "docs\tdocs/**\nREADME.md\tREADME.md\n"
	head := "package p\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n\t\"testing\"\n)\n\nvar _ = os.Getenv\nvar _ = testing.Short\nvar _ = filepath.Join\n\n" +
		"func repoRoot() string { return \"..\" }\n\nfunc repoDir(t *testing.T) string {\n\troot := t.TempDir()\n\treturn root\n}\n\n"
	cases := []struct {
		name, body string
		keep       bool
	}{
		{"temp_helper", "func TestX(t *testing.T) {\n\troot := repoDir(t)\n\t_ = os.MkdirAll(filepath.Join(root, \"docs\", \"deep\"), 0o755)\n}\n", false},
		{"temp_directly", "func TestX(t *testing.T) {\n\troot := t.TempDir()\n\t_ = os.MkdirAll(filepath.Join(root, \"docs\"), 0o755)\n}\n", false},
		{"repository_root", "func TestX(t *testing.T) {\n\troot := repoRoot()\n\t_ = os.MkdirAll(filepath.Join(root, \"docs\"), 0o755)\n}\n", true},
		{"unknown_root", "func TestX(t *testing.T) {\n\troot := os.Getenv(\"R\")\n\t_ = os.MkdirAll(filepath.Join(root, \"docs\"), 0o755)\n}\n", true},
		{"temp_and_repository_roots", "func mk(root string) { _ = os.MkdirAll(filepath.Join(root, \"docs\"), 0o755) }\n\nfunc TestX(t *testing.T) {\n\tmk(repoDir(t))\n\tmk(repoRoot())\n}\n", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			res := planReads(t, readsFixture(t, map[string]string{"p/p_test.go": head + c.body, "README.md": "r\n",
				"internal/cigates/affected/classes.tsv": classes}))
			out := res.stdout + res.stderr
			if got := strings.Contains(out, "p\ttest\tdocs\n"); got != c.keep {
				t.Fatalf("a docs reader row is %v, want %v:\n%s", got, c.keep, out)
			}
		})
	}
}
