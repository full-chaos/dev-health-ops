package affected_test

import (
	"strings"
	"testing"
)

// A name concatenated with string literals and handed to a git helper that runs in a temp repository names a file of
// that repository, as the bare name does. A runtime operand, a literal that leaves the directory (a parent step or an
// absolute start) and a repository directory keep the name a read.
func TestPlanReadsLiteralConcatIntoATempGitHelper(t *testing.T) {
	classes := "docs\tdocs/**\nREADME.md\tREADME.md\n"
	head := func(cat string) string {
		return "package p\n\nimport (\n\t\"os\"\n\t\"os/exec\"\n\t\"strings\"\n\t\"testing\"\n)\n\nvar _ = os.Getenv\nvar _ = strings.TrimSpace\nvar _ = testing.Short\n\n" +
			"func repoRoot() string { return \"..\" }\n\n" +
			"type gitRepo struct {\n\tt   *testing.T\n\tdir string\n}\n\n" +
			"func (r *gitRepo) add(parent, path string) {\n\trun := func(args ...string) {\n\t\tcmd := exec.Command(\"git\", append([]string{\"-c\", \"gc.auto=0\"}, args...)...)\n\t\tcmd.Dir = r.dir\n\t\t_ = cmd.Run()\n\t}\n\trun(\"update-index\", \"--cacheinfo\", " + cat + ")\n}\n\n"
	}
	temp := "func TestX(t *testing.T) {\n\tr := &gitRepo{t: t, dir: t.TempDir()}\n\tr.add(\"abc\", \"README.md\")\n}\n"
	cases := []struct {
		name, cat, body string
		keep            bool
	}{
		{"literal_led_in_a_temp_repo", "\"100644,\"+parent+\",\"+path", temp, false},
		{"literal_led_in_the_repository", "\"100644,\"+parent+\",\"+path", "func TestX(t *testing.T) {\n\tr := &gitRepo{t: t, dir: repoRoot()}\n\tr.add(\"abc\", \"README.md\")\n}\n", true},
		{"runtime_led", "parent+\",\"+path", temp, true},
		{"call_led", "os.Getenv(\"P\")+\",\"+path", temp, true},
		{"parent_step_literal", "\"../\"+parent+\",\"+path", temp, true},
		{"parent_step_later_literal", "\"a,\"+parent+\"/../\"+path", temp, true},
		{"absolute_literal_start", "\"/x,\"+parent+\",\"+path", temp, true},
		{"name_leads", "path+\",\"+parent", temp, true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			res := planReads(t, readsFixture(t, map[string]string{"p/p_test.go": head(c.cat) + c.body, "README.md": "r\n",
				"internal/cigates/affected/classes.tsv": classes}))
			out := res.stdout + res.stderr
			if got := strings.Contains(out, "p\ttest\tREADME.md"); got != c.keep {
				t.Fatalf("a README.md reader row is %v, want %v:\n%s", got, c.keep, out)
			}
		})
	}
}

// A pure function of the module may call the encode and decode methods of a base64 encoding value: they compute from their
// arguments and read, run and open nothing. Any other method of a standard-library value, a value of another package and
// a name the file declares itself keep the name a read.
func TestPlanReadsBareNameIntoAPureLeafWithAStdlibValueMethod(t *testing.T) {
	classes := "docs\tdocs/**\nREADME.md\tREADME.md\n"
	head := "package p\n\nimport (\n\t\"os\"\n\t\"testing\"\n\n\t\"example.com/m/y\"\n)\n\nvar _ = os.Getenv\nvar _ = testing.Short\n\n"
	yhead := "package y\n\nimport (\n\t\"encoding/base64\"\n\t\"os\"\n)\n\nvar _ = base64.StdEncoding\nvar _ = os.Stdout\n\n"
	cases := []struct {
		name, lib string
		keep      bool
	}{
		{"std_encoding_encode", "func Req(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }\n", false},
		{"raw_url_encoding_decode", "func Req(s string) bool { _, err := base64.RawURLEncoding.DecodeString(s); return err == nil }\n", false},
		{"method_not_on_the_list", "func Req(s string) string { return string(base64.StdEncoding.WithPadding('*').EncodeToString([]byte(s))) }\n", true},
		{"writer_value_method", "func Req(s string) int { n, _ := os.Stdout.WriteString(s); return n }\n", true},
		{"package_name_declared_locally", "type enc interface{ EncodeToString([]byte) string }\n\nfunc Req(s string) (base64 struct{ StdEncoding enc }, out string) {\n\tout = base64.StdEncoding.EncodeToString([]byte(s))\n\treturn\n}\n", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			res := planReads(t, readsFixture(t, map[string]string{"p/p_test.go": head + "var _ = y.Req(\"README.md\")\n", "y/y.go": yhead + c.lib,
				"go.mod": "module example.com/m\n", "README.md": "r\n", "internal/cigates/affected/classes.tsv": classes}))
			out := res.stdout + res.stderr
			if got := strings.Contains(out, "p\ttest\tREADME.md"); got != c.keep {
				t.Fatalf("a README.md reader row is %v, want %v:\n%s", got, c.keep, out)
			}
		})
	}
}
