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
