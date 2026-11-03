package affected_test

import (
	"strings"
	"testing"
)

func mdShape(imports, rest string) string {
	return "package x\n\nimport (\n" + imports + ")\n\n" + rest
}

// The Markdown evidence of a constant reaches the patterns.tsv row whatever route the constant takes to the call: a
// constant of another package of the module, a conversion to a named string type, a const group that repeats the
// previous expression, and a slice spread as variadic arguments.
func TestPlanReadsRecordsMarkdownEvidenceShapes(t *testing.T) {
	exec := func(args string) string {
		return "func tracked() ([]byte, error) {\n\treturn exec.Command(" + args + ").Output()\n}\n"
	}
	cases := []struct {
		name  string
		files map[string]string
		want  string
		not   bool
	}{
		{"module_const_to_exec", map[string]string{
			"y/y.go":        "package y\n\nconst Glob = \"*.md\"\n",
			"x/x_a_test.go": mdShape("\t\"m/y\"\n\t\"os/exec\"\n", exec("\"git\", \"ls-files\", \"--\", y.Glob"))}, "x\tx_a_test.go\t*.md\n", false},
		{"module_const_to_glob", map[string]string{
			"y/y.go":        "package y\n\nconst Glob = \"*.md\"\n",
			"x/x_b_test.go": mdShape("\t\"m/y\"\n\t\"path/filepath\"\n", "func g(root string) ([]string, error) {\n\treturn filepath.Glob(filepath.Join(root, y.Glob))\n}\n")}, "x\tx_b_test.go\t**/*.md\n", false},
		{"module_const_not_md", map[string]string{
			"y/y.go":        "package y\n\nconst Glob = \"*.go\"\n",
			"x/x_c_test.go": mdShape("\t\"m/y\"\n\t\"os/exec\"\n", exec("\"git\", \"ls-files\", \"--\", y.Glob"))}, "x\tx_c_test.go\t*.md\n", true},
		{"named_type_conversion_to_glob", map[string]string{
			"x/x_d_test.go": mdShape("\t\"path/filepath\"\n", "type T string\n\nfunc g(root string) ([]string, error) {\n\treturn filepath.Glob(filepath.Join(root, string(T(\"*.md\"))))\n}\n")}, "x\tx_d_test.go\t**/*.md\n", false},
		{"named_type_const_to_glob", map[string]string{
			"x/x_e_test.go": mdShape("\t\"path/filepath\"\n", "type T string\n\nconst pat T = \"*.md\"\n\nfunc g(root string) ([]string, error) {\n\treturn filepath.Glob(filepath.Join(root, string(pat)))\n}\n")}, "x\tx_e_test.go\t**/*.md\n", false},
		{"const_group_repeat", map[string]string{
			"x/x_f_test.go": mdShape("\t\"os/exec\"\n", "const (\n\ta = \"*.md\"\n\tb\n)\n\n"+exec("\"git\", \"ls-files\", \"--\", b"))}, "x\tx_f_test.go\t*.md\n", false},
		{"const_group_repeat_not_md", map[string]string{
			"x/x_g_test.go": mdShape("\t\"os/exec\"\n", "const (\n\ta = \"*.go\"\n\tb\n)\n\n"+exec("\"git\", \"ls-files\", \"--\", b"))}, "x\tx_g_test.go\t*.md\n", true},
		{"spread_slice_var_to_exec", map[string]string{
			"x/x_h_test.go": mdShape("\t\"os/exec\"\n", "func tracked() ([]byte, error) {\n\targs := []string{\"ls-files\", \"--\", \"*.md\"}\n\treturn exec.Command(\"git\", args...).Output()\n}\n")}, "x\tx_h_test.go\t*.md\n", false},
		{"spread_appended_slice_to_exec", map[string]string{
			"x/x_i_test.go": mdShape("\t\"os/exec\"\n", "func tracked() ([]byte, error) {\n\targs := append([]string{\"ls-files\", \"--\"}, \"*.md\")\n\treturn exec.Command(\"git\", args...).Output()\n}\n")}, "x\tx_i_test.go\t*.md\n", false},
		{"spread_slice_var_not_md", map[string]string{
			"x/x_j_test.go": mdShape("\t\"os/exec\"\n", "func tracked() ([]byte, error) {\n\targs := []string{\"ls-files\", \"--\", \"*.go\"}\n\treturn exec.Command(\"git\", args...).Output()\n}\n")}, "x\tx_j_test.go\t*.md\n", true},
		{"spread_slice_var_to_join", map[string]string{
			"x/x_k_test.go": mdShape("\t\"path/filepath\"\n", "func g(root string) ([]string, error) {\n\tparts := []string{root, \"*.md\"}\n\treturn filepath.Glob(filepath.Join(parts...))\n}\n")}, "x\tx_k_test.go\t**/*.md\n", false},
	}
	for _, c := range cases {
		res := planReads(t, readsFixture(t, c.files))
		out := res.stdout + res.stderr
		if has := strings.Contains(out, c.want); has == c.not {
			t.Errorf("%s: want row %q present=%v:\n%s", c.name, c.want, !c.not, out)
		}
	}
}
