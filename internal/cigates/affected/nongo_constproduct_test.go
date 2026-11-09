package affected_test

import "testing"

// CHAOS-10440: a path built with + or Join from constants and sets of constants is the finite set of its combinations,
// each bounded as a constant of its own. The fixture reads "../docs/a.md" through the shape under test: a bounded shape
// needs no dynreads.tsv row (plan-reads is green without one) and selects the package for docs changes only. A shape
// with an operand the code does not spell out (an environment value, a parameter, a changed table) keeps its row.
const cpTbl = "\tnames := []string{\"a.md\", \"b.md\"}\n"

func escapedProductFlow(name, body string) nongoCase {
	c := escapedFlow(name, body)
	c.readsCode = 0
	return c
}

func unrowedProductFlow(name, body string) nongoCase {
	head := flowFile(body)
	head[tablesDir+"reads.tsv"] = withReads("x\ttest\tdocs\n")
	return nongoCase{name: name + "_needs_its_dynreads_row", head: head, changed: []string{".claude/x.json"}, want: "FULL the read tables differ*",
		readsCode: 1, readsWant: []string{"x\tx_flow_test.go"}}
}

func TestNonGoPlanConstantProductsAreBoundedByTheirElements(t *testing.T) {
	var cases []nongoCase
	for _, c := range []struct{ name, body string }{
		{"plus_of_a_prefix_and_a_set", "func TestFlow(t *testing.T) {\n" + cpTbl + "\tfor _, n := range names {\n\t\t_, _ = os.ReadFile(\"../docs/\" + n)\n\t}\n}\n"},
		{"plus_of_a_set_and_a_suffix", "func TestFlow(t *testing.T) {\n\tstems := []string{\"a\", \"b\"}\n\tfor _, s := range stems {\n\t\t_, _ = os.ReadFile(\"../docs/\" + s + \".md\")\n\t}\n}\n"},
		{"join_of_literals_and_a_set", "func TestFlow(t *testing.T) {\n" + cpTbl + "\tfor _, n := range names {\n\t\t_, _ = os.ReadFile(filepath.Join(\"..\", \"docs\", n))\n\t}\n}\n"},
		{"join_of_a_set_and_a_set", "func TestFlow(t *testing.T) {\n\tdirs := []string{\"../docs\", \"../docs/.\"}\n" + cpTbl + "\tfor _, d := range dirs {\n\t\tfor _, n := range names {\n\t\t\t_, _ = os.ReadFile(filepath.Join(d, n))\n\t\t}\n\t}\n}\n"},
	} {
		c.body = "import \"path/filepath\"\n\nvar _ = filepath.Join\n\n" + c.body
		cases = append(cases, boundedFlow(c.name, c.body)...)
	}
	runNongo(t, cases)
}

func TestNonGoPlanConstantProductNearMissesKeepTheirRow(t *testing.T) {
	runNongo(t, []nongoCase{
		escapedProductFlow("plus_of_a_set_and_an_environment_value", "func TestFlow(t *testing.T) {\n"+cpTbl+"\tfor _, n := range names {\n\t\t_, _ = os.ReadFile(\"../docs/\" + n + os.Getenv(\"P\"))\n\t}\n}\n"),
		escapedProductFlow("plus_of_a_set_and_a_parameter", "func read(p string) {\n\tfor _, n := range []string{\"a.md\", \"b.md\"} {\n\t\t_, _ = os.ReadFile(\"../docs/\" + n + p)\n\t}\n}\n\nfunc TestFlow(t *testing.T) { read(os.Getenv(\"P\")) }\n"),
		escapedProductFlow("set_with_an_appended_element", "func TestFlow(t *testing.T) {\n"+cpTbl+"\tnames = append(names, os.Getenv(\"P\"))\n\tfor _, n := range names {\n\t\t_, _ = os.ReadFile(\"../docs/\" + n)\n\t}\n}\n"),
		unrowedProductFlow("plus_of_a_set_and_an_unspelled_tail", "func TestFlow(t *testing.T) {\n"+cpTbl+"\tfor _, n := range names {\n\t\t_, _ = os.ReadFile(n + os.Getenv(\"P\"))\n\t}\n}\n"),
	})
}
