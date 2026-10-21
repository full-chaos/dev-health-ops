package affected_test

import "testing"

// CHAOS-10547: reads whose path flows from a table the code spells out are bounded by the table. The fixture file
// x/x_flow_test.go reads "../docs/a.md" through the flow under test: a bounded flow leaves only the reads.tsv row for
// docs (the package is selected by a docs change and by nothing else), an escape keeps the dynreads.tsv row (the package
// is selected by every change set). Each escape case is one way the table or its range variable can change before the
// read.

const flowHead = "package x\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\n"

func flowFile(body string) map[string]string {
	return map[string]string{"x/x_flow_test.go": flowHead + body}
}

func boundedFlow(name, body string) []nongoCase {
	head := flowFile(body)
	head[tablesDir+"reads.tsv"] = withReads("x\ttest\tdocs\n")
	return []nongoCase{
		{name: name + "_skips_an_unrelated_change", head: head, changed: []string{".claude/x.json"}, want: "NONE"},
		{name: name + "_selects_for_the_docs_it_reads", head: head, changed: []string{"docs/a.md"}, want: "AFFECTED 2", keys: []string{"a", "x"}},
	}
}

func escapedFlow(name, body string) nongoCase {
	head := flowFile(body)
	head[tablesDir+"reads.tsv"] = withReads("x\ttest\tdocs\n")
	head[tablesDir+"dynreads.tsv"] = "x\tx_flow_test.go\n"
	return nongoCase{name: name + "_stays_unbounded", head: head, changed: []string{".claude/x.json"}, want: "AFFECTED 1", keys: []string{"x"}}
}

func TestNonGoPlanTableDrivenReadsAreBoundedByTheTable(t *testing.T) {
	var cases []nongoCase
	for _, c := range []struct{ name, body string }{
		{"range_over_an_inline_literal", "func TestFlow(t *testing.T) {\n\tfor _, p := range []string{\"../docs/a.md\"} {\n\t\t_, _ = os.ReadFile(p)\n\t}\n}\n"},
		{"range_over_a_local_slice", "func TestFlow(t *testing.T) {\n\tfiles := []string{\"../docs/a.md\", \"../docs/a.md\"}\n\tfor _, p := range files {\n\t\t_, _ = os.ReadFile(p)\n\t}\n}\n"},
		{"struct_table_keyed_and_positional", "func TestFlow(t *testing.T) {\n\tcases := []struct{ name, path string }{{\"a\", \"../docs/a.md\"}, {name: \"b\", path: \"../docs/a.md\"}}\n\tfor _, tc := range cases {\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n"},
		{"struct_table_with_a_missing_field", "func TestFlow(t *testing.T) {\n\tcases := []struct{ name, path string }{{name: \"a\", path: \"../docs/a.md\"}, {name: \"b\"}}\n\tfor _, tc := range cases {\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n"},
		{"package_level_struct_table", "var cases = []struct{ path string }{{path: \"../docs/a.md\"}}\n\nfunc TestFlow(t *testing.T) {\n\tfor _, tc := range cases {\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n"},
		{"table_read_inside_a_subtest_closure", "func TestFlow(t *testing.T) {\n\tcases := []struct{ name, path string }{{name: \"a\", path: \"../docs/a.md\"}}\n\tfor _, tt := range cases {\n\t\ttt := tt\n\t\tt.Run(tt.name, func(t *testing.T) {\n\t\t\t_, _ = os.ReadFile(tt.path)\n\t\t})\n\t}\n}\n"},
		{"map_table_of_structs", "func TestFlow(t *testing.T) {\n\tm := map[string]struct{ path string }{\"k\": {path: \"../docs/a.md\"}}\n\tfor _, v := range m {\n\t\t_, _ = os.ReadFile(v.path)\n\t}\n}\n"},
		{"chdir_to_a_temp_dir", "func TestFlow(t *testing.T) {\n\tt.Chdir(t.TempDir())\n\t_, _ = os.ReadFile(\"../docs/a.md\")\n}\n"},
		{"chdir_to_a_temp_dir_held_in_a_variable", "func TestFlow(t *testing.T) {\n\tdir := t.TempDir()\n\tt.Chdir(dir)\n\t_, _ = os.ReadFile(\"../docs/a.md\")\n}\n"},
	} {
		cases = append(cases, boundedFlow(c.name, c.body)...)
	}
	runNongo(t, cases)
}

func TestNonGoPlanTableEscapesStayUnbounded(t *testing.T) {
	const tbl = "\tcases := []struct{ path string }{{path: \"../docs/a.md\"}}\n"
	runNongo(t, []nongoCase{
		escapedFlow("index_assignment_to_the_table", "func TestFlow(t *testing.T) {\n"+tbl+"\tcases[0].path = os.Getenv(\"P\")\n\tfor _, tc := range cases {\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n"),
		escapedFlow("table_passed_to_a_function", "func mutate(c []struct{ path string }) { c[0].path = os.Getenv(\"P\") }\n\nfunc TestFlow(t *testing.T) {\n"+tbl+"\tmutate(cases)\n\tfor _, tc := range cases {\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n"),
		escapedFlow("table_appended_to", "func TestFlow(t *testing.T) {\n"+tbl+"\tcases = append(cases, struct{ path string }{os.Getenv(\"P\")})\n\tfor _, tc := range cases {\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n"),
		escapedFlow("table_redefined_in_a_closure", "func TestFlow(t *testing.T) {\n"+tbl+"\tfunc() {\n\t\tcases := []struct{ path string }{{path: os.Getenv(\"P\")}}\n\t\t_ = cases\n\t}()\n\tfor _, tc := range cases {\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n"),
		escapedFlow("field_assigned_in_the_loop", "func TestFlow(t *testing.T) {\n"+tbl+"\tfor _, tc := range cases {\n\t\ttc.path = os.Getenv(\"P\")\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n"),
		escapedFlow("element_address_taken", "func TestFlow(t *testing.T) {\n"+tbl+"\tfor _, tc := range cases {\n\t\tp := &tc\n\t\tp.path = os.Getenv(\"P\")\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n"),
		escapedFlow("method_called_on_the_element", "func TestFlow(t *testing.T) {\n"+tbl+"\tfor _, tc := range cases {\n\t\ttc.set(os.Getenv(\"P\"))\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n"),
		escapedFlow("element_value_from_the_environment", "func TestFlow(t *testing.T) {\n\tcases := []struct{ path string }{{path: \"../docs/a.md\"}, {path: os.Getenv(\"P\")}}\n\tfor _, tc := range cases {\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n"),
		escapedFlow("element_of_a_named_type", "type row struct{ path string }\n\nfunc TestFlow(t *testing.T) {\n\tcases := []row{{\"../docs/a.md\"}}\n\tfor _, tc := range cases {\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n"),
		escapedFlow("same_range_variable_name_over_a_call", "func load() []struct{ path string } { return nil }\n\nfunc TestFlow(t *testing.T) {\n"+tbl+"\tfor _, tc := range cases {\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n\tfor _, tc := range load() {\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n"),
		escapedFlow("range_variable_is_a_parameter_of_the_function", "func TestFlow(t *testing.T) {\n"+tbl+"\tfor _, tc := range cases {\n\t\t_, _ = os.ReadFile(tc.path)\n\t}\n}\n\nfunc other(tc struct{ path string }) { _, _ = os.ReadFile(tc.path) }\n"),
		escapedFlow("range_over_a_slice_built_by_a_call", "func names() []string { return []string{os.Getenv(\"P\")} }\n\nfunc TestFlow(t *testing.T) {\n\tfor _, p := range names() {\n\t\t_, _ = os.ReadFile(p)\n\t}\n\t_, _ = os.ReadFile(\"../docs/a.md\")\n}\n"),
		escapedFlow("chdir_to_the_environment", "func TestFlow(t *testing.T) {\n\tt.Chdir(os.Getenv(\"D\"))\n\t_, _ = os.ReadFile(\"../docs/a.md\")\n}\n"),
		escapedFlow("chdir_to_a_temp_dir_and_back", "func TestFlow(t *testing.T) {\n\tprev, _ := os.Getwd()\n\tt.Chdir(t.TempDir())\n\t_ = os.Chdir(prev)\n\t_, _ = os.ReadFile(\"../docs/a.md\")\n}\n"),
		escapedFlow("chdir_to_a_repository_directory", "func TestFlow(t *testing.T) {\n\tt.Chdir(\"..\")\n\t_, _ = os.ReadFile(\"docs/a.md\")\n}\n"),
	})
}
