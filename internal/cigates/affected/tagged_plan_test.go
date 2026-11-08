package affected_test

import "testing"

// The integration plan (--tagged integration) plans the test binaries an integration shard runs. A shard runs only the
// tests of the files that need the tag, unless a tag-only file of the package holds a hook (ci/check_go.sh
// discover_integration_tagged_tests). So a test-scope row whose every read lies in the body of a top-level test of an
// untagged file selects nothing there: the go-quality legs run that test. Every other row keeps today's rule.

var tagged = []string{"--tagged", "integration"}

const (
	untaggedTestRead = "package b\n\nimport \"os\"\n\nfunc TestReadChanged() { _, _ = os.ReadFile(os.Getenv(\"CHANGED_PATH\")) }\n"
	dynRow           = "b\tb_changed_test.go\n"
	censusWalk       = "package x\n\nimport (\n\t\"io/fs\"\n\t\"path/filepath\"\n\t\"testing\"\n)\n\nfunc repoRoot() string { return \"..\" }\n\nfunc TestCensus(t *testing.T) {\n\t_ = filepath.WalkDir(repoRoot(), func(string, fs.DirEntry, error) error { return nil })\n}\n"
	walkRow          = "x\tx_census_test.go\t.\ttree\t*\n"
	execRow          = "b\tb_changed_test.go\tunexpressed\n"
)

func taggedCase(name, file, body string, more map[string]string, changed string, want string, keys ...string) nongoCase {
	head := map[string]string{file: body, tablesDir + "dynreads.tsv": dynRow}
	for k, v := range more {
		head[k] = v
	}
	return nongoCase{name: name, head: head, changed: []string{changed}, want: want, keys: keys, extra: tagged}
}

func TestTaggedPlanUntaggedTestRowsLeaveTheIntegrationPlan(t *testing.T) {
	runNongo(t, []nongoCase{
		taggedCase("untagged_test_read_leaves_the_plan", "b/b_changed_test.go", untaggedTestRead, nil, "a/a.go", "AFFECTED 1", "a"),
		{name: "untagged_census_walk_leaves_the_plan", head: map[string]string{"x/x_census_test.go": censusWalk, tablesDir + "walks.tsv": walkRow},
			changed: []string{"a/a.go"}, want: "AFFECTED 1", keys: []string{"a"}, extra: tagged},
		taggedCase("untagged_unexpressed_exec_leaves_the_plan", "b/b_changed_test.go",
			"package b\n\nimport (\n\t\"os\"\n\t\"os/exec\"\n)\n\nfunc TestReadChanged() {\n\t_ = exec.Command(\"docker\", \"ps\")\n\t_, _ = os.ReadFile(os.Getenv(\"CHANGED_PATH\"))\n}\n",
			map[string]string{tablesDir + "edges.tsv": execRow}, "a/a.go", "AFFECTED 1", "a"),
		taggedCase("untagged_compose_exec_leaves_the_plan", "b/b_changed_test.go",
			"package b\n\nimport (\n\t\"os\"\n\t\"os/exec\"\n)\n\nfunc TestReadChanged() {\n\t_ = exec.Command(\"docker-compose\", \"up\")\n\t_, _ = os.ReadFile(os.Getenv(\"CHANGED_PATH\"))\n}\n",
			map[string]string{tablesDir + "edges.tsv": execRow}, "a/a.go", "AFFECTED 1", "a"),
		taggedCase("tagged_file_without_a_hook_leaves_the_plan", "b/b_changed_test.go", untaggedTestRead,
			map[string]string{"b/b_tagged_test.go": "//go:build integration\n\n" + "package b\n\nimport \"testing\"\n\nfunc TestT(t *testing.T) {}\n"},
			"a/a.go", "AFFECTED 1", "a"),
		{name: "untagged_census_walk_stays_without_the_flag", head: map[string]string{"x/x_census_test.go": censusWalk, tablesDir + "walks.tsv": walkRow},
			changed: []string{"a/a.go"}, want: "AFFECTED 2", keys: []string{"a", "x"}},
	})
}

func TestTaggedPlanKeepsEveryRowAShardCanRun(t *testing.T) {
	const tagLine = "//go:build integration\n\n"
	runNongo(t, []nongoCase{
		taggedCase("tagged_test_read_stays", "b/b_changed_test.go", tagLine+untaggedTestRead, nil, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("plus_build_line_stays", "b/b_changed_test.go", "// +build integration\n\n"+untaggedTestRead, nil, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("any_build_line_stays", "b/b_changed_test.go", "//go:build linux\n\n"+untaggedTestRead, nil, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("tagged_test_main_hook_stays", "b/b_changed_test.go", untaggedTestRead,
			map[string]string{"b/b_main_test.go": tagLine + "package b\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\nfunc TestMain(m *testing.M) { os.Exit(m.Run()) }\n"},
			"a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("tagged_init_hook_stays", "b/b_changed_test.go", untaggedTestRead,
			map[string]string{"b/b_init_test.go": tagLine + "package b\n\nfunc init() {}\n"}, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("tagged_package_var_stays", "b/b_changed_test.go", untaggedTestRead,
			map[string]string{"b/b_var_test.go": tagLine + "package b\n\nvar seeded = 1\n"}, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("tagged_var_in_external_test_package_stays", "b/b_changed_test.go", untaggedTestRead,
			map[string]string{"b/b_ext_test.go": tagLine + "package b_test\n\nvar seeded = 1\n"}, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("tagged_blank_import_hook_stays", "b/b_changed_test.go", untaggedTestRead,
			map[string]string{"b/b_blank_test.go": tagLine + "package b\n\nimport _ \"embed\"\n"}, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("tagged_dot_import_hook_stays", "b/b_changed_test.go", untaggedTestRead,
			map[string]string{"b/b_dot_test.go": tagLine + "package b\n\nimport . \"strings\"\n\nfunc TestDot(t *testing.T) { _ = ToUpper(\"x\") }\n"}, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("tagged_registering_import_hook_stays", "b/b_changed_test.go", untaggedTestRead,
			map[string]string{"b/b_png_test.go": tagLine + "package b\n\nimport \"image/png\"\n\nvar _ = png.Decode\n"}, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("tagged_registering_import_without_var_stays", "b/b_changed_test.go", untaggedTestRead,
			map[string]string{"b/b_png_test.go": tagLine + "package b\n\nimport (\n\t\"image/png\"\n\t\"testing\"\n)\n\nfunc TestPNG(t *testing.T) { _ = png.Decode }\n"}, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("builtin_shadow_in_the_directory_stays", "b/b_changed_test.go", untaggedTestRead,
			map[string]string{"b/b_len.go": "package b\n\nfunc len(string) int { return 0 }\n", "b/b_tagged_test.go": tagLine + "package b\n\nimport \"testing\"\n\nfunc TestT(t *testing.T) {}\n"},
			"a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("read_in_a_helper_stays", "b/b_changed_test.go",
			"package b\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\nfunc read() { _, _ = os.ReadFile(os.Getenv(\"CHANGED_PATH\")) }\n\nfunc TestRead(t *testing.T) { read() }\n",
			nil, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("read_in_an_unused_helper_stays", "b/b_changed_test.go",
			"package b\n\nimport \"os\"\n\nfunc readAll() { _, _ = os.ReadFile(os.Getenv(\"CHANGED_PATH\")) }\n",
			nil, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("test_func_called_by_a_tagged_test_stays", "b/b_changed_test.go", untaggedTestRead,
			map[string]string{"b/b_call_test.go": tagLine + "package b\n\nimport \"testing\"\n\nfunc TestCalls(t *testing.T) { TestReadChanged() }\n"},
			"a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("test_func_value_taken_stays", "b/b_changed_test.go", untaggedTestRead+"\nvar all = []func(){TestReadChanged}\n",
			nil, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("tagged_non_test_file_stays", "b/b_changed_test.go", untaggedTestRead,
			map[string]string{"d/d_tagged.go": tagLine + "package d\n"}, "a/a.go", "AFFECTED 2", "a", "b"),
		taggedCase("markdown_only_change_stays", "b/b_changed_test.go", untaggedTestRead,
			map[string]string{"docs/a.md": "changed\n"}, "docs/a.md", "AFFECTED 2", "a", "b"),
	})
}

// Main pushes, merge groups, schedules, releases and manual runs plan FULL, with the flag as without it.
func TestTaggedPlanFullEventsStayFull(t *testing.T) {
	var cases []nongoCase
	for _, event := range []string{"push", "merge_group", "schedule", "workflow_dispatch", "release"} {
		c := taggedCase("event_"+event, "b/b_changed_test.go", untaggedTestRead, nil, "a/a.go", "FULL event "+event+": *")
		c.event = event
		cases = append(cases, c)
	}
	runNongo(t, cases)
}
