package affected_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// One plan verdict (NONE / AFFECTED / FULL) for the integration plan job and the go-quality legs. `cigate plan-verdict`
// is executed as a built binary over a git fixture: a base commit, a head commit, the declared-read tables and a synthetic
// go list. Packages: a (its tests read docs/), b (its code reads contracts/), c (imports b), d (imports the test helper
// internal/testsupport/h), x (its tests read .github/ and ci/).

var (
	cigateOnce sync.Once
	cigateDir  string
	cigateBin  string
	cigateErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if cigateDir != "" {
		_ = os.RemoveAll(cigateDir)
	}
	os.Exit(code)
}

func cigate(t *testing.T) string {
	t.Helper()
	cigateOnce.Do(func() {
		dir, err := os.MkdirTemp("", "cigate-plan-verdict-")
		if err != nil {
			cigateErr = err
			return
		}
		cigateDir = dir
		cigateBin = filepath.Join(dir, "cigate")
		env := inheritedEnv([]string{"GOFLAGS"}, "GOFLAGS=")
		res := cmdSpec{dir: repoRoot(t), env: env}.run("go", "build", "-mod=readonly", "-o", cigateBin, "./cmd/cigate")
		if res.code != 0 {
			cigateErr = fmt.Errorf("go build ./cmd/cigate: %s%s", res.stderr, res.stdout)
		}
	})
	if cigateErr != nil {
		t.Fatal(cigateErr)
	}
	return cigateBin
}

const fixtureClasses = `# class	glob
docs	docs/**
contracts	contracts/**
github	.github/**
ci	ci/**
claude	.claude/**
codex-review	.codex-review*
codex-review	.codex-review*/**
agents	AGENTS.md
README.md	README.md
.dockerignore	.dockerignore
deploy	deploy/**
testdata	**/testdata.manifest.tsv
walked	walked/**
filtered	filtered/**
`

const fixtureReads = `# package	scope	class
a	test	docs
b	code	contracts
x	test	github
x	test	ci
b	code	deploy
`

const fixtureWalks = `# package	site	root	depth	classes
b	b_walk.go	filtered	tree	-
x	x_walk_test.go	walked	tree	-
`

// fixtureWalker walks <dir> below the repository root, as a root-locating repo walk does.
const fixtureWalker = "package %s\n\nimport (\n\t\"io/fs\"\n\t\"path/filepath\"\n)\n\nfunc repoRoot() string { return \"..\" }\n\nfunc walk() error {\n\treturn filepath.WalkDir(filepath.Join(repoRoot(), %q), func(string, fs.DirEntry, error) error { return nil })\n}\n"

const fixtureGoYML = `name: Go
on:
  pull_request:
    paths:
      - '**/*.go'
jobs:
  plan:
    runs-on: ubuntu-latest
    steps:
      - run: bash ci/check_go.sh integration-shard-plan
  shard:
    needs: plan
    runs-on: ubuntu-latest
    steps:
      - run: bash ci/check_go.sh integration-shard 1
  lint:
    runs-on: ubuntu-latest
    steps:
      - run: echo lint
`

const fixtureCheckGo = `#!/usr/bin/env bash
set -euo pipefail
AUTH_PACKAGES="a b"
SHARDS=3
shard_plan() {
  echo "${SHARDS}"
}
auth_check() {
  echo "${AUTH_PACKAGES}"
}
case "${1:-}" in
  integration-shard-plan) shard_plan ;;
  integration-shard) shard_plan ;;
  auth) auth_check ;;
esac
`

type verdictFixture struct {
	root, base, golist, keys string
}

// newVerdictFixture commits the base tree, then head (a value "" deletes the file) as a second commit.
func newVerdictFixture(work string, head map[string]string) (*verdictFixture, error) {
	dir := filepath.Join(work, "repo")
	r := newGitRepo(dir)
	base := map[string]string{
		"go.mod":                                 "module m\n",
		"a/a.go":                                 "package a\n",
		"b/b.go":                                 "package b\n",
		"b/b_walk.go":                            fmt.Sprintf(fixtureWalker, "b", "filtered"),
		"c/c.go":                                 "package c\n",
		"d/d.go":                                 "package d\n",
		"x/x.go":                                 "package x\n",
		"x/x_walk_test.go":                       fmt.Sprintf(fixtureWalker, "x", "walked"),
		"internal/testsupport/h/h.go":            "package h\n",
		"internal/cigates/affected/plan.go":      "package affected\n",
		"internal/cigates/affected/classes.tsv":  fixtureClasses,
		"internal/cigates/affected/reads.tsv":    fixtureReads,
		"internal/cigates/affected/walks.tsv":    fixtureWalks,
		".github/workflows/go.yml":               fixtureGoYML,
		"ci/check_go.sh":                         fixtureCheckGo,
		"ci/go_integration_shards.d/_shards.tsv": "3\n",
		"ci/go_integration_shards.d/a.tsv":       "a\t1\n",
		"docs/a.md":                              "a\n",
	}
	for name, body := range base {
		r.write(name, body)
	}
	r.git("add", "-A")
	r.git("commit", "-q", "-m", "base")
	f := &verdictFixture{root: dir, base: r.git("rev-parse", "HEAD")}
	for name, body := range head {
		if body == "" {
			r.git("rm", "-q", name)
			continue
		}
		r.write(name, body)
	}
	r.git("add", "-A")
	r.git("commit", "-q", "--allow-empty", "-m", "head")
	if r.err != nil {
		return nil, r.err
	}
	mod := map[string]string{"Path": "m"}
	pkg := func(rel string) map[string]any {
		return map[string]any{"ImportPath": "m/" + rel, "Dir": filepath.Join(dir, rel), "Module": mod}
	}
	bin := func(rel string, deps ...string) map[string]any {
		d := []string{"m/" + rel + " [m/" + rel + ".test]"}
		for _, dep := range deps {
			d = append(d, "m/"+dep)
		}
		return map[string]any{"ImportPath": "m/" + rel + ".test", "Deps": d}
	}
	entries := []map[string]any{
		pkg("a"), pkg("b"), pkg("c"), pkg("d"), pkg("x"), pkg("internal/testsupport/h"), pkg("internal/cigates/affected"),
		bin("a"), bin("b"), bin("c", "b"), bin("d", "internal/testsupport/h"), bin("x"),
	}
	var lines []string
	for _, e := range entries {
		raw, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		lines = append(lines, string(raw))
	}
	f.golist = filepath.Join(work, "golist.json")
	f.keys = filepath.Join(work, "keys.txt")
	if err := os.WriteFile(f.golist, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(f.keys, []byte("a\nb\nc\nd\nx\n"), 0o644); err != nil {
		return nil, err
	}
	return f, nil
}

// verdict runs `cigate plan-verdict` and returns its first line and the keys after it.
func (f *verdictFixture) verdict(t *testing.T, work, event string, extra []string, changed ...string) (string, []string, error) {
	list := filepath.Join(work, "changed.txt")
	body := strings.Join(changed, "\x00")
	if len(changed) > 0 {
		body += "\x00"
	}
	if err := os.WriteFile(list, []byte(body), 0o644); err != nil {
		return "", nil, err
	}
	args := append([]string{"plan-verdict", "--event", event, "--changed", list, "--golist", f.golist, "--keys", f.keys,
		"--root", f.root, "--base", f.base}, extra...)
	res := cmdSpec{dir: f.root, env: gitEnv}.run(cigate(t), args...)
	if res.code != 0 {
		return "", nil, exitError{res.code, res.stderr + res.stdout}
	}
	out := strings.Split(strings.TrimRight(res.stdout, "\n"), "\n")
	return out[0], out[1:], nil
}

type exitError struct {
	code int
	out  string
}

func (e exitError) Error() string {
	return fmt.Sprintf("cigate plan-verdict exited %d: %s", e.code, e.out)
}

type verdictRow struct {
	name    string
	head    map[string]string
	event   string
	extra   []string
	changed []string
	want    string   // exact first line, or a prefix when it ends in "*"
	keys    []string // the keys after an AFFECTED line
	wantErr bool
}

func (row verdictRow) check(t *testing.T) error {
	work := t.TempDir()
	f, err := newVerdictFixture(work, row.head)
	if err != nil {
		return err
	}
	event := row.event
	if event == "" {
		event = "pull_request"
	}
	first, keys, err := f.verdict(t, work, event, row.extra, row.changed...)
	if row.wantErr {
		if e, ok := err.(exitError); !ok || e.code != 1 {
			return fmt.Errorf("want exit 1 (a table or flag error), got %q %q %v", first, keys, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if strings.HasSuffix(row.want, "*") {
		if !strings.HasPrefix(first, strings.TrimSuffix(row.want, "*")) {
			return fmt.Errorf("verdict %q, want a line starting %q", first, strings.TrimSuffix(row.want, "*"))
		}
	} else if first != row.want {
		return fmt.Errorf("verdict %q, want %q", first, row.want)
	}
	if len(keys) == 0 || (len(keys) == 1 && keys[0] == "") {
		keys = nil
	}
	if !reflect.DeepEqual(keys, row.keys) {
		return fmt.Errorf("keys %q, want %q", keys, row.keys)
	}
	return nil
}

func goYMLWith(old, repl string) string { return strings.Replace(fixtureGoYML, old, repl, 1) }

func checkGoWith(old, repl string) string { return strings.Replace(fixtureCheckGo, old, repl, 1) }

func planVerdictRows() []verdictRow {
	return []verdictRow{
		{name: "reader_less_paths_are_none", changed: []string{".claude/x.json", ".codex-review/r1.md", ".codex-review-notes.md", "AGENTS.md"}, want: "NONE"},
		{name: "test_reader_selects_only_itself", changed: []string{"docs/a.md"}, want: "AFFECTED 1", keys: []string{"a"}},
		{name: "code_reader_selects_its_reverse_deps", changed: []string{"contracts/auth/v1/x.json"}, want: "AFFECTED 2", keys: []string{"b", "c"}},
		{name: "python_source_in_a_reader_less_class_is_full", changed: []string{".claude/hooks/x.py"}, want: "FULL non-Go file is Python source*"},
		{name: "python_source_in_a_declared_class_is_full", changed: []string{"docs/x.py"}, want: "FULL non-Go file is Python source*"},
		{name: "python_fixture_under_testdata_is_its_package", changed: []string{"x/testdata/fixture.py"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "walk_row_selects_its_walker", changed: []string{"walked/f.txt"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "a_walk_reads_every_file_under_its_root", changed: []string{"walked/skipme/f.txt"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "a_code_walk_selects_its_importers", changed: []string{"filtered/a.json"}, want: "AFFECTED 2", keys: []string{"b", "c"}},
		{name: "a_path_outside_every_walk_root_selects_no_walker", changed: []string{".claude/x.json"}, want: "NONE"},
		{name: "undeclared_env_path_read_is_full", head: map[string]string{"x/x_dyn_test.go": fmt.Sprintf(plantedEnvReader, "x")},
			changed: []string{"docs/a.md"}, want: `FULL dynreads.tsv differs from the tree (x: undeclared dynamic read: x/x_dyn_test.go:5 os.ReadFile(os.Getenv("FIXTURE_PATH"))*`},
		{name: "docs_only_change_with_an_unexpressed_package_plans_affected", head: map[string]string{"x/x_dyn_test.go": fmt.Sprintf(plantedEnvReader, "x"),
			"internal/cigates/affected/dynreads.tsv": "x\tx_dyn_test.go\tunexpressed\n"},
			changed: []string{"docs/a.md"}, want: "AFFECTED 2", keys: []string{"a", "x"}},
		{name: "unexpressed_code_read_selects_its_importers", head: map[string]string{"b/b_dyn.go": fmt.Sprintf(plantedFieldReader, "b"),
			"internal/cigates/affected/dynreads.tsv": "b\tb_dyn.go\tunexpressed\n"},
			changed: []string{"AGENTS.md"}, want: "AFFECTED 2", keys: []string{"b", "c"}},
		{name: "unexpressed_testsupport_read_selects_its_importers", head: map[string]string{"internal/testsupport/h/h_dyn.go": fmt.Sprintf(plantedFieldReader, "h"),
			"internal/cigates/affected/dynreads.tsv": "internal/testsupport/h\th_dyn.go\tunexpressed\n"},
			changed: []string{"AGENTS.md"}, want: "AFFECTED 1", keys: []string{"d"}},
		{name: "undeclared_manifest_driven_read_is_full", head: map[string]string{"x/x_dyn_test.go": fmt.Sprintf(plantedManifestReader, "x")},
			changed: []string{".claude/x.json"}, want: "FULL dynreads.tsv differs from the tree (x: undeclared dynamic read: x/x_dyn_test.go:14 os.ReadFile(line)*"},
		{name: "undeclared_struct_field_read_is_full", head: map[string]string{"b/b_dyn.go": fmt.Sprintf(plantedFieldReader, "b")},
			changed: []string{"AGENTS.md"}, want: "FULL dynreads.tsv differs from the tree (b: undeclared dynamic read: b/b_dyn.go:7 os.ReadFile(c.Path)*"},
		{name: "declared_dynamic_read_selects_its_package",
			head: map[string]string{"x/x_dyn_test.go": fmt.Sprintf(plantedEnvReader, "x"),
				"internal/cigates/affected/dynreads.tsv": "x\tx_dyn_test.go\tcontracts\n"},
			changed: []string{"contracts/auth/v1/x.json"}, want: "AFFECTED 3", keys: []string{"b", "c", "x"}},
		{name: "declared_dynamic_code_read_selects_its_importers",
			head: map[string]string{"b/b_dyn.go": fmt.Sprintf(plantedFieldReader, "b"),
				"internal/cigates/affected/dynreads.tsv": "b\tb_dyn.go\tdocs\n"},
			changed: []string{"docs/a.md"}, want: "AFFECTED 3", keys: []string{"a", "b", "c"}},
		{name: "dynreads_table_is_full", changed: []string{"internal/cigates/affected/dynreads.tsv"}, want: "FULL planner input*"},
		{name: "unexpressed_walk_selects_its_package",
			head: map[string]string{"d/d_walk_test.go": "package d\n\nimport (\n\t\"io/fs\"\n\t\"os\"\n\t\"path/filepath\"\n)\n\nfunc walk() error {\n\treturn filepath.WalkDir(os.Getenv(\"ROOT\"), func(string, fs.DirEntry, error) error { return nil })\n}\n",
				"internal/cigates/affected/walks.tsv": fixtureWalks + "d\td_walk_test.go\tunexpressed\t-\t-\n"},
			changed: []string{"AGENTS.md"}, want: "AFFECTED 1", keys: []string{"d"}},
		{name: "walks_table_is_full", changed: []string{"internal/cigates/affected/walks.tsv"}, want: "FULL planner input*"},
		{name: "undeclared_path_is_full", changed: []string{"zz/new.txt"}, want: "FULL non-Go file*"},
		{name: "go_mod_is_full", changed: []string{".claude/x", "go.mod"}, want: "FULL shared change*"},
		{name: "testsupport_is_full", changed: []string{"internal/testsupport/h/h.go"}, want: "FULL shared change*"},
		{name: "push_is_full", event: "push", changed: []string{".claude/x.json"}, want: "FULL event push*"},
		{name: "merge_group_is_full", event: "merge_group", changed: []string{".claude/x.json"}, want: "FULL event merge_group*"},
		{name: "schedule_is_full", event: "schedule", changed: []string{".claude/x.json"}, want: "FULL event schedule*"},
		{name: "workflow_dispatch_is_full", event: "workflow_dispatch", changed: []string{".claude/x.json"}, want: "FULL event workflow_dispatch*"},
		{name: "range_judges_the_change_set", event: "range", changed: []string{".claude/x.json"}, want: "NONE"},
		{name: "reader_less_beside_go_is_the_go_closure", changed: []string{".claude/x.json", "x/x.go"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "go_files_full_with_go_source", extra: []string{"--go-files-full"}, changed: []string{"docs/a.md", "x/x.go"}, want: "FULL Go source*"},
		{name: "go_files_full_without_go_source_is_a_subset", extra: []string{"--go-files-full"}, changed: []string{"docs/a.md"}, want: "AFFECTED 1", keys: []string{"a"}},
		{name: "empty_change_set_is_full", changed: nil, want: "FULL empty change set*"},
		{name: "reads_table_is_full", changed: []string{"internal/cigates/affected/reads.tsv"}, want: "FULL planner input*"},
		{name: "classes_table_is_full", changed: []string{"internal/cigates/affected/classes.tsv"}, want: "FULL planner input*"},
		{name: "planner_code_is_full", changed: []string{"internal/cigates/affected/plan.go"}, want: "FULL planner input*"},
		{name: "go_yml_paths_and_other_job_select_its_readers",
			head:    map[string]string{".github/workflows/go.yml": goYMLWith("echo lint", "echo lint2") + "\n"},
			changed: []string{".github/workflows/go.yml"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "go_yml_paths_filter_edit_selects_its_readers",
			head:    map[string]string{".github/workflows/go.yml": goYMLWith("      - '**/*.go'\n", "      - '**/*.go'\n      - 'docs/**'\n")},
			changed: []string{".github/workflows/go.yml"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "go_yml_integration_job_edit_is_full",
			head:    map[string]string{".github/workflows/go.yml": goYMLWith("integration-shard 1", "integration-shard 2")},
			changed: []string{".github/workflows/go.yml"}, want: "FULL shared change: .github/workflows/go.yml*"},
		{name: "go_quality_workflow_is_full_for_the_quality_legs", extra: []string{"--go-files-full"}, changed: []string{".github/workflows/go-quality.yml"}, want: "FULL shared change*"},
		{name: "go_quality_workflow_selects_github_readers_for_integration", changed: []string{".github/workflows/go-quality.yml"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "check_go_function_no_verb_reaches_selects_its_readers",
			head:    map[string]string{"ci/check_go.sh": checkGoWith(`echo "${AUTH_PACKAGES}"`, `echo "${AUTH_PACKAGES}" x`)},
			changed: []string{"ci/check_go.sh"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "check_go_reached_function_is_full",
			head:    map[string]string{"ci/check_go.sh": checkGoWith(`echo "${SHARDS}"`, `echo "${SHARDS}" x`)},
			changed: []string{"ci/check_go.sh"}, want: "FULL shared change: ci/check_go.sh*"},
		{name: "check_go_read_global_is_full",
			head:    map[string]string{"ci/check_go.sh": checkGoWith("SHARDS=3", "SHARDS=4")},
			changed: []string{"ci/check_go.sh"}, want: "FULL shared change: ci/check_go.sh*"},
		{name: "shard_manifest_row_selects_its_key", changed: []string{"ci/go_integration_shards.d/a.tsv"}, want: "AFFECTED 2", keys: []string{"a", "x"}},
		{name: "shard_count_is_full", changed: []string{"ci/go_integration_shards.d/_shards.tsv"}, want: "FULL shared change*"},
		{name: "quality_leg_partition_input_is_full_for_the_quality_legs", extra: []string{"--go-files-full"}, changed: []string{"ci/go_test_weights.tsv"}, want: "FULL shared change*"},
		{name: "quality_leg_partition_input_selects_ci_readers_for_integration", changed: []string{"ci/go_test_weights.tsv"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "race_weights_row_is_full_for_the_quality_legs", extra: []string{"--go-files-full"}, changed: []string{"ci/go_race_weights.d/a.tsv"}, want: "FULL shared change*"},
		{name: "race_weights_row_selects_ci_readers_for_integration", changed: []string{"ci/go_race_weights.d/a.tsv"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "diff_producer_is_full", changed: []string{"ci/go_relevant_diff.sh"}, want: "FULL shared change*"},
		{name: "reads_row_for_an_unknown_class_is_an_error",
			head:    map[string]string{"internal/cigates/affected/reads.tsv": fixtureReads + "a\ttest\tnope\n"},
			changed: []string{"docs/a.md"}, wantErr: true},
		{name: "reads_row_with_an_unknown_scope_is_an_error",
			head:    map[string]string{"internal/cigates/affected/reads.tsv": fixtureReads + "a\tsometimes\tdocs\n"},
			changed: []string{"docs/a.md"}, wantErr: true},
		// Non-ASCII input (D6129): the planner never reads a path through \s \w \d; a non-ASCII name is matched rune by rune.
		{name: "non_ascii_name_in_a_class_selects_its_readers", changed: []string{"docs/caf\u00e9.md", "docs/\u0663.md"}, want: "AFFECTED 1", keys: []string{"a"}},
		{name: "a_path_of_non_ascii_space_is_a_path_not_a_blank", changed: []string{"\u00a0"}, want: "FULL non-Go file*"},
		{name: "check_go_global_with_non_ascii_space_is_code_not_an_assignment",
			head:    map[string]string{"ci/check_go.sh": checkGoWith("SHARDS=3\n", "SHARDS=3\nSHARDS\u00a0=4\n")},
			changed: []string{"ci/check_go.sh"}, want: "FULL shared change: ci/check_go.sh: global code changed"},
		{name: "unknown_event_is_an_error", event: "", extra: []string{"--event", "pull_request_target"}, changed: []string{"docs/a.md"}, wantErr: true},
	}
}

func TestPlanVerdict(t *testing.T) {
	for _, row := range planVerdictRows() {
		row := row
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			if err := row.check(t); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// plan-reads is the guard of the declared-read table: every package that names a path of a declared class, or walks
// from the repository root, must hold a reads.tsv row that covers it.

func readsFixture(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	base := map[string]string{
		"go.mod":                                "module m\n",
		"docs/a.md":                             "a\n",
		"contracts/x.json":                      "{}\n",
		"internal/cigates/affected/classes.tsv": "docs\tdocs/**\ncontracts\tcontracts/**\nclaude\t.claude/**\n",
		"internal/cigates/affected/reads.tsv":   "# package\tscope\tclass\n",
	}
	for k, v := range files {
		base[k] = v
	}
	for name, body := range base {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func planReads(t *testing.T, root string) cmdResult {
	t.Helper()
	return cmdSpec{dir: root, env: gitEnv}.run(cigate(t), "plan-reads", "--root", root)
}

const (
	plantedEnvReader      = "package %s\n\nimport \"os\"\n\nfunc read() ([]byte, error) { return os.ReadFile(os.Getenv(\"FIXTURE_PATH\")) }\n"
	plantedManifestReader = "package %s\n\nimport (\n\t\"os\"\n\t\"strings\"\n)\n\nfunc read() error {\n\traw, err := os.ReadFile(\"testdata/manifest.txt\")\n\tif err != nil {\n\t\treturn err\n\t}\n\tfor _, line := range strings.Split(string(raw), \"\\n\") {\n\t\tif _, err := os.ReadFile(line); err != nil {\n\t\t\treturn err\n\t\t}\n\t}\n\treturn nil\n}\n"
	plantedFieldReader    = "package %s\n\nimport \"os\"\n\ntype config struct{ Path string }\n\nfunc read(c config) ([]byte, error) { return os.ReadFile(c.Path) }\n"
	plantedLiteralReader  = "package p\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n)\n\nfunc read() ([]byte, error) {\n\treturn os.ReadFile(filepath.Join(\"..\", \"docs\", \"a.md\"))\n}\n"
	plantedSlashReader    = "package s\n\nvar doc = \"../../docs/a.md\"\n"
	plantedRootWalker     = "package q\n\nimport (\n\t\"io/fs\"\n\t\"path/filepath\"\n)\n\nfunc repoRoot() string { return \"..\" }\n\nfunc walk() error {\n\treturn filepath.WalkDir(repoRoot(), func(string, fs.DirEntry, error) error { return nil })\n}\n"
	plantedCodeReader     = "package r\n\nimport \"path/filepath\"\n\nvar Dir = filepath.Join(\"..\", \"contracts\")\n"
)

func TestPlanReadsGuard(t *testing.T) {
	cases := []struct {
		name  string
		files map[string]string
		code  int
		want  []string
	}{
		{"planted_undeclared_literal_reader_is_named", map[string]string{"p/p_test.go": plantedLiteralReader}, 1, []string{"p/p_test.go", "docs/a.md", "p\ttest\t"}},
		{"planted_undeclared_slash_literal_is_named", map[string]string{"s/s_test.go": plantedSlashReader}, 1, []string{"s/s_test.go", "docs/a.md"}},
		{"planted_undeclared_root_walker_is_named", map[string]string{"q/q_test.go": plantedRootWalker}, 1, []string{"q/q_test.go", "undeclared walk", "filepath.WalkDir(repoRoot()"}},
		{"code_reader_needs_a_code_row", map[string]string{"r/r.go": plantedCodeReader,
			"internal/cigates/affected/reads.tsv": "r\ttest\tcontracts\n"}, 1, []string{"r/r.go", "r\tcode\tcontracts"}},
		{"root_walker_with_only_a_literal_row_is_named", map[string]string{"q/q_test.go": plantedRootWalker + "\nvar doc = \"../docs/a.md\"\n",
			"internal/cigates/affected/reads.tsv": "q\ttest\tdocs\n"}, 1, []string{"q/q_test.go", "undeclared walk", "the generated row is q\tq_test.go\t.\ttree\tcontracts,docs"}},
		{"walk_row_omitting_a_reachable_class_fails", map[string]string{"q/q_test.go": plantedRootWalker,
			"internal/cigates/affected/walks.tsv": "q\tq_test.go\t.\ttree\tdocs\n"}, 1, []string{"q\tq_test.go\t.\ttree\tdocs, the generated row is q\tq_test.go\t.\ttree\tcontracts,docs"}},
		{"walk_row_with_a_root_the_code_does_not_walk_fails", map[string]string{"q/q_test.go": plantedRootWalker,
			"internal/cigates/affected/walks.tsv": "q\tq_test.go\tdocs\ttree\tdocs\n"}, 1, []string{"matches no walk in the code", "undeclared walk"}},
		{"walk_row_for_a_site_that_does_not_walk_fails", map[string]string{"p/p_test.go": plantedLiteralReader,
			"internal/cigates/affected/reads.tsv": "p\ttest\tdocs\n", "internal/cigates/affected/walks.tsv": "p\tp_test.go\t.\ttree\tcontracts,docs\n"}, 1, []string{"matches no walk in the code"}},
		{"declared_readers_pass", map[string]string{"p/p_test.go": plantedLiteralReader, "q/q_test.go": plantedRootWalker, "r/r.go": plantedCodeReader,
			"internal/cigates/affected/reads.tsv": "p\ttest\tdocs\nr\tcode\tcontracts\n", "internal/cigates/affected/walks.tsv": "q\tq_test.go\t.\ttree\tcontracts,docs\n"}, 0, nil},
		{"temp_dir_walk_needs_no_row", map[string]string{"q/q_test.go": "package q\n\nimport (\n\t\"io/fs\"\n\t\"path/filepath\"\n\t\"testing\"\n)\n\nfunc TestWalk(t *testing.T) {\n\t_ = filepath.WalkDir(t.TempDir(), func(string, fs.DirEntry, error) error { return nil })\n}\n"}, 0, nil},
		{"own_join_with_an_env_part_is_a_dynamic_read", map[string]string{"j/j_test.go": "package j\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n)\n\nfunc read() ([]byte, error) { return os.ReadFile(filepath.Join(\".\", os.Getenv(\"READ_PATH\"))) }\n"}, 1,
			[]string{"j/j_test.go", "undeclared dynamic read", "j\tj_test.go\tunexpressed"}},
		{"constant_concat_read_is_a_literal_read", map[string]string{"k/k_test.go": "package k\n\nimport \"os\"\n\nfunc read() ([]byte, error) { return os.ReadFile(\"../do\" + \"cs/a.md\") }\n"}, 1,
			[]string{"k/k_test.go", "undeclared reader", "docs/a.md", "k\ttest\tdocs"}},
		{"undeclared_env_path_read_is_named", map[string]string{"e/e_test.go": fmt.Sprintf(plantedEnvReader, "e")}, 1,
			[]string{"e/e_test.go", "undeclared dynamic read", `os.ReadFile(os.Getenv("FIXTURE_PATH"))`, "e\te_test.go\tunexpressed"}},
		{"undeclared_manifest_driven_read_is_named", map[string]string{"m/m_test.go": fmt.Sprintf(plantedManifestReader, "m")}, 1,
			[]string{"m/m_test.go", "undeclared dynamic read", "os.ReadFile(line)"}},
		{"undeclared_struct_field_read_is_named", map[string]string{"s/s.go": fmt.Sprintf(plantedFieldReader, "s")}, 1,
			[]string{"s/s.go", "undeclared dynamic read", "os.ReadFile(c.Path)"}},
		{"declared_dynamic_reads_pass", map[string]string{"e/e_test.go": fmt.Sprintf(plantedEnvReader, "e"), "m/m_test.go": fmt.Sprintf(plantedManifestReader, "m"),
			"s/s.go": fmt.Sprintf(plantedFieldReader, "s"), "internal/cigates/affected/dynreads.tsv": "e\te_test.go\tdocs\nm\tm_test.go\t-\ns\ts.go\tcontracts\n"}, 0, nil},
		{"dynreads_row_for_a_file_without_one_fails", map[string]string{"p/p_test.go": plantedLiteralReader,
			"internal/cigates/affected/reads.tsv": "p\ttest\tdocs\n", "internal/cigates/affected/dynreads.tsv": "p\tp_test.go\tdocs\n"}, 1, []string{"p/p_test.go holds no read the generator keeps"}},
		{"row_for_an_unknown_class_fails", map[string]string{"internal/cigates/affected/reads.tsv": "p\ttest\tnope\n"}, 1, []string{"nope"}},
		{"row_for_a_package_with_no_go_file_fails", map[string]string{"internal/cigates/affected/reads.tsv": "gone\ttest\tdocs\n"}, 1, []string{"gone"}},
		{"planted_non_ascii_literal_reader_is_named", map[string]string{"n/n_test.go": "package n\n\nvar doc = \"../docs/caf\u00e9.md\"\n"}, 1, []string{"n/n_test.go", "docs/caf\u00e9.md"}},
		{"literal_outside_every_class_needs_no_row", map[string]string{"u/u_test.go": "package u\n\nvar f = \"../internal/x/y.json\"\n"}, 0, nil},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			res := planReads(t, readsFixture(t, c.files))
			out := res.stdout + res.stderr
			if res.code != c.code {
				t.Fatalf("plan-reads exited %d, want %d:\n%s", res.code, c.code, out)
			}
			for _, w := range c.want {
				if !strings.Contains(out, w) {
					t.Fatalf("plan-reads output does not name %q:\n%s", w, out)
				}
			}
		})
	}
}

// The changed list as ci/go_relevant_diff.sh writes it (`git diff -z --no-renames`): NUL-terminated entries; a list with no
// NUL is read one path per line.
func TestPlanVerdictChangedListFormats(t *testing.T) {
	cases := []struct {
		name, raw, want string
		keys            []string
	}{
		{"a_rename_out_of_a_go_package_keeps_the_package", "docs/a.md\x00x/x.go\x00", "AFFECTED 2", []string{"a", "x"}},
		{"newline_delimited_entries_are_separate", "AGENTS.md\ngo.mod\n", "FULL shared change: go.mod", nil},
		{"nul_delimited_entries_are_separate", "AGENTS.md\x00go.mod\x00", "FULL shared change: go.mod", nil},
		{"a_single_nul_terminated_entry_is_one_path", "AGENTS.md\x00", "NONE", nil},
		{"a_newline_entry_of_non_ascii_space_is_kept", "AGENTS.md\n\u00a0\n", "FULL non-Go file in no declared class: \u00a0", nil},
		{"a_yaml_workflow_selects_its_readers", ".github/workflows/only-changed.yaml\x00", "AFFECTED 1", []string{"x"}},
		// Ported from the deleted ci/go_relevance.sh corpus (internal/relevancedecider): list formats.
		{"empty_entries_between_nuls_are_skipped", "\x00docs/a.md\x00\x00", "AFFECTED 1", []string{"a"}},
		{"newline_format_one_go_path", "x/x.go\n", "AFFECTED 1", []string{"x"}},
		{"newline_format_blank_lines_are_skipped", "\ndocs/a.md\n\nAGENTS.md\n", "AFFECTED 1", []string{"a"}},
		{"newline_format_padding_is_part_of_the_path", "  docs/a.md  \n", "FULL non-Go file in no declared class:*", nil},
		{"newline_format_left_padding_is_part_of_the_path", "  x/x.go\n", "FULL Go file outside any listed package:*", nil},
		{"nul_format_unterminated_last_entry", "docs/a.md\x00x/x.go", "AFFECTED 2", []string{"a", "x"}},
		{"newline_format_unterminated_last_line", "docs/a.md\nx/x.go", "AFFECTED 2", []string{"a", "x"}},
		{"nul_format_embedded_newline_in_a_go_path", "x/new\nline/module.go\x00", "FULL Go file outside any listed package: x/new\\nline/module.go", nil},
		{"nul_format_embedded_newline_in_a_class_path", "docs/a\nb.md\x00", "AFFECTED 1", []string{"a"}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			work := t.TempDir()
			f, err := newVerdictFixture(work, nil)
			if err != nil {
				t.Fatal(err)
			}
			list := filepath.Join(work, "changed.txt")
			if err := os.WriteFile(list, []byte(c.raw), 0o644); err != nil {
				t.Fatal(err)
			}
			res := cmdSpec{dir: f.root, env: gitEnv}.run(cigate(t), "plan-verdict", "--event", "pull_request", "--changed", list,
				"--golist", f.golist, "--keys", f.keys, "--root", f.root, "--base", f.base)
			lines := strings.Split(strings.TrimRight(res.stdout, "\n"), "\n")
			var keys []string
			if len(lines) > 1 {
				keys = lines[1:]
			}
			first := lines[0] == c.want || (strings.HasSuffix(c.want, "*") && strings.HasPrefix(lines[0], strings.TrimSuffix(c.want, "*")))
			if res.code != 0 || !first || !reflect.DeepEqual(keys, c.keys) {
				t.Fatalf("plan-verdict over %q: exit %d, %q, want %q %q (%s)", c.raw, res.code, lines, c.want, c.keys, res.stderr)
			}
		})
	}
}

// The decision cases of the deleted ci/go_relevance.sh corpus (internal/relevancedecider), as the go-quality plan
// (--go-files-full) now answers them over the fixture tables. Old answers: relevant=true|false from go.yml's path list.
func TestDeletedDeciderCasesArePlanned(t *testing.T) {
	q := []string{"--go-files-full"}
	rows := []verdictRow{
		{name: "docs_only", extra: q, changed: []string{"docs/guide.md", "README.md"}, want: "AFFECTED 1", keys: []string{"a"}},
		{name: "go_only", extra: q, changed: []string{"x/x.go"}, want: "FULL Go source*"},
		{name: "root_go_file_and_go_mod", extra: q, changed: []string{"main.go", "go.mod", "go.sum"}, want: "FULL*"},
		{name: "chart_only", extra: q, changed: []string{"deploy/helm/dev-health/values.yaml"}, want: "AFFECTED 2", keys: []string{"b", "c"}},
		{name: "chart_sibling_is_in_the_deploy_class", extra: q, changed: []string{"deploy/helm/other-chart/values.yaml"}, want: "AFFECTED 2", keys: []string{"b", "c"}},
		{name: "mixed_docs_go_and_an_undeclared_tree", extra: q, changed: []string{"docs/a.md", "x/y.go", "web/readme.md"}, want: "FULL*"},
		{name: "deleted_go_file", extra: q, changed: []string{"internal/old/removed.go"}, want: "FULL*"},
		{name: "rename_as_delete_and_add", extra: q, changed: []string{"x/x.go", "docs/a.md"}, want: "FULL Go source*"},
		{name: "rename_within_an_undeclared_tree_is_full", extra: q, changed: []string{"notes/old.txt", "notes/new.txt"}, want: "FULL non-Go file*"},
		{name: "testdata_is_its_package", extra: q, changed: []string{"x/testdata/x.txt"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "manifest_tsv_in_a_package_dir_is_full", extra: q, changed: []string{"x/testdata.manifest.tsv"}, want: "FULL non-Go file*"},
		{name: "exact_check_go_entry_unchanged_content", extra: q, changed: []string{"ci/check_go.sh"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "near_miss_of_check_go", extra: q, changed: []string{"ci/check_go.sh.bak"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "python_source_is_full", extra: q, changed: []string{"src/dev_health_ops/api/routes.py"}, want: "FULL non-Go file*"},
		{name: "regex_special_characters_in_a_path", extra: q, changed: []string{"x/a+b(c)/x.txt"}, want: "FULL non-Go file*"},
		{name: "non_ascii_paths", extra: q, changed: []string{"docs/r\u00e9sum\u00e9.md"}, want: "AFFECTED 1", keys: []string{"a"}},
		{name: "workflow_file_itself", extra: q, changed: []string{".github/workflows/go-quality.yml"}, want: "FULL shared change*"},
		{name: "dotfile_entry_reader_less", extra: q, changed: []string{".dockerignore"}, want: "NONE"},
		{name: "empty_change_set", extra: q, changed: nil, want: "FULL empty change set*"},
	}
	for _, row := range rows {
		row := row
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			if err := row.check(t); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A verdict that rests on a root walk says so in the job log: one stderr line per walker, its declared classes against the
// classes the change reaches.
func TestPlanVerdictLogsEachRootWalkDecision(t *testing.T) {
	dyn := map[string]string{"x/x_dyn_test.go": fmt.Sprintf(plantedEnvReader, "x"), "internal/cigates/affected/dynreads.tsv": "x\tx_dyn_test.go\tcontracts\n"}
	cases := []struct {
		head          map[string]string
		changed, want string
	}{
		{nil, "docs/a.md", "root walk in x: classes declared [], reachable [docs]; not selected"},
		{nil, "walked/f.txt", "root walk in x: classes declared [], reachable [walked]; selected"},
		{dyn, "contracts/auth/v1/x.json", "dynamic read in x: classes declared [contracts], reachable [contracts]; selected"},
		{dyn, "docs/a.md", "dynamic read in x: classes declared [contracts], reachable [docs]; not selected"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.changed, func(t *testing.T) {
			t.Parallel()
			work := t.TempDir()
			f, err := newVerdictFixture(work, c.head)
			if err != nil {
				t.Fatal(err)
			}
			list := filepath.Join(work, "changed.txt")
			if err := os.WriteFile(list, []byte(c.changed+"\x00"), 0o644); err != nil {
				t.Fatal(err)
			}
			res := cmdSpec{dir: f.root, env: gitEnv}.run(cigate(t), "plan-verdict", "--event", "pull_request", "--changed", list,
				"--golist", f.golist, "--keys", f.keys, "--root", f.root, "--base", f.base)
			if res.code != 0 || !strings.Contains(res.stderr, c.want+"\n") {
				t.Fatalf("plan-verdict over %s: exit %d, stderr %q, want the line %q", c.changed, res.code, res.stderr, c.want)
			}
		})
	}
}

// In a git checkout the guard reads tracked files only: an untracked file in a declared class (a local note, a review
// context file) changes neither a walk's computed classes nor the generated dynreads table.
func TestPlanReadsCountsTrackedFilesOnly(t *testing.T) {
	work := t.TempDir()
	r := newGitRepo(filepath.Join(work, "repo"))
	for name, body := range map[string]string{
		"go.mod":                                "module m\n",
		"docs/a.md":                             "a\n",
		"contracts/x.json":                      "{}\n",
		"q/q_test.go":                           plantedRootWalker,
		"internal/cigates/affected/classes.tsv": "docs\tdocs/**\ncontracts\tcontracts/**\nclaude\t.claude/**\n",
		"internal/cigates/affected/walks.tsv":   "q\tq_test.go\t.\ttree\tcontracts,docs\n",
	} {
		r.write(name, body)
	}
	r.git("add", "-A")
	r.git("commit", "-q", "-m", "base")
	r.write(".claude/local.json", "{}\n")
	if r.err != nil {
		t.Fatal(r.err)
	}
	if res := planReads(t, r.dir); res.code != 0 {
		t.Fatalf("an untracked .claude file changed the guard's view (exit %d):\n%s%s", res.code, res.stdout, res.stderr)
	}
}
