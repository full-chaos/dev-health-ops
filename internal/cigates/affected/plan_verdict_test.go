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
	cigateBin  string
	cigateErr  error
)

func cigate(t *testing.T) string {
	t.Helper()
	cigateOnce.Do(func() {
		dir, err := os.MkdirTemp("", "cigate-plan-verdict-")
		if err != nil {
			cigateErr = err
			return
		}
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
`

const fixtureReads = `# package	scope	class
a	test	docs
b	code	contracts
x	test	github
x	test	ci
`

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
		"c/c.go":                                 "package c\n",
		"d/d.go":                                 "package d\n",
		"x/x.go":                                 "package x\n",
		"internal/testsupport/h/h.go":            "package h\n",
		"internal/cigates/affected/plan.go":      "package affected\n",
		"internal/cigates/affected/classes.tsv":  fixtureClasses,
		"internal/cigates/affected/reads.tsv":    fixtureReads,
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
	if len(keys) == 1 && keys[0] == "" {
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
		{name: "reader_less_paths_are_none", changed: []string{".claude/settings.json", ".codex-review/r1.md", ".codex-review-notes.md", "AGENTS.md"}, want: "NONE"},
		{name: "test_reader_selects_only_itself", changed: []string{"docs/a.md"}, want: "AFFECTED 1", keys: []string{"a"}},
		{name: "code_reader_selects_its_reverse_deps", changed: []string{"contracts/auth/v1/x.json"}, want: "AFFECTED 2", keys: []string{"b", "c"}},
		{name: "undeclared_path_is_full", changed: []string{"zz/new.txt"}, want: "FULL non-Go file*"},
		{name: "go_mod_is_full", changed: []string{".claude/x", "go.mod"}, want: "FULL shared change*"},
		{name: "testsupport_is_full", changed: []string{"internal/testsupport/h/h.go"}, want: "FULL shared change*"},
		{name: "push_is_full", event: "push", changed: []string{".claude/settings.json"}, want: "FULL event push*"},
		{name: "merge_group_is_full", event: "merge_group", changed: []string{".claude/settings.json"}, want: "FULL event merge_group*"},
		{name: "schedule_is_full", event: "schedule", changed: []string{".claude/settings.json"}, want: "FULL event schedule*"},
		{name: "workflow_dispatch_is_full", event: "workflow_dispatch", changed: []string{".claude/settings.json"}, want: "FULL event workflow_dispatch*"},
		{name: "range_judges_the_change_set", event: "range", changed: []string{".claude/settings.json"}, want: "NONE"},
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
		{name: "go_quality_workflow_is_full", changed: []string{".github/workflows/go-quality.yml"}, want: "FULL shared change*"},
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
		{name: "quality_leg_partition_input_is_full", changed: []string{"ci/go_test_weights.tsv"}, want: "FULL shared change*"},
		{name: "race_weights_row_is_full", changed: []string{"ci/go_race_weights.d/a.tsv"}, want: "FULL shared change*"},
		{name: "diff_producer_is_full", changed: []string{"ci/go_relevant_diff.sh"}, want: "FULL shared change*"},
		{name: "reads_row_for_an_unknown_class_is_an_error",
			head:    map[string]string{"internal/cigates/affected/reads.tsv": fixtureReads + "a\ttest\tnope\n"},
			changed: []string{"docs/a.md"}, wantErr: true},
		{name: "reads_row_with_an_unknown_scope_is_an_error",
			head:    map[string]string{"internal/cigates/affected/reads.tsv": fixtureReads + "a\tsometimes\tdocs\n"},
			changed: []string{"docs/a.md"}, wantErr: true},
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
	plantedLiteralReader = "package p\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n)\n\nfunc read() ([]byte, error) {\n\treturn os.ReadFile(filepath.Join(\"..\", \"docs\", \"a.md\"))\n}\n"
	plantedSlashReader   = "package s\n\nvar doc = \"../../docs/a.md\"\n"
	plantedRootWalker    = "package q\n\nimport (\n\t\"io/fs\"\n\t\"path/filepath\"\n)\n\nfunc repoRoot() string { return \"..\" }\n\nfunc walk() error {\n\treturn filepath.WalkDir(repoRoot(), func(string, fs.DirEntry, error) error { return nil })\n}\n"
	plantedCodeReader    = "package r\n\nimport \"path/filepath\"\n\nvar Dir = filepath.Join(\"..\", \"contracts\")\n"
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
		{"planted_undeclared_root_walker_is_named", map[string]string{"q/q_test.go": plantedRootWalker}, 1, []string{"q/q_test.go", "walks from the repository root"}},
		{"code_reader_needs_a_code_row", map[string]string{"r/r.go": plantedCodeReader,
			"internal/cigates/affected/reads.tsv": "r\ttest\tcontracts\n"}, 1, []string{"r/r.go", "r\tcode\tcontracts"}},
		{"declared_readers_pass", map[string]string{"p/p_test.go": plantedLiteralReader, "q/q_test.go": plantedRootWalker, "r/r.go": plantedCodeReader,
			"internal/cigates/affected/reads.tsv": "p\ttest\tdocs\nq\ttest\tdocs\nr\tcode\tcontracts\n"}, 0, nil},
		{"row_for_an_unknown_class_fails", map[string]string{"internal/cigates/affected/reads.tsv": "p\ttest\tnope\n"}, 1, []string{"nope"}},
		{"row_for_a_package_with_no_go_file_fails", map[string]string{"internal/cigates/affected/reads.tsv": "gone\ttest\tdocs\n"}, 1, []string{"gone"}},
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

func TestPlanReadsRealRepository(t *testing.T) {
	root := repoRoot(t)
	if res := planReads(t, root); res.code != 0 {
		t.Fatalf("the declared-read table does not cover the repository (plan-reads exited %d):\n%s%s", res.code, res.stdout, res.stderr)
	}
}
