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

// One plan verdict for the integration plan job and the go-quality legs: a change set of .go files only plans AFFECTED
// (the reverse import closure of the changed packages); any other path plans FULL. `cigate plan-verdict` is executed as
// a built binary over a git fixture: a base commit, a head commit and a synthetic go list. Packages: a, b, c (imports b),
// d (imports the test helper internal/testsupport/h), x. The fixture also carries the declared-read tables the planner of
// 0b4d992e9b read, so the rows of the four bounded-read cases replay that planner's answer on the same tree.

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
	removeRealPlanDir()
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
			return fmt.Errorf("want exit 1 (a flag or event error), got %q %q %v", first, keys, err)
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

func runRows(t *testing.T, rows []verdictRow) {
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

const nonGo = "FULL non-Go file*"

func TestPlanVerdict(t *testing.T) {
	q := []string{"--go-files-full"}
	runRows(t, []verdictRow{
		// .go files only: the reverse import closure.
		{name: "go_change_selects_its_reverse_closure", changed: []string{"b/b.go"}, want: "AFFECTED 2", keys: []string{"b", "c"}},
		{name: "test_file_change_selects_its_package", changed: []string{"x/x_test.go"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "two_go_packages", changed: []string{"a/a.go", "x/x.go"}, want: "AFFECTED 2", keys: []string{"a", "x"}},
		{name: "go_fixture_under_testdata_is_its_package", changed: []string{"x/testdata/f.go"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "non_ascii_go_file_name", changed: []string{"x/caf\u00e9.go"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "go_file_outside_any_package_is_full", changed: []string{"zz/z.go"}, want: "FULL Go file outside any listed package*"},
		{name: "testsupport_go_is_full", changed: []string{"internal/testsupport/h/h.go"}, want: "FULL shared change*"},
		{name: "planner_code_is_full", changed: []string{"internal/cigates/affected/plan.go"}, want: "FULL planner input*"},
		{name: "planner_command_is_full", changed: []string{"cmd/cigate/main.go"}, want: "FULL planner input*"},
		{name: "always_select_list_is_full", changed: []string{"internal/cigates/affected/always.tsv"}, want: "FULL planner input*"},
		{name: "go_quality_yml_is_full", changed: []string{".github/workflows/go-quality.yml"}, want: "FULL planner input*"},
		{name: "planner_test_file_is_not_a_planner_input", changed: []string{"x/x_test.go", "internal/cigates/affected/plan_test.go"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "go_quality_go_source_is_full", extra: q, changed: []string{"x/x.go"}, want: "FULL Go source*"},
		// any other path: FULL.
		{name: "reader_less_path_is_full", changed: []string{".claude/x.json"}, want: nonGo},
		{name: "docs_path_is_full", changed: []string{"docs/a.md"}, want: nonGo},
		{name: "go_beside_a_non_go_path_is_full", changed: []string{"x/x.go", "docs/a.md"}, want: nonGo},
		{name: "testdata_data_is_full", changed: []string{"x/testdata/a.json"}, want: nonGo},
		{name: "plan_tables_are_full", changed: []string{"internal/cigates/affected/reads.tsv"}, want: "FULL planner input*"},
		{name: "go_mod_is_full", changed: []string{"go.mod"}, want: "FULL shared change*"},
		{name: "go_yml_is_full", changed: []string{".github/workflows/go.yml"}, want: "FULL planner input*"},
		{name: "check_go_is_full", changed: []string{"ci/check_go.sh"}, want: "FULL shared change*"},
		{name: "go_quality_non_go_is_full", extra: q, changed: []string{"docs/a.md"}, want: nonGo},
		// the relevance inputs of the deleted go-quality decider never plan below FULL.
		{name: "python_source_is_full", changed: []string{"scripts/x.py"}, want: nonGo},
		{name: "python_in_docs_is_full", changed: []string{"docs/x.py"}, want: nonGo},
		{name: "ci_requirements_is_full", changed: []string{"ci/requirements-docs.txt"}, want: "FULL shared change*"},
		{name: "mkdocs_is_full", changed: []string{"mkdocs.yml"}, want: nonGo},
		{name: "provider_limits_doc_is_full", changed: []string{"docs/reference/limits-and-compatibility/provider-limits.md"}, want: nonGo},
		{name: "local_validate_is_full", changed: []string{"ci/local_validate.sh"}, want: "FULL shared change*"},
		{name: "live_backend_e2e_is_full", changed: []string{"ci/run_live_backend_e2e.sh"}, want: "FULL shared change*"},
		// non-ASCII names (D6129) and odd list entries.
		{name: "non_ascii_doc_is_full", changed: []string{"docs/caf\u00e9.md"}, want: nonGo},
		{name: "a_path_of_non_ascii_space_is_a_path_not_a_blank", changed: []string{"\u00a0"}, want: nonGo},
		// events.
		{name: "push_is_full", event: "push", changed: []string{"x/x.go"}, want: "FULL event push*"},
		{name: "merge_group_is_full", event: "merge_group", changed: []string{"x/x.go"}, want: "FULL event merge_group*"},
		{name: "schedule_is_full", event: "schedule", changed: []string{"x/x.go"}, want: "FULL event schedule*"},
		{name: "workflow_dispatch_is_full", event: "workflow_dispatch", changed: []string{"x/x.go"}, want: "FULL event workflow_dispatch*"},
		{name: "range_judges_the_change_set", event: "range", changed: []string{"x/x.go"}, want: "AFFECTED 1", keys: []string{"x"}},
		{name: "empty_change_set_is_full", changed: nil, want: "FULL empty change set*"},
		{name: "unknown_event_is_an_error", extra: []string{"--event", "pull_request_target"}, changed: []string{"x/x.go"}, wantErr: true},
	})
}

// The four bounded-read cases an adversarial review executed against the 0b4d992e9b planner, which tried to plan a
// non-Go change below FULL. Each fixture holds the tables that planner generated for it; there each case skipped a test
// that the change breaks (wrong answer in the comment). The planner plans every one FULL.
const (
	plantedEnvReader = "package %s\n\nimport \"os\"\n\nfunc Load() ([]byte, error) { return os.ReadFile(os.Getenv(\"READ_PATH\")) }\n"
	plantedOwnJoin   = "package x\n\nimport (\n\t\"os\"\n\t\"path/filepath\"\n)\n\nfunc read() ([]byte, error) { return os.ReadFile(filepath.Join(\".\", os.Getenv(\"READ_PATH\"))) }\n"
	plantedConcat    = "package x\n\nimport \"os\"\n\nfunc read() ([]byte, error) { return os.ReadFile(\"../do\" + \"cs/a.md\") }\n"
	plantedRepoWalk  = "package x\n\nimport (\n\t\"io/fs\"\n\t\"path/filepath\"\n)\n\nfunc repoRoot() string { return \"..\" }\n\nfunc walk() error {\n\treturn filepath.WalkDir(repoRoot(), func(string, fs.DirEntry, error) error { return nil })\n}\n"
)

func TestPlanVerdictBoundedReadCasesPlanFull(t *testing.T) {
	runRows(t, []verdictRow{
		// 0b4d992e9b: AFFECTED 1 [b]; c, whose test calls b.Load, was skipped.
		{name: "importer_of_an_env_reader", head: map[string]string{"b/b_dyn.go": fmt.Sprintf(plantedEnvReader, "b"),
			"internal/cigates/affected/dynreads.tsv": "b\tb_dyn.go\tunexpressed\n"},
			changed: []string{"AGENTS.md"}, want: nonGo},
		// 0b4d992e9b: AFFECTED 1 [a]; x, reading ../docs through READ_PATH, was skipped.
		{name: "own_join_escape", head: map[string]string{"x/x_own_test.go": plantedOwnJoin},
			changed: []string{"docs/a.md"}, want: nonGo},
		// 0b4d992e9b: AFFECTED 1 [a]; x, reading "../do"+"cs/a.md", was skipped.
		{name: "constant_concat", head: map[string]string{"x/x_cat_test.go": plantedConcat},
			changed: []string{"docs/a.md"}, want: nonGo},
		// 0b4d992e9b: AFFECTED 2 [b c]; x, walking the repository root under a row declaring docs/, was skipped.
		{name: "root_walk_with_a_docs_row", head: map[string]string{"x/x_walk_test.go": plantedRepoWalk,
			"internal/cigates/affected/walks.tsv": "x\ttest\tx_walk_test.go\tdocs\t-\t*\tdocs\n"},
			changed: []string{"contracts/auth/v1/x.json"}, want: nonGo},
	})
}

// plan-reads is the guard of the declared-read table: every package that names a path of a declared class must hold a
// reads.tsv row that covers it.

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
		{"code_reader_needs_a_code_row", map[string]string{"r/r.go": plantedCodeReader,
			"internal/cigates/affected/reads.tsv": "r\ttest\tcontracts\n"}, 1, []string{"r/r.go", "r\tcode\tcontracts"}},
		{"declared_readers_pass", map[string]string{"p/p_test.go": plantedLiteralReader, "r/r.go": plantedCodeReader,
			"internal/cigates/affected/reads.tsv": "p\ttest\tdocs\nr\tcode\tcontracts\n"}, 0, nil},
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

// In a git checkout the guard reads tracked files only: an untracked Go file (a local scratch test) never changes its view.
func TestPlanReadsCountsTrackedFilesOnly(t *testing.T) {
	work := t.TempDir()
	r := newGitRepo(filepath.Join(work, "repo"))
	for name, body := range map[string]string{
		"go.mod":                                "module m\n",
		"docs/a.md":                             "a\n",
		"internal/cigates/affected/classes.tsv": "docs\tdocs/**\n",
	} {
		r.write(name, body)
	}
	r.git("add", "-A")
	r.git("commit", "-q", "-m", "base")
	r.write("p/p_test.go", plantedLiteralReader)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if res := planReads(t, r.dir); res.code != 0 {
		t.Fatalf("an untracked Go file changed the guard's view (exit %d):\n%s%s", res.code, res.stdout, res.stderr)
	}
}

// The changed list as ci/go_relevant_diff.sh writes it (`git diff -z --no-renames`): NUL-terminated entries; a list with no
// NUL is read one path per line.
func TestPlanVerdictChangedListFormats(t *testing.T) {
	cases := []struct {
		name, raw, want string
		keys            []string
	}{
		{"a_rename_out_of_a_go_package_is_full", "docs/a.md\x00x/x.go\x00", "FULL non-Go file: docs/a.md", nil},
		{"newline_delimited_entries_are_separate", "x/x.go\ngo.mod\n", "FULL shared change: go.mod", nil},
		{"nul_delimited_entries_are_separate", "x/x.go\x00go.mod\x00", "FULL shared change: go.mod", nil},
		{"a_single_nul_terminated_entry_is_one_path", "x/x.go\x00", "AFFECTED 1", []string{"x"}},
		{"a_yaml_workflow_is_full", ".github/workflows/only-changed.yaml\x00", "FULL shared change: .github/workflows/only-changed.yaml", nil},
		{"empty_entries_between_nuls_are_skipped", "\x00x/x.go\x00\x00", "AFFECTED 1", []string{"x"}},
		{"newline_format_one_go_path", "x/x.go\n", "AFFECTED 1", []string{"x"}},
		{"newline_format_blank_lines_are_skipped", "\nx/x.go\n\na/a.go\n", "AFFECTED 2", []string{"a", "x"}},
		{"newline_format_padding_is_part_of_the_path", "  x/x.go  \n", "FULL non-Go file*", nil},
		{"newline_format_left_padding_is_part_of_the_path", "  x/x.go\n", "FULL Go file outside any listed package:*", nil},
		{"nul_format_unterminated_last_entry", "a/a.go\x00x/x.go", "AFFECTED 2", []string{"a", "x"}},
		{"newline_format_unterminated_last_line", "a/a.go\nx/x.go", "AFFECTED 2", []string{"a", "x"}},
		{"nul_format_embedded_newline_in_a_go_path", "x/new\nline/module.go\x00", "FULL Go file outside any listed package: x/new\\nline/module.go", nil},
		{"a_newline_entry_of_non_ascii_space_is_kept", "x/x.go\n\u00a0\n", "FULL non-Go file: \u00a0", nil},
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
// (--go-files-full) answers them. Old answers: relevant=true|false from go.yml's path list; relevant=true ran every
// package, the one FULL the go-quality legs know.
func TestDeletedDeciderCasesArePlanned(t *testing.T) {
	q := []string{"--go-files-full"}
	runRows(t, []verdictRow{
		{name: "docs_only", extra: q, changed: []string{"docs/guide.md", "README.md"}, want: nonGo},
		{name: "go_only", extra: q, changed: []string{"x/x.go"}, want: "FULL Go source*"},
		{name: "root_go_file_and_go_mod", extra: q, changed: []string{"main.go", "go.mod", "go.sum"}, want: "FULL*"},
		{name: "chart_only", extra: q, changed: []string{"deploy/helm/dev-health/values.yaml"}, want: nonGo},
		{name: "chart_sibling", extra: q, changed: []string{"deploy/helm/other-chart/values.yaml"}, want: nonGo},
		{name: "mixed_docs_and_go", extra: q, changed: []string{"docs/a.md", "x/y.go", "web/readme.md"}, want: "FULL*"},
		{name: "deleted_go_file", extra: q, changed: []string{"internal/old/removed.go"}, want: "FULL*"},
		{name: "rename_as_delete_and_add", extra: q, changed: []string{"x/x.go", "docs/a.md"}, want: "FULL*"},
		{name: "rename_to_an_irrelevant_path_only", extra: q, changed: []string{"notes/old.txt", "notes/new.txt"}, want: nonGo},
		{name: "testdata_data", extra: q, changed: []string{"x/testdata/x.txt"}, want: nonGo},
		{name: "manifest_tsv", extra: q, changed: []string{"x/testdata.manifest.tsv"}, want: nonGo},
		{name: "exact_check_go_entry", extra: q, changed: []string{"ci/check_go.sh"}, want: "FULL shared change*"},
		{name: "near_miss_of_check_go", extra: q, changed: []string{"ci/check_go.sh.bak"}, want: "FULL shared change*"},
		{name: "python_source", extra: q, changed: []string{"src/dev_health_ops/api/routes.py"}, want: nonGo},
		{name: "regex_special_characters_in_a_path", extra: q, changed: []string{"x/a+b(c)/x.txt"}, want: nonGo},
		{name: "non_ascii_paths", extra: q, changed: []string{"docs/r\u00e9sum\u00e9.md"}, want: nonGo},
		{name: "workflow_file_itself", extra: q, changed: []string{".github/workflows/go-quality.yml"}, want: "FULL planner input*"},
		{name: "dotfile_entry", extra: q, changed: []string{".dockerignore"}, want: nonGo},
		{name: "empty_change_set", extra: q, changed: nil, want: "FULL empty change set*"},
	})
}

// A failing guard names the table file and prints each row to add as an exact TSV line, ready to paste; a test-only row
// that non-test code outgrew is printed as the code row that replaces it.
func TestPlanReadsPrintsTheRowsToPaste(t *testing.T) {
	res := planReads(t, readsFixture(t, map[string]string{
		"p/p_test.go":                         plantedLiteralReader,
		"q/q_test.go":                         plantedLiteralReader,
		"r/r.go":                              plantedCodeReader,
		"internal/cigates/affected/reads.tsv": "r\ttest\tcontracts\n",
	}))
	if res.code != 1 {
		t.Fatalf("plan-reads exited %d, want 1:\n%s", res.code, res.stdout+res.stderr)
	}
	lines := map[string]bool{}
	for _, l := range strings.Split(res.stdout, "\n") {
		lines[l] = true
	}
	for _, want := range []string{
		"add these rows to internal/cigates/affected/reads.tsv:",
		"p\ttest\tdocs",
		"q\ttest\tdocs",
		"replace these rows in internal/cigates/affected/reads.tsv:",
		"r\ttest\tcontracts -> r\tcode\tcontracts",
	} {
		if !lines[want] {
			t.Errorf("plan-reads output has no line %q:\n%s", want, res.stdout)
		}
	}
}
