package suitecatalog_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-11422 (lock of the whole step): the lock hashes the canonical step mapping, aliases resolved, for every step
// kind, plus one row per job and per workflow for the keys outside the steps. Each plant below is a change that the
// run-text lock alone did not see. Named limit: the scripts a step calls are NOT covered; the lock covers workflow and
// action YAML only.

const syntheticFlow = `name: zz
on: push
jobs:
  a:
    runs-on: ubuntu-latest
    steps:
      - id: one
        name: One
        run: echo one
      - id: two
        name: Two
        run: echo two
      - uses: actions/checkout@abc
        with:
          fetch-depth: 1
      - name: Three
        run: echo three
`

func syntheticLocked(t *testing.T) string {
	t.Helper()
	root := plantTree(t)
	zz := filepath.Join(root, ".github/workflows/zz.yml")
	if err := os.WriteFile(zz, []byte(syntheticFlow), 0o644); err != nil {
		t.Fatal(err)
	}
	steps, _ := scanSteps(t, root)
	old, _ := readLock(root)
	rendered := renderLock(steps, old)
	var out []string
	for _, l := range strings.Split(rendered, "\n") {
		cells := strings.Split(l, "\t")
		if len(cells) == 7 && cells[0] == ".github/workflows/zz.yml" {
			cells[5], cells[6] = "none", "synthetic workflow: echo only"
			l = strings.Join(cells, "\t")
		}
		out = append(out, l)
	}
	if err := os.WriteFile(filepath.Join(root, lockPath), []byte(strings.Join(out, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func lockOnly(t *testing.T, root string) string {
	t.Helper()
	catalog, _ := readCatalog(root)
	return strings.Join(lockViolations(t, root, catalog), "\n")
}

func TestWholeStepLockBaselineIsClean(t *testing.T) {
	root := syntheticLocked(t)
	if v := lockOnly(t, root); v != "" {
		t.Fatalf("the synthetic workflow must lock clean:\n%s", v)
	}
}

func TestWholeStepLockPlants(t *testing.T) {
	cases := []struct {
		name string
		edit func(string) string
		want string
	}{
		{"step env BASH_ENV", func(s string) string {
			return strings.Replace(s, "        run: echo one\n", "        env:\n          BASH_ENV: /tmp/x\n        run: echo one\n", 1)
		}, "sha256 mismatch"},
		{"workflow env BASH_ENV", func(s string) string { return strings.Replace(s, "jobs:\n", "env:\n  BASH_ENV: /tmp/x\njobs:\n", 1) }, "sha256 mismatch"},
		{"job env BASH_ENV", func(s string) string {
			return strings.Replace(s, "    runs-on: ubuntu-latest\n", "    runs-on: ubuntu-latest\n    env:\n      BASH_ENV: /tmp/x\n", 1)
		}, "sha256 mismatch"},
		{"job container", func(s string) string {
			return strings.Replace(s, "    runs-on: ubuntu-latest\n", "    runs-on: ubuntu-latest\n    container: golang\n", 1)
		}, "sha256 mismatch"},
		{"workflow defaults shell", func(s string) string {
			return strings.Replace(s, "jobs:\n", "defaults:\n  run:\n    shell: python\njobs:\n", 1)
		}, "sha256 mismatch"},
		{"step if added", func(s string) string {
			return strings.Replace(s, "        run: echo one\n", "        if: always()\n        run: echo one\n", 1)
		}, "sha256 mismatch"},
		{"continue-on-error added", func(s string) string {
			return strings.Replace(s, "        run: echo one\n", "        continue-on-error: true\n        run: echo one\n", 1)
		}, "sha256 mismatch"},
		{"working-directory added", func(s string) string {
			return strings.Replace(s, "        run: echo one\n", "        working-directory: ci\n        run: echo one\n", 1)
		}, "sha256 mismatch"},
		{"with changed on a uses step", func(s string) string { return strings.Replace(s, "fetch-depth: 1", "fetch-depth: 0", 1) }, "sha256 mismatch"},
		{"uses version changed", func(s string) string { return strings.Replace(s, "checkout@abc", "checkout@def", 1) }, "sha256 mismatch"},
		{"docker uses step added", func(s string) string {
			return s + "      - uses: docker://golang\n        with:\n          entrypoint: go\n          args: test ./x\n"
		}, "run step has no row"},
		{"third-party go test action added", func(s string) string {
			return s + "      - uses: robherley/go-test-action@v0\n"
		}, "run step has no row"},
		{"github-script step added", func(s string) string {
			return s + "      - uses: actions/github-script@v7\n        with:\n          script: require('child_process').execSync('go test ./x')\n"
		}, "run step has no row"},
		{"job-level reusable workflow added", func(s string) string {
			return s + "  b:\n    uses: ./.github/workflows/other.yml\n"
		}, "run step has no row"},
		{"run replaced by an alias", func(s string) string {
			s = strings.Replace(s, "on: push\n", "on: push\nx-cmd: &cmd go test ./x\n", 1)
			return strings.Replace(s, "        run: echo three\n", "        run: *cmd\n", 1)
		}, "sha256 mismatch"},
		{"whole-step alias added", func(s string) string {
			s = strings.Replace(s, "      - name: Three\n        run: echo three\n", "      - &three\n        name: Three\n        run: echo three\n      - *three\n", 1)
			return s
		}, "run step has no row"},
		{"id-keyed steps swapped", func(s string) string {
			a := "      - id: one\n        name: One\n        run: echo one\n"
			b := "      - id: two\n        name: Two\n        run: echo two\n"
			return strings.Replace(s, a+b, b+a, 1)
		}, "stale row"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := syntheticLocked(t)
			edit(t, filepath.Join(root, ".github/workflows/zz.yml"), c.edit)
			if v := lockOnly(t, root); !strings.Contains(v, c.want) {
				t.Fatalf("want %q, got:\n%s", c.want, v)
			}
		})
	}
	t.Run("a changed sha clears suites and reason when the table is regenerated", func(t *testing.T) {
		steps := []runStep{{"a.yml", "j", "#1 s", "default", "newsha", "x"}}
		old := []lockRow{{"a.yml", "j", "#1 s", "default", "oldsha", "suite-a", "kept before"}}
		got := renderLock(steps, old)
		if !strings.Contains(got, "a.yml\tj\t#1 s\tdefault\tnewsha\t\t\n") {
			t.Fatalf("a changed sha must clear suites and reason:\n%s", got)
		}
	})
}
