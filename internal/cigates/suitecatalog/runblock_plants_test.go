package suitecatalog_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-11422: every run step in every workflow and every action file must match a reviewed row of ci/run_block_lock.tsv
// (file, job, step, shell, sha256 of the run text, suites, reason). A shell-splitting denylist cannot be complete, so
// each way of hiding a test command below must fail as "no row" or "sha mismatch", whatever the shell does with it.

const lockPath = "ci/run_block_lock.tsv"

const riverRun = "run: bash ci/check_river_compat_static.sh"

func plantRunText(t *testing.T, extra string) string {
	t.Helper()
	root := plantTree(t)
	edit(t, filepath.Join(root, ".github/workflows/go-quality.yml"), func(s string) string {
		body := strings.ReplaceAll(extra, "\n", "\n          ")
		return strings.Replace(s, riverRun, "run: |\n          bash ci/check_river_compat_static.sh\n          "+body, 1)
	})
	return root
}

func TestEveryHiddenRunTextBreaksTheLock(t *testing.T) {
	lines := map[string]string{
		"env prefix":           `env FOO=1 $RUNNER test`,
		"nice":                 `nice $RUNNER test`,
		"timeout":              `timeout 5 $RUNNER test`,
		"sudo -E":              `sudo -E $RUNNER test`,
		"exec -a":              `exec -a x $RUNNER test`,
		"command -p":           `command -p $RUNNER test`,
		"time -p":              `time -p $RUNNER test`,
		"coproc":               `coproc $RUNNER test`,
		"builtin eval":         `builtin eval "$CMD"`,
		"background":           `true & $RUNNER test`,
		"pipe and":             `echo x |& $RUNNER`,
		"inside echo":          `echo $($RUNNER test)`,
		"assignment":           `out=$($RUNNER test)`,
		"process substitution": `tee >($RUNNER test)`,
		"here string":          `bash <<< "$X"`,
		"piped to bash":        `echo "$X" | bash`,
		"bash -xc":             `bash -xc "$CMD"`,
		"trap":                 `trap '$RUNNER test' EXIT`,
		"find -exec":           `find . -exec $RUNNER test {} +`,
		"here document body":   "cat <<'EOF'\n$RUNNER test\nEOF",
		"false heredoc start":  "x=$((1<<3))\n$RUNNER test",
		"false quote":          "echo it\\'s\n$RUNNER test",
		"function":             `f() { $RUNNER test; }; f`,
		"quoted argument":      `go "$SUB"`,
		"expression argument":  `go ${{ matrix.verb }}`,
		"quoted command word":  `"go" test ./x`,
		"backslash command":    `\go test ./x`,
		"absolute path":        `/usr/local/go/bin/go test ./x`,
		"double space":         `go  test ./x`,
		"go -C":                `go -C internal test ./x`,
		"bash -c literal":      `bash -c 'go test ./x'`,
		"copied binary":        "cp \"$(command -v go)\" \"${RUNNER_TEMP}/cigate-base\"\n\"${RUNNER_TEMP}/cigate-base\" test ./x",
		"plain variable":       `$RUNNER test ./x`,
		"command substitution": `$(echo go) test ./x`,
		"eval":                 `eval "$CMD"`,
		"xargs":                `echo ./x | xargs go test`,
	}
	for name, line := range lines {
		t.Run(name, func(t *testing.T) {
			v := strings.Join(violations(t, plantRunText(t, line)), "\n")
			if !strings.Contains(v, "sha256 mismatch") {
				t.Fatalf("want a sha256 mismatch, got:\n%s", v)
			}
		})
	}
}

func TestRunBlockLockPlants(t *testing.T) {
	t.Run("a new step has no row", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, ".github/workflows/go-quality.yml"), func(s string) string {
			return strings.Replace(s, "      - name: Validate River compatibility harness", "      - name: Extra\n        run: echo hi\n\n      - name: Validate River compatibility harness", 1)
		})
		wantViolation(t, root, "run step has no row")
	})
	t.Run("a shell: python step has no row", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, ".github/workflows/go-quality.yml"), func(s string) string {
			return strings.Replace(s, "      - name: Validate River compatibility harness", "      - name: Py\n        shell: python\n        run: |\n          import subprocess\n          subprocess.run(['go', 'test'])\n\n      - name: Validate River compatibility harness", 1)
		})
		wantViolation(t, root, "run step has no row")
	})
	t.Run("a changed shell is refused", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, ".github/workflows/go-quality.yml"), func(s string) string {
			return strings.Replace(s, riverRun, "shell: sh\n        "+riverRun, 1)
		})
		wantViolation(t, root, "shell differs")
	})
	for _, rel := range []string{".github/actions/a/b/action.yml", "tools/act/action.yaml", "action.yml", ".github/actions/x/action.yml"} {
		t.Run("an action file at "+rel+" has no row", func(t *testing.T) {
			root := plantTree(t)
			p := filepath.Join(root, rel)
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			action := "runs:\n  using: composite\n  steps:\n    - shell: bash\n      run: echo hi\n"
			if err := os.WriteFile(p, []byte(action), 0o644); err != nil {
				t.Fatal(err)
			}
			wantViolation(t, root, "run step has no row")
		})
	}
	t.Run("a local uses target that does not exist is refused", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, ".github/workflows/go-quality.yml"), func(s string) string {
			return strings.Replace(s, "      - name: Validate River compatibility harness", "      - uses: ./tools/gone\n\n      - name: Validate River compatibility harness", 1)
		})
		wantViolation(t, root, "local action ./tools/gone")
	})
	t.Run("a row whose step is gone is stale", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, ".github/workflows/go-quality.yml"), func(s string) string {
			return strings.Replace(s, "      - name: Test the auth key-set Secret script\n", "      - name: Renamed key-set step\n", 1)
		})
		wantViolation(t, root, "stale row")
	})
	t.Run("a duplicate row is refused", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, lockPath), func(s string) string {
			lines := strings.Split(s, "\n")
			for _, l := range lines {
				if strings.Contains(l, "go-quality.yml") {
					return s + l + "\n"
				}
			}
			return s
		})
		wantViolation(t, root, "duplicate row")
	})
	t.Run("a row with an empty reason is refused", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, lockPath), func(s string) string {
			lines := strings.Split(s, "\n")
			for i, l := range lines {
				cells := strings.Split(l, "\t")
				if len(cells) == 7 && cells[0] != "file" && !strings.HasPrefix(l, "#") {
					cells[6] = ""
					lines[i] = strings.Join(cells, "\t")
					break
				}
			}
			return strings.Join(lines, "\n")
		})
		wantViolation(t, root, "empty reason")
	})
	t.Run("a none row whose text runs a test command is refused", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, lockPath), func(s string) string {
			lines := strings.Split(s, "\n")
			for i, l := range lines {
				cells := strings.Split(l, "\t")
				if len(cells) == 7 && cells[0] == ".github/workflows/go-quality.yml" && cells[5] != "none" {
					cells[5] = "none"
					lines[i] = strings.Join(cells, "\t")
					break
				}
			}
			return strings.Join(lines, "\n")
		})
		wantViolation(t, root, "none row runs a test command")
	})
	t.Run("an unknown suite id is refused", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, lockPath), func(s string) string {
			lines := strings.Split(s, "\n")
			for i, l := range lines {
				cells := strings.Split(l, "\t")
				if len(cells) == 7 && cells[0] != "file" && cells[5] != "none" {
					cells[5] = "no-such-suite"
					lines[i] = strings.Join(cells, "\t")
					break
				}
			}
			return strings.Join(lines, "\n")
		})
		wantViolation(t, root, "unknown suite")
	})
	t.Run("an edited sha in the table is a mismatch", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, lockPath), func(s string) string {
			lines := strings.Split(s, "\n")
			for i, l := range lines {
				cells := strings.Split(l, "\t")
				if len(cells) == 7 && cells[0] != "file" && !strings.HasPrefix(l, "#") {
					cells[4] = strings.Repeat("0", 64)
					lines[i] = strings.Join(cells, "\t")
					break
				}
			}
			return strings.Join(lines, "\n")
		})
		wantViolation(t, root, "sha256 mismatch")
	})
	t.Run("a reorder that changes step indexes fails as stale and missing", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, ".github/workflows/go-quality.yml"), func(s string) string {
			a := "      - name: Test the session-issue Secret create script\n"
			b := "      - name: Test the auth key-set Secret script\n"
			i, j := strings.Index(s, a), strings.Index(s, b)
			if i < 0 || j < i {
				t.Fatal("steps not found in order")
			}
			end := strings.Index(s[j:], "\n      - name: Validate River")
			return s[:i] + s[j:j+end+1] + s[i:j] + s[j+end+1:]
		})
		wantViolation(t, root, "run step has no row")
		wantViolation(t, root, "stale row")
	})
	t.Run("a catalog suite named by no row is refused", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, lockPath), func(s string) string {
			return strings.ReplaceAll(s, "river-frozen-evidence", "none")
		})
		wantViolation(t, root, `catalog suite "river-frozen-evidence" is named by no lock row`)
	})
	t.Run("a workflow-level permissions change is a sha256 mismatch on the top row", func(t *testing.T) {
		for name, to := range map[string]string{"scalar": "permissions: write-all\n", "widened": "permissions:\n  contents: write\n", "deleted": ""} {
			root := plantTree(t)
			edit(t, filepath.Join(root, ".github/workflows/test.yml"), func(s string) string {
				if !strings.Contains(s, "\npermissions:\n  contents: read\n") {
					t.Fatal("planted defect did not land: test.yml has no workflow-level permissions block")
				}
				return strings.Replace(s, "\npermissions:\n  contents: read\n", "\n"+to, 1)
			})
			v := strings.Join(violations(t, root), "\n")
			if !strings.Contains(v, `sha256 mismatch: .github/workflows/test.yml job (workflow) step "(top)"`) {
				t.Errorf("%s: the top row did not notice the permissions change:\n%s", name, v)
			}
		}
	})
	t.Run("a top row whose reason does not say permissions is refused", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, lockPath), func(s string) string {
			lines := strings.Split(s, "\n")
			for i, l := range lines {
				cells := strings.Split(l, "\t")
				if len(cells) == 7 && cells[1] == "(workflow)" && cells[2] == "(top)" {
					cells[6] = "pins on, env and defaults"
					lines[i] = strings.Join(cells, "\t")
					break
				}
			}
			return strings.Join(lines, "\n")
		})
		wantViolation(t, root, "does not say that the row pins the workflow permissions")
	})
	t.Run("a trailing whitespace change keeps the lock green", func(t *testing.T) {
		root := plantTree(t)
		edit(t, filepath.Join(root, ".github/workflows/go-quality.yml"), func(s string) string {
			return strings.Replace(s, riverRun+"\n", riverRun+"   \n", 1)
		})
		if v := strings.Join(violations(t, root), "\n"); strings.Contains(v, "sha256 mismatch") {
			t.Fatalf("trailing whitespace must not break the lock:\n%s", v)
		}
	})
}
