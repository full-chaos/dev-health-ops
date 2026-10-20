package scripts_test

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10174 PR C: Go port of tests/tooling/test_go_relevant_diff.py. ci/go_relevant_diff.sh is the diff-range
// producer the Go-relevance decider consumes; a wrong range yields a plausible, non-empty, WRONG file list.
// The three matcher tests of the Python file drove ci/go_relevance.py, which no workflow calls any more; the live
// decider is ci/go_relevance.sh (internal/relevancedecider pins its decisions), so the same stdin-format contract is
// executed against it here.

type diffFixture struct {
	repo                                      *gitRepo
	base, trunk, pushHead, feature1, feature2 string
}

// buildDiffFixture: base -- trunk -- pushHead (push) and trunk -- feature1 -- feature2 (pull request).
func buildDiffFixture(work string) (*diffFixture, error) {
	r := newGitRepo(filepath.Join(work, "scratch"))
	r.git("config", "user.email", "t@example.invalid")
	f := &diffFixture{repo: r}
	f.base = r.commit("base.txt", "base")
	f.trunk = r.commit("trunk_only.txt", "trunk")
	r.git("branch", "feature")
	f.pushHead = r.commit("push_change.go", "push")
	r.git("checkout", "-q", "feature")
	f.feature1 = r.commit("feature_change.go", "feature1")
	f.feature2 = r.commit("feature_change2.go", "feature2")
	return f, r.err
}

func runDiff(script string, r *gitRepo, head string, env ...string) (cmdResult, error) {
	r.git("checkout", "-q", head)
	if r.err != nil {
		return cmdResult{}, r.err
	}
	return cmdSpec{dir: r.dir, env: append([]string{basePath}, env...)}.run("bash", script), nil
}

func diffNames(script string, r *gitRepo, head, base string) ([]string, error) {
	res, err := runDiff(script, r, head, "BASE_SHA="+base)
	if err != nil {
		return nil, err
	}
	if res.code != 0 {
		return nil, fmt.Errorf("script exited %d: stdout=%q stderr=%q", res.code, res.stdout, res.stderr)
	}
	var names []string
	for _, p := range strings.Split(res.stdout, "\x00") {
		if p != "" {
			names = append(names, p)
		}
	}
	sort.Strings(names)
	return names, nil
}

func wantNames(got []string, err error, want ...string) error {
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("diff produced %q, want %q", got, want)
	}
	return nil
}

func relevantDiffScenarios() []scenario {
	withFixture := func(f func(script string, fx *diffFixture) error) func(string, string) error {
		return func(script, work string) error {
			fx, err := buildDiffFixture(work)
			if err != nil {
				return err
			}
			return f(script, fx)
		}
	}
	return []scenario{
		{"push_shape_diffs_against_the_previous_commit", withFixture(func(s string, fx *diffFixture) error {
			got, err := diffNames(s, fx.repo, fx.pushHead, "")
			return wantNames(got, err, "push_change.go")
		})},
		{"pull_request_shape_diffs_against_its_base_sha", withFixture(func(s string, fx *diffFixture) error {
			got, err := diffNames(s, fx.repo, fx.feature2, fx.trunk)
			return wantNames(got, err, "feature_change.go", "feature_change2.go")
		})},
		{"merge_group_shape_diffs_against_its_own_base_sha", withFixture(func(s string, fx *diffFixture) error {
			got, err := diffNames(s, fx.repo, fx.feature1, fx.base)
			return wantNames(got, err, "feature_change.go", "trunk_only.txt")
		})},
		{"a_wrong_base_sha_yields_a_different_nonempty_list", withFixture(func(s string, fx *diffFixture) error {
			correct, err := diffNames(s, fx.repo, fx.feature2, fx.trunk)
			if err != nil {
				return err
			}
			wrong, err := diffNames(s, fx.repo, fx.feature2, fx.base)
			if err != nil {
				return err
			}
			if reflect.DeepEqual(correct, wrong) {
				return fmt.Errorf("base and trunk gave the same list %q: BASE_SHA is ignored", correct)
			}
			return wantNames(wrong, nil, "feature_change.go", "feature_change2.go", "trunk_only.txt")
		})},
		{"multicommit_push_sees_every_commit_not_just_the_last", withFixture(func(s string, fx *diffFixture) error {
			got, err := diffNames(s, fx.repo, fx.feature2, fx.trunk)
			if err := wantNames(got, err, "feature_change.go", "feature_change2.go"); err != nil {
				return err
			}
			if last := fx.repo.git("diff", "--name-only", "HEAD^...HEAD"); last != "feature_change2.go" {
				return fmt.Errorf("control: HEAD^...HEAD saw %q, want only feature_change2.go", last)
			}
			return fx.repo.err
		})},
		{"all_zeros_base_sha_refuses_rather_than_guessing", withFixture(func(s string, fx *diffFixture) error {
			res, err := runDiff(s, fx.repo, fx.feature2, "BASE_SHA="+strings.Repeat("0", 40))
			if err != nil {
				return err
			}
			if res.code == 0 {
				return fmt.Errorf("must refuse, not silently fall back to HEAD^...HEAD")
			}
			if !strings.Contains(res.stderr, "all-zeros") {
				return fmt.Errorf("refused without naming all-zeros: %q", res.stderr)
			}
			return nil
		})},
		{"non_ancestor_base_sha_refuses_rather_than_guessing", withFixture(func(s string, fx *diffFixture) error {
			res, err := runDiff(s, fx.repo, fx.pushHead, "BASE_SHA="+fx.feature2)
			if err != nil {
				return err
			}
			if res.code == 0 {
				return fmt.Errorf("must refuse, not silently diff against a non-ancestor")
			}
			if !strings.Contains(res.stderr, "not an ancestor") {
				return fmt.Errorf("refused without naming the ancestor check: %q", res.stderr)
			}
			return nil
		})},
		{"non_ascii_filename_is_not_lost_to_git_quoting", func(script, work string) error {
			r := newGitRepo(filepath.Join(work, "unicode"))
			base := r.commit("base.txt", "base")
			r.commit("internal/café.go", "package x\n")
			quoted := r.git("diff", "--name-only", base+"...HEAD")
			if quoted == "internal/café.go" {
				return fmt.Errorf("control: git did not C-quote the path, so this scenario measures nothing")
			}
			got, err := diffNames(script, r, "HEAD", base)
			return wantNames(got, err, "internal/café.go")
		}},
		{"a_rename_out_of_a_relevant_path_reports_both_paths", func(script, work string) error {
			r := newGitRepo(filepath.Join(work, "rename"))
			base := r.commit("internal/a.go", "package x\n")
			r.write("docs/placeholder", "")
			r.git("mv", "internal/a.go", "docs/a.md")
			r.git("commit", "-q", "-m", "rename out of internal")
			got, err := diffNames(script, r, "HEAD", base)
			return wantNames(got, err, "docs/a.md", "internal/a.go")
		}},
	}
}

func TestGoRelevantDiffScript(t *testing.T) {
	script := scriptPath(t, "ci/go_relevant_diff.sh")
	scenarios := relevantDiffScenarios()
	assertAllPass(t, script, scenarios)
	assertPlantsCaught(t, script, scenarios, []plant{
		p("range ignores BASE_SHA", `range="${BASE_SHA}...HEAD"`, `range="HEAD^...HEAD"`,
			"pull_request_shape_diffs_against_its_base_sha", "diff produced"),
		p("push shape diffs the whole branch", `range="HEAD^...HEAD"`, `range="HEAD...HEAD"`,
			"push_shape_diffs_against_the_previous_commit", "diff produced"),
		p("rename detection left on", "--no-renames ", "",
			"a_rename_out_of_a_relevant_path_reports_both_paths", "diff produced"),
		p("git path quoting left on", "--name-only -z", "--name-only",
			"non_ascii_filename_is_not_lost_to_git_quoting", "diff produced"),
		p("all-zeros sentinel not refused", `[ "${BASE_SHA}" = "${ZERO_SHA}" ]`, `false`,
			"all_zeros_base_sha_refuses_rather_than_guessing", "all-zeros"),
		p("non-ancestor base not refused", `! git merge-base --is-ancestor "${BASE_SHA}" HEAD 2>/dev/null`, `false`,
			"non_ancestor_base_sha_refuses_rather_than_guessing", "must refuse"),
	})
}

func relevanceMatcherScenarios(workflow string) []scenario {
	run := func(script string, stdin []byte) cmdResult {
		return cmdSpec{env: []string{basePath, "GO_WORKFLOW=" + workflow}, stdin: stdin}.run("bash", script)
	}
	return []scenario{
		{"a_rename_out_of_a_relevant_path_stays_relevant", func(script, work string) error {
			// the exact bytes ci/go_relevant_diff.sh emits for internal/a.go -> docs/a.md under --no-renames
			res := run(script, []byte("docs/a.md\x00internal/a.go\x00"))
			if res.code != 0 || !strings.Contains(res.stdout, "relevant=true") {
				return fmt.Errorf("a file removed from a Go-relevant path read as irrelevant: code=%d %q", res.code, res.stdout)
			}
			return nil
		}},
		{"the_matcher_accepts_both_nul_and_newline_delimited_input", func(script, work string) error {
			nl := run(script, []byte("go.mod\nREADME.md\n"))
			nul := run(script, []byte("go.mod\x00README.md\x00"))
			for name, res := range map[string]cmdResult{"newline": nl, "nul": nul} {
				if res.code != 0 || !strings.Contains(res.stdout, "relevant=true") {
					return fmt.Errorf("%s-delimited go.mod must be relevant: code=%d %q", name, res.code, res.stdout)
				}
				if !strings.Contains(res.stdout, "changed files: 2;") {
					return fmt.Errorf("%s-delimited input must resolve to 2 changed files: %q", name, res.stdout)
				}
			}
			return nil
		}},
		{"a_single_nul_terminated_entry_is_one_changed_file", func(script, work string) error {
			res := run(script, []byte("go.mod\x00"))
			if res.code != 0 || !strings.Contains(res.stdout, "changed files: 1;") {
				return fmt.Errorf("a single NUL-terminated entry was not parsed as one changed file: code=%d %q", res.code, res.stdout)
			}
			return nil
		}},
		{"an_irrelevant_nul_list_is_not_relevant", func(script, work string) error {
			res := run(script, []byte("README.md\x00"))
			if res.code != 0 || !strings.Contains(res.stdout, "relevant=false") {
				return fmt.Errorf("a docs-only change must be irrelevant: code=%d %q", res.code, res.stdout)
			}
			return nil
		}},
	}
}

func TestGoRelevanceMatcherStdinFormats(t *testing.T) {
	workflow := scriptPath(t, ".github/workflows/go.yml")
	script := scriptPath(t, "ci/go_relevance.sh")
	scenarios := relevanceMatcherScenarios(workflow)
	assertAllPass(t, script, scenarios)
	assertPlantsCaught(t, script, scenarios, []plant{
		p("NUL input read as newline text", `[ "$(tr -d '\0' < "${raw}" | wc -c)" != "$(wc -c < "${raw}")" ]`, `false`,
			"the_matcher_accepts_both_nul_and_newline_delimited_input", "nul-delimited"),
		p("NUL entries glued together", `read -r -d '' path`, `read -r path`,
			"the_matcher_accepts_both_nul_and_newline_delimited_input", "nul-delimited"),
	})
}

// A workflow file named *.yaml runs exactly like a *.yml one, so a change that touches only such a file is Go-relevant.
// The decider reads go.yml's own path list literally. The guard runs the real decider on two copies of go.yml: one with a
// '.github/workflows/*.yaml' path entry (relevant=true) and one without (the planted defect: relevant=false). Asserting it on
// the real go.yml waits until the branch holds the merge that adds the entry (B1, CHAOS-10177).
func TestGoRelevanceCountsYamlWorkflows(t *testing.T) {
	t.Parallel()
	script := scriptPath(t, "ci/go_relevance.sh")
	raw := readText(t, scriptPath(t, ".github/workflows/go.yml"))
	const entry = "      - '.github/workflows/*.yaml'\n"
	const anchor = "      - 'Makefile'\n"
	without := strings.ReplaceAll(raw, entry, "")
	if strings.Count(without, anchor) < 1 {
		t.Fatalf("go.yml has no %q anchor to insert the entry after: the fixture measured nothing", anchor)
	}
	with := strings.ReplaceAll(without, anchor, anchor+entry)
	verdict := func(body string) (string, error) {
		dst := filepath.Join(t.TempDir(), "go.yml")
		if err := os.WriteFile(dst, []byte(body), 0o644); err != nil {
			return "", err
		}
		res := cmdSpec{env: []string{basePath, "GO_WORKFLOW=" + dst}, stdin: []byte(".github/workflows/only-changed.yaml\x00")}.run("bash", script)
		if res.code != 0 {
			return "", fmt.Errorf("go_relevance.sh exited %d: %s", res.code, res.stderr)
		}
		for _, line := range strings.Split(res.stdout, "\n") {
			if strings.HasPrefix(line, "relevant=") {
				return line, nil
			}
		}
		return "", fmt.Errorf("no relevant= line in %q", res.stdout)
	}
	if got, err := verdict(with); err != nil || got != "relevant=true" {
		t.Fatalf("with a *.yaml path entry a .yaml-only workflow change gave %q (%v), want relevant=true", got, err)
	}
	if got, err := verdict(without); err != nil || got != "relevant=false" {
		t.Fatalf("planted defect (no *.yaml entry): the same change gave %q (%v), want relevant=false", got, err)
	}
}
