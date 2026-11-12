package scripts_test

import (
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-10174 PR C: Go port of tests/tooling/test_go_relevant_diff.py. ci/go_relevant_diff.sh is the diff-range
// producer the plan verdict (cmd/cigate plan-verdict) consumes; a wrong range yields a plausible, non-empty, WRONG file
// list. The stdin-format contract of its consumer is pinned beside the planner (internal/cigates/affected,
// TestPlanVerdictChangedListFormats).

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

type mergeFixture struct {
	repo                                          *gitRepo
	eventBase, mainTip, feature1, feature2, merge string
}

// buildMergeFixture: eventBase -- mainTip on main, eventBase -- feature1 -- feature2 on the pull request, and merge =
// GitHub's test merge (parents mainTip, feature2), the refs/pull/N/merge checkout of a run whose event base.sha
// (eventBase) is older than the main the merge was built on.
func buildMergeFixture(work string) (*mergeFixture, error) {
	r := newGitRepo(filepath.Join(work, "merge"))
	f := &mergeFixture{repo: r}
	f.eventBase = r.commit("base.txt", "base")
	r.git("branch", "feature")
	f.mainTip = r.commit("main_only.txt", "main moved on")
	r.git("checkout", "-q", "feature")
	f.feature1 = r.commit("feature_change.go", "feature1")
	f.feature2 = r.commit("feature_change2.go", "feature2")
	r.git("checkout", "-q", "--detach", f.mainTip)
	r.git("merge", "-q", "--no-ff", "--no-edit", "-m", "Merge "+f.feature2+" into "+f.mainTip, f.feature2)
	f.merge = r.git("rev-parse", "HEAD")
	return f, r.err
}

// prDiff runs the script as a pull_request step does: BASE_SHA plus whatever event env the cell sets.
func prDiff(script string, r *gitRepo, head string, env ...string) ([]string, string, error) {
	res, err := runDiff(script, r, head, env...)
	if err != nil {
		return nil, "", err
	}
	if res.code != 0 {
		return nil, res.stderr, fmt.Errorf("script exited %d: stdout=%q stderr=%q", res.code, res.stdout, res.stderr)
	}
	var names []string
	for _, p := range strings.Split(res.stdout, "\x00") {
		if p != "" {
			names = append(names, p)
		}
	}
	sort.Strings(names)
	return names, res.stderr, nil
}

func wantRange(stderr, base string) error {
	if want := "diff range: " + base + "...HEAD"; !strings.Contains(stderr, want) {
		return fmt.Errorf("stderr %q does not say %q", stderr, want)
	}
	return nil
}

func runDiff(script string, r *gitRepo, head string, env ...string) (cmdResult, error) {
	r.git("checkout", "-q", head)
	if r.err != nil {
		return cmdResult{}, r.err
	}
	return cmdSpec{dir: r.dir, env: append([]string{shimBasePath()}, env...)}.run("bash", script), nil
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
	withMerge := func(f func(script string, fx *mergeFixture) error) func(string, string) error {
		return func(script, work string) error {
			fx, err := buildMergeFixture(work)
			if err != nil {
				return err
			}
			return f(script, fx)
		}
	}
	return []scenario{
		{"stale_event_base_on_a_merge_ref_judges_against_its_first_parent", withMerge(func(s string, fx *mergeFixture) error {
			if stale := fx.repo.git("diff", "--name-only", fx.eventBase+"..."+fx.merge); !strings.Contains(stale, "main_only.txt") {
				return fmt.Errorf("control: eventBase...merge saw %q, want main_only.txt in it, so this cell measures nothing", stale)
			}
			got, stderr, err := prDiff(s, fx.repo, fx.merge, "BASE_SHA="+fx.eventBase, "GITHUB_EVENT_NAME=pull_request", "PR_HEAD_SHA="+fx.feature2)
			if err := wantNames(got, err, "feature_change.go", "feature_change2.go"); err != nil {
				return err
			}
			return wantRange(stderr, fx.mainTip)
		})},
		{"a_pull_request_checkout_that_is_no_merge_keeps_the_event_base", withFixture(func(s string, fx *diffFixture) error {
			got, _, err := prDiff(s, fx.repo, fx.feature2, "BASE_SHA="+fx.trunk, "GITHUB_EVENT_NAME=pull_request", "PR_HEAD_SHA="+fx.feature2)
			return wantNames(got, err, "feature_change.go", "feature_change2.go")
		})},
		{"a_merge_whose_second_parent_is_not_the_pr_head_keeps_the_event_base", withMerge(func(s string, fx *mergeFixture) error {
			for _, head := range []string{"PR_HEAD_SHA=" + fx.feature1, "PR_HEAD_SHA="} {
				got, stderr, err := prDiff(s, fx.repo, fx.merge, "BASE_SHA="+fx.eventBase, "GITHUB_EVENT_NAME=pull_request", head)
				if err := wantNames(got, err, "feature_change.go", "feature_change2.go", "main_only.txt"); err != nil {
					return fmt.Errorf("%s: %w", head, err)
				}
				if err := wantRange(stderr, fx.eventBase); err != nil {
					return fmt.Errorf("%s: %w", head, err)
				}
			}
			return nil
		})},
		{"a_merge_ref_outside_a_pull_request_event_keeps_the_event_base", withMerge(func(s string, fx *mergeFixture) error {
			for _, event := range []string{"GITHUB_EVENT_NAME=merge_group", "GITHUB_EVENT_NAME=push", "GITHUB_EVENT_NAME="} {
				got, _, err := prDiff(s, fx.repo, fx.merge, "BASE_SHA="+fx.eventBase, event, "PR_HEAD_SHA="+fx.feature2)
				if err := wantNames(got, err, "feature_change.go", "feature_change2.go", "main_only.txt"); err != nil {
					return fmt.Errorf("%s: %w", event, err)
				}
			}
			return nil
		})},
		{"an_event_base_off_the_first_parent_keeps_the_event_base", withMerge(func(s string, fx *mergeFixture) error {
			got, stderr, err := prDiff(s, fx.repo, fx.merge, "BASE_SHA="+fx.feature1, "GITHUB_EVENT_NAME=pull_request", "PR_HEAD_SHA="+fx.feature2)
			if err := wantNames(got, err, "feature_change2.go", "main_only.txt"); err != nil {
				return err
			}
			return wantRange(stderr, fx.feature1)
		})},
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
		p("merge-parent base never taken", `BASE_SHA="$(git rev-parse HEAD^1)"`, `:`,
			"stale_event_base_on_a_merge_ref_judges_against_its_first_parent", "diff produced"),
		p("event guard dropped", `[ "${GITHUB_EVENT_NAME:-}" = pull_request ]`, `true`,
			"a_merge_ref_outside_a_pull_request_event_keeps_the_event_base", "diff produced"),
		p("merge shape not required", `parent2="$(git rev-parse -q --verify 'HEAD^2^{commit}')"`, `parent2="${PR_HEAD_SHA:-}"`,
			"a_pull_request_checkout_that_is_no_merge_keeps_the_event_base", "diff produced"),
		p("second parent not matched to the PR head", `[ "${parent2}" = "${PR_HEAD_SHA:-}" ]`, `true`,
			"a_merge_whose_second_parent_is_not_the_pr_head_keeps_the_event_base", "diff produced"),
		p("first-parent ancestry not required", `git merge-base --is-ancestor "${BASE_SHA}" HEAD^1`, `true`,
			"an_event_base_off_the_first_parent_keeps_the_event_base", "diff produced"),
		p("non-ancestor base not refused", `! git merge-base --is-ancestor "${BASE_SHA}" HEAD 2>/dev/null`, `false`,
			"non_ancestor_base_sha_refuses_rather_than_guessing", "must refuse"),
	})
}

const prHeadExpr = "${{ github.event.pull_request.head.sha }}"

// prHeadWiringDefects: every step that runs ci/go_relevant_diff.sh with the pull_request base sha must hand over the PR
// head. Without PR_HEAD_SHA the script cannot tell GitHub's test merge from any other merge and keeps the event's base
// sha, which can be behind the merge's first parent. n counts the steps found, so a parse that finds none is no pass.
func prHeadWiringDefects(workflows map[string]string) (n int, d []string) {
	for _, name := range []string{".github/workflows/go-quality.yml", ".github/workflows/go.yml", ".github/workflows/venue-oracles.yml"} {
		var doc struct {
			Jobs map[string]struct {
				Steps []struct {
					Name string            `yaml:"name"`
					Env  map[string]string `yaml:"env"`
					Run  string            `yaml:"run"`
				} `yaml:"steps"`
			} `yaml:"jobs"`
		}
		if err := yaml.Unmarshal([]byte(workflows[name]), &doc); err != nil {
			d = append(d, name+" is not valid YAML: "+err.Error())
			continue
		}
		for job, j := range doc.Jobs {
			for _, st := range j.Steps {
				if !strings.Contains(st.Run, "ci/go_relevant_diff.sh") || !strings.Contains(st.Env["BASE_SHA"], "github.event.pull_request.base.sha") {
					continue
				}
				n++
				if got := st.Env["PR_HEAD_SHA"]; got != prHeadExpr {
					d = append(d, fmt.Sprintf("%s job %s step %q: PR_HEAD_SHA is %q, want %q", name, job, st.Name, got, prHeadExpr))
				}
			}
		}
	}
	return n, d
}

func TestGoRelevantDiffPullRequestCallersPassThePRHead(t *testing.T) {
	workflows := map[string]string{}
	for _, name := range []string{".github/workflows/go-quality.yml", ".github/workflows/go.yml", ".github/workflows/venue-oracles.yml"} {
		workflows[name] = readText(t, scriptPath(t, name))
	}
	n, d := prHeadWiringDefects(workflows)
	if n != 3 {
		t.Fatalf("found %d pull_request steps that run ci/go_relevant_diff.sh, want 3 (go.yml integration plan and golden drift, go-quality.yml plan)", n)
	}
	if len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "; "))
	}
	for _, name := range []string{".github/workflows/go-quality.yml", ".github/workflows/go.yml"} {
		name := name
		t.Run("plant/PR_HEAD_SHA dropped from "+name, func(t *testing.T) {
			line := "\n          PR_HEAD_SHA: " + prHeadExpr
			if strings.Count(workflows[name], line) < 1 {
				t.Fatalf("anchor %q is absent from %s: the plant measured nothing", line, name)
			}
			planted := map[string]string{}
			for k, v := range workflows {
				planted[k] = v
			}
			planted[name] = strings.Replace(workflows[name], line, "", 1)
			if _, d := prHeadWiringDefects(planted); len(d) != 1 || !strings.Contains(d[0], "PR_HEAD_SHA is \"\"") {
				t.Fatalf("dropping PR_HEAD_SHA from %s not caught: %q", name, d)
			}
		})
	}
}
