package workflows_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Behaviour of the docker-images fan-in job's tag-application step (`_inspect_classify`), run under bash with a recording
// `docker` shim standing in for the registry and a `git` shim that records `log` and `merge-base --is-ancestor` separately
// (testdata/fanin_*_shim.sh). A swapped rc==1/rc==2 mapping, an inverted ancestry check or a deleted `::error::` decline
// leaves every source-text assertion green; these cases observe what the step DOES.

const faninLabelledRevision = "baseshabaseshabaseshabaseshabaseshabase"

type faninOpts struct {
	ancestor         bool
	candidates       []string
	matchShort       string
	sourceTag        string
	overrideFamily   string
	overrideScenario string
}

type faninResult struct {
	record    string
	gitLog    bool
	mergeBase bool
	stdout    string
	code      int
}

func faninLatestTagScript(t *testing.T, root string) string {
	t.Helper()
	var matches []string
	for name, job := range jobsOf(loadWf(t, root, "docker-images.yml")) {
		if !strings.Contains(name, "fan-in") && !strings.Contains(name, "fan_in") {
			continue
		}
		for _, s := range stepsOf(job) {
			if run := asString(s["run"]); strings.Contains(run, "_inspect_classify") {
				matches = append(matches, run)
			}
		}
	}
	if len(matches) != 1 {
		t.Fatalf("expected exactly one fan-in step whose run text contains _inspect_classify, found %d", len(matches))
	}
	return matches[0]
}

func runFaninStep(t *testing.T, scenario string, o faninOpts) faninResult {
	t.Helper()
	root := rootOf(t)
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, src := range map[string]string{"docker": "fanin_docker_shim.sh", "git": "fanin_git_shim.sh"} {
		body := readFile(t, filepath.Join(root, "internal", "cigates", "workflows", "testdata", src))
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bin, "sleep"), []byte("#!/usr/bin/env bash\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(dir, "fanin.sh")
	if err := os.WriteFile(script, []byte(faninLatestTagScript(t, root)), 0o644); err != nil {
		t.Fatal(err)
	}
	record, gitLog, mergeBase := filepath.Join(dir, "record.txt"), filepath.Join(dir, "git_log_called.txt"), filepath.Join(dir, "git_merge_base_called.txt")
	if err := os.WriteFile(record, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	ancestor := "false"
	if o.ancestor {
		ancestor = "true"
	}
	env := inheritedEnv(nil,
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"BUILT_SHA=deadbeef1234567890deadbeef1234567890dead0", "OWNER=full-chaos", "IS_RELEASE=false", "IS_MAIN_REF=true",
		"REF_IS_BRANCH=false", "BRANCH_TAG=", "SCENARIO="+scenario, "RECORD_FILE="+record,
		"GIT_LOG_CALLED_FILE="+gitLog, "GIT_MERGE_BASE_CALLED_FILE="+mergeBase, "LABELLED_REVISION="+faninLabelledRevision,
		"MERGE_BASE_IS_ANCESTOR="+ancestor, "DIGEST_WALK_CANDIDATES="+strings.Join(o.candidates, "\n"),
		"DIGEST_WALK_MATCH_SHORT="+o.matchShort, "SOURCE_TAG_SCENARIO="+o.sourceTag,
		"SOURCE_TAG_OVERRIDE_FAMILY="+o.overrideFamily, "SOURCE_TAG_OVERRIDE_SCENARIO="+o.overrideScenario)
	res := cmdSpec{dir: dir, env: env}.run("bash", script)
	exists := func(p string) bool { _, err := os.Stat(p); return err == nil }
	return faninResult{record: readFile(t, record), gitLog: exists(gitLog), mergeBase: exists(mergeBase), stdout: res.stdout, code: res.code}
}

func faninWant(t *testing.T, ok bool, format string, a ...any) {
	t.Helper()
	if !ok {
		t.Fatalf(format, a...)
	}
}

const faninNoOp = "0 of 7 families had a moving tag applied this run"

func TestFanInLatestCheckBootstrapsOnlyOnConfirmedAbsence(t *testing.T) {
	absent := runFaninStep(t, "absent", faninOpts{})
	faninWant(t, absent.code == 0 && strings.Contains(absent.record, "imagetools create") && strings.Contains(absent.record, ":latest"),
		"CONFIRMED ABSENT must record a bootstrap `imagetools create ... :latest` (exit %d): %q", absent.code, absent.record)
	faninWant(t, !absent.gitLog && !absent.mergeBase, "CONFIRMED ABSENT must short-circuit to bootstrap without git")

	unknown := runFaninStep(t, "unknown", faninOpts{})
	faninWant(t, unknown.code != 0 && unknown.record == "", "UNKNOWN must fail the step and tag nothing (exit %d): %q", unknown.code, unknown.record)
	faninWant(t, !unknown.gitLog && !unknown.mergeBase, "UNKNOWN must decline without git")

	empty := runFaninStep(t, "empty_success", faninOpts{})
	faninWant(t, empty.code != 0 && empty.record == "", "an empty :latest read must fail closed (exit %d): %q", empty.code, empty.record)
	faninWant(t, !empty.gitLog && !empty.mergeBase, "an empty :latest read must decline without git")
	faninWant(t, strings.Contains(empty.stdout, "::error::"), "an empty :latest read emits no ::error:: annotation: %q", empty.stdout)

	found := runFaninStep(t, "unlabelled", faninOpts{})
	faninWant(t, found.gitLog && !found.mergeBase, "unlabelled must reach the digest walk, never the ancestry check")
	faninWant(t, found.code == 1, "no digest-walk match for every family must fail the run, got exit %d:\n%s", found.code, found.stdout)
	faninWant(t, strings.Contains(found.stdout, "WARNING: ") && strings.Contains(found.stdout, "has no revision label on :latest"),
		"the per-family decline must keep its WARNING line: %q", found.stdout)
}

func TestFanInLatestCheckTagsOnlyTrueDescendants(t *testing.T) {
	d := runFaninStep(t, "labelled", faninOpts{ancestor: true})
	faninWant(t, d.code == 0 && d.mergeBase && !d.gitLog, "the descendant case must reach the ancestry check, not the digest walk (exit %d)", d.code)
	faninWant(t, strings.Contains(d.record, "imagetools create") && strings.Contains(d.record, ":latest"), "a true descendant must be tagged: %q", d.record)
	n := runFaninStep(t, "labelled", faninOpts{ancestor: false})
	faninWant(t, n.code == 0 && n.mergeBase && !n.gitLog, "the non-descendant case must reach the ancestry check (exit %d)", n.code)
	faninWant(t, n.record == "", "a non-descendant build must decline, never overwrite a newer latest: %q", n.record)
}

func TestFanInDigestWalkFindsAMatchPartwayThroughHistory(t *testing.T) {
	r := runFaninStep(t, "unlabelled", faninOpts{
		candidates: []string{"1111111111111111111111111111111111111a", "2222222222222222222222222222222222222b", "3333333333333333333333333333333333333c"},
		matchShort: "2222222",
	})
	faninWant(t, r.code == 0 && r.gitLog && !r.mergeBase, "the digest walk must be reached, not the ancestry check (exit %d)", r.code)
	faninWant(t, strings.Contains(r.record, "imagetools create") && strings.Contains(r.record, ":latest"), "a walk with a match must tag latest: %q / %q", r.record, r.stdout)
	faninWant(t, strings.Contains(r.stdout, "2222222222222222222222222222222222222b"), "the fallback log line must name the matched candidate: %q", r.stdout)
}

func TestFanInSourceTagDistinguishesConfirmedAbsentFromUnknown(t *testing.T) {
	a := runFaninStep(t, "labelled", faninOpts{sourceTag: "absent"})
	faninWant(t, a.record == "" && !a.gitLog && !a.mergeBase, "a confirmed-absent source tag must tag nothing and skip git: %q", a.record)
	faninWant(t, strings.Contains(a.stdout, "SKIP") && strings.Contains(a.stdout, "confirmed absent") && !strings.Contains(a.stdout, "::error::"),
		"confirmed absent must be a SILENT skip naming the absence: %q", a.stdout)
	u := runFaninStep(t, "labelled", faninOpts{sourceTag: "unknown"})
	faninWant(t, u.code != 0 && u.record == "" && !u.gitLog && !u.mergeBase, "an unknown source tag must fail the step and tag nothing (exit %d)", u.code)
	faninWant(t, strings.Contains(u.stdout, "::error::") && strings.Contains(u.stdout, "could not confirm whether"), "an unknown source tag must emit ::error::: %q", u.stdout)
}

func TestFanInNoOpRunsFailTheJob(t *testing.T) {
	for _, c := range []struct {
		name     string
		scenario string
		o        faninOpts
		git      bool
		extra    string
	}{
		{"every source tag unknown", "labelled", faninOpts{sourceTag: "unknown"}, false, ""},
		{"a mix of unknown and confirmed absent with none found", "labelled", faninOpts{sourceTag: "unknown", overrideFamily: "dev-health-go-migrate", overrideScenario: "absent"}, false, "SKIP dev-health-go-migrate"},
		{"every latest read unknown", "unknown", faninOpts{}, false, ""},
		{"every family's digest walk exhausted", "unlabelled", faninOpts{}, true, ""},
		{"every family's platform labels disagree", "disagree", faninOpts{}, false, "platform labels disagree"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := runFaninStep(t, c.scenario, c.o)
			faninWant(t, r.code == 1, "expected exit 1, got %d:\n%s", r.code, r.stdout)
			faninWant(t, r.record == "", "the failing run still recorded an `imagetools create`: %q", r.record)
			faninWant(t, r.gitLog == c.git && !r.mergeBase, "git use: log=%v mergeBase=%v, want log=%v and no merge-base", r.gitLog, r.mergeBase, c.git)
			faninWant(t, strings.Contains(r.stdout, faninNoOp), "the job-level annotation %q is missing:\n%s", faninNoOp, r.stdout)
			if c.extra != "" {
				faninWant(t, strings.Contains(r.stdout, c.extra), "expected %q in the output:\n%s", c.extra, r.stdout)
			}
		})
	}
}
