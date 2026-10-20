//go:build !race

package scripts_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ci/merge-tree-vet.sh catches what a textually clean merge hides: #3044 added `Peek` to httpapi.CounterStore while
// #3041 added a test stub without it; each was green on its own base and `git merge-tree` said clean, yet main went red.
// The script builds the merge tree of <base> and <head> in a scratch directory and runs `check_go.sh vet`,
// `check_go.sh integration-vet` and the venue registry check on it, printing one OK/FAIL line. Each case runs the real
// script on a scratch git repository holding a tiny Go module and the real ci scripts, and each guard is proved against a
// planted defect: the same repository with the defect absent passes.

var mergeTreeVetCIFiles = []string{
	"ci/check_go.sh", "ci/check_venue_oracle_registry.sh", "ci/venue_oracle_discovery.awk", "ci/venue_oracle_names.awk",
	"ci/venue_oracle_proof.awk", "ci/lib/venue_oracle_registry.sh", "ci/merge-tree-vet.sh",
}

const (
	mtvStore     = "package store\n\ntype Store interface{ Get() int }\n\ntype Real struct{}\n\nfunc (Real) Get() int { return 1 }\n"
	mtvStoreTest = "package store\n\nimport \"testing\"\n\ntype stub struct{}\n\nfunc (stub) Get() int { return 0 }\n\nfunc TestStub(t *testing.T) { var _ Store = stub{} }\n"
	mtvStoreV2   = "package store\n\ntype Store interface {\n\tGet() int\n\tPeek() int\n}\n\ntype Real struct{}\n\nfunc (Real) Get() int  { return 1 }\nfunc (Real) Peek() int { return 1 }\n"
	mtvVenueTest = "package venue\n\nimport \"testing\"\n\n// A compiling stand-in for the harness import: the registry check reads the\n// call, the scratch module has no harness package to import.\nvar venueoracle = struct{ WriteProof func(*testing.T) }{func(*testing.T) {}}\n\nfunc TestVenueOracleOne(t *testing.T) { venueoracle.WriteProof(t) }\n"
	mtvRegistry  = "internal/venue\tTestVenueOracleOne\trun\n"
	mtvSecond    = "package venue\n\nimport \"testing\"\n\nfunc TestVenueOracleTwo(t *testing.T) {}\n"
)

type mtvEnv struct {
	repo    *gitRepo
	root    string
	tmp     string
	goCache string
}

func newMTV(t *testing.T) *mtvEnv {
	t.Helper()
	root := rootOf(t)
	base := t.TempDir()
	e := &mtvEnv{repo: newGitRepo(filepath.Join(base, "repo")), root: root, tmp: filepath.Join(base, "tmp"), goCache: filepath.Join(base, "go-cache")}
	for _, d := range []string{e.tmp, e.goCache} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	e.repo.git("branch", "-m", "main0")
	e.repo.write("go.mod", "module example.com/x\n\ngo 1.24\n")
	e.repo.write("internal/store/store.go", mtvStore)
	e.repo.write("internal/store/store_test.go", mtvStoreTest)
	e.repo.write("internal/venue/venue_test.go", mtvVenueTest)
	e.repo.write("ci/venue_oracle_registry.d/internal__venue.tsv", mtvRegistry)
	for _, f := range mergeTreeVetCIFiles {
		e.repo.write(f, readText(t, filepath.Join(root, filepath.FromSlash(f))))
		if err := os.Chmod(filepath.Join(e.repo.dir, filepath.FromSlash(f)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	e.commit("base")
	return e
}

func (e *mtvEnv) commit(msg string) {
	e.repo.git("add", "-A")
	e.repo.git("commit", "-q", "-m", msg)
}

func (e *mtvEnv) branch(name string, start ...string) {
	args := append([]string{"checkout", "-q", "-b", name}, start...)
	e.repo.git(args...)
}

func (e *mtvEnv) makeMainAndPRs() {
	e.branch("main1")
	e.repo.write("internal/store/store.go", mtvStoreV2)
	e.repo.write("internal/store/store_test.go", strings.Replace(mtvStoreTest, "func (stub) Get() int { return 0 }", "func (stub) Get() int  { return 0 }\nfunc (stub) Peek() int { return 0 }", 1))
	e.commit("main gains Peek")
	e.branch("pr-clean", "main0")
	e.repo.write("internal/other/other.go", "package other\n")
	e.commit("unrelated")
	e.branch("pr-stub", "main0")
	e.repo.write("internal/store/extra_test.go", "package store\n\nimport \"testing\"\n\ntype stub2 struct{}\n\nfunc (stub2) Get() int { return 2 }\n\nfunc TestStub2(t *testing.T) { var _ Store = stub2{} }\n")
	e.commit("adds a stub that only knows Get")
	e.branch("pr-tagged", "main0")
	e.repo.write("internal/store/integ_test.go", "//go:build integration\n\npackage store\n\nvar _ = undefinedUnderTheTag\n")
	e.commit("a break only the integration tag compiles")
	e.branch("pr-unregistered", "main0")
	e.repo.write("internal/venue/second_test.go", mtvSecond)
	e.commit("a venue test with no registry row")
	e.branch("pr-conflict", "main0")
	e.repo.write("internal/store/store.go", strings.Replace(mtvStore, "return 1", "return 7", 1))
	e.commit("edits the line main also edited")
}

func (e *mtvEnv) run(t *testing.T, args ...string) cmdResult {
	t.Helper()
	if e.repo.err != nil {
		t.Fatal(e.repo.err)
	}
	env := inheritedEnv(nil, "TMPDIR="+e.tmp, "DEV_HEALTH_GO_CACHE="+e.goCache, "GIT_CONFIG_GLOBAL="+os.DevNull)
	return freeSpec{dir: e.repo.dir, env: env}.run("bash", append([]string{filepath.Join(e.repo.dir, "ci", "merge-tree-vet.sh")}, args...)...)
}

func (e *mtvEnv) leftovers() []string {
	m, _ := filepath.Glob(filepath.Join(e.tmp, "merge-tree-vet.*"))
	return m
}

func outAll(r cmdResult) string { return r.stdout + r.stderr }

func TestMergeTreeVetScript(t *testing.T) {
	t.Parallel()
	must := func(t *testing.T, cond bool, r cmdResult, what string) {
		t.Helper()
		if !cond {
			t.Fatalf("%s (exit %d):\n%s", what, r.code, outAll(r))
		}
	}
	has := func(s string, subs ...string) bool {
		for _, x := range subs {
			if !strings.Contains(s, x) {
				return false
			}
		}
		return true
	}

	t.Run("a clean pr is ok", func(t *testing.T) {
		t.Parallel()
		e := newMTV(t)
		e.makeMainAndPRs()
		r := e.run(t, "--base", "main1", "--head", "pr-clean")
		must(t, r.code == 0 && has(r.stdout, "PASS vet", "PASS integration-vet", "PASS registry", "merge-tree-vet: OK pr=-"), r, "clean pr")
		if l := e.leftovers(); len(l) != 0 {
			t.Errorf("the scratch tree was not removed: %v", l)
		}
	})
	t.Run("the incident shape merges clean but fails vet", func(t *testing.T) {
		t.Parallel()
		e := newMTV(t)
		e.makeMainAndPRs()
		textual := freeSpec{dir: e.repo.dir, env: gitEnv}.run("git", "merge-tree", "--write-tree", "main1", "pr-stub")
		if textual.code != 0 {
			t.Fatalf("the incident shape must merge textually clean: %s", outAll(textual))
		}
		r := e.run(t, "--base", "main1", "--head", "pr-stub")
		must(t, r.code == 1 && has(r.stderr, "FAIL vet", "stub2", "Peek") && has(r.stdout, "merge-tree-vet: FAIL"), r, "incident shape")
		ok := e.run(t, "--base", "main1", "--head", "pr-clean")
		must(t, ok.code == 0, ok, "the same repository without the defect")
		if l := e.leftovers(); len(l) != 0 {
			t.Errorf("leftovers: %v", l)
		}
	})
	t.Run("a break only the integration tag sees fails integration-vet", func(t *testing.T) {
		t.Parallel()
		e := newMTV(t)
		e.makeMainAndPRs()
		r := e.run(t, "--base", "main1", "--head", "pr-tagged")
		must(t, r.code == 1 && has(r.stdout, "PASS vet", "PASS registry") && has(r.stderr, "FAIL integration-vet"), r, "tagged break")
	})
	t.Run("an unregistered venue test fails the registry stage", func(t *testing.T) {
		t.Parallel()
		e := newMTV(t)
		e.makeMainAndPRs()
		r := e.run(t, "--base", "main1", "--head", "pr-unregistered")
		must(t, r.code == 1 && has(r.stdout, "PASS vet") && has(r.stderr, "FAIL registry", "TestVenueOracleTwo"), r, "unregistered venue test")
	})
	t.Run("a textual conflict fails at merge", func(t *testing.T) {
		t.Parallel()
		e := newMTV(t)
		e.makeMainAndPRs()
		r := e.run(t, "--base", "main1", "--head", "pr-conflict")
		must(t, r.code == 1 && has(r.stderr, "textual conflict") && has(r.stdout, "merge-tree-vet: FAIL"), r, "conflict")
		if l := e.leftovers(); len(l) != 0 {
			t.Errorf("leftovers: %v", l)
		}
	})
	t.Run("every stage runs even after one fails", func(t *testing.T) {
		t.Parallel()
		e := newMTV(t)
		e.makeMainAndPRs()
		e.branch("pr-both", "pr-stub")
		e.repo.write("internal/venue/second_test.go", mtvSecond)
		e.commit("and an unregistered venue test")
		r := e.run(t, "--base", "main1", "--head", "pr-both")
		must(t, r.code == 1 && has(r.stderr, "FAIL vet", "FAIL registry"), r, "a later stage must still run after vet failed")
	})
	t.Run("a missing stage script is a failure not a skip", func(t *testing.T) {
		t.Parallel()
		e := newMTV(t)
		e.makeMainAndPRs()
		e.branch("pr-no-registry-script", "main0")
		e.repo.git("rm", "-q", "ci/check_venue_oracle_registry.sh")
		e.repo.git("commit", "-q", "-m", "delete the registry script")
		r := e.run(t, "--base", "main1", "--head", "pr-no-registry-script")
		must(t, r.code == 1 && has(r.stderr, "check_venue_oracle_registry.sh is missing"), r, "missing stage script")
	})
	for i, args := range [][]string{{}, {"--head"}, {"abc"}, {"--head", "nope-not-a-rev", "--base", "main0"}, {"1", "2"}} {
		t.Run(fmt.Sprintf("usage error %d exits two", i), func(t *testing.T) {
			t.Parallel()
			e := newMTV(t)
			r := e.run(t, args...)
			must(t, r.code == 2, r, fmt.Sprint(args))
		})
	}
	t.Run("the invoking checkout is left untouched", func(t *testing.T) {
		t.Parallel()
		e := newMTV(t)
		e.makeMainAndPRs()
		e.repo.git("checkout", "-q", "main0")
		before, headBefore := e.repo.git("status", "--porcelain"), e.repo.git("rev-parse", "HEAD")
		r := e.run(t, "--base", "main1", "--head", "pr-clean")
		must(t, r.code == 0, r, "clean pr")
		if e.repo.git("status", "--porcelain") != before || e.repo.git("rev-parse", "HEAD") != headBefore {
			t.Error("the invoking checkout changed")
		}
	})
}

// Where the Go build cache and the scratch tree go. A /tmp Go cache once reached 33.5 GB on a shared host. The stages are
// stubbed (a check_go.sh that records what it was given), so these cases need no Go build.

const probeCheckGo = "#!/usr/bin/env bash\nprintf '%s\\n%s\\n' \"${DEV_HEALTH_GO_CACHE-}\" \"$(pwd -P)\" >> \"${PROBE_FILE}\"\nexit 0\n"

func (e *mtvEnv) probe(t *testing.T, tmpRoot string, probeEnv map[string]string, unset ...string) (cmdResult, []string) {
	t.Helper()
	e.branch("probe")
	e.repo.write("ci/check_go.sh", probeCheckGo)
	e.commit("stub check_go.sh")
	probe := filepath.Join(tmpRoot, "probe.txt")
	env := inheritedEnv(unset, "PROBE_FILE="+probe, "GIT_CONFIG_GLOBAL="+os.DevNull)
	for k, v := range probeEnv {
		env = append(env, k+"="+v)
	}
	r := freeSpec{dir: e.repo.dir, env: env}.run("bash", filepath.Join(e.repo.dir, "ci", "merge-tree-vet.sh"), "--base", "main0", "--head", "probe")
	if r.code != 0 {
		t.Fatalf("probe run failed (exit %d): %s", r.code, outAll(r))
	}
	return r, strings.Split(strings.TrimRight(readText(t, probe), "\n"), "\n")
}

func fakeGoBin(t *testing.T, dir, body string) string {
	t.Helper()
	bin := filepath.Join(dir, "fakebin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin + string(os.PathListSeparator) + os.Getenv("PATH")
}

func TestMergeTreeVetGoCacheAndScratchPlacement(t *testing.T) {
	t.Parallel()
	t.Run("the default go cache is the invoker's GOCACHE not /tmp", func(t *testing.T) {
		t.Parallel()
		e := newMTV(t)
		dir := t.TempDir()
		_, seen := e.probe(t, dir, map[string]string{"TMPDIR": e.tmp, "GOCACHE": filepath.Join(dir, "mine")}, "DEV_HEALTH_GO_CACHE")
		if seen[0] != filepath.Join(dir, "mine") {
			t.Errorf("go cache %q", seen[0])
		}
		if _, err := os.Stat(filepath.Join(e.tmp, "merge-tree-vet-gocache")); err == nil {
			t.Error("a /tmp go cache was created")
		}
	})
	t.Run("the default go cache falls back to go env GOCACHE", func(t *testing.T) {
		t.Parallel()
		e := newMTV(t)
		dir := t.TempDir()
		path := fakeGoBin(t, dir, "#!/usr/bin/env bash\n[ \"$*\" = \"env GOCACHE\" ] && echo "+filepath.Join(dir, "goenv")+"\n")
		_, seen := e.probe(t, dir, map[string]string{"TMPDIR": e.tmp, "PATH": path}, "DEV_HEALTH_GO_CACHE", "GOCACHE")
		if seen[0] != filepath.Join(dir, "goenv") {
			t.Errorf("go cache %q", seen[0])
		}
	})
	t.Run("an explicit DEV_HEALTH_GO_CACHE beats GOCACHE", func(t *testing.T) {
		t.Parallel()
		e := newMTV(t)
		dir := t.TempDir()
		_, seen := e.probe(t, dir, map[string]string{"DEV_HEALTH_GO_CACHE": filepath.Join(dir, "explicit"), "GOCACHE": filepath.Join(dir, "mine")})
		if seen[0] != filepath.Join(dir, "explicit") {
			t.Errorf("go cache %q", seen[0])
		}
	})
	t.Run("the scratch root hosts the go cache only when go names none", func(t *testing.T) {
		t.Parallel()
		e := newMTV(t)
		dir := t.TempDir()
		scratch := filepath.Join(dir, "scratch")
		path := fakeGoBin(t, dir, "#!/usr/bin/env bash\nexit 1\n")
		_, seen := e.probe(t, dir, map[string]string{"DEV_HEALTH_SCRATCH": scratch, "PATH": path}, "DEV_HEALTH_GO_CACHE", "GOCACHE")
		if seen[0] != filepath.Join(scratch, "merge-tree-vet-gocache") {
			t.Errorf("go cache %q", seen[0])
		}
	})
	t.Run("the scratch tree honours DEV_HEALTH_SCRATCH over TMPDIR", func(t *testing.T) {
		t.Parallel()
		e := newMTV(t)
		dir := t.TempDir()
		scratch := filepath.Join(dir, "lane-scratch", "nested")
		_, seen := e.probe(t, dir, map[string]string{"TMPDIR": e.tmp, "DEV_HEALTH_SCRATCH": scratch})
		resolved, err := filepath.EvalSymlinks(filepath.Dir(scratch))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(seen[1], filepath.Join(resolved, "nested")+string(os.PathSeparator)+"merge-tree-vet.") {
			t.Errorf("scratch tree %q is not under %s", seen[1], scratch)
		}
		if l := e.leftovers(); len(l) != 0 {
			t.Errorf("leftovers in TMPDIR: %v", l)
		}
		if m, _ := filepath.Glob(filepath.Join(scratch, "merge-tree-vet.*")); len(m) != 0 {
			t.Errorf("the scratch tree was not removed on exit: %v", m)
		}
	})
}
