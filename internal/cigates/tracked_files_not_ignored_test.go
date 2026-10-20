package cigates_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// CHAOS-10174 PR C: Go port of tests/tooling/test_tracked_files_are_not_ignored.py. A tracked file that matches one of
// the repository's OWN ignore rules is invisible to rg and every gitignore-aware walker while git still tracks it
// (a bare `dho workers` rule once hid internal/workersctl/). `git check-ignore` skips indexed files unless --no-index,
// so the obvious diagnostic hides the defect. Only repository-owned .gitignore files count: .git/info/exclude and the
// global ignore are per-machine. Negations (`!pattern`) are matches meaning "not ignored".

func repoIgnoredTrackedFiles(dir string) ([]string, error) {
	env := append(inheritedEnv([]string{"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"}), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	listed := cmdSpec{dir: dir, env: env}.run("git", "ls-files", "-z")
	if listed.code != 0 {
		return nil, fmt.Errorf("git ls-files: %s", listed.stderr)
	}
	// check-ignore exits 1 when nothing matches: that is a valid empty answer, anything above 1 is a failure
	explained := cmdSpec{dir: dir, env: env, stdin: []byte(listed.stdout)}.run("git", "check-ignore", "-v", "--no-index", "-z", "--stdin")
	if explained.code > 1 || explained.code < 0 {
		return nil, fmt.Errorf("git check-ignore exited %d: %s", explained.code, explained.stderr)
	}
	if strings.Count(listed.stdout, "\x00") == 0 {
		return nil, fmt.Errorf("git ls-files listed nothing: the check measured nothing")
	}
	fields := strings.Split(explained.stdout, "\x00")
	var offenders []string
	for i := 0; i+3 < len(fields); i += 4 {
		source, pattern, path := fields[i], fields[i+2], fields[i+3]
		if strings.HasPrefix(pattern, "!") {
			continue
		}
		if source == ".gitignore" || strings.HasSuffix(source, "/.gitignore") {
			offenders = append(offenders, fmt.Sprintf("%s: %s  ->  %s", source, pattern, path))
		}
	}
	sort.Strings(offenders)
	return offenders, nil
}

func TestNoTrackedFileMatchesARepositoryIgnoreRule(t *testing.T) {
	offenders, err := repoIgnoredTrackedFiles(rootOf(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(offenders) > 0 {
		t.Fatalf("these tracked files match one of the repository's own ignore rules, so rg and every gitignore-aware tool skips them "+
			"while git still tracks them:\n\n  %s\n\nFix the RULE, not the file: anchor a bare name that matches a directory (/dho workers); "+
			"negate beside a broad content glob (!path/to/tracked); rewrite a trailing-slash directory rule as dir/** before negating.",
			strings.Join(offenders, "\n  "))
	}
}

func TestTrackedFilesGuardCatchesPlantedDefects(t *testing.T) {
	fixture := func(t *testing.T, files map[string]string, ignoreFiles map[string]string, infoExclude string) string {
		t.Helper()
		r := newGitRepo(t.TempDir())
		for name, body := range files {
			r.write(name, body)
		}
		r.git("add", "-A")
		// ignore files are written AFTER the files are indexed: git tracks the file, the rule then matches it
		for name, body := range ignoreFiles {
			r.write(name, body)
		}
		if infoExclude != "" {
			if err := os.WriteFile(filepath.Join(r.dir, ".git", "info", "exclude"), []byte(infoExclude), 0o644); err != nil && r.err == nil {
				r.err = err
			}
		}
		if r.err != nil {
			t.Fatal(r.err)
		}
		return r.dir
	}
	tests := []struct {
		name  string
		files map[string]string
		ign   map[string]string
		excl  string
		want  []string
	}{
		{"bare name rule hides a tracked directory", map[string]string{"internal/workersctl/a.go": "package a\n"},
			map[string]string{".gitignore": "workersctl\n"}, "",
			[]string{".gitignore: workersctl  ->  internal/workersctl/a.go"}},
		{"nested gitignore counts", map[string]string{"sub/keep.txt": "x\n"},
			map[string]string{"sub/.gitignore": "keep.txt\n"}, "",
			[]string{"sub/.gitignore: keep.txt  ->  sub/keep.txt"}},
		{"negation re-includes the tracked file", map[string]string{"keep/b.go": "package b\n"},
			map[string]string{".gitignore": "*.go\n!keep/*.go\n"}, "", nil},
		{"per-clone exclude is not a repository rule", map[string]string{"a/b.txt": "x\n"},
			nil, "a/\n", nil},
		{"anchored rule hides only its target", map[string]string{"dho": "bin\n", "internal/workersctl/a.go": "package a\n"},
			map[string]string{".gitignore": "/dho\n"}, "",
			[]string{".gitignore: /dho  ->  dho"}},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			dir := fixture(t, tc.files, tc.ign, tc.excl)
			got, err := repoIgnoredTrackedFiles(dir)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(got, "|") != strings.Join(tc.want, "|") {
				t.Fatalf("offenders %q, want %q", got, tc.want)
			}
		})
	}
}
