package scripts_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// Two tracked paths that differ only in letter case check out to one file on a case-insensitive filesystem (macOS
// APFS by default): the second write wins and the checkout is dirty, so the deploy repo's verify-pins refuses its ops
// submodule there. Linux CI cannot see it, so this guard does.

func caseOnlyPathGroups(dir string) ([][]string, error) {
	env := append(inheritedEnv([]string{"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"}), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	listed := cmdSpec{dir: dir, env: env}.run("git", "ls-files", "-z")
	if listed.code != 0 {
		return nil, fmt.Errorf("git ls-files: %s", listed.stderr)
	}
	paths := strings.Split(strings.TrimSuffix(listed.stdout, "\x00"), "\x00")
	if len(paths) == 0 || paths[0] == "" {
		return nil, fmt.Errorf("git ls-files listed nothing: the check measured nothing")
	}
	byFolded := map[string][]string{}
	for _, path := range paths {
		folded := strings.ToLower(path)
		byFolded[folded] = append(byFolded[folded], path)
	}
	var groups [][]string
	for _, group := range byFolded {
		if len(group) > 1 {
			sort.Strings(group)
			groups = append(groups, group)
		}
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i][0] < groups[j][0] })
	return groups, nil
}

func TestNoTwoTrackedPathsDifferOnlyInCase(t *testing.T) {
	groups, err := caseOnlyPathGroups(rootOf(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) > 0 {
		lines := make([]string, len(groups))
		for i, group := range groups {
			lines[i] = strings.Join(group, "  ==  ")
		}
		t.Fatalf("these tracked paths differ only in letter case, so a case-insensitive checkout (macOS) keeps one file for each group "+
			"and shows the checkout as modified:\n\n  %s\n\nRename one path of each group to a name that differs in more than case.",
			strings.Join(lines, "\n  "))
	}
}

func TestCaseOnlyPathGuardCatchesPlantedPairs(t *testing.T) {
	tests := []struct {
		name  string
		files []string
		want  [][]string
	}{
		{"distinct names pass", []string{"a/B.md", "a/c.md", "a-b/x.md"}, nil},
		{"file pair", []string{"docs/B.md", "docs/b.md"}, [][]string{{"docs/B.md", "docs/b.md"}}},
		{"extension case only", []string{"docs/UP.MD", "docs/Up.Md", "docs/up.md"}, [][]string{{"docs/UP.MD", "docs/Up.Md", "docs/up.md"}}},
		{"directory case only", []string{"site/A/x.html", "site/a/x.html"}, [][]string{{"site/A/x.html", "site/a/x.html"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newGitRepo(t.TempDir())
			for _, name := range tc.files {
				r.write(name, name+"\n")
			}
			r.git("add", "-A")
			if r.err != nil {
				t.Fatal(r.err)
			}
			got, err := caseOnlyPathGroups(r.dir)
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("groups = %v, want %v", got, tc.want)
			}
		})
	}
}
