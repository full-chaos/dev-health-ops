package ratchet

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func run(t *testing.T, repo string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", repo}, args...)...)
	command.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.test", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.test", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	out, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func write(t *testing.T, repo, path, text string) {
	t.Helper()
	full := filepath.Join(repo, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

const listPath = "lists/closed.txt"

func list(entries ...string) string {
	return "# a closed list\n" + strings.Join(entries, "\n") + "\n"
}

// newRepo is a repository on main holding one closed list of five entries and a manifest that names it.
func newRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	run(t, repo, "init", "-q", "-b", "main")
	write(t, repo, ManifestPath, "# test\nclosed\t"+listPath+"\tfor the test\n")
	write(t, repo, listPath, list("a", "b", "c", "d", "e"))
	run(t, repo, "add", "-A")
	run(t, repo, "commit", "-q", "-m", "base")
	return repo
}

func checkNow(t *testing.T, repo, base string) (Result, string, error) {
	t.Helper()
	entries, err := ReadManifest(repo)
	if err != nil {
		t.Fatal(err)
	}
	return Check(repo, base, entries[0])
}

func TestCountIgnoresCommentsAndBlankLines(t *testing.T) {
	if got := Count([]byte("# c\n\n a \n\t\n#x\nb\n")); got != 2 {
		t.Fatalf("Count = %d, want 2", got)
	}
}

func TestAListThatShrinksOrStaysPassesAndOneThatGrowsFails(t *testing.T) {
	repo := newRepo(t)
	base := run(t, repo, "rev-parse", "HEAD")
	for _, row := range []struct {
		name    string
		entries []string
		grew    bool
	}{
		{"unchanged", []string{"a", "b", "c", "d", "e"}, false},
		{"one entry removed", []string{"a", "b", "c", "d"}, false},
		{"emptied but for one", []string{"a"}, false},
		{"one entry added", []string{"a", "b", "c", "d", "e", "f"}, true},
		{"one removed and one added elsewhere is still the same size", []string{"a", "b", "c", "d", "z"}, false},
		{"two added, one removed", []string{"a", "b", "c", "d", "e", "x", "y"}, true},
	} {
		t.Run(row.name, func(t *testing.T) {
			write(t, repo, listPath, list(row.entries...))
			result, grew, err := checkNow(t, repo, base)
			if err != nil {
				t.Fatal(err)
			}
			if (grew != "") != row.grew {
				t.Fatalf("base %d now %d: grew=%q, want grew=%v", result.BaseCount, result.NowCount, grew, row.grew)
			}
			if row.grew && !strings.Contains(grew, "only shrinks") {
				t.Fatalf("the failure text %q does not say what is wrong", grew)
			}
		})
	}
}

// The failures that must be loud: a measurement that did not happen is never a pass.
func TestABaseThatCannotBeReadFails(t *testing.T) {
	repo := newRepo(t)
	t.Run("a base that is not a commit", func(t *testing.T) {
		if _, err := Resolve(repo, strings.Repeat("0", 40)); err == nil || !strings.Contains(err.Error(), "not a commit") {
			t.Fatalf("Resolve = %v, want an error naming the missing commit", err)
		}
	})
	t.Run("no base given and no origin/main", func(t *testing.T) {
		if _, err := Resolve(repo, ""); err == nil || !strings.Contains(err.Error(), "RATCHET_BASE_SHA") {
			t.Fatalf("Resolve = %v, want an error that says how to give the base", err)
		}
	})
	t.Run("the list is not in the base commit", func(t *testing.T) {
		write(t, repo, "lists/other.txt", list("a"))
		run(t, repo, "add", "-A")
		run(t, repo, "commit", "-q", "-m", "second list")
		entries := []Entry{{Name: "other", Path: "lists/other.txt"}}
		base := run(t, repo, "rev-parse", "HEAD~1")
		if _, _, err := Check(repo, base, entries[0]); err == nil || !strings.Contains(err.Error(), "cannot read") {
			t.Fatalf("Check = %v, want a failure for a list the base does not hold", err)
		}
	})
	t.Run("the list is missing in the working tree", func(t *testing.T) {
		base := run(t, repo, "rev-parse", "HEAD")
		if err := os.Remove(filepath.Join(repo, filepath.FromSlash(listPath))); err != nil {
			t.Fatal(err)
		}
		if _, _, err := checkNow(t, repo, base); err == nil {
			t.Fatal("a deleted list passed")
		}
	})
	t.Run("the list is empty at the base", func(t *testing.T) {
		empty := newRepo(t)
		write(t, empty, listPath, "# nothing\n")
		run(t, empty, "add", "-A")
		run(t, empty, "commit", "-q", "-m", "empty")
		if _, _, err := checkNow(t, empty, run(t, empty, "rev-parse", "HEAD")); err == nil {
			t.Fatal("an empty list at the base passed")
		}
	})
	t.Run("an empty or malformed manifest", func(t *testing.T) {
		bad := newRepo(t)
		for _, text := range []string{"", "# only a comment\n", "name-only\n", "a\tb\n", "a\tb\tc\na\td\te\n"} {
			write(t, bad, ManifestPath, text)
			if _, err := ReadManifest(bad); err == nil {
				t.Fatalf("manifest %q was accepted", text)
			}
		}
	})
}

func TestResolveWithoutABaseUsesTheMergeBaseOnABranchAndTheParentOnMain(t *testing.T) {
	repo := newRepo(t)
	first := run(t, repo, "rev-parse", "HEAD")
	run(t, repo, "update-ref", "refs/remotes/origin/main", first)
	run(t, repo, "checkout", "-q", "-b", "topic")
	write(t, repo, listPath, list("a", "b"))
	run(t, repo, "commit", "-q", "-am", "shrink")
	got, err := Resolve(repo, "")
	if err != nil || got != first {
		t.Fatalf("on a branch Resolve = %q, %v, want the merge base %s", got, err, first)
	}
	run(t, repo, "checkout", "-q", "main")
	write(t, repo, listPath, list("a", "b", "c"))
	run(t, repo, "commit", "-q", "-am", "main moves")
	run(t, repo, "update-ref", "refs/remotes/origin/main", "HEAD")
	got, err = Resolve(repo, "")
	if err != nil || got != first {
		t.Fatalf("on main Resolve = %q, %v, want the parent %s", got, err, first)
	}
}

// The reason the package exists: two pull requests that each remove a different entry of the same list merge one after
// the other with no conflict, and the list is still held to the base. The control is the old shape, a number beside the
// code that each pull request lowers: the second merge conflicts.
func TestTwoPullRequestsThatLowerTheSameListMergeWithoutAConflict(t *testing.T) {
	repo := newRepo(t)
	write(t, repo, "const.txt", "const ceiling = 5\n")
	run(t, repo, "add", "-A")
	run(t, repo, "commit", "-q", "-m", "the old shape: a constant")
	base := run(t, repo, "rev-parse", "HEAD")

	branch := func(name, removed string, entries ...string) {
		run(t, repo, "checkout", "-q", "-b", name, base)
		write(t, repo, listPath, list(entries...))
		// Each pull request writes the number it believes in: the one that removes two entries writes a smaller one.
		write(t, repo, "const.txt", "const ceiling = "+string(rune('0'+len(entries)))+"\n")
		run(t, repo, "commit", "-q", "-am", name+" removes "+removed)
	}
	branch("pr-a", "b", "a", "c", "d", "e")
	branch("pr-b", "d and e", "a", "b", "c")

	run(t, repo, "checkout", "-q", "-B", "list-only", base)
	run(t, repo, "checkout", "-q", "pr-a", "--", listPath)
	run(t, repo, "commit", "-q", "-am", "pr-a list part")
	run(t, repo, "checkout", "-q", "-B", "list-only-b", base)
	run(t, repo, "checkout", "-q", "pr-b", "--", listPath)
	run(t, repo, "commit", "-q", "-am", "pr-b list part")

	run(t, repo, "checkout", "-q", "-B", "main2", base)
	run(t, repo, "merge", "-q", "--no-edit", "list-only")
	run(t, repo, "merge", "-q", "--no-edit", "list-only-b") // no conflict: the lists differ on different lines
	result, grew, err := checkNow(t, repo, base)
	if err != nil || grew != "" || result.BaseCount != 5 || result.NowCount != 2 {
		t.Fatalf("after both merges: base %d now %d grew=%q err=%v, want 5 -> 2 and ok", result.BaseCount, result.NowCount, grew, err)
	}

	// The control: the same two pull requests with their constant edit conflict on the second merge.
	run(t, repo, "checkout", "-q", "-B", "main3", base)
	run(t, repo, "merge", "-q", "--no-edit", "pr-a")
	command := exec.Command("git", "-C", repo, "merge", "--no-edit", "pr-b")
	command.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.test", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.test", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null")
	if out, err := command.CombinedOutput(); err == nil {
		t.Fatalf("the control merge did not conflict, so the test does not show what it claims:\n%s", out)
	}
}

func TestAPullRequestThatRaisesTheListIsRedAfterTheMerge(t *testing.T) {
	repo := newRepo(t)
	base := run(t, repo, "rev-parse", "HEAD")
	run(t, repo, "checkout", "-q", "-b", "raise")
	write(t, repo, listPath, list("a", "b", "c", "d", "e", "f"))
	run(t, repo, "commit", "-q", "-am", "raise")
	if _, grew, err := checkNow(t, repo, base); err != nil || grew == "" {
		t.Fatalf("a list one entry longer than on the base: grew=%q err=%v, want a failure text", grew, err)
	}
}
