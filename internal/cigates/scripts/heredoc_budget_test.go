package scripts_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// A here-document is written into a pipe the writing shell also holds the read end of; at or above this size it was measured to block forever on small-pipe hosts.
const heredocBudgetBytes = 400

var heredocStart = regexp.MustCompile(`<<-?\s*[A-Za-z_"']`)

var heredocDelimiter = regexp.MustCompile(`^<<-?\s*(["']?)([A-Za-z_][A-Za-z0-9_]*)(["']?)`)

type heredoc struct {
	line  int
	delim string
	bytes int
}

func scanHeredocs(text string) ([]heredoc, error) {
	lines := strings.Split(text, "\n")
	var found []heredoc
	for i := 0; i < len(lines); {
		line := lines[i]
		if strings.HasPrefix(strings.TrimLeft(line, " \t"), "#") {
			i++
			continue
		}
		delims := heredocDelims(line)
		j := i + 1
		for _, delim := range delims {
			size := 0
			for j < len(lines) && strings.TrimSpace(lines[j]) != delim {
				size += len(lines[j]) + 1
				j++
			}
			if j >= len(lines) {
				return nil, fmt.Errorf("unterminated here-document <<%s opened at line %d", delim, i+1)
			}
			found = append(found, heredoc{line: i + 1, delim: delim, bytes: size})
			j++
		}
		i = j
	}
	return found, nil
}

// heredocDelims returns every "<<[-]DELIM" delimiter on the line, in order; here-strings are skipped. Bash reads the bodies one after another.
func heredocDelims(line string) []string {
	var out []string
	for _, idx := range heredocStart.FindAllStringIndex(line, -1) {
		if idx[0] > 0 && line[idx[0]-1] == '<' {
			continue
		}
		if m := heredocDelimiter.FindStringSubmatch(line[idx[0]:]); m != nil && m[1] == m[3] {
			out = append(out, m[2])
		}
	}
	return out
}

func oversized(t *testing.T, name, text string) []string {
	t.Helper()
	found, err := scanHeredocs(text)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	var bad []string
	for _, h := range found {
		if h.bytes >= heredocBudgetBytes {
			bad = append(bad, fmt.Sprintf("%s:%d <<%s %d bytes", name, h.line, h.delim, h.bytes))
		}
	}
	return bad
}

func TestHeredocScannerFlagsOversizedAndIgnoresHereStringsAndComments(t *testing.T) {
	planted := "cat <<'EOF'\n" + strings.Repeat("x", heredocBudgetBytes) + "\nEOF\n"
	found, err := scanHeredocs(planted)
	if err != nil || len(found) != 1 || found[0].line != 1 || found[0].delim != "EOF" || found[0].bytes <= heredocBudgetBytes {
		t.Fatalf("planted oversized here-document not flagged: %+v err=%v", found, err)
	}
	small := "cat <<-\"EOF\"\nhello\n\tEOF\n"
	if f, err := scanHeredocs(small); err != nil || len(f) != 1 || f[0].bytes >= heredocBudgetBytes {
		t.Fatalf("small here-document mis-sized: %+v err=%v", f, err)
	}
	for _, s := range []string{
		"read -r a b c <<<\"${target}\"\n",
		"  # Using `cat >file <<EOF` would reintroduce the pipe\n",
	} {
		if f, err := scanHeredocs(s); err != nil || len(f) != 0 {
			t.Fatalf("%q must yield no here-document: %+v err=%v", s, f, err)
		}
	}
	if _, err := scanHeredocs("cat <<EOF\nnever closed\n"); err == nil {
		t.Fatal("unterminated here-document must be an error, not silence")
	}
}

func trackedCIScripts(t *testing.T, root string) []string {
	t.Helper()
	cmd := exec.Command("git", "ls-files", "-z", "--", "ci/*.sh")
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files failed: %v: %s", err, stderr.String())
	}
	var names []string
	for _, n := range strings.Split(string(out), "\x00") {
		if n != "" {
			names = append(names, n)
		}
	}
	return names
}

func hasExecutableStatement(text string) bool {
	for _, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.Fields(line)[0] == "set" {
			continue
		}
		return true
	}
	return false
}

func TestNoCIScriptHasHeredocOverBudget(t *testing.T) {
	root := repoRoot(t)
	names := trackedCIScripts(t, root)
	have := map[string]bool{}
	for _, n := range names {
		have[filepath.Base(n)] = true
	}
	for _, req := range []string{
		"check_go.sh", "check_go_containers.sh", "check_river_compat_static.sh",
		"local_validate.sh", "run_live_backend_e2e.sh", "run_tests.sh",
	} {
		if !have[req] {
			t.Fatalf("scan found no %s among %d tracked ci/*.sh; a scan that reads nothing looks clean", req, len(names))
		}
	}
	var bad []string
	for _, n := range names {
		b, err := os.ReadFile(filepath.Join(root, n))
		if err != nil {
			t.Fatal(err)
		}
		if !hasExecutableStatement(string(b)) {
			t.Fatalf("%s has no executable statement", n)
		}
		bad = append(bad, oversized(t, n, string(b))...)
	}
	if len(bad) > 0 {
		t.Fatalf("here-document(s) at or above %d bytes: %v; write the payload to a file with the printf builtin instead", heredocBudgetBytes, bad)
	}
}

func TestHeredocBudgetFailsOnOversizedHeredocPlantedInRunTests(t *testing.T) {
	root := repoRoot(t)
	b, err := os.ReadFile(filepath.Join(root, "ci", "run_tests.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if bad := oversized(t, "ci/run_tests.sh", string(b)); len(bad) != 0 {
		t.Fatalf("baseline ci/run_tests.sh already oversized: %v", bad)
	}
	planted := string(b) + "\ncat <<'PLANTED'\n" + strings.Repeat("y", 400) + "\nPLANTED\n"
	if bad := oversized(t, "ci/run_tests.sh", planted); len(bad) != 1 {
		t.Fatalf("a 401-byte here-document planted in ci/run_tests.sh must fail the guard, got %v", bad)
	}
}

func TestHeredocScannerSizesEveryHeredocOnOneCommandLine(t *testing.T) {
	text := "cat <<A <<'B'\nsmall\nA\n" + strings.Repeat("z", heredocBudgetBytes) + "\nB\necho after\n"
	found, err := scanHeredocs(text)
	if err != nil || len(found) != 2 || found[0].delim != "A" || found[1].delim != "B" || found[1].bytes <= heredocBudgetBytes || found[0].bytes >= heredocBudgetBytes {
		t.Fatalf("second heredoc on the same line not sized: %+v err=%v", found, err)
	}
}
