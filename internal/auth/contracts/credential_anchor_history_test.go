//go:build authhistory

package contracts

import (
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var historyOld = regexp.MustCompile(`^(src/dev_health_ops/[\w/]+\.py)@([0-9a-f]{12})\^:(\d+)(?:-(\d+))?$`)

// The retired-anchor evidence is checked against git, not against itself: the row set equals the Python anchors of the
// registry at the base commit the evidence names, each commit is the one that deleted its file, each cited range fits
// the file at that commit's parent, and each old_text is that line. Needs full history (-tags authhistory).
func TestRetiredAnchorEvidenceMatchesGitHistory(t *testing.T) {
	checkRetiredEvidenceAgainstHistory(t)
}

func checkRetiredEvidenceAgainstHistory(t *testing.T) {
	root := testRoot(t)
	git := func(args ...string) (string, error) {
		out, err := exec.Command("git", append([]string{"-c", "maintenance.auto=false", "-c", "gc.auto=0", "-C", root}, args...)...).Output()
		return string(out), err
	}
	if out, err := git("rev-parse", "--is-shallow-repository"); err != nil || strings.TrimSpace(out) != "false" {
		t.Fatalf("the retired-anchor history check needs full git history (shallow=%q, err=%v)", strings.TrimSpace(out), err)
	}
	rows, base := loadRetiredEvidence(t)
	baseDoc, err := git("show", base+":contracts/auth/v1/credential-classes.json")
	if err != nil {
		t.Fatalf("the registry at base %s does not resolve: %v", base, err)
	}
	refs, err := collectCredentialAnchorRefs([]byte(baseDoc))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for _, ref := range refs {
		if ref.repo == "ops" && strings.HasPrefix(ref.path, "src/dev_health_ops/") && strings.HasSuffix(ref.path, ".py") {
			want[fmt.Sprintf("%s\t%s\t%s:%d-%d", ref.classID, ref.where, ref.path, ref.line, ref.end)] = true
		}
	}
	if len(want) == 0 {
		t.Fatalf("the registry at base %s cites no Python file: the check would measure nothing", base)
	}
	var failures []string
	got := map[string]bool{}
	deletedBy := map[string]string{}
	sources := map[string][]string{}
	for _, r := range rows {
		m := historyOld.FindStringSubmatch(r.old)
		if m == nil {
			failures = append(failures, fmt.Sprintf("%s %s: old %q is not path@commit12^:line[-end]", r.classID, r.where, r.old))
			continue
		}
		path, commit := m[1], m[2]
		line, _ := strconv.Atoi(m[3])
		end := 0
		if m[4] != "" {
			end, _ = strconv.Atoi(m[4])
		}
		key := fmt.Sprintf("%s\t%s\t%s:%d-%d", r.classID, r.where, path, line, end)
		if got[key] {
			failures = append(failures, "listed twice: "+key)
		}
		got[key] = true
		if _, ok := deletedBy[path]; !ok {
			out, _ := git("log", base, "-1", "--format=%H", "--diff-filter=D", "--", path)
			deletedBy[path] = strings.TrimSpace(out)
		}
		if d := deletedBy[path]; d == "" || !strings.HasPrefix(d, commit) {
			failures = append(failures, fmt.Sprintf("%s %s: %s was deleted by %q, not by %s", r.classID, r.where, path, d, commit))
			continue
		}
		src, ok := sources[commit+":"+path]
		if !ok {
			out, err := git("show", commit+"^:"+path)
			if err != nil {
				failures = append(failures, fmt.Sprintf("%s %s: %s^:%s does not resolve", r.classID, r.where, commit, path))
				continue
			}
			src = strings.Split(out, "\n")
			sources[commit+":"+path] = src
		}
		if line < 1 || line > len(src) || (end != 0 && (end < line || end > len(src))) {
			failures = append(failures, fmt.Sprintf("%s %s: the range %d-%d does not fit %s (%d lines)", r.classID, r.where, line, end, path, len(src)))
			continue
		}
		if text := strings.ReplaceAll(strings.TrimSpace(src[line-1]), "\t", " "); text != r.oldText {
			failures = append(failures, fmt.Sprintf("%s %s: old_text %q, but %s^:%s:%d reads %q", r.classID, r.where, r.oldText, commit, path, line, text))
		}
	}
	for key := range want {
		if !got[key] {
			failures = append(failures, "the base registry cites a Python line the evidence lacks: "+key)
		}
	}
	for key := range got {
		if !want[key] {
			failures = append(failures, "the evidence lists a row the base registry does not cite: "+key)
		}
	}
	sort.Strings(failures)
	for _, f := range failures {
		t.Error(f)
	}
	t.Logf("retired-anchor evidence: %d rows checked against %s", len(rows), base)
}
