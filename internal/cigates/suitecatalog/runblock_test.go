package suitecatalog_test

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The run-block lock: every run step of every workflow and of every action file in the tree must match one reviewed row
// of ci/run_block_lock.tsv. The literal command parser (normalizeAll) is an advisory aid only: it proposes the suites
// column and refuses a "none" row whose text it sees running a catalogued or go test command. The verdict is the table.
// The CI check never writes; `go test ./internal/cigates/suitecatalog -run TestRunBlockLockUpdate -update` regenerates it.

var updateLock = flag.Bool("update", false, "rewrite ci/run_block_lock.tsv from the tree (new rows get an empty reason a reviewer must fill)")

var lockColumns = []string{"file", "job", "step", "shell", "sha256", "suites", "reason"}

type runStep struct {
	file, job, step, shell, sha, text string
}

func normRunText(s string) string { return strings.TrimRight(s, " \t\r\n") + "\n" }

func shaOf(s string) string {
	sum := sha256.Sum256([]byte(normRunText(s)))
	return hex.EncodeToString(sum[:])
}

func docRoot(n *yaml.Node) *yaml.Node {
	if n != nil && n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		return n.Content[0]
	}
	return n
}

func mapGet(n *yaml.Node, key string) *yaml.Node {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}

func scalarOf(n *yaml.Node) string {
	if n == nil {
		return ""
	}
	return n.Value
}

func defaultShell(n *yaml.Node) string {
	return scalarOf(mapGet(mapGet(mapGet(n, "defaults"), "run"), "shell"))
}

// actionFiles walks the whole tree (any depth) for action.yml and action.yaml; .git and node_modules are not walked.
func actionFiles(root string) []string {
	var out []string
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "node_modules") {
			return filepath.SkipDir
		}
		if !d.IsDir() && (d.Name() == "action.yml" || d.Name() == "action.yaml") {
			rel, _ := filepath.Rel(root, p)
			out = append(out, filepath.ToSlash(rel))
		}
		return nil
	})
	return out
}

// scanSteps returns every run step, in file order then document order, plus the local actions it could not follow.
func scanSteps(t *testing.T, root string) ([]runStep, []string) {
	t.Helper()
	var files []string
	wf, _ := filepath.Glob(filepath.Join(root, ".github", "workflows", "*.y*ml"))
	for _, p := range wf {
		rel, _ := filepath.Rel(root, p)
		files = append(files, filepath.ToSlash(rel))
	}
	files = append(files, actionFiles(root)...)
	seen := map[string]bool{}
	var out []runStep
	var bad []string
	for len(files) > 0 {
		rel := files[0]
		files = files[1:]
		if seen[rel] {
			continue
		}
		seen[rel] = true
		raw, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			bad = append(bad, fmt.Sprintf("%s: %v", rel, err))
			continue
		}
		var doc yaml.Node
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		top := docRoot(&doc)
		follow := func(uses string) {
			if !strings.HasPrefix(uses, "./") {
				return
			}
			target := filepath.ToSlash(filepath.Clean(uses))
			for _, cand := range []string{target, target + "/action.yml", target + "/action.yaml"} {
				if st, err := os.Stat(filepath.Join(root, cand)); err == nil && !st.IsDir() {
					files = append(files, cand)
					return
				}
			}
			bad = append(bad, fmt.Sprintf("%s: local action %s is not found", rel, uses))
		}
		emit := func(job string, steps *yaml.Node, jobShell string) {
			if steps == nil {
				return
			}
			for i, st := range steps.Content {
				follow(scalarOf(mapGet(st, "uses")))
				run := mapGet(st, "run")
				if run == nil {
					continue
				}
				key := fmt.Sprintf("#%d", i+1)
				if id := scalarOf(mapGet(st, "id")); id != "" {
					key = "id=" + id
				}
				if name := strings.ReplaceAll(scalarOf(mapGet(st, "name")), "\t", " "); name != "" {
					key += " " + name
				}
				shell := scalarOf(mapGet(st, "shell"))
				if shell == "" {
					shell = jobShell
				}
				if shell == "" {
					shell = "default"
				}
				out = append(out, runStep{rel, job, key, shell, shaOf(run.Value), run.Value})
			}
		}
		if jobs := mapGet(top, "jobs"); jobs != nil {
			wfShell := defaultShell(top)
			for i := 0; i+1 < len(jobs.Content); i += 2 {
				job := jobs.Content[i+1]
				follow(scalarOf(mapGet(job, "uses")))
				shell := defaultShell(job)
				if shell == "" {
					shell = wfShell
				}
				emit(jobs.Content[i].Value, mapGet(job, "steps"), shell)
			}
		}
		emit("-", mapGet(mapGet(top, "runs"), "steps"), "")
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].file < out[j].file })
	return out, bad
}

type lockRow struct{ file, job, step, shell, sha, suites, reason string }

func (r lockRow) key() string { return r.file + "\t" + r.job + "\t" + r.step }

func readLock(root string) ([]lockRow, []string) {
	raw, err := os.ReadFile(filepath.Join(root, lockPath))
	if err != nil {
		return nil, []string{fmt.Sprintf("%s: %v", lockPath, err)}
	}
	var rows []lockRow
	var bad []string
	header := false
	for i, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cells := strings.Split(line, "\t")
		if !header {
			header = true
			if strings.Join(cells, ",") != strings.Join(lockColumns, ",") {
				bad = append(bad, fmt.Sprintf("%s:%d: header must be %s", lockPath, i+1, strings.Join(lockColumns, "<TAB>")))
			}
			continue
		}
		if len(cells) != len(lockColumns) {
			bad = append(bad, fmt.Sprintf("%s:%d: %d cells, want %d", lockPath, i+1, len(cells), len(lockColumns)))
			continue
		}
		rows = append(rows, lockRow{cells[0], cells[1], cells[2], cells[3], cells[4], cells[5], cells[6]})
	}
	return rows, bad
}

// detectedSuites is the advisory parse: the catalog suite ids whose command the step's literal lines run, and whether
// the lines run a go test command at all.
func detectedSuites(text string, byCommand map[string]string) (ids []string, runsTest bool) {
	set := map[string]bool{}
	for _, line := range strings.Split(strings.ReplaceAll(text, "\\\n", " "), "\n") {
		for _, c := range normalizeAll(line) {
			runsTest = true
			if id, ok := byCommand[c]; ok {
				set[id] = true
			}
		}
	}
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, runsTest
}

func lockViolations(t *testing.T, root string, catalog []row) []string {
	t.Helper()
	steps, bad := scanSteps(t, root)
	rows, more := readLock(root)
	bad = append(bad, more...)
	byCommand := map[string]string{}
	known := map[string]bool{}
	for _, r := range catalog {
		byCommand[r["command"]] = r["suite"]
		known[r["suite"]] = true
	}
	table := map[string]lockRow{}
	for _, r := range rows {
		if _, dup := table[r.key()]; dup {
			bad = append(bad, fmt.Sprintf("duplicate row: %s job %s step %q", r.file, r.job, r.step))
		}
		table[r.key()] = r
		if strings.TrimSpace(r.reason) == "" {
			bad = append(bad, fmt.Sprintf("%s job %s step %q: empty reason", r.file, r.job, r.step))
		}
		if r.suites == "" {
			bad = append(bad, fmt.Sprintf("%s job %s step %q: empty suites (use none)", r.file, r.job, r.step))
		}
		if r.suites != "none" {
			for _, id := range strings.Split(r.suites, ",") {
				if !known[id] {
					bad = append(bad, fmt.Sprintf("%s job %s step %q: unknown suite %q", r.file, r.job, r.step, id))
				}
			}
		}
	}
	used := map[string]bool{}
	for _, s := range steps {
		k := s.file + "\t" + s.job + "\t" + s.step
		r, ok := table[k]
		if !ok {
			bad = append(bad, fmt.Sprintf("run step has no row: %s job %s step %q (sha256 %s)", s.file, s.job, s.step, s.sha))
			continue
		}
		used[k] = true
		if r.shell != s.shell {
			bad = append(bad, fmt.Sprintf("shell differs: %s job %s step %q: row %s, step %s", s.file, s.job, s.step, r.shell, s.shell))
		}
		if r.sha != s.sha {
			bad = append(bad, fmt.Sprintf("sha256 mismatch: %s job %s step %q: row %s, text %s", s.file, s.job, s.step, r.sha, s.sha))
		}
		ids, runsTest := detectedSuites(s.text, byCommand)
		if r.suites == "none" && runsTest {
			bad = append(bad, fmt.Sprintf("none row runs a test command: %s job %s step %q", s.file, s.job, s.step))
		}
		have := map[string]bool{}
		for _, id := range strings.Split(r.suites, ",") {
			have[id] = true
		}
		for _, id := range ids {
			if r.suites != "none" && !have[id] {
				bad = append(bad, fmt.Sprintf("row suites lack %s: %s job %s step %q", id, s.file, s.job, s.step))
			}
		}
	}
	for _, r := range rows {
		if !used[r.key()] {
			bad = append(bad, fmt.Sprintf("stale row: %s job %s step %q matches no run step", r.file, r.job, r.step))
		}
	}
	return bad
}

func TestRunBlockLockUpdate(t *testing.T) {
	if !*updateLock {
		t.Skip("run with -update to rewrite " + lockPath)
	}
	root := repoRoot(t)
	catalog, _ := readCatalog(root)
	byCommand := map[string]string{}
	for _, r := range catalog {
		byCommand[r["command"]] = r["suite"]
	}
	steps, _ := scanSteps(t, root)
	old, _ := readLock(root)
	prev := map[string]lockRow{}
	for _, r := range old {
		prev[r.key()] = r
	}
	var b strings.Builder
	b.WriteString("# CHAOS-11422: the run-block lock. Every run step of every workflow and action file has one reviewed row.\n")
	b.WriteString("# sha256 = of the run text with trailing white space stripped and one final newline. suites = catalog suite ids or none.\n")
	b.WriteString("# Regenerate the shas with: go test ./internal/cigates/suitecatalog -run TestRunBlockLockUpdate -update. A new row has an empty reason a reviewer must fill.\n")
	b.WriteString(strings.Join(lockColumns, "\t") + "\n")
	for _, s := range steps {
		suites, reason := "none", ""
		if p, ok := prev[s.file+"\t"+s.job+"\t"+s.step]; ok {
			suites, reason = p.suites, p.reason
		} else if ids, _ := detectedSuites(s.text, byCommand); len(ids) > 0 {
			suites = strings.Join(ids, ",")
		}
		b.WriteString(strings.Join([]string{s.file, s.job, s.step, s.shell, s.sha, suites, reason}, "\t") + "\n")
	}
	if err := os.WriteFile(filepath.Join(root, lockPath), []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}
