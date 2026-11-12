package retiredcredentialnames

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// plantedRepo is a temporary git repository holding files; the ones in track are staged (tracked), the rest are left
// untracked.
func plantedRepo(t *testing.T, files map[string]string, track ...string) string {
	t.Helper()
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-c", "maintenance.auto=false", "-c", "gc.auto=0", "-C", dir}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	run("init", "-q")
	for rel, body := range files {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if len(track) > 0 {
		run(append([]string{"add", "--"}, track...)...)
	}
	return dir
}

func joined(hits []hit) string {
	lines := make([]string, len(hits))
	for i, h := range hits {
		lines[i] = h.String()
	}
	return strings.Join(lines, "\n")
}

func TestCensusFindsEachPlantedReference(t *testing.T) {
	for _, c := range []struct {
		name, path, body, want string
	}{
		{"acr prefix in a test", "internal/x/x_test.go", "tok := \"svc_acr_\" + body\n", "internal/x/x_test.go:1: svc_acr"},
		{"session issuer prefix in a script", "scripts/x.sh", "#!/bin/sh\nTOKEN=svc_authsess_abc\n", "scripts/x.sh:2: svc_authsess"},
		{"worker prefix in docs", "docs/x.md", "Bearer svc_worker_abc\n", "docs/x.md:1: svc_worker"},
		{"session issue token env in a chart", "deploy/x.yaml", "- name: AUTH_SESSION_ISSUE_TOKEN\n", "deploy/x.yaml:1: session_issue_token"},
		{"legacy ingest setting", "deploy/y.yaml", "- name: DEV_HEALTH_LEGACY_INGEST_ENABLED\n", "deploy/y.yaml:1: legacy_ingest"},
		{"legacy ingest import", "cmd/x/main.go", "import \"x/internal/api/legacyingest\"\n", "cmd/x/main.go:1: legacy_ingest"},
		{"acr class id", "contracts/x.json", "{\"class_id\": \"internal_svc_acr_token\"}\n", "contracts/x.json:1: class_acr"},
		{"session issuer class id", "contracts/y.json", "[\"internal_svc_authsess_token\"]\n", "contracts/y.json:1: class_authsess"},
		{"worker class id", "contracts/z.json", "[\"internal_svc_worker_token\"]\n", "contracts/z.json:1: class_worker"},
		{"name in a path", "internal/api/legacyingest/doc.go", "package legacyingest\n", "internal/api/legacyingest/doc.go:path: legacy_ingest"},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := plantedRepo(t, map[string]string{c.path: c.body}, c.path)
			if got := joined(census(t, dir, nil)); got != c.want {
				t.Errorf("census = %q, want %q", got, c.want)
			}
		})
	}
}

// A class id holds the prefix text after an underscore, so it is the class id name only, never the prefix name too.
func TestAClassIDIsNotThePrefix(t *testing.T) {
	dir := plantedRepo(t, map[string]string{"c.json": "\"internal_svc_worker_token\"\n"}, "c.json")
	if got, want := joined(census(t, dir, nil)), "c.json:1: class_worker"; got != want {
		t.Errorf("census = %q, want %q", got, want)
	}
}

func TestCensusSkipsAllowedUntrackedAndWaitingNames(t *testing.T) {
	files := map[string]string{
		"docs/allowed.md":   "the retired svc_acr_ credential\n",
		"docs/untracked.md": "svc_worker_ in a file git does not track\n",
		"docs/waiting.md":   "JWT_SECRET_KEY and legacy_hs256 wait for the HS256 retirement\n",
	}
	dir := plantedRepo(t, files, "docs/allowed.md", "docs/waiting.md")
	allow := map[string]allowance{"docs/allowed.md": {[]string{nameSvcAcr}, history}}
	if got := census(t, dir, allow); len(got) != 0 {
		t.Errorf("census = %q, want no hit", joined(got))
	}
}

// An allowlist entry permits only its own names: a second retired name in the same file is a hit.
func TestAnEntryPermitsOnlyItsNames(t *testing.T) {
	dir := plantedRepo(t, map[string]string{"d.md": "svc_acr_ and AUTH_SESSION_ISSUE_TOKEN\n"}, "d.md")
	allow := map[string]allowance{"d.md": {[]string{nameSvcAcr}, history}}
	if got, want := joined(census(t, dir, allow)), "d.md:1: session_issue_token"; got != want {
		t.Errorf("census = %q, want %q", got, want)
	}
}

func TestStaleEntriesAreNamed(t *testing.T) {
	dir := plantedRepo(t, map[string]string{"gone.md": "nothing retired here\n", "odd.md": "svc_acr_\n"}, "gone.md", "odd.md")
	allow := map[string]allowance{
		"gone.md":    {[]string{nameSvcAcr}, history},
		"odd.md":     {[]string{nameSvcAcr}, "later"},
		"missing.md": {[]string{nameSvcAcr}, sweep},
		"unknown.md": {[]string{"no_such_name"}, history},
	}
	got := strings.Join(staleEntries(t, dir, allow), "\n")
	for _, want := range []string{
		"gone.md: no longer holds the svc_acr name",
		"missing.md: not a tracked file",
		"odd.md: stage \"later\"",
		"unknown.md: not a tracked file",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("stale entries %q do not hold %q", got, want)
		}
	}
}
