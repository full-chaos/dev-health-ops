// Package chparams guards ClickHouse query parameter names. ClickHouse Cloud parses a query parameter whose name equals
// a server setting name as that setting, so a parameter named limit fails with code 26 ("Cannot parse quoted string:
// expected opening quote"). A parameter is never named after a setting. The setting names are pinned in
// testdata/cloud-setting-names.txt, read from a Cloud service; it is the source of truth until it is refreshed.
package chparams_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// placeholderRe is a ClickHouse query parameter `{name:Type}`; the type starts with an upper-case letter, which tells it
// apart from a Go composite literal such as `{limit: 20}` or `{limit:5}`.
var placeholderRe = regexp.MustCompile(`\{([A-Za-z_][A-Za-z0-9_]*):([A-Z][A-Za-z0-9]*(?:\([^{}()]*(?:\([^{}()]*\))?[^{}()]*\))?)\}`)

func loadSettingNames(t *testing.T) map[string]bool {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", "cloud-setting-names.txt"))
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		names[line] = true
	}
	if len(names) < 2000 || !names["limit"] || !names["offset"] || !names["max_threads"] {
		t.Fatalf("setting names fixture holds %d names (limit=%v offset=%v max_threads=%v): it is truncated or the wrong file", len(names), names["limit"], names["offset"], names["max_threads"])
	}
	return names
}

func badPlaceholders(text string, settings map[string]bool) []string {
	var bad []string
	for _, m := range placeholderRe.FindAllStringSubmatch(text, -1) {
		if settings[m[1]] {
			bad = append(bad, m[0])
		}
	}
	return bad
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func trackedFiles(t *testing.T, root string, pathspecs ...string) []string {
	t.Helper()
	args := append([]string{"-C", root, "ls-files", "-z", "--"}, pathspecs...)
	cmd := exec.Command("git", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files in %s: %v: %s", root, err, stderr.String())
	}
	var files []string
	for _, rel := range strings.Split(string(out), "\x00") {
		if rel == "" {
			continue
		}
		if _, err := os.Stat(filepath.Join(root, rel)); err != nil {
			continue
		}
		files = append(files, rel)
	}
	return files
}

func TestNoClickHouseParameterIsNamedAfterASetting(t *testing.T) {
	settings := loadSettingNames(t)
	root := repoRoot(t)
	goFiles := trackedFiles(t, root, "*.go")
	sqlFiles := trackedFiles(t, root, "*.sql")
	if len(goFiles) < 100 || len(sqlFiles) < 10 {
		t.Fatalf("found %d tracked Go files and %d SQL files: the guard would scan too little", len(goFiles), len(sqlFiles))
	}
	var problems []string
	for _, rel := range append(goFiles, sqlFiles...) {
		body, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range badPlaceholders(string(body), settings) {
			problems = append(problems, rel+": "+p)
		}
	}
	sort.Strings(problems)
	for _, p := range problems {
		t.Errorf("%s names a ClickHouse query parameter after a server setting; ClickHouse Cloud parses it as the setting (code 26). Rename it with a suffix, for example limit_n", p)
	}
}

func TestPlaceholderScannerFlagsSettingNames(t *testing.T) {
	settings := loadSettingNames(t)
	planted := []string{"{" + "limit:UInt64}", "{" + "offset:UInt32}", "{" + "readonly:String}", "{" + "max_threads:Array(String)}"}
	text := "LIMIT " + planted[0] + " OFFSET " + planted[1] + " AND x = " + planted[2] + " AND y IN " + planted[3]
	if got := badPlaceholders(text, settings); strings.Join(got, " ") != strings.Join(planted, " ") {
		t.Fatalf("flagged %q, want %q", got, planted)
	}
	clean := "LIMIT {limit_n:UInt64} OFFSET {offset_n:UInt32} AND org_id = {org_id:String} AND t = {ts:DateTime64(3, 'UTC')}"
	if got := badPlaceholders(clean, settings); len(got) != 0 {
		t.Fatalf("flagged clean placeholders %v", got)
	}
	if got := badPlaceholders("shadowLedger{limit: 1000}, x{limit:5}", settings); len(got) != 0 {
		t.Fatalf("flagged a Go composite literal %v", got)
	}
}
