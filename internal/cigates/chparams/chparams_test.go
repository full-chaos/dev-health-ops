// Package chparams guards ClickHouse query parameter names. ClickHouse Cloud parses a query parameter whose name equals
// a server setting name as that setting, so a parameter named limit fails with code 26 ("Cannot parse quoted string: expected
// opening quote"). A parameter is never named after a setting.
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

// settingNames are server setting names that are plausible parameter names. The parameter name to use instead is the
// setting name plus a suffix describing the value, for example limit_n and offset_n.
var settingNames = map[string]bool{
	"database":           true,
	"format":             true,
	"limit":              true,
	"max_execution_time": true,
	"max_memory_usage":   true,
	"max_result_rows":    true,
	"max_rows_to_read":   true,
	"max_threads":        true,
	"offset":             true,
	"priority":           true,
	"readonly":           true,
	"timeout":            true,
	"timeout_before_checking_execution_speed": true,
	"user": true,
}

// placeholderRe is a ClickHouse query parameter `{name:Type}`; the type starts with an upper-case letter, which tells it
// apart from a Go composite literal such as `{limit: 20}` or `{limit:5}`.
var placeholderRe = regexp.MustCompile(`\{([A-Za-z_][A-Za-z0-9_]*):([A-Z][A-Za-z0-9]*(?:\([^{}()]*(?:\([^{}()]*\))?[^{}()]*\))?)\}`)

func badPlaceholders(text string) []string {
	var bad []string
	for _, m := range placeholderRe.FindAllStringSubmatch(text, -1) {
		if settingNames[strings.ToLower(m[1])] {
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

func trackedGoFiles(t *testing.T, root string) []string {
	t.Helper()
	cmd := exec.Command("git", "-C", root, "ls-files", "-z", "--", "*.go")
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
	root := repoRoot(t)
	files := trackedGoFiles(t, root)
	if len(files) < 100 {
		t.Fatalf("only %d tracked Go files found: the guard would scan nothing", len(files))
	}
	var problems []string
	for _, rel := range files {
		body, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range badPlaceholders(string(body)) {
			problems = append(problems, rel+": "+p)
		}
	}
	sort.Strings(problems)
	for _, p := range problems {
		t.Errorf("%s names a ClickHouse query parameter after a server setting; ClickHouse Cloud parses it as the setting (code 26). Rename it, for example limit_n", p)
	}
}

func TestPlaceholderScannerFlagsSettingNames(t *testing.T) {
	text := "LIMIT {" + "limit:UInt64} OFFSET {" + "offset:UInt32} AND x = {" + "database:String} AND y IN {" + "User:Array(String)}"
	if got := strings.Join(badPlaceholders(text), " "); got != "{limit:UInt64} {offset:UInt32} {database:String} {User:Array(String)}" {
		t.Fatalf("flagged %q", got)
	}
	clean := "LIMIT {limit_n:UInt64} OFFSET {offset_n:UInt32} AND org_id = {org_id:String} AND t = {ts:DateTime64(3, 'UTC')}"
	if got := badPlaceholders(clean); len(got) != 0 {
		t.Fatalf("flagged clean placeholders %v", got)
	}
	if got := badPlaceholders("shadowLedger{limit: 1000}, x{limit:5}"); len(got) != 0 {
		t.Fatalf("flagged a Go composite literal %v", got)
	}
}
