package docstools_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckValidatedSite(t *testing.T) {
	const rev = "0123456789abcdef0123456789abcdef01234567"
	setup := func(t *testing.T, index bool, stamp *string) (site, stampPath string) {
		t.Helper()
		dir := t.TempDir()
		site, stampPath = filepath.Join(dir, "docs"), filepath.Join(dir, "docs-validated.sha")
		if err := os.MkdirAll(site, 0o755); err != nil {
			t.Fatal(err)
		}
		if index {
			if err := os.WriteFile(filepath.Join(site, "index.html"), []byte("<html></html>"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if stamp != nil {
			if err := os.WriteFile(stampPath, []byte(*stamp), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return site, stampPath
	}
	str := func(s string) *string { return &s }
	cases := []struct {
		name     string
		index    bool
		stamp    *string
		revision string
		code     int
		want     string
	}{
		{"same revision", true, str(rev + "\n"), rev, 0, "is for revision " + rev},
		{"another revision", true, str("deadbeef\n"), rev, 1, `is for revision "deadbeef", not`},
		{"no stamp", true, nil, rev, 1, "no validated-site stamp"},
		{"empty stamp", true, str(""), rev, 1, `is for revision ""`},
		{"no built site", false, str(rev), rev, 1, "no built site"},
		{"unknown revision", true, str("unknown"), "unknown", 1, "no source revision"},
		{"empty revision", true, str(""), "", 2, "expected one argument"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			site, stamp := setup(t, c.index, c.stamp)
			code, stdout, stderr := runDocs("check-validated-site", "--site-dir", site, "--stamp", stamp, "--revision", c.revision)
			if code != c.code {
				t.Fatalf("exit %d, want %d (stdout %q stderr %q)", code, c.code, stdout, stderr)
			}
			if !strings.Contains(stdout+stderr, c.want) {
				t.Fatalf("output %q does not mention %q", stdout+stderr, c.want)
			}
		})
	}
	t.Run("missing flag", func(t *testing.T) {
		if code, _, _ := runDocs("check-validated-site", "--site-dir", t.TempDir()); code != 2 {
			t.Fatalf("exit %d, want 2", code)
		}
	})
}
