package local

import (
	"go/parser"
	"go/token"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/passwordwriters"
)

func TestRaisesTokenVersionReadsTheSetClauseOnly(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want bool
	}{
		{"set raises", "UPDATE users SET password_hash = $2, token_version = token_version + 1 WHERE id = $1", true},
		{"set raises, no where", "UPDATE users SET password_hash = $2, token_version = token_version + 1", true},
		{"token_version only in where", "UPDATE users SET password_hash = $2 WHERE id = $1 AND token_version = $3", false},
		{"token_version only in returning", "UPDATE users SET password_hash = $2 WHERE id = $1 RETURNING token_version = 1", false},
		{"no token_version", "UPDATE users SET password_hash = $2 WHERE id = $1", false},
	}
	for _, c := range cases {
		if got := raisesTokenVersion(c.sql); got != c.want {
			t.Errorf("%s: raisesTokenVersion(%q) = %v, want %v", c.name, c.sql, got, c.want)
		}
	}
}

func TestCensusReadsASplitLiteralAsOneStatement(t *testing.T) {
	const src = "package x\n" +
		"var a = \"UPDATE users SET \" + \"password_hash = $2\"\n" +
		"var b = \"UPDATE users SET password_hash = $2, \" + \"token_version = token_version + 1 WHERE id = $1\"\n" +
		"var c = prefix + \"UPDATE users SET password_hash = $2 WHERE id = $1\"\n"
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "x.go", src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	sites := passwordwriters.FileSites(fset, file, "x.go")
	if len(sites) != 3 {
		t.Fatalf("sites = %d, want 3: %+v", len(sites), sites)
	}
	if raisesTokenVersion(sites[0].SQL) {
		t.Errorf("split literal without token_version passed: %q", sites[0].SQL)
	}
	if !raisesTokenVersion(sites[1].SQL) {
		t.Errorf("split literal with token_version in SET failed: %q", sites[1].SQL)
	}
	if raisesTokenVersion(sites[2].SQL) {
		t.Errorf("runtime-prefixed writer without token_version passed: %q", sites[2].SQL)
	}
}
