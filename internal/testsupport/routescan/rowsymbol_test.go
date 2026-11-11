package routescan

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const rowFixture = `package p

const (
	PatternCreate = "/internal/v1/x/create"
	PatternRead   = "/internal/v1/x/read"
)

var PatternTwice = "/internal/v1/x/twice"

const PatternTwiceAgain = "/internal/v1/x/twice"

func Routes() []Route {
	return []Route{
		{Method: http.MethodPost, Pattern: PatternCreate, Handler: h},
		{Method: http.MethodGet, Pattern: PatternJWKS, Handler: h},
		{Method: http.MethodGet, Pattern: "/dup", Handler: h},
		{Method: http.MethodGet, Pattern: "/dup", Handler: h},
	}
}
`

func rowRegs(t *testing.T) ([]Registration, []byte, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "routes.go")
	if err := os.WriteFile(path, []byte(rowFixture), 0o644); err != nil {
		t.Fatal(err)
	}
	regs, err := ScanWithMux(dir)
	if err != nil {
		t.Fatal(err)
	}
	return regs, []byte(rowFixture), filepath.ToSlash(path)
}

func TestRowSymbolPrefersTheRegistrationThenTheConstant(t *testing.T) {
	regs, src, file := rowRegs(t)
	for _, tc := range []struct {
		name, method, route, ident string
		line                       int
	}{
		{"registration through a same-package constant", "POST", "/internal/v1/x/create", "", 14},
		{"registration through another package's constant", "GET", "/.well-known/jwks.json", "PatternJWKS", 15},
		{"constant only (the registration is elsewhere)", "POST", "/internal/v1/x/read", "", 5},
	} {
		if at, err := RowSymbol(regs, src, file, tc.method, tc.route, tc.ident); err != nil || at != tc.line {
			t.Errorf("%s: line %d, %v; want %d", tc.name, at, err, tc.line)
		}
	}
	for _, tc := range []struct {
		name, method, route, ident, want string
	}{
		{"nothing names the route", "POST", "/internal/v1/x/gone", "", "source anchor stale: "},
		{"wrong method, no constant", "DELETE", "/dup", "", "source anchor stale: "},
		{"an unknown ident", "GET", "/.well-known/jwks.json", "PatternOther", "source anchor stale: "},
		{"two registrations", "GET", "/dup", "", "source anchor ambiguous: " + file + " holds 2 registrations"},
		{"two constants, no registration", "POST", "/internal/v1/x/twice", "", "holds 2 constants"},
	} {
		if _, err := RowSymbol(regs, src, file, tc.method, tc.route, tc.ident); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want %q", tc.name, err, tc.want)
		}
	}
	if _, err := RowSymbol(nil, []byte("package p\nconst ("), file, "GET", "/x", ""); err == nil || !strings.Contains(err.Error(), "does not parse") {
		t.Errorf("a source that does not parse: %v", err)
	}
}

func TestScanNamesAnIdentifierPattern(t *testing.T) {
	regs, _, _ := rowRegs(t)
	var idents []string
	for _, reg := range regs {
		if reg.PatternIdent != "" {
			idents = append(idents, reg.PatternIdent+"="+reg.Pattern)
		}
	}
	if strings.Join(idents, ",") != "PatternCreate=/internal/v1/x/create,PatternJWKS=" {
		t.Errorf("identifier patterns: %v", idents)
	}
}
