package scripts_test

import "testing"

const orgUUID = "0123456789abcdef0123456789abcdef0123"

// Planted non-ASCII rows: each ported Python regex must match across the whitespace Python's \s matches (U+00A0,
// U+3000, U+2028, \x1c) and must not match across non-space non-ASCII ("cafe" with an accent, an Arabic-Indic digit).
func TestPortedRegexesUseUnicodeSpace(t *testing.T) {
	for _, sp := range []string{"\u00a0", "\u3000", "\u2028", "\x1c", "\u1680"} {
		for name, row := range map[string]struct {
			hit  bool
			text string
			re   interface{ MatchString(string) bool }
		}{
			"insert":  {true, "INSERT" + sp + "INTO" + sp + "public.sync_run_units", ledgerInsertPattern},
			"update":  {true, "UPDATE" + sp + "public.sync_run_units", ledgerUpdatePattern},
			"success": {true, "SET" + sp + "status" + sp + "=" + sp + "'success'", ledgerSuccessPattern},
			"e2e":     {true, "E2E_ORG_ID" + sp + "=" + sp + `"` + orgUUID[:36] + `"`, e2eOrgIDLiteral},
			"replace": {true, "E2E_ORG_ID" + sp + "=" + sp + `"` + orgUUID[:36] + `"`, e2eOrgIDReplace},
			"uuid":    {true, "uuidPattern" + sp + "=" + sp + "regexp.MustCompile(`^x$`)", uuidPatternDecl},
			"flag":    {true, "--sink" + sp + `"${SCRATCH_URI}`, lvArgvFlagPattern("--sink")},
		} {
			if row.re.MatchString(row.text) != row.hit {
				t.Errorf("%s with %U: match = %v, want %v", name, []rune(sp)[0], !row.hit, row.hit)
			}
		}
	}
	for name, row := range map[string]struct {
		text string
		re   interface{ MatchString(string) bool }
	}{
		"insert-accent": {"INSERT\u00e9INTO public.sync_run_units", ledgerInsertPattern},
		"update-digit":  {"UPDATE\u0663public.sync_run_units", ledgerUpdatePattern},
		"flag-accent":   {"--sink\u00e9${SCRATCH_URI}", lvArgvFlagPattern("--sink")},
	} {
		if row.re.MatchString(row.text) {
			t.Errorf("%s: a non-space non-ASCII rune must not count as whitespace", name)
		}
	}
}

func TestPyStripAndFieldsMatchStr(t *testing.T) {
	if got := pyStrip("\x1c\u00a0 x \u3000\x1f"); got != "x" {
		t.Fatalf("pyStrip = %q", got)
	}
	if got := pyFields("a\x1cb\u00a0c\u00e9"); len(got) != 3 || got[0] != "a" || got[2] != "c\u00e9" {
		t.Fatalf("pyFields = %q", got)
	}
}
