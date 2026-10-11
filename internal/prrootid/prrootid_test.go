package prrootid

import "testing"

const repo = "33333333-3333-4333-8333-333333333333"

func TestFormatIsTheStoredEdgeTableForm(t *testing.T) {
	if got, want := Format(repo, "434"), repo+":434"; got != want {
		t.Fatalf("Format = %q, want %q", got, want)
	}
	if got, want := FormatNumber(repo, 434), repo+":434"; got != want {
		t.Fatalf("FormatNumber = %q, want %q", got, want)
	}
}

func TestParseAcceptsOnlyWhatFormatMakes(t *testing.T) {
	if r, n, ok := Parse(Format(repo, "7")); !ok || r != repo || n != "7" {
		t.Fatalf("Parse(Format) = %q %q %v", r, n, ok)
	}
	for name, id := range map[string]string{
		"hash separator":      repo + "#7",
		"unit form":           repo + "#pr7",
		"metric ref part":     "migration_prs:" + repo,
		"no number":           repo + ":",
		"signed number":       repo + ":-1",
		"letters":             repo + ":7x",
		"unicode digits":      repo + ":١٢",
		"upper case repo":     "AAAAAAAA-1111-4111-8111-111111111111:7",
		"not a uuid":          "acme/api:7",
		"empty":               "",
		"extra part":          repo + ":7:8",
		"braced uuid":         "{" + repo + "}:7",
		"urn uuid":            "urn:uuid:" + repo + ":7",
		"uuid without hyphen": "33333333333343338333333333333333:7",
	} {
		if r, n, ok := Parse(id); ok {
			t.Errorf("%s: Parse(%q) = %q %q, want a refusal", name, id, r, n)
		}
	}
}
