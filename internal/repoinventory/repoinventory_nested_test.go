package repoinventory

import (
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/repokey"
)

var (
	sqlSlugPattern = regexp.MustCompile(`match\(\s*(?:canonical_repo|lowerUTF8\(trimBoth\(repo\)\)),\s*'([^']*)'\s*\)`)
	sqlLengthBound = regexp.MustCompile(`length\((?:canonical_repo|lowerUTF8\(trimBoth\(repo\)\))\) <= (\d+)`)
)

// sqlGrammar is the slug rule one statement applies in ClickHouse: its match()
// pattern (RE2, as ClickHouse runs it) and its length() bound in bytes, or -1
// when the statement holds no bound.
type sqlGrammar struct {
	pattern string
	re      *regexp.Regexp
	bound   int
}

func (g sqlGrammar) keeps(value string) bool {
	return (g.bound < 0 || len(value) <= g.bound) && g.re.MatchString(value)
}

func statementGrammars(t *testing.T) map[string]sqlGrammar {
	t.Helper()
	out := map[string]sqlGrammar{}
	for name, sql := range map[string]string{"Statement": Statement(""), "ExistsStatement": ExistsStatement("")} {
		found := sqlSlugPattern.FindStringSubmatch(sql)
		if found == nil {
			t.Fatalf("%s: no match(...) slug pattern in:\n%s", name, sql)
		}
		grammar := sqlGrammar{pattern: found[1], re: regexp.MustCompile(found[1]), bound: -1}
		if bound := sqlLengthBound.FindStringSubmatch(sql); bound != nil {
			grammar.bound, _ = strconv.Atoi(bound[1])
		}
		out[name] = grammar
	}
	return out
}

func canonicalKeeps(t *testing.T, value string) bool {
	t.Helper()
	got, err := Canonical([]Row{{value, 1}})
	if err != nil {
		t.Fatal(err)
	}
	return len(got) == 1
}

func TestInventoryKeepsAGitLabNestedGroupSlug(t *testing.T) {
	const nested = "grp-synth/sub-synth/svc-synth"
	for name, grammar := range statementGrammars(t) {
		if !grammar.keeps(nested) {
			t.Errorf("%s drops the GitLab nested-group slug %q (pattern %s)", name, nested, grammar.pattern)
		}
	}
	got, err := Canonical([]Row{{nested, 1}})
	if err != nil {
		t.Fatal(err)
	}
	if want := []Item{{Slug: nested, Count: 1}}; !reflect.DeepEqual(got, want) {
		t.Errorf("Canonical = %v, want %v", got, want)
	}
}

func TestInventoryNeverHoldsTheOrganizationWideToken(t *testing.T) {
	for _, value := range []string{"*", " * "} {
		if canonicalKeeps(t, value) {
			t.Errorf("Canonical keeps %q", value)
		}
	}
	for name, grammar := range statementGrammars(t) {
		if grammar.keeps("*") {
			t.Errorf("%s keeps \"*\" (pattern %s)", name, grammar.pattern)
		}
	}
}

func TestInventoryHoldsTheRepositoryKeyBounds(t *testing.T) {
	segments := func(n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = "g" + strconv.Itoa(i)
		}
		return strings.Join(parts, "/")
	}
	of := func(n int) string { return strings.Repeat("a", n) }
	cases := []struct {
		name  string
		value string
		keep  bool
	}{
		{"one segment", "solo-synth", false},
		{"two segments", "acme-synth/api", true},
		{"20 segments", segments(20), true},
		{"21 segments", segments(21), false},
		{"255 bytes", of(100) + "/" + of(100) + "/" + of(53), true},
		{"256 bytes", of(100) + "/" + of(100) + "/" + of(54), false},
		{"segment of 100", of(100) + "/api", true},
		{"segment of 101", of(101) + "/api", false},
		{"empty segment", "grp-synth//api", false},
		{"trailing slash", "grp-synth/api/", false},
		{"group ends in -", "grp-/api", false},
		{"leading-dot group", ".grp/api", false},
		{"leading-dot name", "grp-synth/.github", true},
		{"upper case after lowering", "Grp-Synth/Sub/API", true},
	}
	grammars := statementGrammars(t)
	for _, tc := range cases {
		lowered := strings.ToLower(tc.value)
		if want := repokey.Valid(lowered) && lowered != "*"; want != tc.keep {
			t.Fatalf("%s: table says keep %v, repokey.Valid says %v", tc.name, tc.keep, want)
		}
		if got := canonicalKeeps(t, tc.value); got != tc.keep {
			t.Errorf("%s: Canonical keeps %q = %v, want %v", tc.name, tc.value, got, tc.keep)
		}
		for name, grammar := range grammars {
			if got := grammar.keeps(lowered); got != tc.keep {
				t.Errorf("%s: %s keeps %q = %v, want %v", tc.name, name, lowered, got, tc.keep)
			}
		}
	}
}

func TestInventorySQLPatternIsTheRepositoryKeyWithoutTheStar(t *testing.T) {
	const starArm = `|\*)$`
	if !strings.HasSuffix(repokey.SchemaPattern, starArm) {
		t.Fatalf("repokey.SchemaPattern does not end with %q", starArm)
	}
	want := strings.TrimSuffix(repokey.SchemaPattern, starArm) + `)$`
	if strings.ContainsAny(want, `'\`) {
		t.Fatalf("the slug pattern holds a quote or a backslash: not one ClickHouse literal as written")
	}
	for name, grammar := range statementGrammars(t) {
		if grammar.pattern != want {
			t.Errorf("%s pattern = %s, want repokey.SchemaPattern without the star arm", name, grammar.pattern)
		}
		if grammar.bound != repokey.MaxBytes {
			t.Errorf("%s length bound = %d, want repokey.MaxBytes %d", name, grammar.bound, repokey.MaxBytes)
		}
	}
}
