package investment

import (
	"errors"
	"strings"
	"testing"
)

// TestRequireOrganizationScope pins the guard against Python's gate, including
// the one place the two deliberately differ.
func TestRequireOrganizationScope(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		org       string
		refused   bool
		rationale string
	}{
		{name: "ordinary org", org: "70d529e0-3c06-4597-8480-794fd02328b6", refused: false},

		// Python: `not (config.org_id or "").strip()` — all three are refused
		// there too (for a real provider without allow_unscoped).
		{name: "empty", org: "", refused: true, rationale: "python refuses; unscoped read fuses tenants"},
		{name: "spaces", org: "   ", refused: true, rationale: "python .strip()s before testing"},
		{name: "tab and newline", org: "\t\n", refused: true, rationale: "python .strip()s before testing"},

		// NOT a scope check's job: whether the org exists is the query's answer,
		// not this function's. Refusing a well-formed-but-unknown org here would
		// be a new gate, which is exactly what this must not become.
		{name: "unknown but non-empty org is allowed through", org: "not-a-real-org", refused: false,
			rationale: "existence is the query's answer, not the guard's"},
		{name: "zero uuid is allowed through", org: "00000000-0000-0000-0000-000000000000", refused: false,
			rationale: "non-empty; matching python's truthiness, and consistent with CHAOS-4804's repo_id gate"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := RequireOrganizationScope(testCase.org)
			if refused := err != nil; refused != testCase.refused {
				t.Fatalf("org %q: refused=%v, want %v (%s)",
					testCase.org, refused, testCase.refused, testCase.rationale)
			}
			if err == nil {
				return
			}
			if !errors.Is(err, ErrOrganizationScopeRequired) {
				t.Errorf("error should wrap ErrOrganizationScopeRequired, got %v", err)
			}
			// The message must say WHAT goes wrong, not just that something did:
			// an operator seeing this needs to know it is a tenant-isolation
			// refusal, not a missing-argument nag.
			if !strings.Contains(err.Error(), "CHAOS-4804") {
				t.Errorf("error should cite the defect it prevents, got %q", err)
			}
		})
	}
}

// TestGuardHasNoUnscopedEscapeHatch pins the deliberate divergence from Python.
//
// Python permits an empty org for `mock`/`none` providers and for
// allow_unscoped runs; those escape hatches are what make CHAOS-4804 reachable
// at all. The Go guard takes only the org, so there is no argument that could
// re-open them — the absence is structural, not a policy someone can flip.
func TestGuardHasNoUnscopedEscapeHatch(t *testing.T) {
	// The signature itself is the assertion: one string in, one error out. If a
	// future change adds an allowUnscoped bool, this test stops compiling and
	// the divergence gets re-decided deliberately rather than by accident.
	var guard func(string) error = RequireOrganizationScope
	if err := guard(""); err == nil {
		t.Fatal("an empty org must be refused unconditionally")
	}
}
