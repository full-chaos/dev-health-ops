package scripts_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates"
)

func TestAuthScopeVerdict(t *testing.T) {
	const (
		keys   = "m/internal/auth/keys"
		audit  = "m/internal/api/audit"
		sso    = "m/internal/api/sso"
		push   = "m/internal/apiservice/customerpush"
		header = "# package<TAB>class<TAB>reason\n"
	)
	cases := []struct {
		name, gated, sources, exempt string
		rc                           int
		want                         string
	}{
		{"gated candidate passes", keys, keys + "\tsegment", "", 0, "auth scope: 1 candidate packages, all gated or exempt"},
		{"package row passes", keys, keys + "\tsegment\n" + audit + "\tsegment", audit + "\tpackage\tthe api audit trail", 0, "2 candidate packages"},
		{"package row with a non-test import passes", keys, keys + "\tsegment\n" + push + "\timport\n" + push + "\ttestimport", push + "\tpackage\tappends push lifecycle events", 0, "2 candidate packages"},
		{"test-only row with test imports only passes", keys, keys + "\tsegment\n" + sso + "\ttestimport", sso + "\ttest-only\tan integration test reads the auth schema", 0, "2 candidate packages"},
		{"comment and blank lines are skipped", keys, keys + "\tsegment\n\n" + sso + "\ttestimport\n", header + "\n" + sso + "\ttest-only\tan integration test\n\n", 0, "2 candidate packages"},
		{"missing candidate fails", keys, keys + "\tsegment\n" + sso + "\ttestimport", "", 2, "neither in the gate nor exempt with a reason in ci/auth_gate_scope_exempt.txt: " + sso},
		{"stale row of a gated package fails", keys, keys + "\tsegment", keys + "\tpackage\tgated anyway", 2, "stale or reasonless exemption(s)"},
		{"stale row of a non-candidate fails", keys, keys + "\tsegment", "m/internal/gone\tpackage\tgone", 2, "stale or reasonless exemption(s)"},
		{"reasonless row fails", keys, keys + "\tsegment\n" + audit + "\tsegment", audit + "\tpackage\t  ", 2, audit + "(no reason)"},
		{"test-only row with a non-test import fails", keys, keys + "\tsegment\n" + sso + "\timport\n" + sso + "\ttestimport", sso + "\ttest-only\tan integration test reads the auth schema", 2, "test-only exemption(s) with a non-test auth schema import: " + sso},
		{"test-only row with a non-test import in one tag set only fails", keys, keys + "\tsegment\n" + sso + "\timport\n" + sso + "\ttestimport\n" + sso + "\ttestimport", sso + "\ttest-only\tan integration test", 2, "test-only exemption(s) with a non-test auth schema import: " + sso},
		{"test-only row on an auth-looking path fails", keys, keys + "\tsegment\n" + audit + "\tsegment", audit + "\ttest-only\tthe api audit trail", 2, "test-only exemption(s) on an auth-looking path: " + audit},
		{"package row with test imports only is over-broad", keys, keys + "\tsegment\n" + sso + "\ttestimport", sso + "\tpackage\tan integration test", 2, "over-broad exemption(s) (test imports only; use test-only): " + sso},
		{"row without a class fails", keys, keys + "\tsegment\n" + sso + "\ttestimport", sso + "\tan integration test reads the auth schema", 2, "exemption row(s) with no or unknown class"},
		{"row with an unknown class fails", keys, keys + "\tsegment\n" + sso + "\ttestimport", sso + "\ttests-only\tan integration test", 2, "exemption row(s) with no or unknown class"},
		{"empty gate list fails", "", keys + "\tsegment", "", 2, "the gate selects no package"},
		{"empty sources fail", keys, "", "", 2, "no candidate package found"},
		{"blank-only sources fail", keys, "\n \n\t\n", "", 2, "no candidate package found"},
		{"malformed source line fails", keys, keys, "", 2, "malformed candidate source line"},
		{"unknown source fails", keys, keys + "\tvendor", "", 2, "unknown candidate source"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var out, errb bytes.Buffer
			rc := cigates.AuthScopeVerdict(strings.NewReader(c.gated), strings.NewReader(c.sources), strings.NewReader(c.exempt), &out, &errb)
			got := out.String() + errb.String()
			if rc != c.rc || !strings.Contains(got, c.want) {
				t.Fatalf("rc=%d want %d; output %q does not contain %q", rc, c.rc, got, c.want)
			}
			if rc != 0 && out.Len() != 0 {
				t.Fatalf("a refusal printed the pass line: %q", out.String())
			}
		})
	}
}

func TestAuthScopeCandidateSources(t *testing.T) {
	const mod = "example.com/m"
	const adapter = "example.com/m/internal/storage/postgres/authschema"
	in := strings.Join([]string{
		`{"ImportPath":"example.com/m/internal/auth/keys","Imports":["` + adapter + `"]}`,
		`{"ImportPath":"example.com/m/internal/billing","Imports":["` + adapter + `"],"TestImports":["` + adapter + `"]}`,
		`{"ImportPath":"example.com/m/internal/x","TestImports":["` + adapter + `"]}`,
		`{"ImportPath":"example.com/m/internal/y","XTestImports":["` + adapter + `"]}`,
		`{"ImportPath":"example.com/m/internal/unrelated","Imports":["fmt"]}`,
	}, "\n")
	var out, errb bytes.Buffer
	if rc := cigates.AuthScopeCandidateSources(mod, strings.NewReader(in), &out, &errb); rc != 0 {
		t.Fatalf("rc=%d err=%q", rc, errb.String())
	}
	want := "example.com/m/internal/auth/keys\tsegment\n" +
		"example.com/m/internal/auth/keys\timport\n" +
		"example.com/m/internal/billing\timport\n" +
		"example.com/m/internal/billing\ttestimport\n" +
		"example.com/m/internal/x\ttestimport\n" +
		"example.com/m/internal/y\ttestimport\n"
	if out.String() != want {
		t.Fatalf("got %q want %q", out.String(), want)
	}
}
