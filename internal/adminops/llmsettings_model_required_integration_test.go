//go:build integration

package adminops

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestLLMSettingsSetRefusesABYOProviderWithoutAModel is the PUT route's model rule on the operator
// path: a BYO provider saved without a model (flag absent, empty, or blank; the verb cannot send
// null, so absent is its null) exits 1, names the model, and leaves every row as it was, whether
// the org has no row yet or a row with a model (a key rotation without a model is refused in both,
// as the route does). A provider the route reads as "no BYO" (auto, mock, none) saves without a model.
// Go-only: the Python verb saved such a row.
func TestLLMSettingsSetRefusesABYOProviderWithoutAModel(t *testing.T) {
	db := startDatabase(t)
	db.reset(t)
	ctx := context.Background()
	if _, err := db.conn.Exec(ctx, "TRUNCATE settings, feature_flags CASCADE"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ('%s', 'team-org', 'Team', 'team', 'manual', true, now(), now())`, teamOrg)); err != nil {
		t.Fatal(err)
	}
	credential := "model-rule-" + "0123456789abcdef"
	rotated := "model-rule-rotated-" + "0123456789"
	shapes := map[string][]string{
		"model absent":              {},
		"model absent, key rotated": {"--api-key", rotated},
		"model empty":               {"--model", ""},
		"model whitespace":          {"--model", "   "},
		"model blank, key rotated":  {"--model", " \t ", "--api-key", rotated},
	}
	run := func(extra ...string) (int, string) {
		args := append([]string{"llm-settings", "set", "--org", teamOrg, "--provider", "openai"}, extra...)
		return goVerbEnv(t, db, llmEnv(false), args)
	}
	refuse := func(stage string) {
		t.Helper()
		before := db.llmState(t)
		for name, extra := range shapes {
			code, stdout := run(extra...)
			if code != 1 || !strings.Contains(stdout, "model is required when a BYO LLM provider is saved") {
				t.Errorf("%s, %s: exit %d, stdout %q", stage, name, code, stdout)
			}
			if after := db.llmState(t); after != before {
				t.Errorf("%s, %s: a refused save changed the rows:\n%s\n%s", stage, name, before, after)
			}
			if strings.Contains(stdout, rotated) {
				t.Errorf("%s, %s: the refusal prints the key", stage, name)
			}
		}
	}

	refuse("no row yet")
	if state := db.llmState(t); state != "[]" {
		t.Fatalf("no row yet: rows left %s", state)
	}

	if code, stdout := run("--model", "m1", "--api-key", credential); code != 0 {
		t.Fatalf("a BYO provider with a model: exit %d, %s", code, stdout)
	}
	refuse("row with a model")
	if code, stdout := run("--model", "m1", "--api-key", rotated); code != 0 {
		t.Fatalf("key rotation with a model: exit %d, %s", code, stdout)
	}

	for _, provider := range []string{"auto", "MOCK", " none "} {
		args := []string{"llm-settings", "set", "--org", teamOrg, "--provider", provider}
		if code, stdout := goVerbEnv(t, db, llmEnv(false), args); code != 0 {
			t.Errorf("provider %q needs no model: exit %d, %s", provider, code, stdout)
		}
	}

	before := db.llmState(t)
	code, stdout := goVerbEnv(t, db, llmEnv(false), []string{"llm-settings", "set", "--org", teamOrg, "--provider", "   "})
	if code != 1 || !strings.Contains(stdout, "provider must not be empty") || db.llmState(t) != before {
		t.Errorf("a blank provider: exit %d, stdout %q (refused by the provider rule, rows unchanged)", code, stdout)
	}

	baseURLBefore := db.llmState(t)
	code, stdout = run("--base-url", "http://169.254.169.254/latest")
	if code != 1 || strings.Contains(stdout, "model is required") || db.llmState(t) != baseURLBefore {
		t.Errorf("an invalid base URL is refused before the model rule: exit %d, stdout %q", code, stdout)
	}

	venueoracle.WriteGoOnlyProof(t, "Go-only rule test: the Python verb saved a model-less BYO provider, so there is no Python answer to compare; the verb's refusals and the rows after them are asserted directly")
}
