package admin

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

// D6733: the TypeSafe categorize PUT refuses the base_url key itself, whatever
// its value, in any letter case, duplicated or escaped. Each cell is the raw
// body as the route reads it: pybody decode, the PUT field read, the decision
// rule of the purpose write.
func TestTypeSafeCategorizePutRefusesTheBaseURLKey(t *testing.T) {
	decide := func(t *testing.T, provider, model, extra string) (refused bool, code string) {
		t.Helper()
		raw := `{"provider":"` + provider + `","purpose":"categorize","model":"` + model + `"` + extra + `}`
		r := httptest.NewRequest("PUT", "/llm-settings", strings.NewReader(raw))
		r.Header.Set("Content-Type", "application/json")
		body, outcome, failure, err := pybody.Read(r)
		if err != nil || outcome != pybody.Ready || failure != nil {
			t.Fatalf("read %s: outcome=%v failure=%v err=%v", raw, outcome, failure, err)
		}
		var errs pybody.Errors
		object, ok := errs.Object(body)
		if !ok {
			t.Fatalf("not an object: %s", raw)
		}
		put := readLLMPut(&errs, object)
		if len(errs) > 0 || put.provider == nil {
			t.Fatalf("validation of %s: %v", raw, errs)
		}
		refusal := put.purposeUpsert(llmorgsettings.PurposeCategorize).decisionRefusal()
		if refusal == nil {
			return false, ""
		}
		return true, refusal.message()
	}
	post := llmorgsettings.TypeSafePostURL
	for _, tc := range []struct {
		name, extra string
		refused     bool
	}{
		{"omitted", ``, false},
		{"null", `,"base_url":null`, true},
		{"empty", `,"base_url":""`, true},
		{"whitespace", `,"base_url":" "`, true},
		{"typesafe-root", `,"base_url":"https://api.typesafe.ai"`, true},
		{"fixed-post-url", `,"base_url":"` + post + `"`, true},
		{"other-url", `,"base_url":"https://example.com/v1"`, true},
		{"case-Base_URL", `,"Base_URL":"https://example.com/v1"`, true},
		{"case-BASE_URL-null", `,"BASE_URL":null`, true},
		{"case-fold-long-s", `,"baſe_url":"https://example.com/v1"`, true},
		{"escaped-key-null", `,"base_url":null`, true},
		{"dup-url-then-null", `,"base_url":"https://example.com/v1","base_url":null`, true},
		{"dup-null-then-url", `,"base_url":null,"base_url":"https://example.com/v1"`, true},
		{"dup-case-then-exact-null", `,"BASE_URL":"https://example.com/v1","base_url":null`, true},
		{"dup-exact-null-then-case", `,"base_url":null,"Base_URL":"https://example.com/v1"`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refused, msg := decide(t, "typesafe", "jev-1.13.0", tc.extra)
			result := "accepted"
			if refused {
				result = "refused"
			}
			t.Logf("cell=%s result=%s", tc.name, result)
			if refused != tc.refused {
				t.Fatalf("cell=%s refused=%v (%q), want refused=%v", tc.name, refused, msg, tc.refused)
			}
		})
	}
	// The key rule is TypeSafe's: another provider's categorize body keeps its base_url.
	if refused, msg := decide(t, "openai", "gpt-x", `,"base_url":null,"Base_URL":"https://example.com/v1"`); refused {
		t.Fatalf("openai categorize with base_url keys refused: %q", msg)
	}
}
