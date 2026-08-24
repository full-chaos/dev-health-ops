package syncdispatchruntime

import "testing"

func TestParseBudgetLimits(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want map[string]int
	}{
		{"empty string -> empty map", "", map[string]int{}},
		{"malformed JSON -> empty map", "{not json", map[string]int{}},
		{"not an object -> empty map", `[1,2,3]`, map[string]int{}},
		{"numeric values", `{"a":10,"b":0}`, map[string]int{"a": 10, "b": 0}},
		{"negative value floors at 0", `{"a":-5}`, map[string]int{"a": 0}},
		{"string-numeric value coerces", `{"a":"7"}`, map[string]int{"a": 7}},
		{"one bad key is skipped, others survive", `{"a":10,"b":"not-a-number","c":20}`, map[string]int{"a": 10, "c": 20}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := parseBudgetLimits(testCase.raw)
			if len(got) != len(testCase.want) {
				t.Fatalf("parseBudgetLimits(%q)=%v want=%v", testCase.raw, got, testCase.want)
			}
			for key, value := range testCase.want {
				if got[key] != value {
					t.Fatalf("parseBudgetLimits(%q)[%q]=%d want=%d", testCase.raw, key, got[key], value)
				}
			}
		})
	}
}

func TestLimitForBucket(t *testing.T) {
	bucket := budgetEstimateBucket{Provider: "github", OrgID: "org-1", Host: "api.github.com", CredentialFingerprint: "fp-1", Dimension: "rest_core"}

	t.Run("falls back to default when nothing matches", func(t *testing.T) {
		if got := limitForBucket(bucket, "work-items", map[string]int{}, 1000); got != 1000 {
			t.Fatalf("got=%d want=1000", got)
		}
	})
	t.Run("wildcard matches when nothing more specific does", func(t *testing.T) {
		limits := map[string]int{"*": 42}
		if got := limitForBucket(bucket, "work-items", limits, 1000); got != 42 {
			t.Fatalf("got=%d want=42", got)
		}
	})
	t.Run("most specific key wins over a less specific one", func(t *testing.T) {
		limits := map[string]int{
			"rest_core": 10,
			"github:org-1:api.github.com:fp-1:rest_core:work-items": 99,
		}
		if got := limitForBucket(bucket, "work-items", limits, 1000); got != 99 {
			t.Fatalf("got=%d want=99 (the fully-qualified key must win over the bare dimension)", got)
		}
	})
}

func TestBudgetKeyFor(t *testing.T) {
	bucket := budgetEstimateBucket{Provider: "github", OrgID: "org-1", Host: "api.github.com", CredentialFingerprint: "fp-1", Dimension: "rest_core"}
	got := budgetKeyFor(bucket, "work-items")
	want := "github:org-1:api.github.com:fp-1:rest_core:work-items"
	if got != want {
		t.Fatalf("budgetKeyFor()=%q want=%q", got, want)
	}
}
