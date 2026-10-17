package investmentexplain

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type trialFixture struct {
	Case                 string             `json:"case"`
	Text                 string             `json:"text"`
	ThemeSharesPct       map[string]float64 `json:"theme_shares_pct"`
	SubcategorySharesPct map[string]float64 `json:"subcategory_shares_pct"`
	Want                 string             `json:"want"`
	WantRule             string             `json:"want_rule"`
}

// The fixtures are raw model outputs from a 20-scope trial against the
// production prompt. The accepted case failed on main because the model wrote
// display names into evidence.theme and evidence.subcategory. The rejected
// cases must stay rejected: the wording validator and the key-membership
// check are the contract.
func TestParseInvestmentMixResponseTrialOutputs(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "explain_trial__*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no explain_trial fixtures: %v", err)
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var fx trialFixture
		if err := json.Unmarshal(data, &fx); err != nil {
			t.Fatal(err)
		}
		t.Run(fx.Case, func(t *testing.T) {
			result := ParseInvestmentMixResponse(fx.Text, ParseOptions{
				ThemeSharesPct:       fx.ThemeSharesPct,
				SubcategorySharesPct: fx.SubcategorySharesPct,
				ShareNoun:            "work",
			})
			switch fx.Want {
			case "accepted":
				if result.Output == nil {
					t.Fatalf("want accepted, got status=%s rule=%s path=%s snippet=%s", result.Status, result.ReasonRule, result.ReasonPath, result.ReasonSnippet)
				}
				for _, f := range result.Output.TopFindings {
					if _, ok := fx.ThemeSharesPct[f.Evidence.Theme]; !ok {
						t.Fatalf("evidence.theme %q is not a canonical key", f.Evidence.Theme)
					}
					if f.Evidence.Subcategory != nil {
						if _, ok := fx.SubcategorySharesPct[*f.Evidence.Subcategory]; !ok {
							t.Fatalf("evidence.subcategory %q is not a canonical key", *f.Evidence.Subcategory)
						}
					}
				}
			case "rejected":
				if result.Output != nil {
					t.Fatalf("want rejection %q, output was accepted", fx.WantRule)
				}
				if result.ReasonRule != fx.WantRule {
					t.Fatalf("want rule %q, got %q (path=%s)", fx.WantRule, result.ReasonRule, result.ReasonPath)
				}
			default:
				t.Fatalf("bad want %q", fx.Want)
			}
		})
	}
}

func TestResolveCanonicalKeyVariants(t *testing.T) {
	themes := map[string]float64{"quality": 1, "feature_delivery": 2, "operational": 3, "maintenance": 4, "risk": 5}
	cases := []struct {
		raw, want string
		ok        bool
	}{
		{"quality", "quality", true},
		{"Quality / Reliability", "quality", true},
		{"quality/reliability", "quality", true},
		{"QUALITY RELIABILITY", "quality", true},
		{"quality-reliability", "quality", true},
		{"Feature Delivery", "feature_delivery", true},
		{"feature delivery", "feature_delivery", true},
		{"Maintenance / Tech Debt", "maintenance", true},
		{"Operational / Support", "operational", true},
		{"Risk / Security", "risk", true},
		{"Quality / Security", "", false},
		{"unknown", "", false},
		{"", "", false},
		{"  / ", "", false},
	}
	for _, c := range cases {
		got, _, ok := resolveCanonicalKey(c.raw, themes, themeLabel)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("resolve(%q) = %q,%v want %q,%v", c.raw, got, ok, c.want, c.ok)
		}
	}
	subs := map[string]float64{"quality.bugfix": 1, "quality.testing": 2}
	if got, _, ok := resolveCanonicalKey("Quality / Reliability · Bugfix", subs, subcategoryLabel); !ok || got != "quality.bugfix" {
		t.Errorf("subcategory label not resolved: %q %v", got, ok)
	}
	if _, _, ok := resolveCanonicalKey("Bugfix", subs, subcategoryLabel); ok {
		t.Error("a bare leaf must stay unresolved")
	}
	ambiguous := map[string]float64{"a_b": 1, "a.b": 2}
	if _, _, ok := resolveCanonicalKey("A B", ambiguous, strings.ToUpper); ok {
		t.Error("two keys with one normalized form must stay unresolved")
	}
}

func TestExplainPromptStatesAllowedKeysAndBannedWords(t *testing.T) {
	prompt := LoadPrompt()
	for _, want := range []string{
		"feature_delivery, operational, maintenance, quality, risk",
		"quality.bugfix", "risk.vulnerability", "maintenance.upgrade",
		"If those lists are empty, return \"top_findings\": []",
		"Example row:",
		`Banned words in every string of the output, including anti_claims, why, and where: "is", "was", "should", "determined", "detected", "definitely", "certainly", "undoubtedly", "without question"`,
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt lacks %q", want)
		}
	}
}
