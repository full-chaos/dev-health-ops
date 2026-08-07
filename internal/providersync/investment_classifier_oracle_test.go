package providersync

import (
	"path/filepath"
	"runtime"
	"testing"
)

// Both sides resolve a config NAME to the same path, so neither engine can be
// handed rules the other never saw. The primary cases use the REAL config the
// production call site loads; the synthetic one covers only the matcher forms
// the real file never expresses.
func investmentConfigPath(t *testing.T, name string) string {
	t.Helper()
	root := investmentRepoRoot(t)
	switch name {
	case "real":
		return filepath.Join(root, "src/dev_health_ops/config/investment_areas.yaml")
	case "quirks":
		return filepath.Join(root, "internal/providersync/testdata/investment_configs/quirks.yaml")
	case "missing":
		return filepath.Join(root, "internal/providersync/testdata/investment_configs/nope.yaml")
	default:
		t.Fatalf("unknown investment config %q", name)
		return ""
	}
}

// investmentRepoRoot walks up from THIS source file rather than from the test's
// working directory, so the path does not depend on how the test was invoked.
func investmentRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the oracle test source file")
	}
	// internal/providersync/<this file> -> repo root
	return filepath.Dir(filepath.Dir(filepath.Dir(file)))
}

func investmentOracleCases() []oracleCase {
	return []oracleCase{
		{
			// REAL CONFIG, ordinary label path: "security" is a sec_* label at
			// priority 10, the band where 37 rules tie.
			ID: "real_label_security",
			Input: map[string]any{
				"Config":   "real",
				"Artifact": map[string]any{"Labels": []any{"security"}},
			},
		},
		{
			// REAL CONFIG, no labels at all: every label rule rejects, and the
			// only rule left is product_default (priority 999, `always: true`).
			// Confidence must be 1.0 and the rule id product_default -- NOT the
			// 0.0/legacy_default fallback, which this config can never reach
			// BECAUSE of that catch-all.
			ID: "real_no_labels_hits_always_catchall",
			Input: map[string]any{
				"Config":   "real",
				"Artifact": map[string]any{"Labels": []any{}},
			},
		},
		{
			// REAL CONFIG, label casing. The label arm lower-cases BOTH sides,
			// so a shouty label still matches a lower-case rule.
			ID: "real_label_is_case_folded",
			Input: map[string]any{
				"Config":   "real",
				"Artifact": map[string]any{"Labels": []any{"SECURITY"}},
			},
		},
		{
			// REAL CONFIG + the production component value. WorkItem has no
			// `component` attribute, so the call site always passes "". This
			// pins that data_component (match: ['Data Platform','Analytics'])
			// does NOT fire even for an artifact whose other labels are absent.
			ID: "real_component_empty_cannot_match",
			Input: map[string]any{
				"Config": "real",
				"Artifact": map[string]any{
					"Labels": []any{}, "Component": "",
				},
			},
		},
		{
			// REAL CONFIG with a component value that WOULD match
			// data_component if the call site could ever produce it. It cannot
			// -- this case exists to show the rule is dead because of the
			// CALLER, not because the matcher is broken.
			ID: "real_component_data_platform_would_match",
			Input: map[string]any{
				"Config": "real",
				"Artifact": map[string]any{
					"Labels": []any{}, "Component": "Data Platform",
				},
			},
		},
		{
			// REAL CONFIG, STABLE SORT where it actually bites. This artifact
			// carries labels matching FOUR different priority-10 rules
			// (sec_auth, infra_k8s, qual_test, prod_feat). 37 rules tie at that
			// priority in the real file, so an unstable sort genuinely permutes
			// them and the first-match winner changes. The synthetic 3-rule tie
			// below is too small to force Go's sort to reorder; this is the
			// case that measures the constraint.
			ID: "real_multiple_priority_10_matches_first_in_file_wins",
			Input: map[string]any{
				"Config": "real",
				"Artifact": map[string]any{
					"Labels": []any{"feature", "testing", "kubernetes", "auth"},
				},
			},
		},
		{
			// QUIRK: the EMPTY match:{} catch-all, reached only when nothing
			// above it matches. Without this case the lowest-priority rule in
			// the synthetic config would never be exercised.
			ID: "quirks_empty_match_is_the_catchall",
			Input: map[string]any{
				"Config":   "quirks",
				"Artifact": map[string]any{"Labels": []any{"matches-no-rule"}},
			},
		},
		{
			// QUIRK: STABLE SORT. Three rules tie at priority 10 and all three
			// match "shared"; the FIRST in file order must win. An unstable
			// sort could return any of the three.
			ID: "quirks_equal_priority_first_in_file_wins",
			Input: map[string]any{
				"Config":   "quirks",
				"Artifact": map[string]any{"Labels": []any{"shared"}},
			},
		},
		{
			// QUIRK: `always: false` is tested for TRUTHINESS, so it does not
			// short-circuit; the rule falls through to its (absent) remaining
			// criteria and therefore matches everything anyway.
			ID: "quirks_always_false_falls_through",
			Input: map[string]any{
				"Config":   "quirks",
				"Artifact": map[string]any{"Labels": []any{"alwaysfalse"}},
			},
		},
		{
			// QUIRK: a component rule whose list contains "" is the ONLY shape
			// that can fire from the work-item call site. Labels are chosen so
			// the earlier tie rules do not shadow it.
			ID: "quirks_component_empty_string_matches",
			Input: map[string]any{
				"Config": "quirks",
				"Artifact": map[string]any{
					"Labels": []any{"componentcase"}, "Component": "",
				},
			},
		},
		{
			// QUIRK: the path_prefix arm WORKS when the caller supplies paths.
			// The work-item call site never does, which is what makes the three
			// real path_prefix rules dead from that path.
			ID: "quirks_path_prefix_matches_when_paths_supplied",
			Input: map[string]any{
				"Config": "quirks",
				"Artifact": map[string]any{
					"Labels": []any{"pathcase"},
					"Paths":  []any{"services/api/handler.go"},
				},
			},
		},
		{
			// QUIRK: a rule with NO priority key sorts as 100.
			ID: "quirks_missing_priority_defaults_to_100",
			Input: map[string]any{
				"Config": "quirks",
				"Artifact": map[string]any{
					"Labels": []any{"unprioritised"},
				},
			},
		},
		{
			// QUIRK: a matched rule with NO output block falls back to the
			// legacy product/general values while keeping confidence 1.0 and
			// its OWN rule id -- which is what distinguishes it from the
			// no-match fallback that uses "legacy_default" and 0.0.
			ID: "quirks_matched_rule_without_output",
			Input: map[string]any{
				"Config": "quirks",
				"Artifact": map[string]any{
					"Labels": []any{"nooutput"},
				},
			},
		},
		{
			// MISSING CONFIG: Python warns and classifies with an EMPTY rule
			// list rather than raising, so this is the ONLY way to observe the
			// 0.0 / legacy_default fallback -- the real config's `always`
			// catch-all makes it unreachable there.
			ID: "missing_config_falls_back_to_legacy_default",
			Input: map[string]any{
				"Config":   "missing",
				"Artifact": map[string]any{"Labels": []any{"security"}},
			},
		},
	}
}

func TestInvestmentClassifierMatchesLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"analytics/investment/classify",
		investmentOracleCases(),
		func(t *testing.T, input map[string]any) InvestmentClassification {
			t.Helper()
			classifier, err := NewInvestmentClassifier(
				investmentConfigPath(t, input["Config"].(string)),
			)
			if err != nil {
				t.Fatal(err)
			}
			return classifier.Classify(investmentOracleArtifact(input))
		},
		nil,
	)
}

func investmentOracleArtifact(input map[string]any) InvestmentArtifact {
	raw, _ := input["Artifact"].(map[string]any)
	artifact := InvestmentArtifact{Provider: "github"}
	if raw == nil {
		return artifact
	}
	for _, label := range investmentOracleStrings(raw, "Labels") {
		artifact.Labels = append(artifact.Labels, label)
	}
	// Paths is left nil unless the case sets it, matching the production call
	// site, which supplies no paths at all.
	if _, present := raw["Paths"]; present {
		artifact.Paths = investmentOracleStrings(raw, "Paths")
	}
	if value, ok := raw["Component"].(string); ok {
		artifact.Component = value
	}
	if value, ok := raw["Title"].(string); ok {
		artifact.Title = value
	}
	if value, ok := raw["Provider"].(string); ok {
		artifact.Provider = value
	}
	return artifact
}

func investmentOracleStrings(raw map[string]any, key string) []string {
	values, ok := raw[key].([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, value.(string))
	}
	return result
}
