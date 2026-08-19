package providersync

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
)

// Both sides resolve a config NAME to the same path by the same RULE, not
// through a table each maintains separately: "real" is the file production
// loads, and every other name is <name>.yaml under one testdata directory.
// A per-name table on each side is a drift surface -- a case pointed at a name
// the two sides spell differently would compare two different files and still
// look green. See testdata/oracle_pairs/_investment_helpers.py for the Python
// half of the same rule.
func investmentConfigPath(t *testing.T, name string) string {
	t.Helper()
	root := investmentRepoRoot(t)
	if name == "real" {
		return filepath.Join(root, "src/dev_health_ops/config/investment_areas.yaml")
	}
	return filepath.Join(
		root, "internal/providersync/testdata/investment_configs", name+".yaml")
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

// The "missing" config case is the ONLY way to observe the 0.0/legacy_default
// fallback, because the real config's `always: true` catch-all makes it
// unreachable there. That case is only meaningful while the file genuinely does
// not exist: if someone ever creates it, the case silently starts measuring an
// ordinary load and the fallback goes untested while still reading as covered.
func TestInvestmentMissingConfigIsActuallyMissing(t *testing.T) {
	t.Parallel()
	path := investmentConfigPath(t, "missing")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("%s exists (stat err %v), so the missing-config case no longer "+
			"measures the missing-file branch", path, err)
	}
}

func investmentOracleCases() []oracleCase {
	return []oracleCase{
		{
			// REAL CONFIG, ordinary label path. "security" resolves to
			// sec_general, the SOLE rule at priority 20 -- not a member of the
			// 37-rule tie at priority 10, which is what the tie cases below
			// exercise.
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
			// REAL CONFIG, and the asymmetry between the two arms: the COMPONENT
			// arm is exact-match, NOT case-folded like the label arm.
			// data_component lists 'Analytics', and this lower-case "analytics"
			// must therefore fall through to the catch-all.
			//
			// An earlier version of this case passed Component:"" -- byte for
			// byte the same artifact as real_no_labels_hits_always_catchall
			// above, so it could not fail for any reason that case would not
			// also fail for, and the case-sensitivity of the component arm was
			// untested while this case's name implied otherwise.
			ID: "real_component_lowercase_analytics_cannot_match",
			Input: map[string]any{
				"Config": "real",
				"Artifact": map[string]any{
					"Labels": []any{}, "Component": "analytics",
				},
			},
		},
		{
			// REAL CONFIG with a component value that WOULD match
			// data_component if the call site could ever produce it. It cannot
			// -- this case exists to show the rule is dead because of the
			// CALLER, not because the matcher is broken. Paired with the
			// lower-case case above, it also pins that the exact spelling is
			// what decides.
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
			// QUIRK, and the counterpart to the case above: the SAME rule and
			// the SAME labels, with the `component` key ABSENT rather than "".
			// Python reads it with `artifact.get("component")` -- no default --
			// so absent is None, and `None not in ["", "Data Platform"]` is
			// true: the rule rejects and the artifact falls to the catch-all.
			// This is what makes InvestmentArtifact.Component a POINTER; with a
			// plain string the two states are indistinguishable and this case
			// would return the previous one's answer.
			ID: "quirks_component_absent_key_is_not_the_empty_string",
			Input: map[string]any{
				"Config":   "quirks",
				"Artifact": map[string]any{"Labels": []any{"componentcase"}},
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
			// `id:` PRESENT and NULL -> rule_id is None. Not "legacy_rule", and
			// not "". The previous port collapsed this into "legacy_rule" and
			// claimed a case pinned it; none existed.
			ID: "ids_present_and_null_is_none",
			Input: map[string]any{
				"Config":   "ids",
				"Artifact": map[string]any{"Labels": []any{"idnull"}},
			},
		},
		{
			// `id: ""` -> rule_id is the empty string, again not "legacy_rule".
			ID: "ids_present_and_empty_stays_empty",
			Input: map[string]any{
				"Config":   "ids",
				"Artifact": map[string]any{"Labels": []any{"idempty"}},
			},
		},
		{
			// `id` ABSENT -> and only now is it "legacy_rule". These three cases
			// are only distinguishable because the rule's id is decoded as a
			// node rather than a string.
			ID: "ids_absent_is_legacy_rule",
			Input: map[string]any{
				"Config":   "ids",
				"Artifact": map[string]any{"Labels": []any{"idabsent"}},
			},
		},
		{
			// NULL values inside an output block: `.get(key, DEFAULT)` returns
			// None for a present-but-null key, so BOTH fields are None rather
			// than product/general -- despite the Python dataclass annotating
			// investment_area as `str`. The same artifact also traverses an
			// earlier rule whose whole `output:` block is null WITHOUT matching
			// it, which pins that the null-output AttributeError is a property
			// of matching and not of iteration.
			ID: "output_null_values_are_none_not_defaults",
			Input: map[string]any{
				"Config":   "output_nulls",
				"Artifact": map[string]any{"Labels": []any{"nullvalues"}},
			},
		},
		{
			// ONE null output value at a time. The both-null case above cannot
			// tell a port that reads project_stream correctly from one that
			// reads neither -- a single case over a compound condition reports
			// coverage it does not have. This one pins project_stream (F5) with
			// investment_area holding an ordinary value beside it.
			ID: "output_null_project_stream_only",
			Input: map[string]any{
				"Config":   "output_nulls",
				"Artifact": map[string]any{"Labels": []any{"streamnull"}},
			},
		},
		{
			// ...and its mirror image, pinning investment_area alone. Python
			// annotates that field `str` and does not enforce it, so None
			// reaches the call site regardless.
			ID: "output_null_investment_area_only",
			Input: map[string]any{
				"Config":   "output_nulls",
				"Artifact": map[string]any{"Labels": []any{"areanull"}},
			},
		},
		{
			// A YAML MERGE KEY, resolved: the rule matches on the merged label
			// list exactly as PyYAML's constructor makes it.
			ID: "merge_key_is_resolved_into_the_match_block",
			Input: map[string]any{
				"Config":   "merge_keys",
				"Artifact": map[string]any{"Labels": []any{"merged-label"}},
			},
		},
		{
			// The half that catches an UNRESOLVED merge, and the reason merges
			// are mirrored rather than declared. A port that leaves `<<` as an
			// ordinary key sees a match block with no criteria, which matches
			// EVERY artifact -- so it returns merged_area here, where Python
			// falls to legacy_default. Fail-open, and invisible without this
			// case.
			ID: "merge_key_unresolved_would_match_every_artifact",
			Input: map[string]any{
				"Config":   "merge_keys",
				"Artifact": map[string]any{"Labels": []any{"not-the-merged-label"}},
			},
		},
		{
			// MERGE PRECEDENCE: an explicit key beats a merged one even though
			// the `<<` comes first in the mapping. Under last-wins-by-position
			// the merged label would survive and this artifact would miss.
			ID: "merge_explicit_key_beats_merged_key",
			Input: map[string]any{
				"Config":   "merge_precedence",
				"Artifact": map[string]any{"Labels": []any{"explicit"}},
			},
		},
		{
			// ...and the overridden rule really is overridden: "from-first"
			// falls past it to the list-merge rule below.
			ID: "merge_overridden_label_no_longer_matches",
			Input: map[string]any{
				"Config":   "merge_precedence",
				"Artifact": map[string]any{"Labels": []any{"from-first"}},
			},
		},
		{
			// MERGE PRECEDENCE in a list: `<<: [*first, *second]` keeps the
			// FIRST source's label, so the second's must not match at all.
			ID: "merge_first_list_source_beats_the_second",
			Input: map[string]any{
				"Config":   "merge_precedence",
				"Artifact": map[string]any{"Labels": []any{"from-second"}},
			},
		},
		{
			// A BOOLEAN priority. Python's bool IS an int subclass, so `true`
			// sorts as 1 -- BEFORE the 9 beside it -- rather than raising.
			// Routing bool to "Python cannot order this" made the port claim a
			// TypeError Python does not raise, which is the false-mirror class:
			// PythonException is the load-bearing field of the refusal pair, so
			// a wrong value there asserts Python does something it does not.
			ID: "bool_priority_sorts_as_one",
			Input: map[string]any{
				"Config":   "bool_priorities",
				"Artifact": map[string]any{"Labels": []any{}},
			},
		},
		{
			// Pins the VALUE of `true`, not merely that it is numeric. Python's
			// True == 1, so the priority-0 rule sorts first and answers. File
			// order is load-bearing: the true rule is written first, so a port
			// decoding True as 0 would tie them and the stable sort would hand
			// the answer to the true rule. bool_priorities.yaml cannot catch
			// that -- 0 and 1 both sort before 9 -- and a wrong-value decode
			// SURVIVED against it until this case existed.
			ID: "bool_true_priority_is_exactly_one",
			Input: map[string]any{
				"Config":   "bool_true_is_one",
				"Artifact": map[string]any{"Labels": []any{}},
			},
		},
		{
			// The mirror image: False == 0, not 1.
			ID: "bool_false_priority_is_exactly_zero",
			Input: map[string]any{
				"Config":   "bool_false_is_zero",
				"Artifact": map[string]any{"Labels": []any{}},
			},
		},
		{
			// The same path reached through THIS PORT's OWN normalisation:
			// unquoted `yes` is a YAML 1.1 boolean, and the bool pass here
			// manufactures the !!bool node. So the fix has to hold for a node
			// this file created, not only for a literal `true` yaml.v3 tagged.
			ID: "normalised_yes_priority_sorts_as_one",
			Input: map[string]any{
				"Config":   "yes_priorities",
				"Artifact": map[string]any{"Labels": []any{}},
			},
		},
		{
			// `priority:` PRESENT and NULL with exactly ONE rule. sorted()
			// computes every key up front but never compares a single element,
			// so Python does NOT raise. The two-rule form is a refusal case;
			// this pair is what stops "any null priority is fatal" from passing
			// as a mirror.
			ID: "priority_null_single_rule_does_not_raise",
			Input: map[string]any{
				"Config":   "priority_null_single",
				"Artifact": map[string]any{"Labels": []any{"lonely"}},
			},
		},
		{
			// `path_prefix:` PRESENT and NULL, with an artifact carrying NO
			// paths. Python resolves the prefix list inside the loop over the
			// artifact's paths, so the loop body never runs and the arm merely
			// rejects. The same file WITH paths is a refusal case.
			ID: "path_prefix_null_without_paths_only_rejects",
			Input: map[string]any{
				"Config":   "path_prefix_null",
				"Artifact": map[string]any{"Labels": []any{}},
			},
		},
		{
			// A DUPLICATE mapping key. PyYAML keeps the LAST silently, so
			// rule_id is "second". yaml.v3's typed decoder rejects the document
			// outright, which is why the port walks nodes -- this was a declared
			// divergence before that change and is a mirrored quirk after it.
			ID: "duplicate_id_key_keeps_the_last",
			Input: map[string]any{
				"Config":   "duplicate_ids",
				"Artifact": map[string]any{"Labels": []any{}},
			},
		},
		{
			// A BARE STRING `component:` makes `in` mean SUBSTRING CONTAINMENT.
			// "analy" is a strict substring of "analytics" and is not equal to
			// it, so a mirror that used equality would reject here.
			ID: "bare_string_component_is_substring_containment",
			Input: map[string]any{
				"Config": "bare_component",
				"Artifact": map[string]any{
					"Labels": []any{"barecomponent"}, "Component": "analy",
				},
			},
		},
		{
			// The negative half: a component that is NOT a substring rejects,
			// so the case above is not passing merely because the arm was
			// skipped.
			ID: "bare_string_component_rejects_a_non_substring",
			Input: map[string]any{
				"Config": "bare_component",
				"Artifact": map[string]any{
					"Labels": []any{"barecomponent"}, "Component": "zzz",
				},
			},
		},
		{
			// A BARE STRING `label:` is ITERATED, so the target set is the
			// string's characters: the single letter "s" matches `label:
			// security`.
			ID: "bare_string_label_iterates_characters",
			Input: map[string]any{
				"Config":   "bare_label",
				"Artifact": map[string]any{"Labels": []any{"s"}},
			},
		},
		{
			// ...and the whole word does NOT, which is what proves the previous
			// case measured character iteration rather than an ordinary match.
			ID: "bare_string_label_does_not_match_the_whole_word",
			Input: map[string]any{
				"Config":   "bare_label",
				"Artifact": map[string]any{"Labels": []any{"security"}},
			},
		},
		{
			// `always:` holding a non-empty STRING is TRUTHY and short-circuits
			// to a match -- including the quoted "no" that YAML would otherwise
			// resolve to the boolean false. Contrast
			// quirks_always_false_falls_through, where it is a real boolean.
			ID: "always_non_empty_string_is_truthy",
			Input: map[string]any{
				"Config":   "always_truthy_string",
				"Artifact": map[string]any{"Labels": []any{}},
			},
		},
		{
			// An UNQUOTED `always: no` is the BOOLEAN false to PyYAML's YAML 1.1
			// resolver, so the rule does not short-circuit and falls through to
			// its label criterion -- which this artifact satisfies.
			ID: "always_unquoted_no_is_false_and_falls_through",
			Input: map[string]any{
				"Config":   "always_unquoted_no",
				"Artifact": map[string]any{"Labels": []any{"unquotedno"}},
			},
		},
		{
			// The half that catches the resolver difference. To yaml.v3's YAML
			// 1.2 core schema the same word is the non-empty STRING "no", which
			// is TRUTHY: an unnormalised port short-circuits here and classifies
			// EVERY artifact under this rule. This case must fall to
			// legacy_default.
			ID: "always_unquoted_no_does_not_short_circuit",
			Input: map[string]any{
				"Config":   "always_unquoted_no",
				"Artifact": map[string]any{"Labels": []any{"something-else"}},
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
			// A refusal here is itself a divergence -- Python classified this
			// case, so Go refusing it is the declared, loud direction and must
			// stop the run. The message names the config AND the artifact
			// because those two together identify the case: t.Fatal fires while
			// the row is being BUILT, before the comparator has opened the
			// per-case subtest that would otherwise supply the name, and two
			// cases in this list can share a config file.
			classifier, err := NewInvestmentClassifier(
				investmentConfigPath(t, input["Config"].(string)),
			)
			if err != nil {
				t.Fatalf("load config %q for artifact %v: %v",
					input["Config"], input["Artifact"], err)
			}
			classification, err := classifier.Classify(investmentOracleArtifact(input))
			if err != nil {
				t.Fatalf("classify config %q for artifact %v: %v",
					input["Config"], input["Artifact"], err)
			}
			return classification
		},
		nil,
	)
}

// investmentRefusalRow is the compared surface of the refusal pair: the NAME of
// the Python exception class, which the Go side reproduces from its own
// *InvestmentConfigError. Comparing the class rather than a bare "both refused"
// boolean is what makes a Go engine that refuses everything for one blanket
// reason fail -- Python's AttributeError (dereferencing a None as a dict) and
// TypeError (iterating or comparing one) are two different bugs in the same
// file, and a port that cannot tell them apart has not mirrored either.
// The classification fields ride along, and must all be nil: a port that
// refused and still produced a classification would differ here. They are
// compared rather than declared as exclusions because they are absent for a
// reason that belongs to the DATA (neither engine produced one), not to the
// pair.
type investmentRefusalRow struct {
	Raises         string   `json:"raises"`
	InvestmentArea *string  `json:"investment_area"`
	ProjectStream  *string  `json:"project_stream"`
	Confidence     *float64 `json:"confidence"`
	RuleID         *string  `json:"rule_id"`
}

func investmentRefusalCases() []oracleCase {
	return []oracleCase{
		// --- shapes that raise at LOAD ---
		{
			// A zero-byte file: safe_load returns None and `data.get(...)` is
			// an attribute lookup on None. A typed Go decode read this as "no
			// rules" and happily returned product/general/0.0/legacy_default.
			ID:    "raises_empty_file",
			Input: map[string]any{"Config": "raises_empty"},
		},
		{
			// The same None, from a file that has bytes in it. Separate case
			// because a reader would not expect a file with content to parse to
			// nothing, and a decode that special-cased "zero length" would pass
			// the case above while failing this one.
			ID:    "raises_comment_only_file",
			Input: map[string]any{"Config": "raises_comments_only"},
		},
		{
			// `rules:` PRESENT and NULL. The `.get` default is NOT applied,
			// because the key exists, so sorted() is handed None.
			ID:    "raises_rules_key_present_and_null",
			Input: map[string]any{"Config": "raises_rules_null"},
		},
		{
			// The document is a list, so `data.get` is an attribute lookup on a
			// list rather than on None -- a different Python type reaching the
			// same failure, which a decode that only checked for null would
			// miss.
			ID:    "raises_document_is_a_list",
			Input: map[string]any{"Config": "raises_document_is_list"},
		},
		{
			// A rules ENTRY that is not a mapping. sorted() applies its key
			// function to every element before comparing anything, so this
			// raises at load even with a single entry.
			ID:    "raises_rule_entry_is_not_a_mapping",
			Input: map[string]any{"Config": "raises_rule_not_mapping"},
		},
		{
			// `priority:` PRESENT and NULL with TWO rules, so sorted() actually
			// compares. Its single-rule counterpart is a classify case that must
			// NOT raise.
			ID:    "raises_null_priority_with_two_rules",
			Input: map[string]any{"Config": "raises_priority_null"},
		},
		{
			// MIXED priority types: one int, one string. Python compares only
			// WITHIN a type, so sorted() raises -- while an ALL-string config
			// sorts fine and is a declared divergence instead. Without this case
			// "non-numeric priority" is one undifferentiated branch, and the
			// port could claim a mirrored TypeError for the config Python
			// happily sorts.
			ID:    "raises_mixed_int_and_string_priorities",
			Input: map[string]any{"Config": "raises_mixed_priority_types"},
		},
		{
			// A DATE priority beside an INT one. Python compares only within a
			// type, so this raises -- while an ALL-date config sorts cleanly and
			// is a declared divergence. The two together stop "date" from being
			// one undifferentiated branch.
			ID:    "raises_date_priority_beside_an_int",
			Input: map[string]any{"Config": "raises_date_and_int_priorities"},
		},
		{
			// A SEXAGESIMAL in a label list. PyYAML resolves `1:30` to the int
			// 90, and an int has no .lower(). This was FAIL-OPEN before the
			// literal was recognised: yaml.v3 leaves it a string, so the port
			// lower-cased it, matched nothing, and quietly classified where
			// Python refuses.
			ID:    "raises_sexagesimal_label_entry",
			Input: map[string]any{"Config": "raises_sexagesimal_label"},
		},
		{
			// A `<<` whose value is neither a mapping nor a list of mappings.
			// PyYAML raises while CONSTRUCTING the document, so this dies before
			// _load_rules runs at all -- and it is the only case here whose
			// exception class is neither AttributeError nor TypeError, which is
			// what stops the phase-and-class value from being guessable.
			ID:    "raises_merge_source_is_not_a_mapping",
			Input: map[string]any{"Config": "raises_merge_source_not_a_mapping"},
		},
		// --- shapes that raise at CLASSIFY ---
		{
			// `match:` PRESENT and NULL. A typed decode read this as "no match
			// block", which matches EVERYTHING: the rule FIRED and returned a
			// classification where Python raises. That is the worst shape in
			// this list -- not a wrong value, an invented one.
			ID:    "raises_null_match_block",
			Input: map[string]any{"Config": "raises_match_null"},
		},
		{
			// `output:` PRESENT and NULL on a rule that MATCHES.
			ID:    "raises_null_output_block_on_a_matching_rule",
			Input: map[string]any{"Config": "raises_output_null"},
		},
		{
			// `label:` PRESENT and NULL: the arm runs because the key exists,
			// and the generator iterates None.
			ID:    "raises_null_label_list",
			Input: map[string]any{"Config": "raises_label_null"},
		},
		{
			// A label entry YAML resolves to a BOOLEAN -- `no` is False to
			// PyYAML, not the string "no" -- and False has no .lower(). The
			// artifact carries no labels, which pins that the whole generator is
			// consumed before any intersection is taken.
			ID:    "raises_boolean_label_entry",
			Input: map[string]any{"Config": "raises_label_bool"},
		},
		{
			// The same arm with an INT entry. Its own case so the bool case
			// cannot stand in for it.
			ID:    "raises_integer_label_entry",
			Input: map[string]any{"Config": "raises_label_int"},
		},
		{
			// `component:` PRESENT and NULL: `x not in None` is a containment
			// test against something that is not a container.
			ID:    "raises_null_component_list",
			Input: map[string]any{"Config": "raises_component_null"},
		},
		{
			// `path_prefix:` PRESENT and NULL, WITH paths on the artifact. The
			// same file with no paths is a classify case that must not raise:
			// Python resolves the prefix list inside the loop over the
			// artifact's paths, so the raise belongs to the artifact, not to the
			// config.
			ID: "raises_null_path_prefix_only_when_paths_are_supplied",
			Input: map[string]any{
				"Config": "path_prefix_null",
				"Artifact": map[string]any{
					"Paths": []any{"services/api/handler.go"},
				},
			},
		},
	}
}

func TestInvestmentClassifierRefusesWhatLivePythonRefuses(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"analytics/investment/refusal",
		investmentRefusalCases(),
		func(t *testing.T, input map[string]any) investmentRefusalRow {
			t.Helper()
			return investmentRefusalRow{
				Raises: investmentGoRefusal(t, input),
			}
		},
		nil,
	)
}

// investmentGoRefusal returns the Python exception class this port claims for a
// case, or a DESCRIPTION of what it did instead. It never returns "" for a
// non-refusal: the description is compared against Python's class name and
// therefore fails loudly, carrying the classification Go invented into the
// failure message rather than reporting a bare inequality.
// The value is PHASE-QUALIFIED ("construct:TypeError", "classify:AttributeError")
// because which of the two raises is a real property of the config shape, not an
// implementation detail: `rules:` null dies in _load_rules, while `match:` null
// LOADS FINE and dies only once classify() reaches that rule. A port that
// refuses the right file for the right exception class at the wrong moment
// rejects a classifier Python is willing to construct, and comparing the class
// alone would let that pass as agreement.
func investmentGoRefusal(t *testing.T, input map[string]any) string {
	t.Helper()
	phase := "construct"
	classifier, err := NewInvestmentClassifier(
		investmentConfigPath(t, input["Config"].(string)),
	)
	if err == nil {
		phase = "classify"
		var classification InvestmentClassification
		classification, err = classifier.Classify(investmentOracleArtifact(input))
		if err == nil {
			return fmt.Sprintf(
				"<no refusal: classified as %s>",
				investmentDescribeClassification(classification))
		}
	}
	var configError *InvestmentConfigError
	if !errors.As(err, &configError) {
		return fmt.Sprintf("<%s: not a mirrored refusal: %v>", phase, err)
	}
	if configError.PythonException == "" {
		// A declared divergence is Go refusing where Python PROCEEDS. Letting
		// one satisfy a refusal case would be the framework agreeing with
		// itself: the case says Python raises, and this says it does not.
		return fmt.Sprintf("<declared divergence, not a mirror: %s>", configError.Detail)
	}
	return phase + ":" + configError.PythonException
}

// investmentDeclaredDivergenceRow asserts the RELATIONSHIP between the engines
// for a shape this port declares it does not mirror, rather than either
// engine's answer.
//
// Relation carries a sentinel both sides emit only when their own half holds --
// Python classified, and this port refused with a DECLARED refusal. The rows
// therefore match only when the whole declaration is still true. A divergence
// that lives only in a comment decays silently the day someone makes the
// engines agree; this one fails the build instead.
//
// GoRefusal is declared through the harness's goOnlyFields mechanism, which
// enforces both directions of its own claim: it fails if the field ever shows
// up on the Python side, and it fails if it never shows up on the Go side.
type investmentDeclaredDivergenceRow struct {
	Relation  string  `json:"relation"`
	GoRefusal *string `json:"go_refusal"`
}

func investmentDeclaredDivergenceCases() []oracleCase {
	return []oracleCase{
		{
			// `id: 7` -- Python's rule_id is the int 7, in a field its own
			// dataclass annotates `str`.
			ID:    "declared_rule_id_is_not_a_string",
			Input: map[string]any{"Config": "declared_id_not_a_string"},
		},
		{
			// The same shape on an output field: `investment_area: true`.
			ID:    "declared_investment_area_is_not_a_string",
			Input: map[string]any{"Config": "declared_area_not_a_string"},
		},
		{
			// Every priority is a string, so they share one orderable Python
			// type and sorted() succeeds lexicographically. Its neighbours are
			// mirrored TypeErrors -- a null priority, or a mixed int/str
			// config -- which is what makes this a divergence rather than a
			// missing branch.
			ID:    "declared_all_string_priorities_sort_lexicographically",
			Input: map[string]any{"Config": "declared_string_priorities"},
		},
		{
			// A leading-zero priority: octal 8 to PyYAML, decimal 10 to
			// yaml.v3, so the two engines order the rules differently.
			ID:    "declared_leading_zero_priority_is_octal_to_pyyaml",
			Input: map[string]any{"Config": "declared_octal_priority"},
		},
		{
			// Every priority is a DATE. PyYAML resolves plain `2024-01-02` to
			// datetime.date and orders dates among themselves, so the 2023 rule
			// answers; this port has no ordering to offer that is Python's.
			ID:    "declared_all_date_priorities_order_among_dates",
			Input: map[string]any{"Config": "declared_date_priorities"},
		},
		{
			// A SEXAGESIMAL priority. `1:30` is the int 90 to PyYAML and a
			// string to yaml.v3, so Python sorts 9 first and this port cannot
			// reproduce the order. Note it is NOT a mixed-type raise -- 90 is an
			// int, so it shares an ordering group with 9.
			ID:    "declared_sexagesimal_priority_is_ninety_to_pyyaml",
			Input: map[string]any{"Config": "declared_sexagesimal_priority"},
		},
		{
			// The same literal as a rule ID, where Python's value is the NUMBER
			// 90 and a *string cannot hold it. Refusing is loud; emitting
			// "1:30" would be a silent wrong value.
			ID:    "declared_sexagesimal_rule_id_is_a_number",
			Input: map[string]any{"Config": "declared_sexagesimal_id"},
		},
		{
			// A merge key pointing at its own mapping. PyYAML builds a
			// self-referential dict and classifies; this port refuses rather
			// than recurse.
			ID:    "declared_merge_key_cycle",
			Input: map[string]any{"Config": "declared_merge_cycle"},
		},
	}
}

func TestInvestmentClassifierDeclaredDivergencesStillDiverge(t *testing.T) {
	sentinel := investmentDivergenceSentinel(t)
	compareRowsAgainstPythonOracle(
		t,
		"analytics/investment/divergence",
		investmentDeclaredDivergenceCases(),
		func(t *testing.T, input map[string]any) investmentDeclaredDivergenceRow {
			t.Helper()
			return investmentGoDeclaredDivergence(t, sentinel, input)
		},
		map[string]string{
			"go_refusal": "the text of this port's refusal. It exists on the Go " +
				"side only BY CONSTRUCTION -- Python classifies these shapes, which " +
				"is half of what each case asserts -- so the harness's own check " +
				"that a goOnlyField never appears on the Python side is itself an " +
				"assertion that the divergence still has the shape claimed.",
		},
	)
}

func investmentGoDeclaredDivergence(
	t *testing.T, sentinel string, input map[string]any,
) investmentDeclaredDivergenceRow {
	t.Helper()
	classifier, err := NewInvestmentClassifier(
		investmentConfigPath(t, input["Config"].(string)),
	)
	var classification InvestmentClassification
	if err == nil {
		classification, err = classifier.Classify(investmentOracleArtifact(input))
	}
	if err == nil {
		return investmentDeclaredDivergenceRow{
			Relation: fmt.Sprintf(
				"<BOTH ENGINES CLASSIFY (%s): the declared divergence is GONE. If "+
					"that is deliberate, delete the declaration in "+
					"investment_classifier.go and the PR body and move this case to "+
					"the classify pair -- do not leave a note describing a divergence "+
					"that no longer exists>",
				investmentDescribeClassification(classification)),
		}
	}
	var configError *InvestmentConfigError
	if !errors.As(err, &configError) {
		return investmentDeclaredDivergenceRow{
			Relation: fmt.Sprintf("<refused with an unclassified error: %v>", err),
		}
	}
	if configError.PythonException != "" {
		return investmentDeclaredDivergenceRow{
			Relation: fmt.Sprintf(
				"<refused as a MIRROR of Python %s, but Python classified this "+
					"config -- a mirrored refusal cannot stand in for a declared one>",
				configError.PythonException),
		}
	}
	detail := configError.Detail
	return investmentDeclaredDivergenceRow{Relation: sentinel, GoRefusal: &detail}
}

// investmentDivergenceSentinel reads the sentinel out of the Python pair rather
// than restating it in Go. Hand-copying it would put the agreed sentence in two
// places, and the copy that drifts is the one nobody reads.
func investmentDivergenceSentinel(t *testing.T) string {
	t.Helper()
	source := filepath.Join(investmentRepoRoot(t),
		"internal/providersync/testdata/oracle_pairs/analytics_investment_divergence.py")
	contents, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile(`(?m)^DIVERGENCE_HOLDS = "([^"]+)"$`).
		FindSubmatch(contents)
	if match == nil {
		t.Fatalf("no DIVERGENCE_HOLDS assignment found in %s -- the Go side cannot "+
			"emit a sentinel it cannot read, and hard-coding a copy here is what "+
			"this lookup exists to avoid", source)
	}
	return string(match[1])
}

func investmentDescribeClassification(classification InvestmentClassification) string {
	return fmt.Sprintf(
		"area=%s stream=%s confidence=%v rule=%s",
		investmentDescribeString(classification.InvestmentArea),
		investmentDescribeString(classification.ProjectStream),
		classification.Confidence,
		investmentDescribeString(classification.RuleID))
}

func investmentDescribeString(value *string) string {
	if value == nil {
		return "None"
	}
	return fmt.Sprintf("%q", *value)
}

func investmentOracleArtifact(input map[string]any) InvestmentArtifact {
	raw, _ := input["Artifact"].(map[string]any)
	artifact := InvestmentArtifact{Provider: "github"}
	if raw == nil {
		return artifact
	}
	artifact.Labels = investmentOracleStrings(raw, "Labels")
	// Paths and Component are populated ONLY when the case sets them, matching
	// _investment_helpers.artifact on the Python side. The production call site
	// always supplies component (always "") and never supplies paths, and an
	// ABSENT key is a different value from "" for the membership test and a
	// different control flow for the path arm -- defaulting either here would
	// make those states unreachable from any case.
	if _, present := raw["Paths"]; present {
		artifact.Paths = investmentOracleStrings(raw, "Paths")
	}
	if value, ok := raw["Component"].(string); ok {
		artifact.Component = investmentString(value)
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
