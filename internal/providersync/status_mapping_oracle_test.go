package providersync

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
)

// Differential oracles for the StatusMapping port. Two pairs cross two
// genuinely different boundaries and are NOT interchangeable:
//
//   - status/mapping/load compares the four index maps the loader builds, in
//     full, PLUS the outcome (phase + Python exception class) when loading
//     fails.
//   - status/mapping/normalize compares BOTH normalizers' outputs per case. Its
//     Python-side reflector parses StatusMapping's own `normalize_*` method
//     names, so neither method can drop out of the comparison silently.
//
// Both sides resolve a config NAME through the same rule, so a case names a
// config and neither engine can quietly read a different file.
//
// The case tables below pin EVERY row that was executed during review, not a
// representative subset: the whole point is that verification runs through the
// real code on both sides rather than against a hand-copied summary of it.

const statusMappingPathEnvVar = "STATUS_MAPPING_PATH"

// d20GoBehaviour is the Go half of the declared D20 timestamp divergence. It is
// deliberately unequal to the Python half and is declared in the pair's
// excluded_fields with a written reason; see that registration, and
// TestTimestampScalarIsDeclaredDivergenceD20 for the assertion that the
// divergence still exists.
const d20GoBehaviour = "go:refuses-timestamp-loudly"

func statusMappingRepoRoot(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed: cannot locate the repository root")
	}
	return filepath.Join(filepath.Dir(currentFile), "..", "..")
}

// resolveStatusMappingConfig mirrors resolve_config in the Python pair.
func resolveStatusMappingConfig(t *testing.T, name string) string {
	t.Helper()
	root := statusMappingRepoRoot(t)
	if name == "real" {
		return filepath.Join(root, "src/dev_health_ops/config/status_mapping.yaml")
	}
	return filepath.Join(
		root, "internal/providersync/testdata/status_mapping_configs", name+".yaml")
}

// applyStatusMappingEnv mirrors the Python pair's status_mapping_path_env: set
// the variable when the case declares one, and make sure it is EMPTY otherwise.
// Clearing is not bookkeeping -- an ambient STATUS_MAPPING_PATH would redirect
// every explicit-path case at one file and make the pair pass vacuously.
func applyStatusMappingEnv(t *testing.T, input map[string]any) {
	t.Helper()
	if name, ok := input["env_config"].(string); ok && name != "" {
		t.Setenv(statusMappingPathEnvVar, resolveStatusMappingConfig(t, name))
		return
	}
	t.Setenv(statusMappingPathEnvVar, "")
}

func loadStatusMappingForCase(t *testing.T, input map[string]any) (*StatusMapping, error) {
	t.Helper()
	applyStatusMappingEnv(t, input)
	explicit := ""
	if name, ok := input["explicit_config"].(string); ok && name != "" {
		explicit = resolveStatusMappingConfig(t, name)
	}
	return LoadStatusMapping(explicit)
}

// loadOutcome renders a load error the way the Python pair renders an
// exception: "<phase>:<ExceptionType>". A Go error that is NOT a mirrored
// Python failure is reported verbatim so it can never masquerade as one.
func loadOutcome(err error) string {
	if err == nil {
		return "ok"
	}
	if kind := pythonFailureKind(err); kind != "" {
		return "load:" + kind
	}
	return "load:UNMIRRORED(" + err.Error() + ")"
}

// pythonStrOfCaseValue mirrors Python's str() over the JSON-ish values a case
// can carry. The harness previously SKIPPED non-string labels while Python
// str()'d them, which quietly narrowed every label case to the string subset.
func pythonStrOfCaseValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return "None"
	case string:
		return typed
	case bool:
		if typed {
			return "True"
		}
		return "False"
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case float64:
		if typed == math.Trunc(typed) && !math.IsInf(typed, 0) && !math.IsNaN(typed) {
			return pythonReprFloat(typed)
		}
		return pythonReprFloat(typed)
	default:
		return fmt.Sprintf("%v", typed)
	}
}

// statusMappingLoadRow is the Go row for status/mapping/load.
//
// The four index maps are POINTERS so that "Python raised, there is no mapping"
// encodes as null rather than as an empty map -- an empty map is a legitimate,
// different result (see structural_categories_empty_list) and the two must not
// collapse into the same row.
type statusMappingLoadRow struct {
	Outcome               string                        `json:"outcome"`
	StatusByProvider      *map[string]map[string]string `json:"status_by_provider"`
	LabelStatusByProvider *map[string]map[string]string `json:"label_status_by_provider"`
	TypeByProvider        *map[string]map[string]string `json:"type_by_provider"`
	LabelTypeByProvider   *map[string]map[string]string `json:"label_type_by_provider"`
	D20TimestampProbe     string                        `json:"d20_timestamp_probe"`
}

func buildStatusMappingLoadRow(t *testing.T, input map[string]any) statusMappingLoadRow {
	t.Helper()
	mapping, err := loadStatusMappingForCase(t, input)
	row := statusMappingLoadRow{
		Outcome:           loadOutcome(err),
		D20TimestampProbe: d20GoBehaviour,
	}
	if err != nil {
		return row
	}
	row.StatusByProvider = &mapping.StatusByProvider
	row.LabelStatusByProvider = &mapping.LabelStatusByProvider
	row.TypeByProvider = &mapping.TypeByProvider
	row.LabelTypeByProvider = &mapping.LabelTypeByProvider
	return row
}

func configCase(id, config string) oracleCase {
	return oracleCase{ID: id, Input: map[string]any{"explicit_config": config}}
}

func TestStatusMappingLoadMatchesPythonOracle(t *testing.T) {
	cases := []oracleCase{
		// The REAL config: CHAOS-3505 (no linear key), the empty github/gitlab
		// `types` sections, the empty jira `type_labels`, and the CHAOS-3512
		// `{'type': 'bug'}` key produced by str() on the YAML MAPPING at line 82.
		configCase("real_config", "real"),

		// Loader quirks the real file cannot express.
		configCase("quirks_synthetic", "quirks"),
		configCase("empty_file", "empty"),
		configCase("type_priority_gap_config", "type_priority_gap"),

		// --- R3/R4: PyYAML 1.1 vs go-yaml 1.2 scalar resolution. go-yaml calls
		// `yes` a string and `1e3` a float; PyYAML calls the first a bool and
		// the second a string. Every row in this config diverged before the fix.
		configCase("pyyaml_scalar_resolution", "pyyaml_scalars"),

		// --- R2: duplicate keys. Last value wins, first position kept; the
		// duplicated category must NOT apply twice as a union.
		configCase("duplicate_keys_last_wins", "duplicate_keys"),

		// --- R5: full Unicode case mapping. İ and final sigma fold differently
		// under Python's str.lower() than under Go's strings.ToLower.
		configCase("unicode_case_folding", "case_folding"),

		// --- R1 BLOCKER: truthy non-mapping sections. Python raises
		// AttributeError; returning empty maps instead is silent metric
		// corruption. Each shape is pinned separately because each reaches a
		// DIFFERENT `.items()`/`.get()` call site.
		configCase("structural_categories_list", "structural_categories_list"),
		configCase("structural_providers_scalar", "structural_providers_scalar"),
		configCase("structural_provider_scalar", "structural_provider_scalar"),
		configCase("structural_statuses_list", "structural_statuses_list"),
		configCase("structural_type_labels_scalar", "structural_type_labels_scalar"),
		configCase("structural_root_list", "structural_root_list"),

		// The counterpart that keeps the six above honest: a FALSY non-mapping
		// is swallowed by `or {}` and must NOT raise. Without this, "error on
		// non-mapping" could be over-applied and nothing would notice.
		configCase("structural_categories_empty_list", "structural_categories_empty_list"),

		// --- Python's polymorphic iteration, which the loader never constrains.
		configCase("iteration_string_indexes_characters", "iteration_string_value"),
		configCase("iteration_mapping_indexes_keys", "iteration_mapping_value"),
		configCase("iteration_int_raises_typeerror", "iteration_int_value"),
		configCase("iteration_null_is_skipped", "iteration_null_value"),

		// QUIRK 7: the env var replaces the explicit argument. Discriminating by
		// construction -- the two configs have disjoint contents.
		{
			ID: "env_overrides_explicit_path",
			Input: map[string]any{
				"explicit_config": "quirks",
				"env_config":      "type_priority_gap",
			},
		},
	}

	compareRowsAgainstPythonOracle(
		t, "status/mapping/load", cases, buildStatusMappingLoadRow, nil)
}

// statusMappingNormalizeRow is the Go row for status/mapping/normalize. One
// field per `normalize_*` method the Python class declares, plus the outcome so
// a load failure is distinguishable from a normalize failure.
type statusMappingNormalizeRow struct {
	Outcome         string  `json:"outcome"`
	NormalizeStatus *string `json:"normalize_status"`
	NormalizeType   *string `json:"normalize_type"`
}

func buildStatusMappingNormalizeRow(
	t *testing.T, input map[string]any,
) statusMappingNormalizeRow {
	t.Helper()
	mapping, err := loadStatusMappingForCase(t, input)
	if err != nil {
		return statusMappingNormalizeRow{Outcome: loadOutcome(err)}
	}
	provider, _ := input["provider"].(string)
	labels := make([]string, 0)
	if raw, ok := input["labels"].([]any); ok {
		for _, label := range raw {
			labels = append(labels, pythonStrOfCaseValue(label))
		}
	}
	optional := func(key string) string {
		value, _ := input[key].(string)
		return value
	}
	status := mapping.NormalizeStatus(
		provider, optional("status_raw"), labels, optional("state"))
	itemType := mapping.NormalizeType(provider, optional("type_raw"), labels)
	return statusMappingNormalizeRow{
		Outcome:         "ok",
		NormalizeStatus: &status,
		NormalizeType:   &itemType,
	}
}

func normalizeCase(id string, input map[string]any) oracleCase {
	return oracleCase{ID: id, Input: input}
}

func TestStatusMappingNormalizeMatchesPythonOracle(t *testing.T) {
	withConfig := func(config string, extra map[string]any) map[string]any {
		input := map[string]any{"explicit_config": config}
		for key, value := range extra {
			input[key] = value
		}
		return input
	}
	real := func(extra map[string]any) map[string]any { return withConfig("real", extra) }
	gap := func(extra map[string]any) map[string]any {
		return withConfig("type_priority_gap", extra)
	}
	folding := func(extra map[string]any) map[string]any {
		return withConfig("case_folding", extra)
	}
	scalars := func(extra map[string]any) map[string]any {
		return withConfig("pyyaml_scalars", extra)
	}

	cases := []oracleCase{
		// --- CHAOS-3512, pinned on the REAL config ---
		normalizeCase("gh_conventional_type_bug_label_misses",
			real(map[string]any{"provider": "github", "labels": []any{"type:bug"}})),
		normalizeCase("gl_scoped_type_bug_label_hits",
			real(map[string]any{"provider": "gitlab", "labels": []any{"type::bug"}})),
		normalizeCase("gh_python_dict_repr_label_hits",
			real(map[string]any{"provider": "github", "labels": []any{"{'type': 'bug'}"}})),

		// --- label matches are a SET: order is irrelevant, priority decides ---
		normalizeCase("label_order_bug_then_incident",
			real(map[string]any{"provider": "github", "labels": []any{"bug", "incident"}})),
		normalizeCase("label_order_incident_then_bug",
			real(map[string]any{"provider": "github", "labels": []any{"incident", "bug"}})),

		// --- github/gitlab have NO `types` section ---
		normalizeCase("gh_type_raw_looks_mappable_still_issue",
			real(map[string]any{"provider": "github", "type_raw": "Bug"})),
		normalizeCase("jira_type_raw_maps_with_case_and_padding",
			real(map[string]any{"provider": "jira", "type_raw": "  BUG  "})),

		// --- CHAOS-3505 ---
		normalizeCase("linear_provider_ignored_entirely",
			real(map[string]any{
				"provider": "linear", "labels": []any{"bug"}, "status_raw": "In Progress"})),
		normalizeCase("unknown_provider_defaults_unknown",
			real(map[string]any{"provider": "nope"})),

		// --- truthiness collapse: None and "" take the same branch ---
		normalizeCase("raw_absent_none", real(map[string]any{"provider": "jira"})),
		normalizeCase("raw_absent_empty_string",
			real(map[string]any{"provider": "jira", "type_raw": "", "status_raw": ""})),

		// --- _norm_key internal whitespace collapse ---
		normalizeCase("status_internal_whitespace_collapsed",
			real(map[string]any{"provider": "jira", "status_raw": "In    Progress"})),

		// --- state fallback, each set member separately ---
		normalizeCase("state_fallback_closed_after_status_miss",
			real(map[string]any{
				"provider": "github", "status_raw": "nomatch", "state": "Closed"})),
		normalizeCase("state_fallback_opened",
			real(map[string]any{"provider": "github", "state": "opened"})),
		normalizeCase("state_fallback_merged",
			real(map[string]any{"provider": "github", "state": "merged"})),
		normalizeCase("state_fallback_done_word",
			real(map[string]any{"provider": "github", "state": "  DONE  "})),
		normalizeCase("status_label_priority_done_outranks_todo",
			real(map[string]any{"provider": "github", "labels": []any{"todo", "done"}})),

		// --- QUIRK 8: the _TYPE_PRIORITY gap ---
		normalizeCase("type_gap_pr_alone_falls_through_to_type_raw",
			gap(map[string]any{
				"provider": "github", "type_raw": "Some Raw Type",
				"labels": []any{"PR Label"}})),
		normalizeCase("type_gap_pr_alone_no_type_raw_hits_default",
			gap(map[string]any{"provider": "github", "labels": []any{"PR Label"}})),
		normalizeCase("type_gap_pr_with_bug_bug_wins",
			gap(map[string]any{
				"provider": "github", "type_raw": "Some Raw Type",
				"labels": []any{"PR Label", "Bug Label"}})),
		normalizeCase("type_gap_merge_request_alone_gitlab",
			gap(map[string]any{"provider": "gitlab", "labels": []any{"MR Label"}})),

		// --- QUIRK 7 at the normalize boundary, deliberately discriminating ---
		normalizeCase("env_path_replaces_explicit_argument",
			real(map[string]any{
				"env_config": "type_priority_gap",
				"provider":   "github", "labels": []any{"bug"}})),

		// --- R5: full case mapping. The index holds Python-lowercased keys, so
		// these only match if BOTH sides fold the label the same way. İSSUE is
		// the row where strings.ToLower matched and Python missed.
		normalizeCase("case_folding_dotted_capital_i",
			folding(map[string]any{"provider": "github", "labels": []any{"İSSUE"}})),
		normalizeCase("case_folding_dotted_capital_i_lowercase_input",
			folding(map[string]any{"provider": "github", "labels": []any{"issue"}})),
		normalizeCase("case_folding_final_sigma",
			folding(map[string]any{"provider": "github", "labels": []any{"ΤΕΛΟΣ"}})),
		normalizeCase("case_folding_final_sigma_lowercase_input",
			folding(map[string]any{"provider": "github", "labels": []any{"τελος"}})),
		normalizeCase("case_folding_sharp_s",
			folding(map[string]any{"provider": "github", "labels": []any{"STRASSE"}})),

		// --- R3 at the normalize boundary: a label whose INDEX KEY only exists
		// if the scalar resolved the PyYAML way. "true" is the key `- yes`
		// produces; a port that kept go-yaml's string would index "yes" instead.
		normalizeCase("scalar_bool_label_resolves_to_true",
			scalars(map[string]any{"provider": "jira", "status_raw": "True"})),
		normalizeCase("scalar_sexagesimal_label",
			scalars(map[string]any{"provider": "jira", "status_raw": "43200"})),
		normalizeCase("scalar_dotless_exponent_stays_string",
			scalars(map[string]any{"provider": "jira", "status_raw": "1e3"})),
		normalizeCase("scalar_infinity_repr",
			scalars(map[string]any{"provider": "jira", "status_raw": "inf"})),
		normalizeCase("scalar_nan_repr",
			scalars(map[string]any{"provider": "jira", "status_raw": "nan"})),

		// --- minor: Python str()s every label; the harness must not skip
		// non-strings. These are only meaningful because _labels/pythonStrOf
		// CaseValue now agree.
		normalizeCase("non_string_label_int",
			real(map[string]any{"provider": "github", "labels": []any{5}})),
		normalizeCase("non_string_label_bool",
			real(map[string]any{"provider": "github", "labels": []any{true}})),
		normalizeCase("non_string_label_null",
			real(map[string]any{"provider": "github", "labels": []any{nil}})),

		// --- a load failure surfaces at the NORMALIZE pair too, with the LOAD
		// phase recorded, so an error cannot silently move phases.
		normalizeCase("load_failure_reported_with_load_phase",
			withConfig("structural_statuses_list", map[string]any{"provider": "jira"})),
	}

	compareRowsAgainstPythonOracle(
		t, "status/mapping/normalize", cases, buildStatusMappingNormalizeRow, nil)
}

// TestStatusMappingEnvIsNotAmbient guards the guard: if the harness ever stopped
// clearing STATUS_MAPPING_PATH, every "explicit path" case above would silently
// read whatever the ambient environment pointed at.
func TestStatusMappingEnvIsNotAmbient(t *testing.T) {
	t.Setenv(statusMappingPathEnvVar, filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if _, err := LoadStatusMapping(resolveStatusMappingConfig(t, "real")); err == nil {
		t.Fatal("expected the environment variable to REPLACE the explicit path and " +
			"fail on the missing file; it did not, so quirk 7 is no longer mirrored " +
			"and every explicit-path oracle case may be reading an ambient file")
	}
	if _, err := os.Stat(resolveStatusMappingConfig(t, "real")); err != nil {
		t.Fatalf("the real config must exist for the assertion above to mean "+
			"anything: %v", err)
	}
}

// TestTimestampScalarIsDeclaredDivergenceD20 is the ASSERTION behind the
// declared exclusion in the status/mapping/load pair. PyYAML resolves a plain
// `2026-01-01` scalar to a datetime.date and str()s it into an index key; this
// port refuses loudly rather than carrying a second, unversioned date
// formatter. A divergence recorded only in prose is a claim nothing checks, so
// this fails the moment Go starts accepting the fixture -- forcing the D20
// decision to be revisited rather than silently outgrown.
func TestTimestampScalarIsDeclaredDivergenceD20(t *testing.T) {
	t.Setenv(statusMappingPathEnvVar, "")
	_, err := LoadStatusMapping(resolveStatusMappingConfig(t, "timestamp_d20"))
	if err == nil {
		t.Fatal("LoadStatusMapping accepted a PyYAML timestamp scalar; D20 declares " +
			"that Go refuses it. If timestamp support was added deliberately, the " +
			"declared exclusion in oracle_pairs/status_mapping_load.py and Decision " +
			"Log D20 must be updated in the SAME change -- this test exists so that " +
			"cannot be skipped.")
	}
	if kind := pythonFailureKind(err); kind != "" {
		t.Fatalf("the timestamp refusal should be a plain Go error, not a mirrored "+
			"Python exception (got %s): Python does NOT raise here, it succeeds, "+
			"and mirroring an exception would misrepresent that", kind)
	}
}
