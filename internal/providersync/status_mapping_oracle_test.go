package providersync

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Differential oracles for the StatusMapping port. Two pairs cross two
// genuinely different boundaries and are NOT interchangeable:
//
//   - status/mapping/load compares the four index maps the loader builds, in
//     full. Loader-level divergence is visible at the index itself rather than
//     only where a normalize case happens to probe the affected key.
//   - status/mapping/normalize compares BOTH normalizers' outputs per case. Its
//     Python-side reflector parses StatusMapping's own `normalize_*` method
//     names, so neither method can drop out of the comparison silently.
//
// Both sides resolve a config NAME through the same rule (resolveStatusMappingConfig
// here, resolve_config in status_mapping_load.py), so a case names a config and
// neither engine can quietly read a different file.

const statusMappingPathEnvVar = "STATUS_MAPPING_PATH"

func statusMappingRepoRoot(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed: cannot locate the repository root")
	}
	return filepath.Join(filepath.Dir(currentFile), "..", "..")
}

// resolveStatusMappingConfig mirrors resolve_config in the Python pair. "real"
// is the checked-in production config; anything else is a synthetic fixture.
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

func loadStatusMappingForCase(t *testing.T, input map[string]any) *StatusMapping {
	t.Helper()
	applyStatusMappingEnv(t, input)
	explicit := ""
	if name, ok := input["explicit_config"].(string); ok && name != "" {
		explicit = resolveStatusMappingConfig(t, name)
	}
	mapping, err := LoadStatusMapping(explicit)
	if err != nil {
		t.Fatalf("LoadStatusMapping(%q): %v", explicit, err)
	}
	return mapping
}

// statusMappingLoadRow is the Go row for status/mapping/load. Its four fields
// are the frozen Python dataclass's four fields; typedEncode reflects every one
// exhaustively, so this struct cannot expose a narrowed subset without a
// compile error.
type statusMappingLoadRow struct {
	StatusByProvider      map[string]map[string]string `json:"status_by_provider"`
	LabelStatusByProvider map[string]map[string]string `json:"label_status_by_provider"`
	TypeByProvider        map[string]map[string]string `json:"type_by_provider"`
	LabelTypeByProvider   map[string]map[string]string `json:"label_type_by_provider"`
}

func buildStatusMappingLoadRow(t *testing.T, input map[string]any) statusMappingLoadRow {
	t.Helper()
	mapping := loadStatusMappingForCase(t, input)
	return statusMappingLoadRow{
		StatusByProvider:      mapping.StatusByProvider,
		LabelStatusByProvider: mapping.LabelStatusByProvider,
		TypeByProvider:        mapping.TypeByProvider,
		LabelTypeByProvider:   mapping.LabelTypeByProvider,
	}
}

func TestStatusMappingLoadMatchesPythonOracle(t *testing.T) {
	cases := []oracleCase{
		{
			// The REAL config. This case alone pins CHAOS-3505 (no `linear`
			// key in any of the four maps), the empty github/gitlab `types`
			// sections, the empty jira `type_labels`, and -- most valuably --
			// the `{'type': 'bug'}` index key produced by str() on the YAML
			// MAPPING at status_mapping.yaml:82. Reproducing that key requires
			// Python's dict repr exactly; Go's own formatting would render it
			// `map[type:bug]` and diverge here.
			ID:    "real_config",
			Input: map[string]any{"explicit_config": "real"},
		},
		{
			// Loader quirks the real file cannot express: last-wins across two
			// base categories, a provider `statuses` block overriding the base
			// for one provider only, an unrecognised category skipped in
			// silence, internal whitespace collapsed, an empty raw value
			// skipped.
			ID:    "quirks_synthetic",
			Input: map[string]any{"explicit_config": "quirks"},
		},
		{
			// `yaml.safe_load(handle) or {}`: an empty file yields {}, not a
			// crash, and every provider still gets four empty maps.
			ID:    "empty_file",
			Input: map[string]any{"explicit_config": "empty"},
		},
		{
			// A valid-but-unprioritized type ("pr"/"merge_request") is INDEXED
			// by the loader even though no priority list mentions it -- the
			// loader half of quirk 8, whose consequence the normalize pair
			// then exercises.
			ID:    "type_priority_gap_config",
			Input: map[string]any{"explicit_config": "type_priority_gap"},
		},
		{
			// QUIRK 7, MIRRORED: STATUS_MAPPING_PATH replaces the caller's
			// explicit argument unconditionally. Discriminating by
			// construction -- the two configs have disjoint contents, so this
			// row can only match if the ENV file was the one read.
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
// field per `normalize_*` method the Python class declares; the pair's reflector
// fails every case if that set and these fields ever drift apart.
type statusMappingNormalizeRow struct {
	NormalizeStatus string `json:"normalize_status"`
	NormalizeType   string `json:"normalize_type"`
}

func buildStatusMappingNormalizeRow(
	t *testing.T, input map[string]any,
) statusMappingNormalizeRow {
	t.Helper()
	mapping := loadStatusMappingForCase(t, input)
	provider, _ := input["provider"].(string)
	labels := make([]string, 0)
	if raw, ok := input["labels"].([]any); ok {
		for _, label := range raw {
			if text, ok := label.(string); ok {
				labels = append(labels, text)
			}
		}
	}
	optional := func(key string) string {
		value, _ := input[key].(string)
		return value
	}
	return statusMappingNormalizeRow{
		NormalizeStatus: mapping.NormalizeStatus(
			provider, optional("status_raw"), labels, optional("state")),
		NormalizeType: mapping.NormalizeType(provider, optional("type_raw"), labels),
	}
}

func normalizeCase(id string, input map[string]any) oracleCase {
	return oracleCase{ID: id, Input: input}
}

func TestStatusMappingNormalizeMatchesPythonOracle(t *testing.T) {
	real := func(extra map[string]any) map[string]any {
		input := map[string]any{"explicit_config": "real"}
		for key, value := range extra {
			input[key] = value
		}
		return input
	}
	gap := func(extra map[string]any) map[string]any {
		input := map[string]any{"explicit_config": "type_priority_gap"}
		for key, value := range extra {
			input[key] = value
		}
		return input
	}

	cases := []oracleCase{
		// --- the yaml-mapping mis-parse, pinned on the REAL config ---
		// status_mapping.yaml:82 is `- type: bug` (note the space), which YAML
		// reads as a mapping. The conventional GitHub label therefore MISSES
		// and falls to the default, while GitLab's `type::bug` -- a plain
		// string -- hits. Both spellings are checked so the pair fails if
		// either the typo is fixed or the working one regresses.
		normalizeCase("gh_conventional_type_bug_label_misses",
			real(map[string]any{"provider": "github", "labels": []any{"type:bug"}})),
		normalizeCase("gl_scoped_type_bug_label_hits",
			real(map[string]any{"provider": "gitlab", "labels": []any{"type::bug"}})),
		normalizeCase("gh_python_dict_repr_label_hits",
			real(map[string]any{"provider": "github", "labels": []any{"{'type': 'bug'}"}})),

		// --- label matches collect into a SET: order is irrelevant, priority
		// decides. Same two labels in both orders must agree, and on
		// "incident" (which outranks "bug" in _TYPE_PRIORITY).
		normalizeCase("label_order_bug_then_incident",
			real(map[string]any{"provider": "github", "labels": []any{"bug", "incident"}})),
		normalizeCase("label_order_incident_then_bug",
			real(map[string]any{"provider": "github", "labels": []any{"incident", "bug"}})),

		// --- github/gitlab have NO `types` section, so type_by_provider is
		// empty and a raw type that LOOKS mappable still yields the default.
		normalizeCase("gh_type_raw_looks_mappable_still_issue",
			real(map[string]any{"provider": "github", "type_raw": "Bug"})),
		normalizeCase("jira_type_raw_maps_with_case_and_padding",
			real(map[string]any{"provider": "jira", "type_raw": "  BUG  "})),

		// --- CHAOS-3505: `linear` is in the config but not in the loader's
		// provider tuple, so BOTH normalizers fall through for it.
		normalizeCase("linear_provider_ignored_entirely",
			real(map[string]any{
				"provider": "linear", "labels": []any{"bug"}, "status_raw": "In Progress"})),
		normalizeCase("unknown_provider_defaults_unknown",
			real(map[string]any{"provider": "nope"})),

		// --- `if type_raw:` / `if status_raw:` are TRUTHINESS tests, so None
		// and "" take the same branch. This pair proves the Go port's use of a
		// plain string for both is a safe collapse rather than a lost
		// distinction.
		normalizeCase("raw_absent_none",
			real(map[string]any{"provider": "jira"})),
		normalizeCase("raw_absent_empty_string",
			real(map[string]any{"provider": "jira", "type_raw": "", "status_raw": ""})),

		// --- _norm_key collapses INTERNAL whitespace, which a naive
		// ToLower+TrimSpace port drops.
		normalizeCase("status_internal_whitespace_collapsed",
			real(map[string]any{"provider": "jira", "status_raw": "In    Progress"})),

		// --- state fallback, reached only when labels and raw status both miss.
		normalizeCase("state_fallback_closed_after_status_miss",
			real(map[string]any{
				"provider": "github", "status_raw": "nomatch", "state": "Closed"})),
		normalizeCase("state_fallback_opened",
			real(map[string]any{"provider": "github", "state": "opened"})),
		// "merged" and "done" are their own members of the closed-state set;
		// a mutation dropping either is invisible to the "Closed" case above.
		normalizeCase("state_fallback_merged",
			real(map[string]any{"provider": "github", "state": "merged"})),
		normalizeCase("state_fallback_done_word",
			real(map[string]any{"provider": "github", "state": "  DONE  "})),
		normalizeCase("status_label_priority_done_outranks_todo",
			real(map[string]any{"provider": "github", "labels": []any{"todo", "done"}})),

		// --- QUIRK 8: "pr" is a valid WorkItemType the loader indexes, but it
		// is absent from _TYPE_PRIORITY. Alone, it enters the label arm and
		// then FALLS THROUGH -- the lower-precedence type_raw arm wins, which
		// inverts the documented precedence. Paired with a prioritized label
		// it does not fall through. A port treating the priority list as
		// exhaustive diverges on the first two.
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

		// --- QUIRK 7 at the normalize boundary. Deliberately discriminating:
		// under the REAL config the label "bug" maps to bug, under the gap
		// config it maps to nothing (its label is "Bug Label") and the default
		// wins. So this case answers "bug" if the explicit path were honoured
		// and "issue" if the env replaces it -- it cannot pass by accident.
		normalizeCase("env_path_replaces_explicit_argument",
			real(map[string]any{
				"env_config": "type_priority_gap",
				"provider":   "github", "labels": []any{"bug"}})),
	}

	compareRowsAgainstPythonOracle(
		t, "status/mapping/normalize", cases, buildStatusMappingNormalizeRow, nil)
}

// TestStatusMappingEnvIsNotAmbient guards the guard: if the harness ever stopped
// clearing STATUS_MAPPING_PATH, every "explicit path" case above would silently
// read whatever the ambient environment pointed at and the two pairs would agree
// on the wrong file. Setting a bogus value and asserting the explicit path still
// loses to it proves the variable is genuinely being applied per case.
func TestStatusMappingEnvIsNotAmbient(t *testing.T) {
	t.Setenv(statusMappingPathEnvVar, filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if _, err := LoadStatusMapping(
		resolveStatusMappingConfig(t, "real"),
	); err == nil {
		t.Fatal("expected the environment variable to REPLACE the explicit path and " +
			"fail on the missing file; it did not, so quirk 7 is no longer mirrored " +
			"and every explicit-path oracle case may be reading an ambient file")
	}
	if _, err := os.Stat(resolveStatusMappingConfig(t, "real")); err != nil {
		t.Fatalf("the real config must exist for the assertion above to mean "+
			"anything: %v", err)
	}
}
