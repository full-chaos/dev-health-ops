package providersync

import (
	"testing"
	"time"
)

// team_drift_generic_oracle_test.go proves CHAOS-4444's shared drift-review
// engine (team_drift_review.go / identity_drift_review.go) against the
// LIVE, checked-in Python producers it ports, via the shared live-python-
// oracle harness (ci/check_go.sh live-python-oracles) -- the same mechanism
// every other provider oracle in this package uses.
//
// Scope: these four pairs pin the PURE, deterministic decision functions the
// orchestration (project_team/_project_field_changes/
// split_memberships_for_review) is built from -- exactly where cross-
// language behavior actually diverges (canonical JSON encoding, hash
// computation, conflict-source precedence). The orchestration's own
// STAGE/RESOLVE/SUPERSEDE control flow (which is async and store-driven on
// the Python side, with no dict-literal return this harness's
// build_row/reflected_fields contract can pin) is proven instead by
// team_drift_review_fakeconn_test.go's fake-conn tests, which encode
// Python's documented control flow directly and are mutation-checked for
// vacuity (a flipped SUPERSEDED->RESOLVED transition was confirmed to fail
// the corresponding test before being reverted).

// teamCatalogObservedRowOracle mirrors _observed_row's return dict exactly.
type teamCatalogObservedRowOracle struct {
	OrgID         string    `json:"org_id"`
	Provider      string    `json:"provider"`
	NativeTeamKey string    `json:"native_team_key"`
	TeamID        string    `json:"team_id"`
	Name          *string   `json:"name"`
	Description   *string   `json:"description"`
	Members       []string  `json:"members"`
	ProjectKeys   []string  `json:"project_keys"`
	RepoPatterns  []string  `json:"repo_patterns"`
	IsActive      int       `json:"is_active"`
	ParentTeamID  *string   `json:"parent_team_id"`
	DiscoveredAt  time.Time `json:"discovered_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func driftOracleOptionalString(input map[string]any, key string) *string {
	if v, ok := input[key].(string); ok {
		return &v
	}
	return nil
}

// oracleStringList mirrors _list_field: filters nil entries, stringifies
// the rest, preserves order -- deliberately NOT sorted or deduped (that is
// _comparison_list_field, a later, separate step _observed_row never
// applies).
func oracleStringList(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return []string{}
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func buildTeamCatalogObservedRowOracle(t *testing.T, input map[string]any) teamCatalogObservedRowOracle {
	t.Helper()
	id := input["team_row_id"].(string)
	provider, _ := input["provider"].(string)
	nativeTeamKey := driftOracleOptionalString(input, "native_team_key")
	key := id
	if nativeTeamKey != nil && *nativeTeamKey != "" {
		key = *nativeTeamKey
	}
	isActive := 1
	if v, ok := input["is_active"].(bool); ok && !v {
		isActive = 0
	}
	now, err := time.Parse(time.RFC3339Nano, input["now"].(string))
	if err != nil {
		t.Fatal(err)
	}
	return teamCatalogObservedRowOracle{
		OrgID: input["org_id"].(string), Provider: provider, NativeTeamKey: key, TeamID: id,
		Name: driftOracleOptionalString(input, "name"), Description: driftOracleOptionalString(input, "description"),
		Members: oracleStringList(input["members"]), ProjectKeys: oracleStringList(input["project_keys"]),
		RepoPatterns: oracleStringList(input["repo_patterns"]), IsActive: isActive,
		ParentTeamID: driftOracleOptionalString(input, "parent_team_id"), DiscoveredAt: now, UpdatedAt: now,
	}
}

func TestTeamCatalogObservedRowMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "team-catalog/drift/observed-row",
		[]oracleCase{
			{ID: "native_team_key_present", Input: map[string]any{
				"org_id": "org-1", "team_row_id": "gh:team-a", "provider": "github", "native_team_key": "team-a",
				"name": "Platform", "description": "Platform team", "members": []any{"alice", "bob"},
				"project_keys": []any{"CHAOS"}, "repo_patterns": []any{"acme/api"},
				"now": "2026-08-29T08:39:01.077Z",
			}},
			{ID: "native_team_key_falls_back_to_id", Input: map[string]any{
				"org_id": "org-1", "team_row_id": "gl:org/team-b", "provider": "gitlab",
				"name": "Ops", "description": nil, "members": []any{},
				"project_keys": []any{}, "repo_patterns": []any{},
				"now": "2026-08-29T08:39:01Z",
			}},
			{ID: "inactive_team_non_ascii_name", Input: map[string]any{
				"org_id": "org-1", "team_row_id": "linear:team-c", "provider": "linear", "native_team_key": "CHAOS",
				"name": "café 日本", "description": "Équipe", "members": []any{"carol"},
				"project_keys": []any{"A", "B"}, "repo_patterns": []any{}, "is_active": false,
				"parent_team_id": "linear:team-parent",
				"now":            "2026-08-29T08:39:01.5Z",
			}},
		},
		buildTeamCatalogObservedRowOracle,
		nil,
	)
}

type changeIDOracleRow struct {
	ChangeID string `json:"change_id"`
}

func buildTeamCatalogChangeIDOracle(t *testing.T, input map[string]any) changeIDOracleRow {
	t.Helper()
	return changeIDOracleRow{ChangeID: changeIDForTeamField(
		input["org_id"].(string), input["team_id"].(string), input["field"].(string),
		input["old_value_json"].(string), input["new_value_json"].(string),
	)}
}

func TestTeamCatalogChangeIDMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "team-catalog/drift/change-id",
		[]oracleCase{
			{ID: "name_field_ascii", Input: map[string]any{
				"org_id": "org-1", "team_id": "gh:team-a", "field": "name",
				"old_value_json": `"Old Name"`, "new_value_json": `"New Name"`,
			}},
			{ID: "members_field_list", Input: map[string]any{
				"org_id": "org-1", "team_id": "gl:org", "field": "members",
				"old_value_json": `["alice","bob"]`, "new_value_json": `["alice","carol"]`,
			}},
			{ID: "description_field_non_ascii_and_null", Input: map[string]any{
				"org_id": "org-1", "team_id": "linear:team-c", "field": "description",
				"old_value_json": `null`, "new_value_json": `"café 日本"`,
			}},
		},
		buildTeamCatalogChangeIDOracle,
		nil,
	)
}

func buildIdentityDriftChangeIDOracle(t *testing.T, input map[string]any) changeIDOracleRow {
	t.Helper()
	return changeIDOracleRow{ChangeID: changeIDForIdentityMembership(
		input["org_id"].(string), input["team_id"].(string), input["provider"].(string),
		input["member_id"].(string), input["field"].(string),
		input["old_value_json"].(string), input["new_value_json"].(string),
	)}
}

func TestIdentityDriftChangeIDMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "identity-drift/review/change-id",
		[]oracleCase{
			{ID: "team_memberships_conflict", Input: map[string]any{
				"org_id": "org-1", "team_id": "gh:team-a", "provider": "github", "member_id": "gh:alice",
				"field": "team_memberships", "old_value_json": `{"team_id":"gh:team-b"}`, "new_value_json": `{"team_id":"gh:team-a"}`,
			}},
			{ID: "member_fallback_conflict", Input: map[string]any{
				"org_id": "org-1", "team_id": "gl:org/team-a", "provider": "gitlab", "member_id": "gitlab:bob",
				"field": "manual_attribution_fallbacks.member", "old_value_json": `{"team_id":"gl:org"}`, "new_value_json": `{"team_id":"gl:org/team-a"}`,
			}},
		},
		buildIdentityDriftChangeIDOracle,
		nil,
	)
}

type conflictDecisionOracleRow struct {
	ConflictField *string `json:"conflict_field"`
}

func buildIdentityConflictDecisionOracle(t *testing.T, input map[string]any) conflictDecisionOracleRow {
	t.Helper()
	rowInput, ok := input["row"].(map[string]any)
	if !ok {
		t.Fatalf("case row must be a map, got %T", input["row"])
	}
	row := teamDriftMembershipView{
		Provider: rowInput["provider"].(string), TeamID: rowInput["team_id"].(string), MemberID: rowInput["member_id"].(string),
		RawProviderUserID: driftOracleOptionalString(rowInput, "raw_provider_user_id"),
		RawEmail:          driftOracleOptionalString(rowInput, "raw_email"),
		IdentityFacets:    oracleStringList(rowInput["identity_facets"]),
	}
	if v, ok := rowInput["source"].(string); ok {
		row.Source = v
	}

	var manualRows []manualMembershipRow
	for _, raw := range input["manual_memberships"].([]any) {
		m := raw.(map[string]any)
		manualRows = append(manualRows, manualMembershipRow{
			Provider: m["provider"].(string), TeamID: m["team_id"].(string), MemberID: m["member_id"].(string),
		})
	}
	var fallbackRows []memberFallbackRow
	for _, raw := range input["member_fallbacks"].([]any) {
		f := raw.(map[string]any)
		fallbackRows = append(fallbackRows, memberFallbackRow{
			Provider: f["provider"].(string), ScopeID: f["scope_id"].(string), TeamID: f["team_id"].(string),
		})
	}

	detail := conflictDetailForMembership(row, manualRows, fallbackRows)
	if detail == nil {
		return conflictDecisionOracleRow{}
	}
	field := detail.Field
	return conflictDecisionOracleRow{ConflictField: &field}
}

// TestIdentityDriftConflictDecisionMatchesLivePythonProducer deliberately
// never uses row.source="manual" -- Python's _conflict_for short-circuits
// to None for a manual-source row (a case a native provider_access
// collector's own rows never construct; the pre-existing, already-shipped
// membershipConflictsWithManualState guard this file's detail-lookup only
// ever runs AFTER also lacks that check -- CHAOS-4499 tracks reconciling
// this, out of scope here, see that ticket's description for the full
// analysis).
func TestIdentityDriftConflictDecisionMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "identity-drift/review/conflict-decision",
		[]oracleCase{
			{ID: "same_team_manual_confirms_no_conflict", Input: map[string]any{
				"row":                map[string]any{"provider": "github", "team_id": "gh:team-a", "member_id": "gh:alice", "source": "provider_access"},
				"manual_memberships": []any{map[string]any{"provider": "github", "team_id": "gh:team-a", "member_id": "gh:alice"}},
				"member_fallbacks":   []any{},
			}},
			{ID: "different_team_manual_conflicts", Input: map[string]any{
				"row":                map[string]any{"provider": "github", "team_id": "gh:team-a", "member_id": "gh:alice", "source": "provider_access"},
				"manual_memberships": []any{map[string]any{"provider": "github", "team_id": "gh:team-b", "member_id": "gh:alice"}},
				"member_fallbacks":   []any{},
			}},
			{ID: "member_has_both_same_and_different_team_still_conflicts", Input: map[string]any{
				"row": map[string]any{"provider": "github", "team_id": "gh:team-a", "member_id": "gh:alice", "source": "provider_access"},
				"manual_memberships": []any{
					map[string]any{"provider": "github", "team_id": "gh:team-a", "member_id": "gh:alice"},
					map[string]any{"provider": "github", "team_id": "gh:team-b", "member_id": "gh:alice"},
				},
				"member_fallbacks": []any{},
			}},
			{ID: "no_manual_match_fallback_same_team_no_conflict", Input: map[string]any{
				"row":                map[string]any{"provider": "gitlab", "team_id": "gl:org", "member_id": "gitlab:bob", "raw_email": "bob@example.com", "source": "provider_access"},
				"manual_memberships": []any{},
				"member_fallbacks":   []any{map[string]any{"provider": "gitlab", "team_id": "gl:org", "scope_id": "bob@example.com"}},
			}},
			{ID: "no_manual_match_fallback_different_team_conflicts", Input: map[string]any{
				"row":                map[string]any{"provider": "gitlab", "team_id": "gl:org/team-a", "member_id": "gitlab:bob", "raw_email": "bob@example.com", "source": "provider_access"},
				"manual_memberships": []any{},
				"member_fallbacks":   []any{map[string]any{"provider": "gitlab", "team_id": "gl:org", "scope_id": "bob@example.com"}},
			}},
			{ID: "no_match_at_all_no_conflict", Input: map[string]any{
				"row":                map[string]any{"provider": "linear", "team_id": "linear:team-c", "member_id": "linear:carol", "source": "provider_access"},
				"manual_memberships": []any{map[string]any{"provider": "linear", "team_id": "linear:team-c", "member_id": "linear:someone-else"}},
				"member_fallbacks":   []any{},
			}},
		},
		buildIdentityConflictDecisionOracle,
		nil,
	)
}
