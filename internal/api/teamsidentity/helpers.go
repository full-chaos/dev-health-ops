package teamsidentity

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// queryBoolDefaultTrue mirrors FastAPI's `active_only: bool = True` query
// parameter: absent means true; "false"/"0" (case-insensitive, matching
// Starlette's bool converter) means false; anything else parses as a
// Python-style bool coercion FastAPI applies via pydantic's bool validator,
// which this admin surface's only two callers (list_teams/list_identities)
// only ever pass "true"/"false" for in practice.
func queryBoolDefaultTrue(r *http.Request, name string) bool {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return true
	}
	value, err := strconv.ParseBool(strings.ToLower(raw))
	if err != nil {
		return true
	}
	return value
}

// pytimeRFC3339 renders a UTC time the way Pydantic/FastAPI's jsonable_encoder
// serializes a datetime: ISO-8601 with a literal "Z" for UTC (matching
// AwareDatetime's default JSON encoding for a tz-aware UTC value).
func pytimeRFC3339(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.999999") + "Z"
}

func sortedKeys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func sortedKeysBool(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func toSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

// unknownTeamIDsDetail mirrors f"Unknown team_id(s): {sorted(set(missing))}"
// -- Python's f-string renders a list via repr(), single-quoted elements,
// comma-space separated.
func unknownTeamIDsDetail(missing []string) string {
	unique := toSet(missing)
	sorted := sortedKeysBool(unique)
	quoted := make([]string, len(sorted))
	for index, value := range sorted {
		quoted[index] = "'" + value + "'"
	}
	return "Unknown team_id(s): [" + strings.Join(quoted, ", ") + "]"
}

// providerIdentityConflictDetail mirrors the 409 message
// identities.py's _assert_provider_identities_unowned raises.
func providerIdentityConflictDetail(provider, identityValue, ownerCanonicalID string) string {
	return "Provider identity '" + provider + ":" + identityValue +
		"' is already linked to a different canonical identity '" + ownerCanonicalID + "'"
}
