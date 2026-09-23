package teamsidentity

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// queryBoolDefaultTrue mirrors FastAPI's `active_only: bool = True` query
// parameter, verified empirically against a live FastAPI/pydantic app: a
// query key that is ABSENT (never `Has`) uses the Python default True
// without validation; a key that is PRESENT -- including present with an
// empty value -- is validated against pydantic's fixed case-insensitive
// bool vocabulary (true/1/yes/y/on/t, false/0/no/n/off/f, exact match, no
// whitespace trim) and anything outside it is a "bool_parsing" 422, never a
// silent default. present is false only on that error path.
func queryBoolDefaultTrue(r *http.Request, name string) (bool, *pybody.Error) {
	query := r.URL.Query()
	if !query.Has(name) {
		return true, nil
	}
	raw := query.Get(name)
	switch strings.ToLower(raw) {
	case "true", "1", "yes", "y", "on", "t":
		return true, nil
	case "false", "0", "no", "n", "off", "f":
		return false, nil
	default:
		return false, &pybody.Error{Type: "bool_parsing", Loc: []pyjson.Value{"query", name},
			Msg: "Input should be a valid boolean, unable to interpret input", Input: raw}
	}
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
//
// KNOWN GAP (CHAOS-6310 r1 finding #6, left open on purpose): Python's
// repr() switches to double quotes (no escaping) when a value holds a
// single quote and no double quote -- e.g. repr("doesn't-exist") is
// "doesn't-exist", not 'doesn't-exist'. This single-quotes unconditionally
// instead of reproducing that rule locally: a shared repr()-matching
// encoder (pythonparity.StrRepr/IsPrintable) already exists in another
// lane's unpushed work and is moving into the CHAOS-6322 shared PR so
// every consumer gets ONE implementation instead of several that can
// drift apart (R299). This switches to it once that PR lands.
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
