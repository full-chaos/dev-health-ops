package teamsidentity

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
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
	raw := httpapi.QueryLast(query, name)
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

// naiveDatetime renders a DateTime64(6) column without an explicit zone the
// way the Python api serializes what its driver returns for it: clickhouse-
// connect hands back a naive datetime when the server zone is UTC and an
// aware one in the server zone otherwise, and pydantic-core prints a naive
// value with no zone and an aware one with its offset ("Z" for zero). The
// Go driver reports the column in the server zone the same way (time.UTC for
// a UTC server), so the location decides. One shared implementation
// (pytime.Pydantic) owns the wire form (R299).
func naiveDatetime(value time.Time) string {
	if value.Location() == time.UTC {
		return pytime.Pydantic(pytime.DateTime{Time: value.UTC()})
	}
	_, offset := value.Zone()
	return pytime.Pydantic(pytime.DateTime{Time: value.UTC(), Aware: true, Offset: offset})
}

// writtenDatetime renders the instant a write route stamped on the row it
// answers with. Python builds that response from the object it just wrote,
// whose updated_at is datetime.now(timezone.utc): aware, so pydantic prints it
// with "Z" whatever zone the ClickHouse server is in (unlike a value read back
// from the column, see naiveDatetime).
func writtenDatetime(value time.Time) string {
	return pytime.Pydantic(pytime.DateTime{Time: value.UTC(), Aware: true})
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
// -- Python's f-string renders a list via repr(), which reprs each element
// with Python's own quoting rule (single quotes, EXCEPT double quotes when
// the value holds a single quote and no double quote -- e.g.
// repr("doesn't-exist") is "doesn't-exist", not 'doesn't-exist'), comma-
// space separated. r2 (CHAOS-6310) found the prior unconditional
// single-quoting was a live, reproducible parity break for any team_id
// containing an apostrophe; pythonparity.StrRepr (landed via CHAOS-6322,
// #2850) is the shared repr()-matching encoder, so this uses it instead of
// a second, narrower implementation (R299: one implementation, not several
// that can drift).
func unknownTeamIDsDetail(missing []string) string {
	unique := toSet(missing)
	sorted := sortedKeysBool(unique)
	quoted := make([]string, len(sorted))
	for index, value := range sorted {
		quoted[index] = pythonparity.StrRepr(value)
	}
	return "Unknown team_id(s): [" + strings.Join(quoted, ", ") + "]"
}

// providerIdentityConflictDetail mirrors the 409 message
// identities.py's _assert_provider_identities_unowned raises.
func providerIdentityConflictDetail(provider, identityValue, ownerCanonicalID string) string {
	return "Provider identity '" + provider + ":" + identityValue +
		"' is already linked to a different canonical identity '" + ownerCanonicalID + "'"
}
