package server

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const (
	declaredGapTicket = "CHAOS-11042"
	unknownValue      = "unknown"
)

var actionFieldKeys = []string{"action", "resource_resolver", "current_state_cache_behavior", "impersonation_policy", "disclosure_behavior"}

var impersonationPolicies = map[string]bool{
	"subject_to_impersonation_override": true,
	"exempt_no_org_context":             true,
	"not_applicable_non_main_app":       true,
	unknownValue:                        true,
}

func rowGaps(row map[string]any) []string {
	var gaps []string
	list, _ := row["gaps"].([]any)
	for _, gap := range list {
		if text, ok := gap.(string); ok {
			gaps = append(gaps, text)
		}
	}
	return gaps
}

func rowFamily(id, route string) string {
	switch {
	case route == "/graphql":
		return "gql"
	case strings.Contains(id, "@mcp"):
		return "mcp"
	case route == "/buildinfo":
		return "build"
	case route == "/query/proof-write":
		return "proofw"
	case strings.HasPrefix(route, "/query"):
		return "query"
	}
	return "rest"
}

// checkRowFields returns every defect of the query-api rows against the rule
// that a row asserts only what a source proof in this change states: a public
// row carries none of the five action fields; a protected row carries all
// five, never null; a value is the catalog action (action only), "unknown"
// with a named gap that names CHAOS-11042, or the exact statement registered
// for the row's family and field, whose every proof needle is present in the
// named source or test file; an impersonation policy is a schema value; the
// REST validator description is the registered statement.
func checkRowFields(rows []map[string]any, cat *policycatalog.Catalog, claims map[string]map[string]rowClaim, read func(file string) string) []string {
	var defects []string
	prove := func(id, what string, claim rowClaim) {
		for _, proof := range claim.proofs {
			text := read(proof.file)
			for _, needle := range proof.needles {
				if !strings.Contains(text, needle) {
					defects = append(defects, fmt.Sprintf("%s: the proof of %s names %q, %s does not contain it", id, what, needle, proof.file))
				}
			}
		}
	}
	for _, row := range rows {
		id, _ := row["id"].(string)
		route, _ := row["route"].(string)
		if row["classification"] == "public" {
			for _, key := range actionFieldKeys {
				if _, present := row[key]; present {
					defects = append(defects, fmt.Sprintf("%s: public row carries %s", id, key))
				}
			}
			continue
		}
		fam := rowFamily(id, route)
		gaps := rowGaps(row)
		for _, key := range actionFieldKeys {
			value, present := row[key]
			if !present {
				defects = append(defects, fmt.Sprintf("%s: protected row has no %s", id, key))
				continue
			}
			if value == nil {
				defects = append(defects, fmt.Sprintf("%s: %s is null, a gap is the value %q with a named gap", id, key, unknownValue))
				continue
			}
			text, isString := value.(string)
			if !isString || text == "" {
				defects = append(defects, fmt.Sprintf("%s: %s is not a non-empty string", id, key))
				continue
			}
			switch {
			case text == unknownValue:
				named := false
				for _, gap := range gaps {
					if strings.HasPrefix(gap, key+":") && strings.Contains(gap, declaredGapTicket) {
						named = true
					}
				}
				if !named {
					defects = append(defects, fmt.Sprintf("%s: %s is unknown without a %q gap naming %s", id, key, key+":", declaredGapTicket))
				}
			case key == "action":
				if _, err := cat.Action(text); err != nil {
					defects = append(defects, fmt.Sprintf("%s: action %q is not in the catalog", id, text))
				}
			default:
				claim, registered := claims[fam][key]
				switch {
				case !registered:
					defects = append(defects, fmt.Sprintf("%s: no proof is registered for %s of family %s, state it as %q with a gap", id, key, fam, unknownValue))
				case claim.value != text:
					defects = append(defects, fmt.Sprintf("%s: %s is not the statement proven for family %s: %.80q", id, key, fam, text))
				default:
					prove(id, key, claim)
				}
			}
			if key == "impersonation_policy" && !impersonationPolicies[text] {
				defects = append(defects, fmt.Sprintf("%s: impersonation_policy %q is not a schema value", id, text))
			}
		}
		if fam == "rest" {
			if pv, ok := row["primary_validator"].(map[string]any); ok {
				description, _ := pv["description"].(string)
				if claim, registered := claims["rest"]["primary_validator"]; registered {
					if description != claim.value {
						defects = append(defects, fmt.Sprintf("%s: primary_validator description is not the proven statement: %.80q", id, description))
					} else {
						prove(id, "primary_validator", claim)
					}
				}
			}
		}
	}
	return defects
}

func readServerSource(t *testing.T) func(file string) string {
	return func(file string) string {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
}

func TestQueryAPIRowsAssertOnlyProvenStatementsOrDeclareAGap(t *testing.T) {
	rows := loadQueryAPIRows(t)
	if len(rows) < 40 {
		t.Fatalf("only %d rows: the row load stopped seeing them", len(rows))
	}
	for _, defect := range checkRowFields(rows, policycatalog.Default(), rowClaims, readServerSource(t)) {
		t.Error(defect)
	}
}

func TestRowFieldGuardCatchesPlantedDefects(t *testing.T) {
	cat := policycatalog.Default()
	claims := map[string]map[string]rowClaim{
		"rest": {
			"resource_resolver":   {value: "proven resolver", proofs: []claimProof{{file: "a.go", needles: []string{"needle-a"}}}},
			"disclosure_behavior": {value: "proven disclosure", proofs: []claimProof{{file: "a.go", needles: []string{"needle-a", "needle-b"}}}},
			"primary_validator":   {value: "proven validator", proofs: []claimProof{{file: "a.go", needles: []string{"needle-a"}}}},
		},
	}
	read := func(string) string { return "needle-a needle-b" }
	gap := func(field string) []any { return []any{field + ": declared gap tracked in " + declaredGapTicket} }
	protected := func() map[string]any {
		gaps := append(gap("action"), gap("current_state_cache_behavior")[0], gap("impersonation_policy")[0])
		return map[string]any{
			"id": "GET /x", "route": "/x", "classification": "protected", "gaps": gaps,
			"action": unknownValue, "resource_resolver": "proven resolver", "current_state_cache_behavior": unknownValue,
			"impersonation_policy": unknownValue, "disclosure_behavior": "proven disclosure",
			"primary_validator": map[string]any{"description": "proven validator"},
		}
	}
	public := func() map[string]any {
		return map[string]any{"id": "GET /p", "classification": "public", "gaps": []any{}}
	}
	if defects := checkRowFields([]map[string]any{protected(), public()}, cat, claims, read); len(defects) != 0 {
		t.Fatalf("the clean fixture is refused: %v", defects)
	}
	with := func(key string, value any) map[string]any { r := protected(); r[key] = value; return r }
	cases := []struct {
		name string
		row  map[string]any
		read func(string) string
		want string
	}{
		{"public row with a field", func() map[string]any { r := public(); r["action"] = "analytics.read"; return r }(), nil, "public row carries action"},
		{"protected row missing a field", func() map[string]any { r := protected(); delete(r, "resource_resolver"); return r }(), nil, "has no resource_resolver"},
		{"null field", with("disclosure_behavior", nil), nil, "is null"},
		{"empty string", with("disclosure_behavior", ""), nil, "not a non-empty string"},
		{"unknown without a gap", with("gaps", gap("action")), nil, "is unknown without"},
		{"unknown gap without the ticket", with("gaps", []any{"action: x", "current_state_cache_behavior: x", "impersonation_policy: x"}), nil, "naming " + declaredGapTicket},
		{"action outside the catalog", with("action", "nope.read"), nil, "not in the catalog"},
		{"unproven statement", with("resource_resolver", "a guess"), nil, "not the statement proven"},
		{"statement for a field with no registered proof", with("current_state_cache_behavior", "none"), nil, "no proof is registered"},
		{"proof needle missing from source", protected(), func(string) string { return "needle-a" }, "does not contain it"},
		{"bad impersonation value", with("impersonation_policy", "maybe"), nil, "not a schema value"},
		{"validator is not the proven statement", with("primary_validator", map[string]any{"description": "every failure answers the same 401"}), nil, "primary_validator description is not the proven statement"},
	}
	for _, c := range cases {
		reader := read
		if c.read != nil {
			reader = c.read
		}
		joined := strings.Join(checkRowFields([]map[string]any{c.row}, cat, claims, reader), "\n")
		if !strings.Contains(joined, c.want) {
			t.Errorf("%s: the guard did not report %q, it reported %q", c.name, c.want, joined)
		}
	}
}
