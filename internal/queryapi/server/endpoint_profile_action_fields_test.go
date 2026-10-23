package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

var actionFieldKeys = []string{"action", "resource_resolver", "current_state_cache_behavior", "impersonation_policy", "disclosure_behavior"}

var impersonationPolicies = map[string]bool{
	"subject_to_impersonation_override": true,
	"exempt_no_org_context":             true,
	"not_applicable_non_main_app":       true,
	"unknown":                           true,
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

// checkActionFieldRows returns every defect of the query-api rows against the
// action fields: a public row carries none of the five (not applicable); a
// protected row carries all five; a null value is paired with a gap that
// begins "<field>:"; an action is a catalog action; the impersonation policy
// is one of the schema's values.
func checkActionFieldRows(rows []map[string]any, cat *policycatalog.Catalog) []string {
	var defects []string
	for _, row := range rows {
		id, _ := row["id"].(string)
		if row["classification"] == "public" {
			for _, key := range actionFieldKeys {
				if _, present := row[key]; present {
					defects = append(defects, fmt.Sprintf("%s: public row carries %s", id, key))
				}
			}
			continue
		}
		gaps := rowGaps(row)
		for _, key := range actionFieldKeys {
			value, present := row[key]
			if !present {
				defects = append(defects, fmt.Sprintf("%s: protected row has no %s", id, key))
				continue
			}
			if value == nil {
				named := false
				for _, gap := range gaps {
					if strings.HasPrefix(gap, key+":") {
						named = true
					}
				}
				if !named {
					defects = append(defects, fmt.Sprintf("%s: %s is null with no %q gap", id, key, key+":"))
				}
				continue
			}
			text, isString := value.(string)
			if !isString || text == "" {
				defects = append(defects, fmt.Sprintf("%s: %s is not a non-empty string", id, key))
				continue
			}
			if key == "action" {
				if _, err := cat.Action(text); err != nil {
					defects = append(defects, fmt.Sprintf("%s: action %q is not in the catalog", id, text))
				}
			}
			if key == "impersonation_policy" && !impersonationPolicies[text] {
				defects = append(defects, fmt.Sprintf("%s: impersonation_policy %q is not a schema value", id, text))
			}
		}
	}
	return defects
}

func TestQueryAPIRowsCarryTheActionFields(t *testing.T) {
	rows := loadQueryAPIRows(t)
	if len(rows) < 40 {
		t.Fatalf("only %d rows: the row load stopped seeing them", len(rows))
	}
	for _, defect := range checkActionFieldRows(rows, policycatalog.Default()) {
		t.Error(defect)
	}
}

func TestActionFieldRowChecksCatchPlantedDefects(t *testing.T) {
	cat := policycatalog.Default()
	protected := func() map[string]any {
		return map[string]any{
			"id": "GET /x", "classification": "protected",
			"action": "analytics.read", "resource_resolver": "r", "current_state_cache_behavior": "c",
			"impersonation_policy": "unknown", "disclosure_behavior": "d", "gaps": []any{},
		}
	}
	public := func() map[string]any { return map[string]any{"id": "GET /p", "classification": "public", "gaps": []any{}} }
	if defects := checkActionFieldRows([]map[string]any{protected(), public()}, cat); len(defects) != 0 {
		t.Fatalf("the clean fixture is refused: %v", defects)
	}
	cases := []struct {
		name string
		row  map[string]any
		want string
	}{
		{"public row with a field", func() map[string]any { r := public(); r["action"] = "analytics.read"; return r }(), "public row carries action"},
		{"protected row missing a field", func() map[string]any { r := protected(); delete(r, "resource_resolver"); return r }(), "has no resource_resolver"},
		{"null without a gap", func() map[string]any { r := protected(); r["action"] = nil; return r }(), "action is null with no"},
		{"unknown action", func() map[string]any { r := protected(); r["action"] = "nope.read"; return r }(), "not in the catalog"},
		{"bad impersonation value", func() map[string]any { r := protected(); r["impersonation_policy"] = "maybe"; return r }(), "not a schema value"},
		{"empty string", func() map[string]any { r := protected(); r["disclosure_behavior"] = ""; return r }(), "not a non-empty string"},
	}
	for _, c := range cases {
		joined := strings.Join(checkActionFieldRows([]map[string]any{c.row}, cat), "\n")
		if !strings.Contains(joined, c.want) {
			t.Errorf("%s: the check did not report %q, it reported %q", c.name, c.want, joined)
		}
	}
}
