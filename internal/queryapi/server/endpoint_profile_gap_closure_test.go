package server

import (
	"sort"
	"strings"
	"testing"
)

// retiredGapTicket is the closed check the declared gaps once named; no row text may name it.
const retiredGapTicket = "CHAOS-11042"

// openDeclaredGaps is the one gap the query-api rows may still declare: the impersonation effect of the MCP caller
// class, whose schema value waits for a ruling.
var openDeclaredGaps = []string{"POST /query [dev-health-query-api @mcp] impersonation_policy"}

// declaredGapDefects returns every gap a row declares beyond allowed ("<row id> <field>"), every declared gap text that
// does not name declaredGapTicket, and every row text that names retiredGapTicket.
func declaredGapDefects(rows []map[string]any, allowed []string) []string {
	var defects, found []string
	for _, row := range rows {
		id, _ := row["id"].(string)
		for _, gap := range rowGaps(row) {
			if strings.Contains(gap, retiredGapTicket) {
				defects = append(defects, id+": a gap names the closed "+retiredGapTicket)
			}
			field, rest, ok := strings.Cut(gap, ":")
			if !ok || !strings.HasPrefix(strings.TrimSpace(rest), "declared gap") {
				continue
			}
			found = append(found, id+" "+field)
			if !strings.Contains(gap, declaredGapTicket) {
				defects = append(defects, id+": the "+field+" gap does not name "+declaredGapTicket)
			}
		}
	}
	sort.Strings(found)
	want := append([]string(nil), allowed...)
	sort.Strings(want)
	if strings.Join(found, "\n") != strings.Join(want, "\n") {
		defects = append(defects, "declared gaps are "+strings.Join(found, "; ")+", want only "+strings.Join(want, "; "))
	}
	return defects
}

func TestQueryAPIRowsDeclareOnlyTheOpenMCPImpersonationGap(t *testing.T) {
	for _, defect := range declaredGapDefects(loadQueryAPIRows(t), openDeclaredGaps) {
		t.Error(defect)
	}
}

func TestDeclaredGapGuardCatchesPlantedRows(t *testing.T) {
	gap := func(field, ticket string) string {
		return field + ": declared gap, not proven by a handler-source test in this change; tracked in " + ticket
	}
	row := func(id string, gaps ...string) map[string]any {
		list := []any{"Per-resolver authorization is not profiled."}
		for _, g := range gaps {
			list = append(list, g)
		}
		return map[string]any{"id": id, "gaps": list}
	}
	allowed := []string{"A impersonation_policy"}
	if defects := declaredGapDefects([]map[string]any{row("A", gap("impersonation_policy", declaredGapTicket)), row("B")}, allowed); len(defects) != 0 {
		t.Fatalf("the clean fixture is refused: %v", defects)
	}
	for name, c := range map[string]struct {
		rows []map[string]any
		want string
	}{
		"a second declared gap":       {[]map[string]any{row("A", gap("impersonation_policy", declaredGapTicket)), row("B", gap("current_state_cache_behavior", declaredGapTicket))}, "want only"},
		"the open gap closed":         {[]map[string]any{row("A")}, "want only"},
		"the gap names the old check": {[]map[string]any{row("A", gap("impersonation_policy", retiredGapTicket))}, "names the closed"},
		"the gap names no ticket":     {[]map[string]any{row("A", "impersonation_policy: declared gap, tracked nowhere")}, "does not name"},
	} {
		if joined := strings.Join(declaredGapDefects(c.rows, allowed), "\n"); !strings.Contains(joined, c.want) {
			t.Errorf("%s: the guard did not report %q, it reported %q", name, c.want, joined)
		}
	}
}
