package streamhandlers

import (
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// teamIDDivergences are the columns where the Go writer differs from the
// frozen Python reference on purpose: CHAOS-8939 stores every team id with the
// source system's prefix (teamid.Of), and the Python reference stores the
// pushed id as sent. Each entry gives the value Go must write for a reference
// row, so the divergence is asserted, never skipped. An entry that no compared
// row reaches fails the comparing test (a stale divergence).
var teamIDDivergences = map[string]func(system string, payload map[string]any, reference any) any{
	"teams.id": func(system string, payload map[string]any, _ any) any {
		return teamid.Of(system, stringField(payload, "id"))
	},
	"teams.team_uuid": func(system string, payload map[string]any, _ any) any {
		return uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+teamid.Of(system, stringField(payload, "id")))).String()
	},
	"teams.native_team_key": func(_ string, payload map[string]any, reference any) any {
		if reference == nil {
			return strings.TrimSpace(stringField(payload, "id"))
		}
		return reference
	},
	"teams.parent_team_id": func(system string, _ map[string]any, reference any) any {
		parent, ok := reference.(string)
		if !ok {
			return reference
		}
		return teamid.Of(system, parent)
	},
	"identities.team_ids": func(system string, _ map[string]any, reference any) any {
		ids, ok := reference.([]any)
		if !ok {
			return reference
		}
		out := make([]any, len(ids))
		for i, id := range ids {
			out[i] = teamid.Of(system, id.(string))
		}
		return out
	},
}

// teamIDDivergenceUse records which declared divergences a test reached.
type teamIDDivergenceUse map[string]bool

// expected returns the value Go must write in table.column for a reference
// value: the declared divergence when one exists, else the reference itself.
func (use teamIDDivergenceUse) expected(table, column, system string, payload map[string]any, reference any) any {
	divergence, ok := teamIDDivergences[table+"."+column]
	if !ok {
		return reference
	}
	use[table+"."+column] = true
	return goldenComparableValue(divergence(system, payload, reference))
}

// checkReached fails the test for a declared divergence that no compared row
// reached.
func (use teamIDDivergenceUse) checkReached(t *testing.T) {
	t.Helper()
	var stale []string
	for key := range teamIDDivergences {
		if !use[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("declared CHAOS-8939 team id divergences that no reference row reached: %v", stale)
	}
}
