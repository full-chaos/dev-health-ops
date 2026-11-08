package devhealth_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// liveE2EFlagRows reads ci/live_e2e_auth_flags.tsv: scope, name, value, source, delta.
func liveE2EFlagRows(t *testing.T) [][]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "ci", "live_e2e_auth_flags.tsv"))
	if err != nil {
		t.Fatalf("FAIL: the live e2e auth flag table is missing: %v", err)
	}
	var rows [][]string
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "scope\t") {
			continue
		}
		cols := strings.Split(line, "\t")
		if len(cols) != 5 {
			t.Fatalf("FAIL: flag table row has %d columns, want 5: %q", len(cols), line)
		}
		rows = append(rows, cols)
	}
	return rows
}

// The web live e2e runs go-api and query-api with the auth flags prod runs (authService.phase 4, usersOwnedStage owned:
// deploy values.prod.yaml; CHAOS-13356: every venue runs users owned). The table must equal the chart's rendering of
// those flags; a flag moved in the chart and not in the table fails here.
func TestLiveE2EAuthFlagsEqualPhase4Chart(t *testing.T) {
	docs := renderPhase(t, 4)
	rows := liveE2EFlagRows(t)
	for _, target := range []struct{ scope, deployment, container string }{
		{"go-api", "a-dev-health-go-api", "go-api"},
		{"query-api", "a-dev-health-query-api", "query-api"},
	} {
		want := map[string]string{}
		deployment := authDoc(docs, "Deployment", target.deployment)
		if deployment == nil {
			t.Fatalf("FAIL: no %s deployment in the phase 4 render", target.scope)
		}
		for _, container := range podContainers(deployment) {
			if name, _ := at(container, "name").(string); name != target.container {
				continue
			}
			env, _ := at(container, "env").([]any)
			for _, entry := range env {
				name, _ := at(entry, "name").(string)
				if strings.HasPrefix(name, "AUTH_PROXY_") && name != "AUTH_PROXY_URL" {
					want[name] = strings.TrimPrefix(envEntryValue(entry), "value:")
				}
			}
		}
		got := map[string]string{}
		for _, row := range rows {
			if row[0] == target.scope && strings.HasPrefix(row[1], "AUTH_PROXY_") {
				got[row[1]] = row[2]
			}
		}
		if len(want) == 0 {
			t.Fatalf("FAIL: the %s render carries no AUTH_PROXY_ flag", target.scope)
		}
		var names []string
		for name := range want {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if got[name] != want[name] {
				t.Errorf("FAIL: %s %s is %q in the flag table, %q in the phase 4 chart", target.scope, name, got[name], want[name])
			}
		}
		for name := range got {
			if _, ok := want[name]; !ok {
				t.Errorf("FAIL: %s %s is in the flag table and not in the phase 4 chart", target.scope, name)
			}
		}
	}
}
