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
	t.Parallel()

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

// Prod runs platform roles owned (deploy values.prod.yaml:1830 authService.platformRolesOwned true, on phase 4
// usersOwnedStage owned). The flag table's AUTH_PLATFORM_ROLES_OWNED rows must be exactly the services the chart renders
// it on in that render: one row per Deployment that carries it, same value. The Jobs that also carry it (auth-migrate,
// auth-grants, auth-backfill, the River step) are no flag table scope: the venue runs their steps itself.
func TestLiveE2EPlatformRolesOwnedRowsEqualProdRender(t *testing.T) {
	t.Parallel()

	docs := renderPhase(t, 4, "--set", "authService.platformRolesOwned=true")
	want := map[string]string{}
	for _, doc := range docs {
		if kind, _ := doc["kind"].(string); kind != "Deployment" {
			continue
		}
		name, _ := at(doc, "metadata", "name").(string)
		for _, container := range podContainers(doc) {
			env, _ := at(container, "env").([]any)
			for _, entry := range env {
				if ename, _ := at(entry, "name").(string); ename == platformRolesEnv {
					want[strings.TrimPrefix(name, "a-dev-health-")] = strings.TrimPrefix(envEntryValue(entry), "value:")
				}
			}
		}
	}
	if len(want) == 0 {
		t.Fatalf("FAIL: no Deployment of the prod-like render carries %s", platformRolesEnv)
	}
	got := map[string]string{}
	for _, row := range liveE2EFlagRows(t) {
		if row[1] == platformRolesEnv {
			got[row[0]] = row[2]
		}
	}
	var scopes []string
	for scope := range want {
		scopes = append(scopes, scope)
	}
	sort.Strings(scopes)
	for _, scope := range scopes {
		if value, ok := got[scope]; !ok || value != want[scope] {
			t.Errorf("FAIL: %s %s is %q (row %v) in the flag table, %q in the prod-like chart render", scope, platformRolesEnv, value, ok, want[scope])
		}
	}
	for scope := range got {
		if _, ok := want[scope]; !ok {
			t.Errorf("FAIL: %s %s is in the flag table and on no Deployment of the prod-like chart render", scope, platformRolesEnv)
		}
	}
}
