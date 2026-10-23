package devhealth_test

import "testing"

var riverRollbackSets = []string{
	"--set", "web.enabled=false", "--set", "goWorkers.enabled=false",
	"--set", "postgresql.enabled=true", "--set", "goApi.enabled=true", "--set", "authService.enabled=false",
	"--set", "migrations.hook.provisionRoles.enabled=true",
	"--set", "migrations.hook.riverMigrate.enabled=true",
	"--set", "migrations.hook.riverMigrate.image=ghcr.io/full-chaos/dho:sha-0123456789ab",
	"--set", "migrations.hook.routeActivate.image=ghcr.io/full-chaos/dho:sha-0123456789ab",
}

func hookAnnotations(t *testing.T, docs []map[string]any, kind, suffix string) map[string]any {
	t.Helper()
	for _, doc := range docs {
		name, _ := at(doc, "metadata", "name").(string)
		if doc["kind"] == kind && len(name) >= len(suffix) && name[len(name)-len(suffix):] == suffix {
			annotations, _ := at(doc, "metadata", "annotations").(map[string]any)
			return annotations
		}
	}
	return nil
}

// helm rollback applies the TARGET revision's manifest and runs the hooks that name pre-rollback.
// The river-migrate Job applies the api role's refresh_tokens grants from AUTH_PROXY_SESSIONS, so it
// (and the hook Secret it reads on a chart-created-secret release) must run on a rollback, and only
// it: the weight-0 migrate Job runs forward-only schema steps. Weight and delete policy do not move.
func TestRiverMigrateHookRunsOnPreRollback(t *testing.T) {
	docs := renderDocs(t, riverRollbackSets...)
	for _, want := range []struct {
		kind, suffix string
		annotations  map[string]string
	}{
		{"Job", "-river-migrate", map[string]string{"helm.sh/hook": "pre-install,pre-upgrade,pre-rollback", "helm.sh/hook-weight": "10", "helm.sh/hook-delete-policy": "before-hook-creation"}},
		{"Secret", "-river-migrate-secrets", map[string]string{"helm.sh/hook": "pre-install,pre-upgrade,pre-rollback", "helm.sh/hook-weight": "-5", "helm.sh/hook-delete-policy": "before-hook-creation,hook-succeeded"}},
	} {
		annotations := hookAnnotations(t, docs, want.kind, want.suffix)
		if annotations == nil {
			t.Fatalf("no %s %s in the render", want.kind, want.suffix)
		}
		for name, value := range want.annotations {
			if annotations[name] != value {
				t.Errorf("%s %s annotation %s = %q, want exactly %q", want.kind, want.suffix, name, annotations[name], value)
			}
		}
	}
}

// Every other hook keeps its events: a rollback must not run provisioning, the forward-only schema
// migrate Job, or the auth hooks.
func TestOtherHooksDoNotRunOnPreRollback(t *testing.T) {
	sets := append(append([]string{}, riverRollbackSets...),
		"--set", "authService.enabled=true",
		"--set", "authService.database.secretName=auth-db", "--set", "authService.migrate.secretName=auth-migrate-db",
		"--set", "authService.signingKey.secretName=auth-key", "--set", "authService.signingKey.keyId=rollback-test", "--set", "authService.networkPolicy.enabled=false")
	docs := renderDocs(t, sets...)
	for _, hook := range []struct{ kind, suffix string }{
		{"Job", "-provision-roles"}, {"Secret", "-provision-roles-secrets"}, {"Secret", "-provision-roles-conn"},
		{"Job", "-migrate"}, {"Secret", "-migrate-secrets"}, {"ConfigMap", "-migrate-config"},
		{"Job", "-auth-migrate"}, {"Job", "-auth-grants"},
	} {
		annotations := hookAnnotations(t, docs, hook.kind, hook.suffix)
		if annotations == nil {
			t.Errorf("no %s %s in the render", hook.kind, hook.suffix)
			continue
		}
		if annotations["helm.sh/hook"] != "pre-install,pre-upgrade" {
			t.Errorf("%s %s helm.sh/hook = %q, want exactly pre-install,pre-upgrade", hook.kind, hook.suffix, annotations["helm.sh/hook"])
		}
	}
}

// The local bundled-PostgreSQL variant (post-install) keeps its events and adds pre-rollback too.
func TestRiverMigrateHookPostInstallVariantRunsOnPreRollback(t *testing.T) {
	sets := append(append([]string{}, riverRollbackSets...),
		"--set", "migrations.hook.localBundledPostgres=true",
		"--set-json", `migrations.hook.events=["post-install","pre-upgrade"]`)
	annotations := hookAnnotations(t, renderDocs(t, sets...), "Job", "-river-migrate")
	if annotations["helm.sh/hook"] != "post-install,pre-upgrade,pre-rollback" {
		t.Errorf("helm.sh/hook = %q, want exactly post-install,pre-upgrade,pre-rollback", annotations["helm.sh/hook"])
	}
}

// With riverMigrate.enabled=false there is no river-migrate hook, so a helm rollback re-grants
// nothing (the weight-0 migrate Job applies River itself and stays forward-only). Prod has the
// river hook on; the PR body names this case.
func TestNoRiverHookMeansNoRollbackRegrant(t *testing.T) {
	sets := []string{"--set", "web.enabled=false", "--set", "goWorkers.enabled=false", "--set", "postgresql.enabled=true",
		"--set", "migrations.hook.routeActivate.enabled=false"}
	docs := renderDocs(t, sets...)
	if hookAnnotations(t, docs, "Job", "-river-migrate") != nil {
		t.Fatal("a river-migrate Job rendered with riverMigrate.enabled=false")
	}
	for _, doc := range docs {
		if annotations, _ := at(doc, "metadata", "annotations").(map[string]any); annotations != nil {
			if hook, _ := annotations["helm.sh/hook"].(string); hook != "" && containsEvent(hook, "pre-rollback") {
				t.Errorf("%v runs on pre-rollback without a river hook", at(doc, "metadata", "name"))
			}
		}
	}
}

func containsEvent(list, event string) bool {
	start := 0
	for i := 0; i <= len(list); i++ {
		if i == len(list) || list[i] == ',' {
			if list[start:i] == event {
				return true
			}
			start = i + 1
		}
	}
	return false
}
