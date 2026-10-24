package devhealth_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

var queryAPIRoleBase = []string{
	"--set", "goApi.enabled=true", "--set", "queryApi.enabled=true",
	"--set", "authService.migrate.sessionReaders.goApi=devhealth_api",
}

func queryAPIRoleArgs(parts ...[]string) []string {
	out := append([]string{}, authBase...)
	out = append(out, queryAPIRoleBase...)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func roleEnv(role string) []string {
	return []string{"--set-json", fmt.Sprintf(`queryApi.extraEnv=[{"name":"QUERY_API_DATABASE_ROLE","value":%q}]`, role)}
}

func TestQueryAPIReaderMustBeTheRoleQueryAPILogsInAs(t *testing.T) {
	reader := []string{"--set", "authService.migrate.sessionReaders.queryApi=devhealth_query_api"}
	if _, out, err := renderAuth(t, queryAPIRoleArgs(reader)...); err == nil || !strings.Contains(out, "QUERY_API_DATABASE_ROLE") {
		t.Errorf("a reader with no query-api role rendered or named the wrong key: err=%v\n%.500s", err, out)
	}
	if _, out, err := renderAuth(t, queryAPIRoleArgs(reader, roleEnv("other_role"))...); err == nil || !strings.Contains(out, "QUERY_API_DATABASE_ROLE") {
		t.Errorf("a mismatched role rendered: err=%v\n%.500s", err, out)
	}
	flip := []string{"--set", "goApi.authProxy.sessions=true", "--set", "authService.sessionKeys.successor.secretName=auth-session"}
	if _, out, err := renderAuth(t, queryAPIRoleArgs(reader, flip, roleEnv("devhealth_query_api"))...); err != nil {
		t.Errorf("matching role refused: %v\n%.500s", err, out)
	}
	if _, out, err := renderAuth(t, queryAPIRoleArgs(roleEnv("devhealth_query_api"))...); err != nil {
		t.Errorf("a role with no reader must render (posture without the sessions flip): %v\n%.500s", err, out)
	}
}

// The query-api reader is granted only together with the sessions flag: with the
// flag off river-migrate's manifest holds no auth read, so a reader then would be
// granted by auth-migrate and revoked by river-migrate on every roll.
func TestQueryAPIReaderIsSetOnlyWithTheSessionsFlag(t *testing.T) {
	reader := []string{"--set", "authService.migrate.sessionReaders.queryApi=devhealth_query_api"}
	_, out, err := renderAuth(t, queryAPIRoleArgs(reader, roleEnv("devhealth_query_api"))...)
	if err == nil || !strings.Contains(out, "goApi.authProxy.sessions") {
		t.Errorf("a query-api reader with the sessions flag off rendered or named the wrong key: err=%v\n%.500s", err, out)
	}
}

func TestQueryAPIReadsTheSessionsFlagForItsPostureCheck(t *testing.T) {
	on := queryAPIRoleArgs(
		[]string{"--set", "authService.migrate.sessionReaders.queryApi=devhealth_query_api", "--set", "goApi.authProxy.sessions=true",
			"--set", "authService.sessionKeys.successor.secretName=auth-session"},
		roleEnv("devhealth_query_api"))
	docs, out, err := renderAuth(t, on...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	if v, _ := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-query-api"), "AUTH_PROXY_SESSIONS"); v != "true" {
		t.Errorf("query-api AUTH_PROXY_SESSIONS = %q, want true", v)
	}
	docs, out, err = renderAuth(t, queryAPIRoleArgs(roleEnv("devhealth_query_api"))...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	if v, set := containerEnvValue(t, authDoc(docs, "Deployment", "a-dev-health-query-api"), "AUTH_PROXY_SESSIONS"); set {
		t.Errorf("flag off, query-api sets AUTH_PROXY_SESSIONS=%q", v)
	}
}

var riverHookSets = []string{
	"--set", "web.enabled=false", "--set", "goWorkers.enabled=false", "--set", "postgresql.enabled=true",
	"--set", "migrations.hook.provisionRoles.enabled=true",
	"--set", "migrations.hook.riverMigrate.enabled=true",
	"--set", "migrations.hook.riverMigrate.image=ghcr.io/full-chaos/dho:sha-0123456789ab",
	"--set", "migrations.hook.routeActivate.image=ghcr.io/full-chaos/dho:sha-0123456789ab",
}

// The river step grants query-api its auth.sessions read, so the auth schema
// must exist first: auth-migrate runs strictly before river-migrate.
func TestAuthMigrateRunsBeforeRiverMigrate(t *testing.T) {
	docs, out, err := renderAuth(t, queryAPIRoleArgs(riverHookSets, roleEnv("devhealth_query_api"))...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	weight := func(suffix string) int {
		for _, suf := range []string{suffix} {
			ann := hookAnnotations(t, docs, "Job", suf)
			if ann == nil {
				t.Fatalf("no Job %s", suf)
			}
			w, err := strconv.Atoi(fmt.Sprint(ann["helm.sh/hook-weight"]))
			if err != nil {
				t.Fatalf("weight of %s: %v", suf, err)
			}
			return w
		}
		return 0
	}
	if am, rm := weight("-auth-migrate"), weight("-river-migrate"); am >= rm {
		t.Errorf("auth-migrate weight %d is not before river-migrate %d", am, rm)
	}
}

func TestRiverMigrateGetsTheQueryAPIRoleAndTheAuthSchema(t *testing.T) {
	sets := queryAPIRoleArgs(riverHookSets, roleEnv("devhealth_query_api"),
		[]string{"--set", "authService.database.schema=acp"})
	docs, out, err := renderAuth(t, sets...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	job := authDoc(docs, "Job", "a-dev-health-river-migrate")
	for name, want := range map[string]string{"QUERY_API_DATABASE_ROLE": "devhealth_query_api", "AUTH_DATABASE_SCHEMA": "acp"} {
		if v, _ := containerEnvValue(t, job, name); v != want {
			t.Errorf("river-migrate %s = %q, want %q", name, v, want)
		}
	}
	docs, out, err = renderAuth(t, queryAPIRoleArgs(riverHookSets)...)
	if err != nil {
		t.Fatalf("render: %v\n%.500s", err, out)
	}
	if v, set := containerEnvValue(t, authDoc(docs, "Job", "a-dev-health-river-migrate"), "QUERY_API_DATABASE_ROLE"); set {
		t.Errorf("no query-api role configured, river-migrate sets QUERY_API_DATABASE_ROLE=%q", v)
	}
}
