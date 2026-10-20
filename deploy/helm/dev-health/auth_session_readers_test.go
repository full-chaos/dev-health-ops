package devhealth_test

import (
	"testing"
)

func containerEnvValue(t *testing.T, doc map[string]any, name string) (string, bool) {
	t.Helper()
	if doc == nil {
		t.Fatal("document not rendered")
	}
	var spec any = doc
	if at(doc, "kind") == "Job" || at(doc, "kind") == "Deployment" {
		spec = at(doc, "spec", "template", "spec")
	}
	containers, _ := at(spec, "containers").([]any)
	for _, container := range containers {
		env, _ := at(container, "env").([]any)
		for _, entry := range env {
			if at(entry, "name") == name {
				value, _ := at(entry, "value").(string)
				return value, true
			}
		}
	}
	return "", false
}

// auth-migrate grants the session read to exactly the listed logins, and only
// when some are listed.
func TestAuthMigrateGrantsSessionReadsToTheListedRoles(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	if value, set := containerEnvValue(t, authDoc(docs, "Job", "a-dev-health-auth-migrate"), "AUTH_SESSION_READER_ROLES"); set {
		t.Errorf("the default render sets AUTH_SESSION_READER_ROLES=%q; want it unset", value)
	}
	docs, out, err = renderAuth(t, append(append([]string{}, authBase...),
		"--set-json", `authService.migrate.sessionReaderRoles=["devhealth_api","devhealth_query"]`)...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	if value, _ := containerEnvValue(t, authDoc(docs, "Job", "a-dev-health-auth-migrate"), "AUTH_SESSION_READER_ROLES"); value != "devhealth_api,devhealth_query" {
		t.Errorf("AUTH_SESSION_READER_ROLES = %q, want devhealth_api,devhealth_query", value)
	}
}

// go-api and query-api read auth-service sessions in the schema auth-service
// uses (authService.database.schema).
func TestGoAPIAndQueryAPIReadSessionsInTheAuthSchema(t *testing.T) {
	sets := append(append([]string{}, authBase...), "--set", "goApi.enabled=true", "--set", "queryApi.enabled=true",
		"--set", "authService.database.schema=acp")
	docs, out, err := renderAuth(t, sets...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	for _, name := range []string{"a-dev-health-go-api", "a-dev-health-query-api"} {
		if value, _ := containerEnvValue(t, authDoc(docs, "Deployment", name), "AUTH_DATABASE_SCHEMA"); value != "acp" {
			t.Errorf("%s AUTH_DATABASE_SCHEMA = %q, want acp", name, value)
		}
	}
	docs, out, err = renderAuth(t, "--set", "goApi.enabled=true", "--set", "queryApi.enabled=true")
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	for _, name := range []string{"a-dev-health-go-api", "a-dev-health-query-api"} {
		if value, set := containerEnvValue(t, authDoc(docs, "Deployment", name), "AUTH_DATABASE_SCHEMA"); set {
			t.Errorf("%s sets AUTH_DATABASE_SCHEMA=%q without auth-service", name, value)
		}
	}
}
