package devhealth_test

import (
	"strings"
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

var sessionsOn = []string{
	"--set", "goApi.enabled=true",
	"--set", "goApi.authProxy.sessions=true",
}

var sessionReaders = []string{
	"--set", "authService.migrate.sessionReaders.goApi=devhealth_api",
	"--set", "authService.migrate.sessionReaders.queryApi=devhealth_query",
}

var sessionSuccessor = []string{
	"--set", "authService.sessionKeys.successor.secretName=auth-session",
}

func sessionsArgs(parts ...[]string) []string {
	out := append([]string{}, authBase...)
	out = append(out, sessionsOn...)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func secretRefOf(t *testing.T, doc map[string]any, name string) (string, string) {
	t.Helper()
	containers, _ := at(doc, "spec", "template", "spec", "containers").([]any)
	for _, container := range containers {
		env, _ := at(container, "env").([]any)
		for _, entry := range env {
			if at(entry, "name") == name {
				if _, literal := at(entry, "value").(string); literal {
					t.Fatalf("%s is a literal value", name)
				}
				secret, _ := at(entry, "valueFrom", "secretKeyRef", "name").(string)
				key, _ := at(entry, "valueFrom", "secretKeyRef", "key").(string)
				return secret, key
			}
		}
	}
	return "", ""
}

func TestSessionsOnNeedsBothReaderRolesAndNamesTheMissingOnes(t *testing.T) {
	_, out, err := renderAuth(t, sessionsArgs(sessionSuccessor)...)
	if err == nil {
		t.Fatal("sessions=true without reader roles rendered")
	}
	for _, want := range []string{"sessionReaders.goApi", "sessionReaders.queryApi"} {
		if !strings.Contains(out, want) {
			t.Errorf("fail text does not name %s:\n%.600s", want, out)
		}
	}
	_, out, err = renderAuth(t, sessionsArgs(sessionSuccessor, []string{"--set", "authService.migrate.sessionReaders.goApi=devhealth_api"})...)
	if err == nil || !strings.Contains(out, "sessionReaders.queryApi") || strings.Contains(out, "sessionReaders.goApi (") {
		t.Errorf("only queryApi should be named missing: err=%v\n%.600s", err, out)
	}
}

func TestSessionsOnNeedsTheSessionKeys(t *testing.T) {
	_, out, err := renderAuth(t, sessionsArgs(sessionReaders)...)
	if err == nil || !strings.Contains(out, "authService.sessionKeys.successor.secretName") {
		t.Fatalf("sessions=true without the successor key: err=%v\n%.600s", err, out)
	}
}

func TestSessionsOnRendersRolesAndKeysByReference(t *testing.T) {
	docs, out, err := renderAuth(t, sessionsArgs(sessionReaders, sessionSuccessor, []string{"--set", "hs256Secret.externalSecretName=hs256-ext"})...)
	if err != nil {
		t.Fatalf("render: %v\n%.600s", err, out)
	}
	if value, _ := containerEnvValue(t, authDoc(docs, "Job", "a-dev-health-auth-migrate"), "AUTH_SESSION_READER_ROLES"); value != "devhealth_api,devhealth_query" {
		t.Errorf("AUTH_SESSION_READER_ROLES = %q", value)
	}
	svc := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	api := authDoc(docs, "Deployment", "a-dev-health-go-api")
	asName, asKey := secretRefOf(t, svc, "AUTH_JWT_SECRET_KEY")
	gaName, gaKey := secretRefOf(t, api, "JWT_SECRET_KEY")
	if asName == "" || asName != gaName || asKey != gaKey {
		t.Errorf("auth-service key ref %s/%s differs from go-api %s/%s", asName, asKey, gaName, gaKey)
	}
	if name, key := secretRefOf(t, svc, "AUTH_SESSION_SUCCESSOR_KEY"); name != "auth-session" || key != "AUTH_SESSION_SUCCESSOR_KEY" {
		t.Errorf("successor ref = %s/%s", name, key)
	}
}

func TestSessionKeysAbsentWhenNotConfigured(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	svc := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	for _, name := range []string{"AUTH_JWT_SECRET_KEY", "AUTH_SESSION_SUCCESSOR_KEY"} {
		if _, set := containerEnvValue(t, svc, name); set {
			t.Errorf("%s rendered without sessionKeys", name)
		}
		if n, _ := secretRefOf(t, svc, name); n != "" {
			t.Errorf("%s secretKeyRef rendered without sessionKeys", name)
		}
	}
}

func TestAuthServiceExtraEnvRejectsSessionKeys(t *testing.T) {
	for _, name := range []string{"AUTH_JWT_SECRET_KEY", "AUTH_SESSION_SUCCESSOR_KEY"} {
		args := append(append([]string{}, authBase...), "--set-json", `authService.extraEnv=[{"name":"`+name+`","value":"x"}]`)
		if _, _, err := renderAuth(t, args...); err == nil {
			t.Errorf("extraEnv %s rendered", name)
		}
	}
}
