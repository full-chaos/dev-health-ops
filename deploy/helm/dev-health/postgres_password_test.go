package devhealth_test

import (
	"reflect"
	"strings"
	"testing"
)

// The bundled PostgreSQL's POSTGRES_PASSWORD is a plain env value, so it is in the
// StatefulSet spec. postgresql.credentialsSecret.enabled moves it to a secretKeyRef
// on the chart's own Secret, which renders with the store whatever the switch says
// (the same value, so the Secret is inert until the StatefulSet reads it). Off keeps
// today's plain value; on changes exactly the one env entry.

// A dummy: the tests never use a real credential.
const pgDummyPassword = "dummy-pg-pass-0001"

const (
	pgCredentialsSecret = "a-dev-health-postgresql-credentials"
	pgPasswordKey       = "POSTGRES_PASSWORD"
)

var (
	pgCredsOff   = append(append([]string{}, pgStoreOn...), "--set", "postgresql.credentials.password="+pgDummyPassword)
	pgCredsOn    = append(append([]string{}, pgCredsOff...), "--set", "postgresql.credentialsSecret.enabled=true")
	pgCredsHbaOn = append(append([]string{}, pgCredsOff...), "--set", "postgresql.hba.enabled=true")
	pgCredsOnHba = append(append([]string{}, pgCredsOn...), "--set", "postgresql.hba.enabled=true")
)

func pgPasswordRender(t *testing.T, args ...string) (map[string]any, map[string]any) {
	t.Helper()
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	return authDoc(docs, "StatefulSet", "a-dev-health-postgresql"), authDoc(docs, "Secret", pgCredentialsSecret)
}

func pgEnv(t *testing.T, sts map[string]any) []any {
	t.Helper()
	env, _ := at(postgresContainer(t, sts), "env").([]any)
	if len(env) == 0 {
		t.Fatal("the postgresql container has no env")
	}
	return env
}

// Knob off (the default): the env is the plain list the chart rendered before, hba on or off, and the Secret is there
// with the same value.
func TestPostgresPasswordKnobOffKeepsThePlainEnvAndRendersTheSecret(t *testing.T) {
	requireHelmUnderCI(t)
	wantEnv := []any{
		map[string]any{"name": "POSTGRES_USER", "value": "devhealth"},
		map[string]any{"name": "POSTGRES_PASSWORD", "value": pgDummyPassword},
		map[string]any{"name": "POSTGRES_DB", "value": "devhealth"},
		map[string]any{"name": "PGDATA", "value": "/var/lib/postgresql/pgdata"},
	}
	explicitOff := append(append([]string{}, pgCredsOff...), "--set", "postgresql.credentialsSecret.enabled=false")
	for name, args := range map[string][]string{"unset": pgCredsOff, "explicit false": explicitOff, "hba on": pgCredsHbaOn} {
		t.Run(name, func(t *testing.T) {
			sts, secret := pgPasswordRender(t, args...)
			if got := pgEnv(t, sts); !reflect.DeepEqual(got, wantEnv) {
				t.Errorf("env = %v, want the plain list %v", got, wantEnv)
			}
			if secret == nil {
				t.Fatalf("Secret %s does not render with the store", pgCredentialsSecret)
			}
			if got, _ := at(secret, "stringData", pgPasswordKey).(string); got != pgDummyPassword {
				t.Errorf("Secret key %s = %q, want the postgresql.credentials.password value", pgPasswordKey, got)
			}
			if at(secret, "type") != "Opaque" {
				t.Errorf("Secret type = %v, want Opaque", at(secret, "type"))
			}
		})
	}
}

// Knob on: exactly the POSTGRES_PASSWORD entry changes (value removed, secretKeyRef name and key added); the rest of
// the StatefulSet is the knob-off one, and the spec holds no copy of the value.
func TestPostgresPasswordKnobOnIsASecretKeyRefAndChangesOnlyThatEntry(t *testing.T) {
	requireHelmUnderCI(t)
	for name, pair := range map[string][2][]string{"hba off": {pgCredsOff, pgCredsOn}, "hba on": {pgCredsHbaOn, pgCredsOnHba}} {
		t.Run(name, func(t *testing.T) {
			off, _ := pgPasswordRender(t, pair[0]...)
			on, secret := pgPasswordRender(t, pair[1]...)
			entry := namedEntry(pgEnv(t, on), "POSTGRES_PASSWORD")
			if _, plain := entry.(map[string]any)["value"]; plain {
				t.Errorf("POSTGRES_PASSWORD still has a plain value: %v", entry)
			}
			ref := at(entry, "valueFrom", "secretKeyRef")
			if at(ref, "name") != pgCredentialsSecret || at(ref, "key") != pgPasswordKey {
				t.Errorf("secretKeyRef = %v, want name %s key %s", ref, pgCredentialsSecret, pgPasswordKey)
			}
			if got := len(entry.(map[string]any)); got != 2 {
				t.Errorf("POSTGRES_PASSWORD entry has %d fields (%v), want name and valueFrom only", got, entry)
			}
			if strings.Contains(yamlText(on), pgDummyPassword) {
				t.Error("the StatefulSet spec still holds the password value")
			}
			if got, _ := at(secret, "stringData", pgPasswordKey).(string); got != pgDummyPassword {
				t.Errorf("Secret key %s = %q, want the postgresql.credentials.password value", pgPasswordKey, got)
			}
			// Put the knob-off entry back: everything else must be identical.
			env := at(on, "spec", "template", "spec", "containers", 0, "env").([]any)
			for i, e := range env {
				if at(e, "name") == "POSTGRES_PASSWORD" {
					env[i] = namedEntry(pgEnv(t, off), "POSTGRES_PASSWORD")
				}
			}
			if yamlText(on) != yamlText(off) {
				t.Errorf("the knob changes more than the POSTGRES_PASSWORD env entry")
			}
		})
	}
}

// The Secret renders with the store only, and also when the shared Secret is external.
func TestPostgresCredentialsSecretRendersOnlyWithTheStore(t *testing.T) {
	requireHelmUnderCI(t)
	if _, secret := pgPasswordRender(t, authBase...); secret != nil {
		t.Errorf("Secret %s renders while postgresql is disabled", pgCredentialsSecret)
	}
	external := append(append([]string{}, pgCredsOff...), "--set", "secrets.create=false", "--set", "secrets.externalSecretName=external-app-secret")
	if _, secret := pgPasswordRender(t, external...); secret == nil {
		t.Errorf("Secret %s does not render with secrets.create=false", pgCredentialsSecret)
	}
	for _, args := range [][]string{pgCredsOff, pgCredsOn} {
		if _, secret := pgPasswordRender(t, args...); secret == nil || at(secret, "metadata", "labels", "app.kubernetes.io/component") != "postgresql" {
			t.Errorf("Secret %s is missing or lacks the postgresql component label: %v", pgCredentialsSecret, secret)
		}
	}
}

// The knob is a data-store step of its own: it changes the postgresql StatefulSet and no other document.
func TestPostgresCredentialsKnobChangesOnlyThePostgresStatefulSet(t *testing.T) {
	requireHelmUnderCI(t)
	got := changedDocs(t, pgCredsOff, pgCredsOn)
	if want := []string{"StatefulSet/a-dev-health-postgresql"}; !reflect.DeepEqual(got, want) {
		t.Errorf("documents changed by postgresql.credentialsSecret.enabled = %v, want %v", got, want)
	}
}
