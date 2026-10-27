package devhealth_test

import (
	"reflect"
	"strings"
	"testing"
)

// postgresql.credentialsSecret.existingSecret names an external Secret that holds
// POSTGRES_PASSWORD. With a name set the chart reads that Secret and renders no
// credentials Secret of its own, so the password value never enters the release
// through the chart. It wins over postgresql.credentialsSecret.enabled. An empty
// name (the default) leaves every render as it was.

const (
	pgExistingName = "external-postgres-credentials"
	pgExistingKey  = "PG_PASSWORD_FROM_OPERATOR"
)

var (
	// The shared Secret is external in production (secrets.create=false), which is the only mode the guard allows.
	pgExternalShared = []string{"--set", "secrets.create=false", "--set", "secrets.externalSecretName=external-app-secret"}
	pgExistingOff    = append(append(append([]string{}, pgCredsOff...), pgExternalShared...), "--set", "postgresql.credentialsSecret.existingSecret.name="+pgExistingName)
	pgExistingOn     = append(append([]string{}, pgExistingOff...), "--set", "postgresql.credentialsSecret.enabled=true")
	pgExistingHba    = append(append([]string{}, pgExistingOff...), "--set", "postgresql.hba.enabled=true")
	pgExistingKeyed  = append(append([]string{}, pgExistingOff...), "--set", "postgresql.credentialsSecret.existingSecret.key="+pgExistingKey)
)

func pgPasswordEntry(t *testing.T, sts map[string]any) map[string]any {
	t.Helper()
	entry, _ := namedEntry(pgEnv(t, sts), "POSTGRES_PASSWORD").(map[string]any)
	if entry == nil {
		t.Fatal("no POSTGRES_PASSWORD env entry")
	}
	return entry
}

// A name set: the env is a secretKeyRef on the external Secret and key, with no plain value, no chart credentials
// Secret, and no copy of the password anywhere in the render (hba off and on, knob off and on).
func TestPostgresExistingSecretIsReadByTheStatefulSetAndRendersNoChartSecret(t *testing.T) {
	requireHelmUnderCI(t)
	cases := map[string][]string{
		"knob unset": pgExistingOff, "knob on": pgExistingOn, "hba on": pgExistingHba,
		"custom key": pgExistingKeyed,
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			docs, out, err := renderAuth(t, args...)
			if err != nil {
				t.Fatalf("render failed: %v\n%.600s", err, out)
			}
			if authDoc(docs, "Secret", pgCredentialsSecret) != nil {
				t.Errorf("Secret %s renders with existingSecret set", pgCredentialsSecret)
			}
			sts := authDoc(docs, "StatefulSet", "a-dev-health-postgresql")
			entry := pgPasswordEntry(t, sts)
			wantKey := pgPasswordKey
			if name == "custom key" {
				wantKey = pgExistingKey
			}
			ref := at(entry, "valueFrom", "secretKeyRef")
			if at(ref, "name") != pgExistingName || at(ref, "key") != wantKey || len(entry) != 2 {
				t.Errorf("POSTGRES_PASSWORD = %v, want only a secretKeyRef on %s/%s", entry, pgExistingName, wantKey)
			}
			if strings.Contains(out, pgDummyPassword) {
				t.Error("the password value is in the render")
			}
		})
	}
}

// The winner when existingSecret AND postgresql.credentialsSecret.enabled are both set is existingSecret: the env reads
// the external Secret, never the chart Secret, and the chart Secret is not rendered.
func TestPostgresExistingSecretWinsOverTheKnob(t *testing.T) {
	requireHelmUnderCI(t)
	sts, secret := pgPasswordRender(t, pgExistingOn...)
	if secret != nil {
		t.Errorf("Secret %s renders when both are set", pgCredentialsSecret)
	}
	if name := at(pgPasswordEntry(t, sts), "valueFrom", "secretKeyRef", "name"); name != pgExistingName {
		t.Errorf("secretKeyRef name = %v, want the external %s (existingSecret wins), not %s", name, pgExistingName, pgCredentialsSecret)
	}
	onSts, _ := pgPasswordRender(t, pgExistingOn...)
	offSts, _ := pgPasswordRender(t, pgExistingOff...)
	if yamlText(onSts) != yamlText(offSts) {
		t.Error("the knob changes the StatefulSet while existingSecret is set")
	}
}

// Empty name (the default, explicit or unset): the renders are the ones the knob alone gives.
func TestPostgresExistingSecretEmptyChangesNothing(t *testing.T) {
	requireHelmUnderCI(t)
	for name, pair := range map[string][2][]string{
		"knob off": {pgCredsOff, append(append([]string{}, pgCredsOff...), "--set", "postgresql.credentialsSecret.existingSecret.name=")},
		"knob on":  {pgCredsOn, append(append([]string{}, pgCredsOn...), "--set", "postgresql.credentialsSecret.existingSecret.name=", "--set", "postgresql.credentialsSecret.existingSecret.key=OTHER")},
	} {
		t.Run(name, func(t *testing.T) {
			if got := changedDocs(t, pair[0], pair[1]); len(got) != 0 {
				t.Errorf("an empty existingSecret.name changes %v", got)
			}
			sts, secret := pgPasswordRender(t, pair[1]...)
			if secret == nil {
				t.Errorf("Secret %s does not render with an empty existingSecret.name", pgCredentialsSecret)
			}
			entry := pgPasswordEntry(t, sts)
			if name == "knob off" && entry["value"] != pgDummyPassword {
				t.Errorf("knob off: POSTGRES_PASSWORD = %v, want the plain value", entry)
			}
			if name == "knob on" && at(entry, "valueFrom", "secretKeyRef", "name") != pgCredentialsSecret {
				t.Errorf("knob on: POSTGRES_PASSWORD = %v, want a secretKeyRef on the chart Secret", entry)
			}
		})
	}
}

// Setting the name changes the postgresql StatefulSet and drops the chart Secret; nothing else.
func TestPostgresExistingSecretChangesOnlyTheStatefulSetAndTheChartSecret(t *testing.T) {
	requireHelmUnderCI(t)
	before := append(append([]string{}, pgCredsOff...), pgExternalShared...)
	got := changedDocs(t, before, pgExistingOff)
	want := []string{"Secret/" + pgCredentialsSecret, "StatefulSet/a-dev-health-postgresql"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("documents changed by existingSecret.name = %v, want %v", got, want)
	}
}

// The guard: with the chart-owned shared Secret (secrets.create=true) the URI Secrets would embed the unused
// password, so the render fails and says why; with secrets.create=false it renders.
func TestPostgresExistingSecretIsRefusedWithTheChartOwnedSharedSecret(t *testing.T) {
	requireHelmUnderCI(t)
	args := append(append([]string{}, pgCredsOff...), "--set", "postgresql.credentialsSecret.existingSecret.name="+pgExistingName)
	for _, create := range []string{"true", ""} {
		extra := args
		if create != "" {
			extra = append(append([]string{}, args...), "--set", "secrets.create="+create)
		}
		_, out, err := renderAuth(t, extra...)
		if err == nil || !strings.Contains(out, "postgresql.credentialsSecret.existingSecret") || !strings.Contains(out, "secrets.create") {
			t.Errorf("secrets.create=%q: the render was not refused with the existingSecret message (err=%v)\n%.400s", create, err, out)
		}
	}
	if _, out, err := renderAuth(t, pgExistingOff...); err != nil {
		t.Errorf("secrets.create=false is refused: %v\n%.400s", err, out)
	}
	// The guard needs the name: an empty one with the chart-owned Secret renders.
	if _, out, err := renderAuth(t, pgCredsOff...); err != nil {
		t.Errorf("no existingSecret with secrets.create=true is refused: %v\n%.400s", err, out)
	}
}
