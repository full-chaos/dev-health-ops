package devhealth_test

import (
	"reflect"
	"strings"
	"testing"
)

// The bundled ClickHouse's CLICKHOUSE_PASSWORD is a plain env value, so it is in the StatefulSet spec.
// clickhouse.credentialsSecret.enabled reads it from a chart Secret instead; existingSecret names a Secret created
// outside the release and wins over the switch. Off and unset leave every render as it was.

// A dummy: the tests never use a real credential.
const chDummyPassword = "dummy-ch-pass-0001"

const (
	chCredentialsSecret = "a-dev-health-clickhouse-credentials"
	chPasswordKey       = "CLICKHOUSE_PASSWORD"
	chExistingName      = "external-clickhouse-credentials"
	chExistingKey       = "CH_PASSWORD_FROM_OPERATOR"
	chStatefulSet       = "a-dev-health-clickhouse"
)

var (
	chOff          = append(append([]string{}, authBase...), "--set", "clickhouse.credentials.password="+chDummyPassword)
	chOn           = append(append([]string{}, chOff...), "--set", "clickhouse.credentialsSecret.enabled=true")
	chExternal     = []string{"--set", "secrets.create=false", "--set", "secrets.externalSecretName=external-app-secret"}
	chOffExternal  = append(append([]string{}, chOff...), chExternal...)
	chExistingOff  = append(append([]string{}, chOffExternal...), "--set", "clickhouse.credentialsSecret.existingSecret.name="+chExistingName)
	chExistingOn   = append(append([]string{}, chExistingOff...), "--set", "clickhouse.credentialsSecret.enabled=true")
	chExistingKeyd = append(append([]string{}, chExistingOff...), "--set", "clickhouse.credentialsSecret.existingSecret.key="+chExistingKey)
)

func chRender(t *testing.T, args ...string) (map[string]any, map[string]any, string) {
	t.Helper()
	docs, out, err := renderAuth(t, args...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	return authDoc(docs, "StatefulSet", chStatefulSet), authDoc(docs, "Secret", chCredentialsSecret), out
}

func chEnv(t *testing.T, sts map[string]any) []any {
	t.Helper()
	if sts == nil {
		t.Fatal("no clickhouse StatefulSet rendered")
	}
	c := at(sts, "spec", "template", "spec", "containers", 0)
	if at(c, "name") != "clickhouse" {
		t.Fatalf("container 0 is %v, want clickhouse", at(c, "name"))
	}
	env, _ := at(c, "env").([]any)
	if len(env) == 0 {
		t.Fatal("the clickhouse container has no env")
	}
	return env
}

func chPasswordEntry(t *testing.T, sts map[string]any) map[string]any {
	t.Helper()
	entry, _ := namedEntry(chEnv(t, sts), chPasswordKey).(map[string]any)
	if entry == nil {
		t.Fatal("no CLICKHOUSE_PASSWORD env entry")
	}
	return entry
}

// Off (the default): the plain env list and no chart Secret.
func TestClickHousePasswordKnobOffKeepsThePlainEnvAndRendersNoSecret(t *testing.T) {
	requireHelmUnderCI(t)
	wantEnv := []any{
		map[string]any{"name": "CLICKHOUSE_USER", "value": "ch"},
		map[string]any{"name": "CLICKHOUSE_PASSWORD", "value": chDummyPassword},
		map[string]any{"name": "CLICKHOUSE_DB", "value": "default"},
		map[string]any{"name": "CLICKHOUSE_LISTEN_HOST", "value": "0.0.0.0"},
	}
	explicitOff := append(append([]string{}, chOff...), "--set", "clickhouse.credentialsSecret.enabled=false")
	for name, args := range map[string][]string{"unset": chOff, "explicit false": explicitOff} {
		t.Run(name, func(t *testing.T) {
			sts, secret, _ := chRender(t, args...)
			if got := chEnv(t, sts); !reflect.DeepEqual(got, wantEnv) {
				t.Errorf("env = %v, want the plain list %v", got, wantEnv)
			}
			if secret != nil {
				t.Errorf("Secret %s renders with the switch off", chCredentialsSecret)
			}
		})
	}
}

// On: the chart Secret carries the same value, the env is a secretKeyRef on it (exactly the three leaves), and the
// rest of the StatefulSet is the switch-off one. The spec holds no copy of the value.
func TestClickHousePasswordKnobOnIsASecretKeyRefAndChangesOnlyThatEntry(t *testing.T) {
	requireHelmUnderCI(t)
	off, _, _ := chRender(t, chOff...)
	on, secret, _ := chRender(t, chOn...)
	if secret == nil {
		t.Fatalf("Secret %s does not render with the switch on", chCredentialsSecret)
	}
	if got, _ := at(secret, "stringData", chPasswordKey).(string); got != chDummyPassword {
		t.Errorf("Secret key %s = %q, want the clickhouse.credentials.password value", chPasswordKey, got)
	}
	if at(secret, "type") != "Opaque" || at(secret, "metadata", "labels", "app.kubernetes.io/component") != "clickhouse" {
		t.Errorf("Secret type or component label wrong: %v", secret)
	}
	entry := chPasswordEntry(t, on)
	if _, plain := entry["value"]; plain || len(entry) != 2 {
		t.Errorf("CLICKHOUSE_PASSWORD = %v, want name and valueFrom only", entry)
	}
	ref := at(entry, "valueFrom", "secretKeyRef")
	if at(ref, "name") != chCredentialsSecret || at(ref, "key") != chPasswordKey {
		t.Errorf("secretKeyRef = %v, want %s/%s", ref, chCredentialsSecret, chPasswordKey)
	}
	if strings.Contains(yamlText(on), chDummyPassword) {
		t.Error("the StatefulSet spec still holds the password value")
	}
	env := at(on, "spec", "template", "spec", "containers", 0, "env").([]any)
	for i, e := range env {
		if at(e, "name") == chPasswordKey {
			env[i] = namedEntry(chEnv(t, off), chPasswordKey)
		}
	}
	if yamlText(on) != yamlText(off) {
		t.Error("the switch changes more than the CLICKHOUSE_PASSWORD env entry")
	}
}

// existingSecret set: the env is a secretKeyRef on it (the default key or a given one), no chart Secret, and no copy
// of the value in the render, with the switch off and on.
func TestClickHouseExistingSecretIsReadByTheStatefulSetAndRendersNoChartSecret(t *testing.T) {
	requireHelmUnderCI(t)
	for name, args := range map[string][]string{"switch off": chExistingOff, "switch on": chExistingOn, "custom key": chExistingKeyd} {
		t.Run(name, func(t *testing.T) {
			sts, secret, out := chRender(t, args...)
			if secret != nil {
				t.Errorf("Secret %s renders with existingSecret set", chCredentialsSecret)
			}
			wantKey := chPasswordKey
			if name == "custom key" {
				wantKey = chExistingKey
			}
			entry := chPasswordEntry(t, sts)
			ref := at(entry, "valueFrom", "secretKeyRef")
			if at(ref, "name") != chExistingName || at(ref, "key") != wantKey || len(entry) != 2 {
				t.Errorf("CLICKHOUSE_PASSWORD = %v, want only a secretKeyRef on %s/%s", entry, chExistingName, wantKey)
			}
			if strings.Contains(out, chDummyPassword) {
				t.Error("the password value is in the render")
			}
		})
	}
}

// The winner when existingSecret and the switch are both set is existingSecret.
func TestClickHouseExistingSecretWinsOverTheSwitch(t *testing.T) {
	requireHelmUnderCI(t)
	on, secret, _ := chRender(t, chExistingOn...)
	off, _, _ := chRender(t, chExistingOff...)
	if secret != nil {
		t.Errorf("Secret %s renders when both are set", chCredentialsSecret)
	}
	if name := at(chPasswordEntry(t, on), "valueFrom", "secretKeyRef", "name"); name != chExistingName {
		t.Errorf("secretKeyRef name = %v, want the external %s (existingSecret wins)", name, chExistingName)
	}
	if yamlText(on) != yamlText(off) {
		t.Error("the switch changes the StatefulSet while existingSecret is set")
	}
}

// An empty name changes nothing.
func TestClickHouseExistingSecretEmptyChangesNothing(t *testing.T) {
	requireHelmUnderCI(t)
	empty := []string{"--set", "clickhouse.credentialsSecret.existingSecret.name=", "--set", "clickhouse.credentialsSecret.existingSecret.key=OTHER"}
	for name, base := range map[string][]string{"switch off": chOff, "switch on": chOn} {
		t.Run(name, func(t *testing.T) {
			if got := changedDocs(t, base, append(append([]string{}, base...), empty...)); len(got) != 0 {
				t.Errorf("an empty existingSecret.name changes %v", got)
			}
		})
	}
}

// The switch adds the chart Secret and changes the StatefulSet; existingSecret changes the StatefulSet only; with the
// switch off and no name nothing is added to the render.
func TestClickHousePasswordChangesOnlyTheStatefulSetAndTheChartSecret(t *testing.T) {
	requireHelmUnderCI(t)
	if got, want := changedDocs(t, chOff, chOn), []string{"Secret/" + chCredentialsSecret, "StatefulSet/" + chStatefulSet}; !reflect.DeepEqual(got, want) {
		t.Errorf("documents changed by the switch = %v, want %v", got, want)
	}
	if got, want := changedDocs(t, chOffExternal, chExistingOff), []string{"StatefulSet/" + chStatefulSet}; !reflect.DeepEqual(got, want) {
		t.Errorf("documents changed by existingSecret.name = %v, want %v", got, want)
	}
}

// The chart Secret renders only with the store, and also when the shared Secret is external.
func TestClickHouseCredentialsSecretRendersOnlyWithTheStore(t *testing.T) {
	requireHelmUnderCI(t)
	noStore := append(append([]string{}, chOn...), "--set", "clickhouse.enabled=false")
	if _, secret, _ := chRender(t, noStore...); secret != nil {
		t.Errorf("Secret %s renders while clickhouse is disabled", chCredentialsSecret)
	}
	onExternal := append(append([]string{}, chOn...), chExternal...)
	if _, secret, _ := chRender(t, onExternal...); secret == nil {
		t.Errorf("Secret %s does not render with secrets.create=false", chCredentialsSecret)
	}
}

// The guard: with the chart-owned shared Secret the URI Secrets would embed the unused password, so the render fails.
func TestClickHouseExistingSecretIsRefusedWithTheChartOwnedSharedSecret(t *testing.T) {
	requireHelmUnderCI(t)
	args := append(append([]string{}, chOff...), "--set", "clickhouse.credentialsSecret.existingSecret.name="+chExistingName)
	for _, create := range []string{"true", ""} {
		extra := args
		if create != "" {
			extra = append(append([]string{}, args...), "--set", "secrets.create="+create)
		}
		_, out, err := renderAuth(t, extra...)
		if err == nil || !strings.Contains(out, "clickhouse.credentialsSecret.existingSecret") || !strings.Contains(out, "secrets.create") {
			t.Errorf("secrets.create=%q: the render was not refused with the existingSecret message (err=%v)\n%.400s", create, err, out)
		}
	}
	if _, out, err := renderAuth(t, chExistingOff...); err != nil {
		t.Errorf("secrets.create=false is refused: %v\n%.400s", err, out)
	}
	if _, out, err := renderAuth(t, chOff...); err != nil {
		t.Errorf("no existingSecret with secrets.create=true is refused: %v\n%.400s", err, out)
	}
}
