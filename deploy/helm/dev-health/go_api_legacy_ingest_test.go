package devhealth_test

import "testing"

func legacyIngestEnv(t *testing.T, args ...string) map[string]string {
	t.Helper()
	docs, out, err := renderAuth(t, goAPIWithAuth(args...)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	deployment := authDoc(docs, "Deployment", "a-dev-health-go-api")
	if deployment == nil {
		t.Fatal("no go-api Deployment in the render")
	}
	got := map[string]string{}
	for _, container := range at(deployment, "spec", "template", "spec", "containers").([]any) {
		env, _ := at(container, "env").([]any)
		for _, entry := range env {
			name, _ := at(entry, "name").(string)
			if name == "DEV_HEALTH_LEGACY_INGEST_ENABLED" || name == "DEV_HEALTH_LEGACY_INGEST_SUNSET" {
				value, _ := at(entry, "value").(string)
				got[name] = value
			}
		}
	}
	return got
}

func TestLegacyIngestChartDefaultIsOffWithNoSunset(t *testing.T) {
	got := legacyIngestEnv(t)
	if got["DEV_HEALTH_LEGACY_INGEST_ENABLED"] != "false" {
		t.Fatalf("enabled = %q, want false", got["DEV_HEALTH_LEGACY_INGEST_ENABLED"])
	}
	if _, ok := got["DEV_HEALTH_LEGACY_INGEST_SUNSET"]; ok {
		t.Fatal("a sunset variable is rendered with no sunset configured")
	}
}

func TestLegacyIngestChartRendersTheFlagAndTheSunset(t *testing.T) {
	got := legacyIngestEnv(t, "--set", "legacyIngest.enabled=true", "--set-string", "legacyIngest.sunset=Tue\\, 01 Dec 2026 00:00:00 GMT")
	if got["DEV_HEALTH_LEGACY_INGEST_ENABLED"] != "true" || got["DEV_HEALTH_LEGACY_INGEST_SUNSET"] != "Tue, 01 Dec 2026 00:00:00 GMT" {
		t.Fatalf("env = %v", got)
	}
}

func TestLegacyIngestChartRefusesExtraEnvForTheFlag(t *testing.T) {
	_, _, err := renderAuth(t, goAPIWithAuth("--set", "goApi.extraEnv[0].name=DEV_HEALTH_LEGACY_INGEST_ENABLED", "--set-string", "goApi.extraEnv[0].value=true")...)
	if err == nil {
		t.Fatal("the render accepted goApi.extraEnv for the legacy ingest flag")
	}
}

func TestLegacyIngestChartRefusesANonBooleanFlag(t *testing.T) {
	for _, value := range []string{"maybe", "yes", "1", "TRUE", "on"} {
		if _, _, err := renderAuth(t, goAPIWithAuth("--set-string", "legacyIngest.enabled="+value)...); err == nil {
			t.Errorf("legacyIngest.enabled=%q rendered; the flag must be a boolean", value)
		}
	}
	for _, value := range []string{"true", "false"} {
		if _, out, err := renderAuth(t, goAPIWithAuth("--set", "legacyIngest.enabled="+value)...); err != nil {
			t.Errorf("legacyIngest.enabled=%s refused: %v %.200s", value, err, out)
		}
	}
}
