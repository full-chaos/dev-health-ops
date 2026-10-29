package devhealth_test

import (
	"strings"
	"testing"
)

// The legacy ingest routes are retired (CHAOS-11813): go-api reads no
// DEV_HEALTH_LEGACY_INGEST_* setting, so the chart renders none, and a values
// file or an extraEnv entry that still names one fails the render loudly
// instead of being ignored.
func TestRetiredLegacyIngestChartRendersNoSetting(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIWithAuth()...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	deployment := authDoc(docs, "Deployment", "a-dev-health-go-api")
	if deployment == nil {
		t.Fatal("no go-api Deployment in the render")
	}
	for _, container := range at(deployment, "spec", "template", "spec", "containers").([]any) {
		env, _ := at(container, "env").([]any)
		for _, entry := range env {
			if name, _ := at(entry, "name").(string); strings.HasPrefix(name, "DEV_HEALTH_LEGACY_INGEST_") {
				t.Errorf("go-api renders the retired setting %s", name)
			}
		}
	}
}

func TestRetiredLegacyIngestChartRefusesTheRetiredSettings(t *testing.T) {
	for name, args := range map[string][]string{
		"values legacyIngest.enabled": {"--set", "legacyIngest.enabled=false"},
		"values legacyIngest.sunset":  {"--set-string", "legacyIngest.sunset=Tue\\, 01 Dec 2026 00:00:00 GMT"},
		"extraEnv flag":               {"--set", "goApi.extraEnv[0].name=DEV_HEALTH_LEGACY_INGEST_ENABLED", "--set-string", "goApi.extraEnv[0].value=true"},
		"extraEnv sunset":             {"--set", "goApi.extraEnv[0].name=DEV_HEALTH_LEGACY_INGEST_SUNSET", "--set-string", "goApi.extraEnv[0].value=x"},
	} {
		_, out, err := renderAuth(t, goAPIWithAuth(args...)...)
		if err == nil {
			t.Errorf("%s: the render accepted a retired legacy ingest setting", name)
			continue
		}
		if !strings.Contains(out+err.Error(), "retired") {
			t.Errorf("%s: the refusal does not say the setting is retired: %v %.300s", name, err, out)
		}
	}
}
