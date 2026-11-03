package devhealth_test

import (
	"os"
	"path/filepath"
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

// goAPIDisabled renders the same values with go-api off: the retired-setting refusal does not depend on it.
func goAPIDisabled(sets ...string) []string {
	return append(goAPIWithAuth(sets...), "--set", "goApi.enabled=false")
}

// A retired setting that would ask for the routes (the flag on, a sunset, an extraEnv entry in any case) fails the
// render, with go-api on and off.
func TestRetiredLegacyIngestChartRefusesTheRetiredSettings(t *testing.T) {
	for name, args := range map[string][]string{
		"values legacyIngest.enabled=true":   {"--set", "legacyIngest.enabled=true"},
		"values legacyIngest.enabled string": {"--set-string", "legacyIngest.enabled=yes"},
		"values legacyIngest.sunset":         {"--set-string", "legacyIngest.sunset=Tue\\, 01 Dec 2026 00:00:00 GMT"},
		"extraEnv flag":                      {"--set", "goApi.extraEnv[0].name=DEV_HEALTH_LEGACY_INGEST_ENABLED", "--set-string", "goApi.extraEnv[0].value=true"},
		"extraEnv flag lower case":           {"--set", "goApi.extraEnv[0].name=dev_health_legacy_ingest_enabled", "--set-string", "goApi.extraEnv[0].value=false"},
		"extraEnv sunset":                    {"--set", "goApi.extraEnv[0].name=DEV_HEALTH_LEGACY_INGEST_SUNSET", "--set-string", "goApi.extraEnv[0].value=x"},
	} {
		for mode, render := range map[string]func(...string) []string{"go-api on": goAPIWithAuth, "go-api off": goAPIDisabled} {
			_, out, err := renderAuth(t, render(args...)...)
			if err == nil {
				t.Errorf("%s, %s: the render accepted a retired legacy ingest setting", name, mode)
				continue
			}
			if !strings.Contains(out+err.Error(), "retired") {
				t.Errorf("%s, %s: the refusal does not say the setting is retired: %v %.300s", name, mode, err, out)
			}
		}
	}
}

// The values that pin the retired flag off render: dev-health-deploy's values.prod.yaml carries these lines under
// its ops key (lines 1155-1157 at 99574cc7), with go-api on.
func TestRetiredLegacyIngestChartAcceptsTheProdPinAndAnEmptySunset(t *testing.T) {
	prod := filepath.Join(t.TempDir(), "values.prod-pin.yaml")
	if err := os.WriteFile(prod, []byte("# The legacy /api/v1/ingest/* routes of go-api are off in prod (explicit pin of the chart default).\nlegacyIngest:\n  enabled: false\ngoApi:\n  enabled: true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"prod pin, go-api on":         {"-f", prod},
		"prod pin, go-api off":        {"-f", prod, "--set", "goApi.enabled=false"},
		"flag false and empty sunset": {"--set", "legacyIngest.enabled=false", "--set-string", "legacyIngest.sunset="},
	} {
		docs, out, err := renderAuth(t, goAPIWithAuth(args...)...)
		if err != nil {
			t.Errorf("%s: the render failed: %v %.400s", name, err, out)
			continue
		}
		deployment := authDoc(docs, "Deployment", "a-dev-health-go-api")
		if deployment == nil {
			continue
		}
		for _, container := range at(deployment, "spec", "template", "spec", "containers").([]any) {
			env, _ := at(container, "env").([]any)
			for _, entry := range env {
				if envName, _ := at(entry, "name").(string); strings.HasPrefix(envName, "DEV_HEALTH_LEGACY_INGEST_") {
					t.Errorf("%s: go-api renders the retired setting %s", name, envName)
				}
			}
		}
	}
}
