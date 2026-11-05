package devhealth_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func queryAPIExtraVolumeSets() []string {
	return []string{"--set", "queryApi.enabled=true", "--set-json", "queryApi.extraVolumes=" + extraVolumesJSON, "--set-json", "queryApi.extraVolumeMounts=" + extraMountsJSON}
}

func TestQueryAPIExtraVolumesDefaultAddsNothing(t *testing.T) {
	docs, out, err := renderAuth(t, "--set", "queryApi.enabled=true")
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	dep := authDoc(docs, "Deployment", "a-dev-health-query-api")
	if dep == nil {
		t.Fatal("no query-api Deployment in the render")
	}
	vols, _ := at(dep, "spec", "template", "spec", "volumes").([]any)
	if len(vols) != 1 || at(vols[0], "name") != "envelope-jwks" {
		t.Errorf("query-api pod volumes = %v, want only envelope-jwks", vols)
	}
	mounts, _ := at(dep, "spec", "template", "spec", "containers", 0, "volumeMounts").([]any)
	if len(mounts) != 1 || at(mounts[0], "name") != "envelope-jwks" {
		t.Errorf("query-api volumeMounts = %v, want only envelope-jwks", mounts)
	}
}

func TestQueryAPIExtraVolumesRenderAfterEnvelopeJWKS(t *testing.T) {
	docs, out, err := renderAuth(t, queryAPIExtraVolumeSets()...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	dep := authDoc(docs, "Deployment", "a-dev-health-query-api")
	if dep == nil {
		t.Fatal("no query-api Deployment in the render")
	}
	vols, _ := at(dep, "spec", "template", "spec", "volumes").([]any)
	if len(vols) != 2 || at(vols[0], "name") != "envelope-jwks" || at(vols[1], "name") != "workload-token" {
		t.Fatalf("query-api pod volumes = %v, want envelope-jwks then workload-token", vols)
	}
	src := at(vols[1], "projected", "sources", 0, "serviceAccountToken")
	if at(src, "audience") != "dev-health-workload-exchange" || at(src, "path") != "token" || !reflect.DeepEqual(at(src, "expirationSeconds"), 3600) {
		t.Errorf("serviceAccountToken = %v", src)
	}
	mounts, _ := at(dep, "spec", "template", "spec", "containers", 0, "volumeMounts").([]any)
	if len(mounts) != 2 || at(mounts[0], "name") != "envelope-jwks" || at(mounts[1], "name") != "workload-token" || at(mounts[1], "mountPath") != "/var/run/dev-health/workload" || at(mounts[1], "readOnly") != true {
		t.Errorf("query-api volumeMounts = %v, want envelope-jwks then the read-only workload-token mount", mounts)
	}
}

func TestQueryAPIExtraVolumesLeaveOtherWorkloadsUnchanged(t *testing.T) {
	base, out, err := renderAuth(t, "--set", "queryApi.enabled=true")
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	with, out, err := renderAuth(t, queryAPIExtraVolumeSets()...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	if len(base) != len(with) {
		t.Fatalf("document count %d -> %d", len(base), len(with))
	}
	for i := range base {
		meta, _ := base[i]["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		if base[i]["kind"] == "Deployment" && name == "a-dev-health-query-api" {
			continue
		}
		if !reflect.DeepEqual(base[i], with[i]) {
			t.Errorf("%v %s changed with queryApi.extraVolumes set", base[i]["kind"], name)
		}
	}
}

func TestQueryAPIExtraVolumesSchemaRefusesNullInEveryForm(t *testing.T) {
	for _, key := range []string{"extraVolumes", "extraVolumeMounts"} {
		valuesFile := filepath.Join(t.TempDir(), "values.yaml")
		if err := os.WriteFile(valuesFile, []byte("queryApi:\n  "+key+": null\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		forms := map[string][]string{
			"values file": {"-f", valuesFile},
			"--set":       {"--set", "queryApi." + key + "=null"},
			"--set-json":  {"--set-json", "queryApi." + key + "=null"},
		}
		for form, args := range forms {
			_, out, err := renderAuth(t, append([]string{"--set", "queryApi.enabled=true"}, args...)...)
			if err == nil {
				t.Errorf("queryApi.%s null via %s rendered; the schema must refuse it", key, form)
			} else if !strings.Contains(out, key) {
				t.Errorf("queryApi.%s null via %s refusal does not name the key:\n%.400s", key, form, out)
			}
		}
	}
}
