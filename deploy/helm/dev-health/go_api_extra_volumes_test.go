package devhealth_test

import (
	"reflect"
	"strings"
	"testing"
)

const (
	extraVolumesJSON = `[{"name":"workload-token","projected":{"sources":[{"serviceAccountToken":{"audience":"dev-health-workload-exchange","expirationSeconds":3600,"path":"token"}}]}}]`
	extraMountsJSON  = `[{"name":"workload-token","mountPath":"/var/run/dev-health/workload","readOnly":true}]`
)

func goAPIExtraVolumeSets() []string {
	return []string{"--set", "goApi.enabled=true", "--set-json", "goApi.extraVolumes=" + extraVolumesJSON, "--set-json", "goApi.extraVolumeMounts=" + extraMountsJSON}
}

func TestGoAPIExtraVolumesDefaultRendersNoVolumes(t *testing.T) {
	docs, out, err := renderAuth(t, "--set", "goApi.enabled=true")
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	dep := authDoc(docs, "Deployment", "a-dev-health-go-api")
	if dep == nil {
		t.Fatal("no go-api Deployment in the render")
	}
	if v := at(dep, "spec", "template", "spec", "volumes"); v != nil {
		t.Errorf("go-api pod volumes = %v, want no key", v)
	}
	if m := at(dep, "spec", "template", "spec", "containers", 0, "volumeMounts"); m != nil {
		t.Errorf("go-api container volumeMounts = %v, want no key", m)
	}
}

func TestGoAPIExtraVolumesRenderInPodSpecAndContainer(t *testing.T) {
	docs, out, err := renderAuth(t, goAPIExtraVolumeSets()...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	dep := authDoc(docs, "Deployment", "a-dev-health-go-api")
	if dep == nil {
		t.Fatal("no go-api Deployment in the render")
	}
	vols, _ := at(dep, "spec", "template", "spec", "volumes").([]any)
	if len(vols) != 1 || at(vols[0], "name") != "workload-token" {
		t.Fatalf("go-api pod volumes = %v, want the one workload-token volume", vols)
	}
	src := at(vols[0], "projected", "sources", 0, "serviceAccountToken")
	if at(src, "audience") != "dev-health-workload-exchange" || at(src, "path") != "token" || !reflect.DeepEqual(at(src, "expirationSeconds"), 3600) {
		t.Errorf("serviceAccountToken = %v, want audience dev-health-workload-exchange, path token, expirationSeconds 3600", src)
	}
	containers, _ := at(dep, "spec", "template", "spec", "containers").([]any)
	if len(containers) != 1 {
		t.Fatalf("go-api has %d containers, want 1", len(containers))
	}
	mounts, _ := at(containers[0], "volumeMounts").([]any)
	if len(mounts) != 1 || at(mounts[0], "name") != "workload-token" || at(mounts[0], "mountPath") != "/var/run/dev-health/workload" || at(mounts[0], "readOnly") != true {
		t.Errorf("go-api volumeMounts = %v, want the one read-only workload-token mount", mounts)
	}
	if at(dep, "spec", "template", "spec", "automountServiceAccountToken") == true {
		t.Error("the projected volume must not turn on the default automount")
	}
}

func TestGoAPIExtraVolumesLeaveOtherWorkloadsUnchanged(t *testing.T) {
	base, out, err := renderAuth(t, "--set", "goApi.enabled=true")
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	with, out, err := renderAuth(t, goAPIExtraVolumeSets()...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	if len(base) != len(with) {
		t.Fatalf("document count %d -> %d", len(base), len(with))
	}
	for i := range base {
		meta, _ := base[i]["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		if base[i]["kind"] == "Deployment" && strings.HasSuffix(name, "-go-api") {
			continue
		}
		if !reflect.DeepEqual(base[i], with[i]) {
			t.Errorf("%v %s changed with goApi.extraVolumes set", base[i]["kind"], name)
		}
	}
}

func TestGoAPIExtraVolumesSchemaRefusesNonLists(t *testing.T) {
	for _, key := range []string{"goApi.extraVolumes", "goApi.extraVolumeMounts"} {
		_, out, err := renderAuth(t, "--set", "goApi.enabled=true", "--set", key+"=oops")
		if err == nil {
			t.Errorf("%s=oops rendered; the schema must refuse a non-list", key)
		} else if !strings.Contains(out, key[strings.Index(key, ".")+1:]) {
			t.Errorf("%s refusal does not name the key:\n%.400s", key, out)
		}
	}
}
