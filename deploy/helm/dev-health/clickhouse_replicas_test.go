package devhealth_test

import (
	"os/exec"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func renderClickHouseSts(t *testing.T, args ...string) map[string]interface{} {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	out, err := exec.Command("helm", append([]string{"template", "t", ".", "--show-only", "templates/clickhouse.yaml"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template: %v: %.500s", err, out)
	}
	dec := yaml.NewDecoder(strings.NewReader(string(out)))
	for {
		var raw map[string]interface{}
		if err := dec.Decode(&raw); err != nil {
			t.Fatal("no StatefulSet in the render")
		}
		if raw["kind"] == "StatefulSet" {
			return raw
		}
	}
}

// The in-cluster ClickHouse replica count is a value. 0 must render as 0 (not fall back to 1) and the claim
// template, hence the PVC claim, must be identical for 0 and 1: scaling to 0 keeps the data volume.
func TestClickHouseReplicasIsAValueAndLeavesTheClaimTemplateAlone(t *testing.T) {
	def := renderClickHouseSts(t)
	zero := renderClickHouseSts(t, "--set", "clickhouse.replicas=0")
	three := renderClickHouseSts(t, "--set", "clickhouse.replicas=3")
	for name, c := range map[string]struct {
		sts  map[string]interface{}
		want int
	}{"default": {def, 1}, "zero": {zero, 0}, "three": {three, 3}} {
		got, ok := c.sts["spec"].(map[string]interface{})["replicas"].(int)
		if !ok || got != c.want {
			t.Fatalf("%s: spec.replicas = %v, want int %d", name, c.sts["spec"].(map[string]interface{})["replicas"], c.want)
		}
	}
	claims := func(s map[string]interface{}) interface{} { return s["spec"].(map[string]interface{})["volumeClaimTemplates"] }
	if claims(def) == nil {
		t.Fatal("default render has no volumeClaimTemplates")
	}
	if !reflect.DeepEqual(claims(def), claims(zero)) || !reflect.DeepEqual(claims(def), claims(three)) {
		t.Fatalf("volumeClaimTemplates differ with the replica count")
	}
	strip := func(s map[string]interface{}) string {
		spec := s["spec"].(map[string]interface{})
		cp := map[string]interface{}{}
		for k, v := range spec {
			if k != "replicas" {
				cp[k] = v
			}
		}
		b, _ := yaml.Marshal(cp)
		return string(b)
	}
	if strip(def) != strip(zero) {
		t.Fatal("the replica count changed more than spec.replicas")
	}
}
