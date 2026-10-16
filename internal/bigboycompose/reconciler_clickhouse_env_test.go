package bigboycompose_test

import (
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-9475: the operator verbs that run in the bigboy go-reconciler container (dho workers metrics
// finalize-redrive, daily-marker-backfill) open ClickHouse through that container's environment. The overlay
// must blank CLICKHOUSE_URI (else the service inherits ops/.env's host URI, which the container cannot reach,
// and a URI beside the components is refused as two forms of one connection) and carry the same DEV_HEALTH_CH_*
// component values as the go-worker fleet's base environment in compose.yml, which this test reads, so the
// two cannot drift.
func TestBigboyReconcilerOpensClickHouseByTheWorkerFleetComponents(t *testing.T) {
	var base struct {
		Env map[string]string `yaml:"x-go-worker-env-base"`
	}
	if err := yaml.Unmarshal([]byte(read(t, filepath.Join(opsRoot(t), "compose.yml"))), &base); err != nil {
		t.Fatal(err)
	}
	var overlay struct {
		Services map[string]struct {
			Environment environment `yaml:"environment"`
		} `yaml:"services"`
	}
	path := filepath.Join(toolsDir(t), composeName)
	if err := yaml.Unmarshal([]byte(read(t, path)), &overlay); err != nil {
		t.Fatal(err)
	}
	reconciler, ok := overlay.Services["go-reconciler"]
	if !ok {
		t.Fatalf("%s has no go-reconciler service: nothing was checked", path)
	}
	if uri, present := reconciler.Environment["CLICKHOUSE_URI"]; !present || uri != "" {
		t.Errorf("go-reconciler CLICKHOUSE_URI = %q (present %v), want it present and blank", uri, present)
	}
	checked := 0
	for _, name := range []string{"DEV_HEALTH_CH_HOST", "DEV_HEALTH_CH_PORT", "DEV_HEALTH_CH_USER", "DEV_HEALTH_CH_PASSWORD", "DEV_HEALTH_CH_DB"} {
		want, ok := base.Env[name]
		if !ok || want == "" {
			t.Fatalf("compose.yml x-go-worker-env-base has no %s: the reference is gone", name)
		}
		if got := reconciler.Environment[name]; got != want {
			t.Errorf("go-reconciler %s = %q, want %q (the go-worker fleet's value)", name, got, want)
		}
		checked++
	}
	if checked != 5 {
		t.Fatalf("checked %d component settings, want 5", checked)
	}
}
