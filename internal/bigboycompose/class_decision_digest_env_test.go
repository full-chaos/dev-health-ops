package bigboycompose_test

import (
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-8744: the migrate one-shot of the base compose file and of bigboy's images overlay (in the cut's
// chain) pass DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST through from the invoking environment, empty by default:
// the compose form of the chart's migrations.hook.classDecisionLiveSchemaDigest.
func TestMigrateServicePassesTheClassDecisionLiveSchemaDigest(t *testing.T) {
	const name, want = "DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST", "${DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST:-}"
	for _, path := range []string{
		filepath.Join(opsRoot(t), "compose.yml"),
		filepath.Join(toolsDir(t), imagesOverlayName),
	} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			var doc struct {
				Services map[string]struct {
					Environment map[string]string `yaml:"environment"`
				} `yaml:"services"`
			}
			if err := yaml.Unmarshal([]byte(read(t, path)), &doc); err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			migrate, ok := doc.Services["migrate"]
			if !ok {
				t.Fatalf("%s has no migrate service: nothing was checked", path)
			}
			if got, ok := migrate.Environment[name]; !ok || got != want {
				t.Fatalf("%s migrate environment %s = %q (present %v), want %q", path, name, got, ok, want)
			}
		})
	}
}
