package admincli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func TestTheSeedVerbIsReachable(t *testing.T) {
	if err := cli.Validate([]cli.Command{Command()}); err != nil {
		t.Fatal(err)
	}
	command := Command()
	if command.Name != "admin" || command.Children[0].Name != "features" || command.Children[0].Children[0].Name != "seed" {
		t.Fatalf("the tree is %+v, want admin features seed", command)
	}
}

// The verb refuses arguments, and a missing database, before connecting.
func TestSeedRefusesBeforeConnecting(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := cli.Env{Args: []string{"extra"}, Stdout: &stdout, Stderr: &stderr, Lookup: func(string) (string, bool) { return "", false }}
	if code := runSeed(context.Background(), env); code != cli.ExitUsage {
		t.Fatalf("a positional argument exited %d, want %d", code, cli.ExitUsage)
	}
	stderr.Reset()
	env.Args = nil
	if code := runSeed(context.Background(), env); code != cli.ExitFailure ||
		!strings.Contains(stderr.String(), "neither MIGRATION_DATABASE_URI nor POSTGRES_URI is set") {
		t.Fatalf("no database exited %d with %q", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

// Every key is unique, and every category and tier is one the Python enums
// define.
func TestStandardFeaturesAreWellFormed(t *testing.T) {
	categories := map[string]bool{"core": true, "analytics": true, "integrations": true, "security": true, "compliance": true, "admin": true}
	tiers := map[string]bool{"community": true, "team": true, "enterprise": true}
	seen := map[string]bool{}
	for _, feature := range StandardFeatures {
		if seen[feature.Key] || feature.Name == "" || !categories[feature.Category] || !tiers[feature.MinTier] {
			t.Fatalf("malformed or duplicate feature %+v", feature)
		}
		seen[feature.Key] = true
	}
	if len(StandardFeatures) == 0 {
		t.Fatal("no standard feature")
	}
}
