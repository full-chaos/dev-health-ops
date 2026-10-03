package pgmigrate_test

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// statesPythonBuild is the build whose Python answered the upgrade, walk and derivation oracles of this package
// (the same build as the downgrade golden).
const statesPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// oldScriptHeadsSHA256 pins testdata/golden/old_script_heads.json (the record verb rewrites it).
const oldScriptHeadsSHA256 = "fb2bed87e6727718c886dab7081189a3c4e8b217f8e0642293847311469c345b"

// oldScriptDerivation is the head derivation the roll pre-check (hook-parity-check.sh, the script `preflight`
// replaces) ran over the Alembic scripts: every revision that no other revision names as its down_revision.
const oldScriptDerivation = `
import re, sys, pathlib
revs, downs = {}, set()
for f in pathlib.Path(sys.argv[1]).glob("[0-9]*.py"):
    t = f.read_text()
    r = re.search(r'^revision\s*(?::[^=]+)?=\s*["\']([^"\']+)["\']', t, re.M)
    d = re.search(r'^down_revision\s*(?::[^=]+)?=\s*(.+)$', t, re.M)
    if not r: continue
    revs[r.group(1)] = 1
    if d: downs.update(re.findall(r'["\']([^"\']+)["\']', d.group(1)))
print(" ".join(sorted(x for x in revs if x not in downs)))
`

// TestBuildHeadsAreTheOldScriptsHeads is the old-script oracle: the heads the roll pre-check derived from the
// Alembic scripts are the heads the preflight reports for this build. The derivation ran once over the scripts
// of statesPythonBuild and its answer is frozen in testdata/golden/old_script_heads.json (CHAOS-7797), so no
// Python starts here and the test outlives the scripts. An empty answer fails: the measurement did not happen.
func TestBuildHeadsAreTheOldScriptsHeads(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/old_script_heads.json",
		PythonBuild: statesPythonBuild,
		SHA256:      oldScriptHeadsSHA256,
		Recipe: "git worktree add --detach $DIR " + statesPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/pgmigrate/ -test '^TestBuildHeadsAreTheOldScriptsHeads$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)
	request := venueoracle.ProgramRequest("old script derivation", oldScriptDerivation, nil, nil)
	answers := golden.Produce(t, root, []venueoracle.Request{request}, func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
		versions := filepath.Join(producer.Root, "src", "dev_health_ops", "alembic", "versions")
		command, err := producer.Command(context.Background(), nil, nil, "-c", oldScriptDerivation, versions)
		if err != nil {
			t.Fatal(err)
		}
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		if err := command.Run(); err != nil {
			t.Fatalf("the old script's derivation failed: %v", pyoracle.RunError(command.Path, err, []byte(stderr.String())))
		}
		return []venueoracle.Response{{Status: 0, Body: stdout.String()}}
	})
	golden.Consumed(t, answers...)
	derived := strings.Fields(answers[0].Body)
	if len(derived) == 0 {
		t.Fatal("the old script's derivation found no revision: the measurement did not happen")
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if got := pgmigrate.Heads(baseline, chain); !reflect.DeepEqual(got, derived) {
		t.Fatalf("the old script derived the heads %v, the preflight's build_heads are %v", derived, got)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}
