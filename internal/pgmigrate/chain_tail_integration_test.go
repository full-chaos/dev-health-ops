//go:build integration

package pgmigrate_test

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// tailRoundTrip is check (g) of the post-freeze rule (CHAOS-10751): for every Go-only revision above FrozenHead, the
// schema after its down file equals the schema the upgrade to the previous revision builds, and up again returns to
// the head. A revision that changes nothing in the recorded shape fails, because the comparison would measure nothing.
func tailRoundTrip(t *testing.T) {
	t.Helper()
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	first := len(pgmigrate.ChainThrough(chain, pgmigrate.FrozenHead))
	if first == len(chain) {
		t.Skip("the Go-only tail is empty")
	}
	d := newDownInstance(t)
	want := map[int]string{}
	for k := first; k <= len(chain); k++ {
		want[k] = schemaShape(t, d.at(t, k))
		if k > first && want[k] == want[k-1] {
			t.Fatalf("chain revision %s changes nothing recorded in the shape: the comparison measures nothing", chain[k-1].Revision)
		}
	}
	head := d.at(t, len(chain))
	for k := len(chain) - 1; k >= first; k-- {
		code, stdout, stderr := goDowngrade(t, head, previousName(chain, k))
		if code != cli.ExitOK {
			t.Fatalf("downgrade %s -> %s: exit %d stderr %s", chain[k].Revision, previousName(chain, k), code, stderr)
		}
		var result pgmigrate.DowngradeResult
		if err := json.Unmarshal([]byte(stdout), &result); err != nil || !reflect.DeepEqual(result.Reverted, []string{chain[k].Revision}) {
			t.Fatalf("downgrade one step from %s: result %q (%v)", chain[k].Revision, stdout, err)
		}
		if got := schemaShape(t, head); got != want[k] {
			t.Fatalf("after reverting %s the schema is not the one the upgrade to %s builds:\n%s", chain[k].Revision, previousName(chain, k), shapeDiff(want[k], got))
		}
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(context.Background(), connect(t, head), baseline, chain); err != nil {
		t.Fatal(err)
	}
	if got := schemaShape(t, head); got != want[len(chain)] {
		t.Fatalf("up, down, up did not return to the head:\n%s", shapeDiff(want[len(chain)], got))
	}
}

func TestEveryGoOnlyTailRevisionGoesUpAndDownToThePreviousSchema(t *testing.T) { tailRoundTrip(t) }

func TestAPlantedTailGoesUpAndDownToThePreviousSchema(t *testing.T) {
	pgmigrate.UsePlantedChain(t, os.DirFS("testdata/postfreeze/two"))
	tailRoundTrip(t)
}
