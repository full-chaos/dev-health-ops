//go:build integration

package syncadmin_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonBuild is the main commit the Python sync admin routes' answers in the
// goldens were executed on: the last build that still carried their bodies.
const pythonBuild = "758a8d9fbb1f7548bef84df452597f9e7ddf145a"

// goldenDigests pins each golden file (the digest its recording run printed).
var goldenDigests = map[string]string{
	"TestSyncConfigDeleteVenueOracle":       "3817745a64a0d6bd2509d285223e552a50932fd0c49c2e793e853e25c5c0ac9f",
	"TestSyncAdminReadsVenueOracle":         "ad16abed4bb357eaa1053ba8ae196accd157c8022945d63449043cfc14eafbfc",
	"TestSyncConfigCreateVenueOracle":       "207f1d6e166872a384300004bfdc862bf8a1e31429a0e047ebe4a8dc0785594c",
	"TestSyncConfigBatchCreateVenueOracle":  "ae1d5921be48edb7d8493d3788290a97886c185bfebf92c786cb9eff586e8b0c",
	"TestSyncConfigUpdateVenueOracle":       "8da3c94c28bb7ae4928cb0700fd5859930c0a9bb9fb1b3d626e9fec71c64c9b1",
	"TestSyncConfigRepositoriesVenueOracle": "4856559d347fedbe7e4bf7f3072d3151fb88f5d2129dd0a4000d20b0e51e4288",
}

// goldenSpec is the frozen Python golden of the test called name.
func goldenSpec(name string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        filepath.Join("testdata", "golden", name+".json"),
		PythonBuild: pythonBuild,
		SHA256:      goldenDigests[name],
		Recipe: "git worktree add --detach <dir> " + pythonBuild + "; from internal/api/syncadmin: DHO_VENUE_GOLDEN_UPDATE=1 " +
			"DHO_VENUE_GOLDEN_PYTHON_ROOT=<dir> DEV_HEALTH_LIVE_PYTHON_ORACLES=1 go test -tags=integration -count=1 -run '^" + name + "$' .",
	}
}

var (
	idMu    sync.Mutex
	idLabel string
	idSeq   int
)

// resetIDs starts the running oracle's id sequence: every id a seed or a
// request needs comes from newID, so the recording run and a frozen run (two
// processes) seed the same ids and send the same requests.
func resetIDs(t *testing.T) {
	t.Helper()
	idMu.Lock()
	defer idMu.Unlock()
	idLabel, idSeq = t.Name(), 0
}

// newID is the next deterministic id of the running oracle. Its order is the
// order the test code calls it in, so no call may sit in a map iteration.
func newID() uuid.UUID {
	idMu.Lock()
	defer idMu.Unlock()
	idSeq++
	return uuid.MustParse(venueoracle.StableUUID(fmt.Sprintf("%s#%d", idLabel, idSeq)))
}
