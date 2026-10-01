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
	"TestSyncConfigDeleteVenueOracle":       "c35f5741f364888840fdd0b28002de2ff1482af95d1a5e38f81443382c4a2646",
	"TestSyncAdminReadsVenueOracle":         "a9a437e5a6d9f4caebc9173688d9dc8d64ecbd6b929f80212ebb202f68958342",
	"TestSyncConfigCreateVenueOracle":       "c7c049c41ab1151571737d3d1099aeb937186702a33397c04f879a060ba2ed07",
	"TestSyncConfigBatchCreateVenueOracle":  "db317b8c6e8c8aaee46bebf600b15505812592399fe0a2980c165eed92931fd0",
	"TestSyncConfigUpdateVenueOracle":       "1c15218b2c21e4438d2e9abea0ac36bbeacedef3e65f88e5b26373435e1242d4",
	"TestSyncConfigRepositoriesVenueOracle": "91d5d70461884f19dfb435f3ceee7d17c50751c24ec43288361f95d58140cc00",
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
