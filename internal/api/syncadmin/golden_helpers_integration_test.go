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
	"TestSyncConfigDeleteVenueOracle":       "38b332f2cfb6179a5ef7236adefa5d1220f98c27993812a5d1daa3be0e980408",
	"TestSyncAdminReadsVenueOracle":         "56c8fede2fe164be0e885d550b43b4c5a0ded91b40a4f1be0cf80992739d4207",
	"TestSyncConfigCreateVenueOracle":       "96c21372fc0cc0153411d7ab304670bcfc73bd461cda4d5d09938699c846de41",
	"TestSyncConfigBatchCreateVenueOracle":  "d17c7a33c289eb6313a048e93580fee82bf66c43ab8d5b9166a810a68aa6a376",
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
