//go:build integration

package syncadmin_test

import (
	"fmt"
	"path/filepath"
	"regexp"
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
	"TestSyncConfigDeleteVenueOracle":       "a79db6c9b207f05caabaabcf0695a4079ac3d093d1fffe166d9d764bc689c8fd",
	"TestSyncAdminReadsVenueOracle":         "a7f908630e66aa2e66820e3dca9b1cf3ecfae1ab067498f251bdae06578d720b",
	"TestSyncConfigCreateVenueOracle":       "4ec4f13e474392b9f6155ab3aa5f479b0d2e6ab0c610e18351e5ced0bcf97be5",
	"TestSyncConfigBatchCreateVenueOracle":  "7d19f263635da2bca6d5bc150f43af5b5b3fc9ed8285c31d8ca78120f3dfce8e",
	"TestSyncConfigUpdateVenueOracle":       "7f9bb3f00c4883ebee21585bdd766ff793b7eb85377f8bec54d1c32be65177a3",
	"TestSyncConfigRepositoriesVenueOracle": "f3d5b9d67ad9c089e5c87151924ae47cf20eb46ffa7fb5be5f49477ee9db6dea",
}

// goldenSpec is the frozen Python golden of the test called name.
func goldenSpec(name string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        filepath.Join("testdata", "golden", name+".json"),
		PythonBuild: pythonBuild,
		SHA256:      goldenDigests[name],
		Recipe: "git worktree add --detach <dir> " + pythonBuild + "; from internal/api/syncadmin: DHO_VENUE_GOLDEN_UPDATE=1 " +
			"DHO_VENUE_GOLDEN_PYTHON_ROOT=<dir> DEV_HEALTH_VENUE_ORACLES=1 go test -tags=integration -count=1 -run '^" + name + "$' .",
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

// derivedTargets is the ONE named, deliberate difference of the config
// responses from the recorded Python answer (owner's decision of 2026-10-06;
// Linear project document "Sync configuration: the dataset row is the single
// owner"): a whole-integration config answers sync_targets from its
// integration's ENABLED dataset rows, where the recorded Python answer echoed
// the stored list. It applies only to the seeded configs named here: each is
// a whole-integration config whose integration has no enabled dataset row, so
// the Go answer is [] and the recorded answer is the stored list. Every other
// config's list, and every other field of these configs, is still compared.
//
// The Go answer is not blanked on trust: inspect requires the named config
// to carry exactly its stated list in the raw Go response, and finish fails
// when a named config never appeared (a difference that stopped occurring).
type derivedTargets struct {
	// want is config id -> the sync_targets JSON the Go api must answer.
	want map[string]string
	seen map[string]bool
}

// newDerivedTargets names the configs by id (a name is not unique across
// the seeded orgs), each with the list the Go api must answer.
func newDerivedTargets(list string, configs ...uuid.UUID) *derivedTargets {
	d := &derivedTargets{want: map[string]string{}, seen: map[string]bool{}}
	for _, config := range configs {
		d.want[config.String()] = list
	}
	return d
}

// configTargets matches one config object's id and sync_targets as the
// response renders them (id, name, provider, credential_id, sync_targets).
var configTargets = regexp.MustCompile(`"id":"([^"]*)","name":"[^"]*","provider":"[^"]*","credential_id":(?:null|"[^"]*"),"sync_targets":(\[[^\]]*\])`)

const derivedTargetsMark = `"<named divergence: derived from the dataset rows>"`

// normalize blanks the list of the named configs, on both planes' bodies.
func (d *derivedTargets) normalize(body string) string {
	return configTargets.ReplaceAllStringFunc(body, func(match string) string {
		parts := configTargets.FindStringSubmatch(match)
		if _, ok := d.want[parts[1]]; !ok {
			return match
		}
		return match[:len(match)-len(parts[2])] + derivedTargetsMark
	})
}

// inspect checks the raw Go body: a named config carries its stated list.
func (d *derivedTargets) inspect(t *testing.T, request string, goBody string) {
	t.Helper()
	for _, parts := range configTargets.FindAllStringSubmatch(goBody, -1) {
		want, ok := d.want[parts[1]]
		if !ok {
			continue
		}
		d.seen[parts[1]] = true
		if parts[2] != want {
			t.Errorf("%s: config %s answers sync_targets %s, want %s (the list its enabled dataset rows give)", request, parts[1], parts[2], want)
		}
	}
}

// finish fails when a named config was never answered by the Go api.
func (d *derivedTargets) finish(t *testing.T) {
	t.Helper()
	for id := range d.want {
		if !d.seen[id] {
			t.Errorf("named divergence for config %s matched no Go response: remove it or fix the id", id)
		}
	}
}
