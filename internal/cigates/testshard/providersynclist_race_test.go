//go:build race

package testshard_test

import (
	"reflect"
	"strings"
	"testing"
)

// Oracle of the derived providersync race names: they equal the real `go test -race -list .* ./internal/providersync` output, the call
// check_go.sh's race leg makes (check_providersync_race_shard, which lists before it runs the leg's packages, so the race test binary
// is already built in the leg's cache). It runs only in a race leg; a plain test leg never compiles that binary.
func TestProvidersyncDerivedNamesEqualTheRealRaceListing(t *testing.T) {
	var real []string
	for _, l := range strings.Split(string(providersyncRealRaceListing(t)), "\n") {
		if strings.HasPrefix(l, "ok") || strings.HasPrefix(l, "FAIL") || strings.HasPrefix(l, "?") || strings.HasPrefix(l, "---") || l == "" {
			continue
		}
		real = append(real, l)
	}
	files, err := providersyncRaceTestFiles(t)
	if err != nil {
		t.Fatal(err)
	}
	if got := providersyncTestNames(files); !reflect.DeepEqual(got, providersyncSorted(real)) {
		t.Errorf("derived %d names, the real race listing has %d (every name of it must be a Test name too)", len(got), len(real))
	}
}
