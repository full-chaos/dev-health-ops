package testshard_test

import (
	"reflect"
	"strings"
	"testing"
)

// The harness derives the providersync race test names from the race build's test files instead of compiling the race test binary
// (about 120 s and 280 CPU-seconds cold, which timed out the test leg: CHAOS-10524). providersynclist_race_test.go holds the derived
// set equal to the real listing; these cases pin the derivation itself.

func providersyncDerivedDiff(base map[string]string, mutate func(map[string]string)) (missing, extra []string) {
	want := map[string]bool{}
	for _, n := range providersyncTestNames(base) {
		want[n] = true
	}
	planted := map[string]string{}
	for k, v := range base {
		planted[k] = v
	}
	mutate(planted)
	got := map[string]bool{}
	for _, n := range providersyncTestNames(planted) {
		got[n] = true
		if !want[n] {
			extra = append(extra, n)
		}
	}
	for n := range want {
		if !got[n] {
			missing = append(missing, n)
		}
	}
	return missing, extra
}

const providersyncListFixture = `package p

import "testing"

func TestAlpha(t *testing.T) {}
func TestBeta_Sub(t *testing.T) {}
func Test(t *testing.T) {}
func TestMain(m *testing.M) {}
func Testlower(t *testing.T) {}
func TestHelperWithArg(n int) {}
func testPrivate(t *testing.T) {}
func TestMultiLine(
	t *testing.T,
) {}
func BenchmarkX(b *testing.B) {}
`

func TestProvidersyncDerivedNamesAreExactlyWhatGoTestLists(t *testing.T) {
	t.Parallel()
	got := providersyncTestNames(map[string]string{"a_test.go": providersyncListFixture})
	if want := []string{"Test", "TestAlpha", "TestBeta_Sub", "TestMultiLine"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("derived %v, want %v", got, want)
	}
}

func TestProvidersyncDerivedNamesPlantedMissingNameIsSeen(t *testing.T) {
	t.Parallel()
	base := map[string]string{"a_test.go": providersyncListFixture}
	missing, extra := providersyncDerivedDiff(base, func(m map[string]string) {
		m["a_test.go"] = strings.Replace(m["a_test.go"], "func TestAlpha(t *testing.T) {}\n", "", 1)
	})
	if !reflect.DeepEqual(missing, []string{"TestAlpha"}) || len(extra) != 0 {
		t.Errorf("missing %v extra %v", missing, extra)
	}
}

func TestProvidersyncDerivedNamesPlantedExtraNameIsSeen(t *testing.T) {
	t.Parallel()
	base := map[string]string{"a_test.go": providersyncListFixture}
	missing, extra := providersyncDerivedDiff(base, func(m map[string]string) {
		m["b_test.go"] = "package p\n\nfunc TestGamma(t *testing.T) {}\n"
	})
	if !reflect.DeepEqual(extra, []string{"TestGamma"}) || len(missing) != 0 {
		t.Errorf("missing %v extra %v", missing, extra)
	}
}

func TestProvidersyncRaceListingIsTheDerivedNamesOfTheRealFiles(t *testing.T) {
	t.Parallel()
	files, err := providersyncRaceTestFiles(t)
	if err != nil || len(files) == 0 {
		t.Fatalf("no providersync race test files: %v", err)
	}
	if got, want := strings.Fields(string(providersyncRaceListing(t))), providersyncTestNames(files); !reflect.DeepEqual(got, want) {
		t.Errorf("replayed listing differs from the derived names")
	}
}
