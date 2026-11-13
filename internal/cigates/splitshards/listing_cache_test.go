//go:build !race

package splitshards_test

import (
	"bytes"
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// A `go test -list` of one package under one build. The package, the tags and -race each change the names a build
// registers, so all three make the cache key.
type listingKey struct {
	pkg, tags string
	race      bool
}

func (k listingKey) cacheKey() string {
	return k.pkg + "|tags=" + k.tags + "|race=" + strconv.FormatBool(k.race)
}

type listingEntry struct {
	done  chan struct{}
	names map[string]bool // Test names
	kinds map[string]bool // every name go test runs: Test, Example (with output), Fuzz
	err   error
}

// At most min(GOMAXPROCS, 4) listings compile at once; a slot is released by defer when its go test -list ends.
var (
	listingMu    sync.Mutex
	listings     = map[string]*listingEntry{}
	listingSlots = make(chan struct{}, min(runtime.GOMAXPROCS(0), 4))
)

// startListing starts the listing of k once per test process and returns its entry; later calls with the same key share it.
func startListing(root, toolchain string, k listingKey) *listingEntry {
	listingMu.Lock()
	defer listingMu.Unlock()
	if e, ok := listings[k.cacheKey()]; ok {
		return e
	}
	e := &listingEntry{done: make(chan struct{})}
	listings[k.cacheKey()] = e
	go func() {
		defer close(e.done)
		listingSlots <- struct{}{}
		defer func() { <-listingSlots }()
		e.names, e.kinds, e.err = runListing(root, toolchain, k)
	}()
	return e
}

func runListing(root, toolchain string, k listingKey) (map[string]bool, map[string]bool, error) {
	args := []string{"test", "-mod=readonly"}
	if k.tags != "" {
		args = append(args, "-tags="+k.tags)
	}
	if k.race {
		args = append(args, "-race")
	}
	args = append(args, "-list", ".*", "./"+k.pkg)
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	cmd.Env = withNoPythonShims(inheritedEnv(nil, "GOTOOLCHAIN="+toolchain, "GOWORK=off"))
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return nil, nil, fmt.Errorf("go %s: %v: %s%s", strings.Join(args, " "), err, out.String(), errb.String())
	}
	names, kinds := map[string]bool{}, map[string]bool{}
	for _, l := range strings.Split(out.String(), "\n") {
		if strings.HasPrefix(l, "Test") {
			names[l] = true
		}
		if strings.HasPrefix(l, "Test") || strings.HasPrefix(l, "Example") || strings.HasPrefix(l, "Fuzz") {
			kinds[l] = true
		}
	}
	return names, kinds, nil
}

// listedOrFail waits for the listing of k. A listing that fails fails the test: never a skip.
func listedOrFail(tb testing.TB, root, toolchain string, k listingKey) map[string]bool {
	tb.Helper()
	e := startListing(root, toolchain, k)
	<-e.done
	if e.err != nil {
		tb.Fatalf("%v", e.err)
	}
	return e.names
}

// listedKindsOrFail is listedOrFail with every kind go test runs (Test, Example with output, Fuzz) from the same listing.
func listedKindsOrFail(tb testing.TB, root, toolchain string, k listingKey) map[string]bool {
	tb.Helper()
	e := startListing(root, toolchain, k)
	<-e.done
	if e.err != nil {
		tb.Fatalf("%v", e.err)
	}
	return e.kinds
}

// The cache must keep listings apart that differ only by tags, or only by -race: each pair below lists one package twice in
// this process, and each half must see its own build's names.
func TestListingCacheKeyHoldsTagsAndRace(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	tc := pinnedGoToolchain(t)
	const tagged = "TestValkeyOrgCacheInvalidatorIncrementsTheEpochWithATTL" // internal/cacheinvalidation, //go:build integration
	plain := listedOrFail(t, root, tc, listingKey{pkg: "internal/cacheinvalidation"})
	withTags := listedOrFail(t, root, tc, listingKey{pkg: "internal/cacheinvalidation", tags: "integration"})
	if plain[tagged] || !withTags[tagged] {
		t.Errorf("tags: %s listed untagged=%v tagged=%v, want false/true (the cache key lost the tags)", tagged, plain[tagged], withTags[tagged])
	}
	const notRace = "TestEverySplitByTestNameRunsInExactlyOnePackagesShard" // this package, //go:build !race
	noRace := listedOrFail(t, root, tc, listingKey{pkg: "internal/cigates/splitshards"})
	race := listedOrFail(t, root, tc, listingKey{pkg: "internal/cigates/splitshards", race: true})
	if !noRace[notRace] || race[notRace] {
		t.Errorf("race: %s listed without -race=%v with -race=%v, want true/false (the cache key lost -race)", notRace, noRace[notRace], race[notRace])
	}
}

// recordingTB keeps the Fatal and Skip calls of the code under test; each ends the calling goroutine like testing.T does.
type recordingTB struct {
	testing.TB
	mu            sync.Mutex
	fatals, skips []string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.mu.Lock()
	r.fatals = append(r.fatals, fmt.Sprintf(format, args...))
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *recordingTB) Skipf(format string, args ...any) {
	r.mu.Lock()
	r.skips = append(r.skips, fmt.Sprintf(format, args...))
	r.mu.Unlock()
	runtime.Goexit()
}

func (r *recordingTB) Skip(args ...any) { r.Skipf("%s", fmt.Sprint(args...)) }
func (r *recordingTB) SkipNow()         { r.Skipf("SkipNow") }

func TestAPackageWhoseListingFailsFailsTheTest(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	tc := pinnedGoToolchain(t)
	rec := &recordingTB{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		listedOrFail(rec, root, tc, listingKey{pkg: "internal/cigates/splitshards/no-such-package", tags: "integration"})
	}()
	<-done
	if len(rec.skips) != 0 || len(rec.fatals) != 1 || !strings.Contains(rec.fatals[0], "-list") {
		t.Errorf("a failed listing must fail once and never skip: fatals %q, skips %q", rec.fatals, rec.skips)
	}
}
