package riverstore

import (
	"fmt"
	"strings"
	"testing"
)

// TestClassifySyncRouteMovesOnlyTheRetiredCelerySeed runs the whole state
// domain of a route row (the table's own vocabulary: two transports, two
// rollback values, paused or not) with and without a live claim. Exactly one
// of the sixteen states is moved: transport celery, no rollback route, not
// paused, no live claim. A row already on river with no rollback route and
// not paused is present. Every other state is held, which means not changed.
func TestClassifySyncRouteMovesOnlyTheRetiredCelerySeed(t *testing.T) {
	moved, present, held := 0, 0, 0
	for _, transport := range []string{"celery", "river"} {
		for _, rollback := range []string{"celery", "none"} {
			for _, paused := range []bool{false, true} {
				for _, liveClaims := range []int64{0, 1} {
					row := syncRouteRow{transport: transport, rollbackTransport: rollback, paused: paused, generation: 2}
					want := SyncRouteHeld
					switch {
					case transport == "river" && rollback == "none" && !paused:
						want = SyncRoutePresent
						present++
					case transport == "celery" && rollback == "none" && !paused && liveClaims == 0:
						want = SyncRouteMoved
						moved++
					default:
						held++
					}
					name := fmt.Sprintf("transport=%s rollback=%s paused=%t live_claims=%d", transport, rollback, paused, liveClaims)
					if got := classifySyncRoute(row, liveClaims); got != want {
						t.Errorf("%s: outcome %q, want %q", name, got, want)
					}
				}
			}
		}
	}
	if moved != 1 || present != 2 || held != 13 {
		t.Fatalf("the domain has %d moved, %d present and %d held states; want 1, 2 and 13", moved, present, held)
	}
}

// TestRetiredCelerySeedIsOneExactState names each clause of the seed
// predicate on its own: changing any one field of the seed row makes it
// something else.
func TestRetiredCelerySeedIsOneExactState(t *testing.T) {
	seed := syncRouteRow{transport: "celery", rollbackTransport: "none", paused: false, generation: 2}
	if !seed.retiredCelerySeed() {
		t.Fatal("the fresh-database row (celery, rollback none, not paused) is not read as the retired seed")
	}
	for name, row := range map[string]syncRouteRow{
		"on river":                {transport: "river", rollbackTransport: "none"},
		"still names a rollback":  {transport: "celery", rollbackTransport: "celery"},
		"paused":                  {transport: "celery", rollbackTransport: "none", paused: true},
		"river with a rollback":   {transport: "river", rollbackTransport: "celery"},
		"an unknown transport":    {transport: "", rollbackTransport: "none"},
		"an unknown rollback":     {transport: "celery", rollbackTransport: ""},
		"paused with a rollback":  {transport: "celery", rollbackTransport: "celery", paused: true},
		"paused river, no rollba": {transport: "river", rollbackTransport: "none", paused: true},
	} {
		if row.retiredCelerySeed() {
			t.Errorf("%s (%+v) is read as the retired seed", name, row)
		}
	}
	for name, row := range map[string]syncRouteRow{
		"celery":                {transport: "celery", rollbackTransport: "none"},
		"river with a rollback": {transport: "river", rollbackTransport: "celery"},
		"paused river":          {transport: "river", rollbackTransport: "none", paused: true},
	} {
		if row.onCheckedInRoute() {
			t.Errorf("%s (%+v) is read as already on the checked-in route", name, row)
		}
	}
	if !(syncRouteRow{transport: "river", rollbackTransport: "none"}).onCheckedInRoute() {
		t.Fatal("a river row with no rollback route, not paused, is not read as on the checked-in route")
	}
}

// TestMigrateRefusesMalformedSyncRouteKinds pins the option guard for the
// sync-dispatch kinds: a kind outside the contract's grammar, or a repeated
// kind, stops the run before it touches the database.
func TestMigrateRefusesMalformedSyncRouteKinds(t *testing.T) {
	base := MigrationOptions{Schema: "river", DomainRole: "route_domain", QueueRole: "route_queue"}
	for _, tc := range []struct {
		name  string
		kinds []string
		ok    bool
	}{
		{"none", nil, true},
		{"empty list", []string{}, true},
		{"the four kinds", []string{"dispatch_sync_run", "finalize_sync_run", "post_sync", "reference_discovery"}, true},
		{"at bound", []string{strings.Repeat("a", 96)}, true},
		{"past bound", []string{strings.Repeat("a", 97)}, false},
		{"empty kind", []string{""}, false},
		{"a job kind", []string{"sync.provider_unit"}, false},
		{"upper case", []string{"Post_Sync"}, false},
		{"leading digit", []string{"1_sync"}, false},
		{"injection", []string{"post_sync'; DROP TABLE x; --"}, false},
		{"duplicate", []string{"post_sync", "post_sync"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options := base
			options.RiverSyncDispatchRoutes = tc.kinds
			err := ValidateMigrationOptions(options)
			if tc.ok != (err == nil) {
				t.Fatalf("ValidateMigrationOptions(%q) err=%v", tc.kinds, err)
			}
		})
	}
}
