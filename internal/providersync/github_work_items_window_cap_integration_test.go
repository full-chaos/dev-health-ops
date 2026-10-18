//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"
)

// windowCapWindows splits the first-sync depth into consecutive windows of at
// most spanDays days, oldest first, the way the planner's window cap plans
// them one per scheduled run (TestWorkItemFamilyWindowsWalkTheDepthOldestFirst
// in internal/scheduler/sync pins that walk).
func windowCapWindows(spanDays int) []windowEquivalenceWindow {
	windows := []windowEquivalenceWindow{}
	for start := windowEquivalenceStart; start.Before(windowEquivalenceNow); {
		end := start.AddDate(0, 0, spanDays)
		if end.After(windowEquivalenceNow) {
			end = windowEquivalenceNow
		}
		windows = append(windows, windowEquivalenceWindow{since: start, before: end})
		start = end
	}
	return windows
}

// TestGitHubWorkItemsOneWindowAndManyWindowsStoreTheSameRows syncs one
// repository's first-sync depth twice through the production route: as ONE
// 90-day unit, and as thirteen capped 7-day windows, each into its own
// ClickHouse built from the real migration chain. Every row of every raw
// destination table the unit writes must be the same in both stores (the
// derived tables are written by the daily job, not by the unit).
func TestGitHubWorkItemsOneWindowAndManyWindowsStoreTheSameRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	_, oneConn := newWorkItemEffectsConn(t)
	_, manyConn := newWorkItemEffectsConn(t)
	pool, repository := newWindowEquivalencePostgres(t, ctx)

	many := windowCapWindows(7)
	if len(many) != 13 {
		t.Fatalf("windows = %d, want 13", len(many))
	}
	oneTables := runWindowEquivalenceArm(t, ctx, pool, repository, oneConn, "11111111",
		[]windowEquivalenceWindow{{since: windowEquivalenceStart, before: windowEquivalenceNow}})
	manyTables := runWindowEquivalenceArm(t, ctx, pool, repository, manyConn, "22222222", many)

	stamped := map[string]bool{}
	different := []string{}
	rowsCompared := 0
	for _, destination := range githubWorkItemRouteDestinations() {
		one := normalizeWindowEquivalenceRows(t, destination, oneTables[destination], stamped)
		manyRows := normalizeWindowEquivalenceRows(t, destination, manyTables[destination], stamped)
		onlyOne, onlyMany := windowEquivalenceDifference(one, manyRows)
		rowsCompared += len(one)
		t.Logf("TABLE %-34s one_unit=%-5d many_units=%-5d only_in_one=%-5d only_in_many=%d",
			destination, len(one), len(manyRows), len(onlyOne), len(onlyMany))
		if len(onlyOne)+len(onlyMany) == 0 {
			continue
		}
		different = append(different, destination)
		logWindowEquivalenceDifference(t, destination, onlyOne, onlyMany, "ONE unit", "MANY units")
	}
	if rowsCompared == 0 {
		t.Fatal("no destination row was compared; the equality proves nothing")
	}
	if len(different) > 0 {
		t.Fatalf("%d of %d destination tables differ between one unit and %d windows: %v",
			len(different), len(githubWorkItemRouteDestinations()), len(many), different)
	}
}
