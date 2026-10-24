package venueoracle

import "testing"

func TestTileFrozenBackfillWindowsClosesOnlyAOneMicrosecondGap(t *testing.T) {
	const (
		row1 = "cfg-bcommitsbackfill2026-09-01 00:00:00 +0000 UTC 2026-09-07 23:59:59.999999 +0000 UTCplannedgithub"
		row2 = "cfg-bcommitsbackfill2026-09-08 00:00:00 +0000 UTC 2026-09-14 23:59:59.999999 +0000 UTCplannedgithub"
		row3 = "cfg-bcommitsbackfill2026-09-15 00:00:00 +0000 UTC 2026-09-30 00:00:00 +0000 UTCplannedgithub"
	)
	tiled1 := "cfg-bcommitsbackfill2026-09-01 00:00:00 +0000 UTC 2026-09-08 00:00:00 +0000 UTCplannedgithub"
	tiled2 := "cfg-bcommitsbackfill2026-09-08 00:00:00 +0000 UTC 2026-09-15 00:00:00 +0000 UTCplannedgithub"
	for name, test := range map[string]struct{ in, want string }{
		"a chain of windows is tiled, the last end stays":   {row1 + " | " + row2 + " | " + row3, tiled1 + " | " + tiled2 + " | " + row3},
		"an unrelated row between does not break the chain": {row1 + " | other-dataset-row 2026-09-08 00:00:00 +0000 UTC 2026-09-09 00:00:00 +0000 UTC | " + row2, tiled1 + " | other-dataset-row 2026-09-08 00:00:00 +0000 UTC 2026-09-09 00:00:00 +0000 UTC | " + row2},
		"a different column keeps the difference":           {row1 + " | " + "cfg-bcommitsbackfill2026-09-08 00:00:00 +0000 UTC 2026-09-14 23:59:59.999999 +0000 UTCfailedgithub", row1 + " | " + "cfg-bcommitsbackfill2026-09-08 00:00:00 +0000 UTC 2026-09-14 23:59:59.999999 +0000 UTCfailedgithub"},
		"a gap of two microseconds is not closed":           {"x 2026-09-01 00:00:00 +0000 UTC 2026-09-07 23:59:59.999998 +0000 UTC y | x 2026-09-08 00:00:00 +0000 UTC 2026-09-14 00:00:00 +0000 UTC y", "x 2026-09-01 00:00:00 +0000 UTC 2026-09-07 23:59:59.999998 +0000 UTC y | x 2026-09-08 00:00:00 +0000 UTC 2026-09-14 00:00:00 +0000 UTC y"},
		"rows without a window are untouched":               {"a | b", "a | b"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := TileFrozenBackfillWindows(test.in); got != test.want {
				t.Fatalf("got:\n%s\nwant:\n%s", got, test.want)
			}
		})
	}
}

func TestDropFrozenEmptyBackfillWindowsAndLowerRunTotals(t *testing.T) {
	const prefix = "ead75594-195e-556b-9ba6-0a85baa684904b7fd41f-61db-57bb-a4b6-23cbbd0ff408"
	units := prefix + "srcAgithubcommitsbackfill2026-09-01 00:00:00 +0000 UTC 2026-09-07 23:59:59.999999 +0000 UTCplanned | " +
		prefix + "srcAgithubcommitsbackfill2026-09-08 00:00:00 +0000 UTC 2026-09-14 23:59:59.999999 +0000 UTCplanned | " +
		prefix + "srcAgithubcommitsbackfill2026-09-15 00:00:00 +0000 UTC 2026-09-15 00:00:00 +0000 UTCplanned | " +
		prefix + "srcBgithubprsbackfill2026-09-01 00:00:00 +0000 UTC 2026-09-07 23:59:59.999999 +0000 UTCplanned | " +
		prefix + "srcBgithubprsbackfill2026-09-08 00:00:00 +0000 UTC 2026-09-15 00:00:00 +0000 UTCplanned"
	wantUnits := prefix + "srcAgithubcommitsbackfill2026-09-01 00:00:00 +0000 UTC 2026-09-07 23:59:59.999999 +0000 UTCplanned | " +
		prefix + "srcAgithubcommitsbackfill2026-09-08 00:00:00 +0000 UTC 2026-09-15 00:00:00 +0000 UTCplanned | " +
		prefix + "srcBgithubprsbackfill2026-09-01 00:00:00 +0000 UTC 2026-09-07 23:59:59.999999 +0000 UTCplanned | " +
		prefix + "srcBgithubprsbackfill2026-09-08 00:00:00 +0000 UTC 2026-09-15 00:00:00 +0000 UTCplanned"
	got, dropped := DropFrozenEmptyBackfillWindows(units)
	if got != wantUnits || len(dropped) != 1 || dropped[prefix] != 1 {
		t.Fatalf("rows:\n%s\ndropped %v, want\n%s", got, dropped, wantUnits)
	}
	if tiled := TileFrozenBackfillWindows(got); tiled == got {
		// srcB's first window still ends 1 microsecond before the next: tiling closes it.
		t.Fatalf("tiling after the drop changed nothing")
	}

	runs := prefix + "backfillplanned12 0 0credential | otherorgotherintegrationbackfillplanned12 0 0credential | " + prefix + "incrementalplanned3 0 0credential"
	wantRuns := prefix + "backfillplanned11 0 0credential | otherorgotherintegrationbackfillplanned12 0 0credential | " + prefix + "incrementalplanned3 0 0credential"
	if got := LowerFrozenRunTotals(runs, dropped); got != wantRuns {
		t.Fatalf("runs:\n%s\nwant\n%s", got, wantRuns)
	}

	// A zero-width row with no row before it, or a gap that is not 1 microsecond, is left alone.
	alone := prefix + "srcAgithubcommitsbackfill2026-09-15 00:00:00 +0000 UTC 2026-09-15 00:00:00 +0000 UTCplanned"
	if got, dropped := DropFrozenEmptyBackfillWindows(alone); got != alone || len(dropped) != 0 {
		t.Fatalf("a lone empty window was changed: %q %v", got, dropped)
	}
	gap := prefix + "srcAgithubcommitsbackfill2026-09-01 00:00:00 +0000 UTC 2026-09-14 12:00:00 +0000 UTCplanned | " +
		prefix + "srcAgithubcommitsbackfill2026-09-15 00:00:00 +0000 UTC 2026-09-15 00:00:00 +0000 UTCplanned"
	if got, dropped := DropFrozenEmptyBackfillWindows(gap); got != gap || len(dropped) != 0 {
		t.Fatalf("an empty window after a wide gap was changed: %q %v", got, dropped)
	}
}
