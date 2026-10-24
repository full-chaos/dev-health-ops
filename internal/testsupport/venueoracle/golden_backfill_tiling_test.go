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
