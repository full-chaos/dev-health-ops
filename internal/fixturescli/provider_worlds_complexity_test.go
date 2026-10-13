package fixturescli

import (
	"crypto/sha256"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
)

// demoComplexityDay is one repo_complexity_daily row of a demo repository.
type demoComplexityDay struct {
	Day                     string
	LOCTotal                uint64
	CyclomaticTotal         uint64
	CyclomaticPerKLOC       float64
	HighComplexityFunctions uint64
	VeryHighComplexityFunc  uint64
}

// demoRepoComplexity is the daily complexity history of one demo repository over the `days` days before
// `end`, oldest first, ending the day before end's day: the levels it drifted from to reach the
// complexity the complexity job measured on end's day. The history is seeded from the repository id (the
// world holds one tree, the one at its instant, so earlier days have no files to scan): two repositories
// in three got more complex (they started at 1/1.4 of today's density), the third got simpler (from
// 1/0.7), and the code grew by a tenth, so the first and second half of any window of 30 days or more
// differ for every repository.
func demoRepoComplexity(repoID string, end demoComplexityDay, days int) []demoComplexityDay {
	hash := sha256.Sum256([]byte("dev-health-demo-complexity:" + repoID))
	endFactor := 1.4
	if hash[3]%3 == 0 {
		endFactor = 0.7
	}
	endDay, err := time.Parse(time.DateOnly, end.Day)
	if err != nil {
		panic(err)
	}
	startLOC := float64(end.LOCTotal) / 1.1
	startPerKLOC := end.CyclomaticPerKLOC / endFactor
	highShare := 0.0
	if end.CyclomaticTotal > 0 {
		highShare = float64(end.HighComplexityFunctions) / float64(end.CyclomaticTotal)
	}
	rows := make([]demoComplexityDay, 0, days)
	for index := 0; index < days; index++ {
		progress := float64(index) / float64(days)
		loc := max(uint64(math.Round(startLOC+(float64(end.LOCTotal)-startLOC)*progress)), 1)
		perKLOC := startPerKLOC + (end.CyclomaticPerKLOC-startPerKLOC)*progress
		cyclomatic := max(uint64(math.Round(float64(loc)*perKLOC/1000)), 1)
		high := uint64(math.Round(float64(cyclomatic) * highShare))
		rows = append(rows, demoComplexityDay{
			Day:                     endDay.AddDate(0, 0, index-days).Format(time.DateOnly),
			LOCTotal:                loc,
			CyclomaticTotal:         cyclomatic,
			CyclomaticPerKLOC:       math.Round(float64(cyclomatic)/(float64(loc)/1000)*100) / 100,
			HighComplexityFunctions: high,
			VeryHighComplexityFunc:  high / 5,
		})
	}
	return rows
}

func TestDemoRepoComplexityDriftsInEveryWindowHalf(t *testing.T) {
	end := demoComplexityDay{Day: "2026-10-12", LOCTotal: 640, CyclomaticTotal: 96, CyclomaticPerKLOC: 150, HighComplexityFunctions: 4}
	for seed := 0; seed < 64; seed++ {
		repoID := uuid.NewSHA1(uuid.NameSpaceURL, []byte{byte(seed)}).String()
		rows := demoRepoComplexity(repoID, end, 180)
		if len(rows) != 180 {
			t.Fatalf("%s: %d days, want 180", repoID, len(rows))
		}
		if rows[179].Day != "2026-10-11" || rows[0].Day != "2026-04-15" {
			t.Fatalf("%s: window %s..%s", repoID, rows[0].Day, rows[179].Day)
		}
		// The last 30 days are the measured day and the 29 seeded days before it.
		var first, second float64
		for _, row := range rows[151:166] {
			first += row.CyclomaticPerKLOC / 15
		}
		for _, row := range rows[166:] {
			second += row.CyclomaticPerKLOC / 14
		}
		if first == second {
			t.Errorf("%s: the two halves of the last 30 days have the same level %v", repoID, first)
		}
		for _, row := range rows {
			if row.LOCTotal == 0 || row.CyclomaticTotal == 0 {
				t.Fatalf("%s: an empty day %+v", repoID, row)
			}
		}
	}
}
