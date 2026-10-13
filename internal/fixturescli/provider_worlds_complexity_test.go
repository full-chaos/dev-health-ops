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

// demoRepoComplexity is the daily complexity of one demo repository over its last `days` days, oldest
// first, ending on the world's instant. The world holds no file text the complexity job could scan, so
// the history is seeded from the repository id: a start level, then a steady drift (two repositories in
// three get more complex, the third gets simpler), so the first and second half of any window of
// 30 days or more differ for every repository.
func demoRepoComplexity(repoID string, frozenAt time.Time, days int) []demoComplexityDay {
	hash := sha256.Sum256([]byte("dev-health-demo-complexity:" + repoID))
	startLOC := 20000 + (uint64(hash[0])<<8|uint64(hash[1]))%100000
	startPerKLOC := 6 + float64(hash[2]%100)/10
	endFactor := 1.4
	if hash[3]%3 == 0 {
		endFactor = 0.7
	}
	last := frozenAt.UTC().Truncate(24 * time.Hour)
	rows := make([]demoComplexityDay, 0, days)
	for index := 0; index < days; index++ {
		progress := float64(index) / float64(days-1)
		loc := startLOC + uint64(index)*(startLOC/2000)
		cyclomatic := uint64(math.Round(float64(loc) * startPerKLOC * (1 + (endFactor-1)*progress) / 1000))
		high := cyclomatic / 500
		rows = append(rows, demoComplexityDay{
			Day:                     last.AddDate(0, 0, index-(days-1)).Format("2006-01-02"),
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
	instant := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	for seed := 0; seed < 64; seed++ {
		repoID := uuid.NewSHA1(uuid.NameSpaceURL, []byte{byte(seed)}).String()
		rows := demoRepoComplexity(repoID, instant, 91)
		if len(rows) != 91 {
			t.Fatalf("%s: %d days, want 91", repoID, len(rows))
		}
		if rows[90].Day != "2026-10-12" || rows[0].Day != "2026-07-14" {
			t.Fatalf("%s: window %s..%s", repoID, rows[0].Day, rows[90].Day)
		}
		var first, second float64
		for _, row := range rows[61:76] {
			first += row.CyclomaticPerKLOC / 15
		}
		for _, row := range rows[76:] {
			second += row.CyclomaticPerKLOC / 15
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
