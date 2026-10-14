package fixturescli

import (
	"math"
	"slices"
	"testing"
	"time"
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

// demoComplexityTrend shapes the history of one repository: it started at 1/From of its measured
// density, and Curve bends the climb (1 is straight; above 1 the climb is steeper toward the end, so the
// last 30 days differ more). From below 1 is a repository that got simpler.
type demoComplexityTrend struct{ From, Curve float64 }

// demoComplexityTrends are the trends of the demo repositories by name (the last path element). They
// are set per repository so each team's repositories net to a different change of the last 30 days,
// from a team that got simpler to a team whose complexity climbs fast.
var demoComplexityTrends = map[string]demoComplexityTrend{
	"payments-gateway": {3.0, 3.2}, "payments-ledger": {2.2, 2.6}, "shared-api": {1.8, 2.0},
	"checkout-api": {1.7, 1.8}, "checkout-web": {1.2, 1.2},
	"support-console": {0.8, 1.0}, "support-macros": {1.5, 1.4},
	"identity-service": {4.0, 3.8}, "identity-sdk": {2.5, 3.2},
	"mobile-app": {1.9, 2.0}, "mobile-bff": {1.3, 1.2},
	"deploy-pipeline": {5.0, 4.5}, "release-tools": {2.0, 2.2}, "shared-infra": {2.2, 2.6},
	"warehouse-etl": {0.75, 1.0}, "metrics-store": {0.6, 1.0},
	"marketing-site": {2.0, 2.2}, "design-system": {1.3, 1.4},
}

// demoRepoComplexity is the daily complexity history of one demo repository over the `days` days before
// `end`, oldest first, ending the day before end's day: the levels it drifted from to reach the
// complexity the complexity job measured on end's day. The history is seeded from the repository name
// (the world holds one tree, the one at its instant, so earlier days have no files to scan) by its
// demoComplexityTrends entry, and the code grew by a tenth, so the first and second half of any window
// of 30 days or more differ for every repository.
func demoRepoComplexity(repoName string, end demoComplexityDay, days int) []demoComplexityDay {
	trend, ok := demoComplexityTrends[repoName]
	if !ok {
		panic("no complexity trend for demo repository " + repoName)
	}
	endDay, err := time.Parse(time.DateOnly, end.Day)
	if err != nil {
		panic(err)
	}
	startLOC := float64(end.LOCTotal) / 1.1
	startPerKLOC := end.CyclomaticPerKLOC / trend.From
	highShare := 0.0
	if end.CyclomaticTotal > 0 {
		highShare = float64(end.HighComplexityFunctions) / float64(end.CyclomaticTotal)
	}
	rows := make([]demoComplexityDay, 0, days)
	for index := 0; index < days; index++ {
		progress := float64(index) / float64(days)
		loc := max(uint64(math.Round(startLOC+(float64(end.LOCTotal)-startLOC)*progress)), 1)
		perKLOC := startPerKLOC + (end.CyclomaticPerKLOC-startPerKLOC)*math.Pow(progress, trend.Curve)
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

// demoComplexityNorm is the complexity part of a risk score the daily job computes for the history:
// the change of the mean cyclomatic_per_kloc between the halves of the last 30 days, over the first
// half (at least 1), over the 0.2 reference, from 0 to 1. The measured day is the last day of the window.
func demoComplexityNorm(rows []demoComplexityDay, end demoComplexityDay) float64 {
	all := append(append([]demoComplexityDay{}, rows...), end)
	window := all[len(all)-30:]
	var first, second float64
	for _, row := range window[:15] {
		first += row.CyclomaticPerKLOC / 15
	}
	for _, row := range window[15:] {
		second += row.CyclomaticPerKLOC / 15
	}
	return min(max((second-first)/max(first, 1), 0)/0.2, 1)
}

func TestDemoRepoComplexityDriftsInEveryWindowHalf(t *testing.T) {
	end := demoComplexityDay{Day: "2026-10-12", LOCTotal: 640, CyclomaticTotal: 96, CyclomaticPerKLOC: 150, HighComplexityFunctions: 4}
	for name := range demoComplexityTrends {
		rows := demoRepoComplexity(name, end, 180)
		if len(rows) != 180 {
			t.Fatalf("%s: %d days, want 180", name, len(rows))
		}
		if rows[179].Day != "2026-10-11" || rows[0].Day != "2026-04-15" {
			t.Fatalf("%s: window %s..%s", name, rows[0].Day, rows[179].Day)
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
			t.Errorf("%s: the two halves of the last 30 days have the same level %v", name, first)
		}
		for _, row := range rows {
			if row.LOCTotal == 0 || row.CyclomaticTotal == 0 {
				t.Fatalf("%s: an empty day %+v", name, row)
			}
		}
	}
}

// Each team's repositories net to a different change of the last 30 days: the mean complexity part of
// a team's repositories (a team of one main owner; a shared repository is left out) is distinct per team,
// and the world has teams from none to most of the scale.
func TestDemoComplexityTrendsGiveEveryTeamADifferentNet(t *testing.T) {
	end := demoComplexityDay{Day: "2026-10-12", LOCTotal: 640, CyclomaticTotal: 96, CyclomaticPerKLOC: 150, HighComplexityFunctions: 4}
	teams := map[string][]string{
		"payments": {"payments-gateway", "payments-ledger"}, "checkout": {"checkout-api", "checkout-web"},
		"support": {"support-console", "support-macros"}, "identity": {"identity-service", "identity-sdk"},
		"mobile": {"mobile-app", "mobile-bff"}, "delivery": {"deploy-pipeline", "release-tools"},
		"data": {"warehouse-etl", "metrics-store"}, "web": {"marketing-site", "design-system"},
	}
	var nets []float64
	for team, repos := range teams {
		var sum float64
		for _, name := range repos {
			sum += demoComplexityNorm(demoRepoComplexity(name, end, 180), end) / float64(len(repos))
		}
		t.Logf("%-9s net complexity part %.3f", team, sum)
		for _, other := range nets {
			if math.Abs(other-sum) < 0.04 {
				t.Errorf("%s: net %.3f is within 0.04 of another team's %.3f", team, sum, other)
			}
		}
		nets = append(nets, sum)
	}
	lowest, highest := slices.Min(nets), slices.Max(nets)
	if lowest > 0.25 || highest < 0.7 {
		t.Errorf("the nets span %.3f..%.3f, want one at most 0.25 and one at least 0.7", lowest, highest)
	}
}
