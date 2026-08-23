package numerical

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type capacityGolden struct {
	Cases     []capacityCase `json:"cases"`
	DateCases []dateCase     `json:"date_cases"`
}

type capacityCase struct {
	Name        string         `json:"name"`
	Throughputs []int          `json:"throughputs"`
	Seed        int64          `json:"seed"`
	TargetItems *int           `json:"target_items"`
	TargetDate  *string        `json:"target_date"`
	Today       string         `json:"today"`
	Simulations int            `json:"simulations"`
	Expected    capacityExpect `json:"expected"`
}

type capacityExpect struct {
	P50Days             *int    `json:"p50_days"`
	P85Days             *int    `json:"p85_days"`
	P95Days             *int    `json:"p95_days"`
	P50Date             *string `json:"p50_date"`
	P85Date             *string `json:"p85_date"`
	P95Date             *string `json:"p95_date"`
	P50Items            *int    `json:"p50_items"`
	P85Items            *int    `json:"p85_items"`
	P95Items            *int    `json:"p95_items"`
	ThroughputMean      string  `json:"throughput_mean"`
	ThroughputStddev    string  `json:"throughput_stddev"`
	HistoryDays         int     `json:"history_days"`
	SimulationCount     int     `json:"simulation_count"`
	InsufficientHistory bool    `json:"insufficient_history"`
	HighVariance        bool    `json:"high_variance"`
}

type dateCase struct {
	Name     string `json:"name"`
	Today    string `json:"today"`
	Days     int    `json:"days"`
	Expected string `json:"expected"`
}

func loadCapacityGolden(t *testing.T) capacityGolden {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(
		numericalRepoRoot(t), "tests", "fixtures", "capacity_forecast_golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var golden capacityGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(golden.Cases) == 0 || len(golden.DateCases) == 0 {
		t.Fatal("golden is empty -- an empty fixture agrees with every implementation")
	}
	return golden
}

func parseDay(t *testing.T, text string) time.Time {
	t.Helper()
	day, err := time.Parse("2006-01-02", text)
	if err != nil {
		t.Fatalf("parse date %q: %v", text, err)
	}
	return day.UTC()
}

// TestCapacityForecastMatchesPython compares the whole kernel -- Monte Carlo,
// percentiles and derived statistics -- against vectors captured from the real
// producer.
//
// This is where the CPython RNG port earns its place: every p50/p85/p95 below
// is a quantile over ten thousand (or two hundred) simulated draws, so a single
// wrong draw anywhere in the stream moves the answer. Agreement across 95 cases
// and five seeds is not plausible under any generator but CPython's.
func TestCapacityForecastMatchesPython(t *testing.T) {
	golden := loadCapacityGolden(t)

	var bothModeCases, rejectingHistoryCases int

	for _, test := range golden.Cases {
		test := test
		if strings.HasPrefix(test.Name, "both/") {
			bothModeCases++
		}
		if length := len(test.Throughputs); length > 1 && length&(length-1) != 0 {
			rejectingHistoryCases++
		}

		t.Run(test.Name, func(t *testing.T) {
			request := ForecastRequest{
				History: Throughput{
					DailyThroughputs: test.Throughputs,
					DaysOfHistory:    len(test.Throughputs),
				},
				TargetItems: test.TargetItems,
				Simulations: test.Simulations,
				Seed:        test.Seed,
			}
			if test.TargetDate != nil {
				target := parseDay(t, *test.TargetDate)
				request.TargetDate = &target
			}

			got, err := ForecastCapacity(request, parseDay(t, test.Today))
			if err != nil {
				t.Fatalf("forecast: %v", err)
			}

			assertIntPointer(t, "p50_days", got.P50Days, test.Expected.P50Days)
			assertIntPointer(t, "p85_days", got.P85Days, test.Expected.P85Days)
			assertIntPointer(t, "p95_days", got.P95Days, test.Expected.P95Days)
			assertIntPointer(t, "p50_items", got.P50Items, test.Expected.P50Items)
			// p85 and p95 items come from the FLIPPED percentiles [15] and [5].
			// A port that asked for [85] and [95] produces larger numbers here,
			// which is why these are asserted rather than only p50.
			assertIntPointer(t, "p85_items", got.P85Items, test.Expected.P85Items)
			assertIntPointer(t, "p95_items", got.P95Items, test.Expected.P95Items)
			assertDatePointer(t, "p50_date", got.P50Date, test.Expected.P50Date)
			assertDatePointer(t, "p85_date", got.P85Date, test.Expected.P85Date)
			assertDatePointer(t, "p95_date", got.P95Date, test.Expected.P95Date)

			assertFloat(t, "throughput_mean", got.ThroughputMean, test.Expected.ThroughputMean)
			assertFloat(t, "throughput_stddev", got.ThroughputStddev, test.Expected.ThroughputStddev)
			if got.HistoryDays != test.Expected.HistoryDays {
				t.Errorf("history_days = %d, want %d", got.HistoryDays, test.Expected.HistoryDays)
			}
			if got.SimulationCount != test.Expected.SimulationCount {
				t.Errorf("simulation_count = %d, want %d", got.SimulationCount, test.Expected.SimulationCount)
			}
			if got.InsufficientHistory != test.Expected.InsufficientHistory {
				t.Errorf("insufficient_history = %v, want %v", got.InsufficientHistory, test.Expected.InsufficientHistory)
			}
			if got.HighVariance != test.Expected.HighVariance {
				t.Errorf("high_variance = %v, want %v", got.HighVariance, test.Expected.HighVariance)
			}
		})
	}

	// Anti-vacuity: both of these cover a divergence that is INVISIBLE without
	// them, so a fixture edit that dropped either would keep passing while the
	// port was wrong.
	if bothModeCases == 0 {
		t.Error(
			"no case runs BOTH forecast modes: a port using `else` instead of a " +
				"second `if`, or sharing one generator across the two " +
				"simulations, would pass")
	}
	if rejectingHistoryCases == 0 {
		t.Error(
			"no non-power-of-two history length: random.choice never rejects on " +
				"powers of two, so a broken rejection loop would pass")
	}
}

// TestDateDerivationMatchesPython closes the hole the parity exclusion opens.
//
// p50_date/p85_date/p95_date are excluded from the whole-table comparison
// because they are derived from the wall clock, which means the comparison
// cannot catch a Go bug in the date arithmetic itself. These vectors do,
// against boundaries the parity fixture would never reach: month ends, year
// ends, a leap day, and a full leap year.
func TestDateDerivationMatchesPython(t *testing.T) {
	golden := loadCapacityGolden(t)

	var sawLeap, sawYearEnd bool
	for _, test := range golden.DateCases {
		test := test
		if strings.Contains(test.Name, "leap") {
			sawLeap = true
		}
		if strings.Contains(test.Name, "year_end") {
			sawYearEnd = true
		}
		t.Run(test.Name, func(t *testing.T) {
			got := AddDays(parseDay(t, test.Today), test.Days)
			if formatted := got.Format("2006-01-02"); formatted != test.Expected {
				t.Errorf("%s + %d days = %s, want %s",
					test.Today, test.Days, formatted, test.Expected)
			}
		})
	}
	if !sawLeap {
		t.Error("no leap-day vector: February's length is the classic date-arithmetic bug")
	}
	if !sawYearEnd {
		t.Error("no year-boundary vector")
	}
}

func assertIntPointer(t *testing.T, field string, got, want *int) {
	t.Helper()
	switch {
	case got == nil && want == nil:
	case got == nil:
		t.Errorf("%s = nil, want %d", field, *want)
	case want == nil:
		t.Errorf("%s = %d, want nil -- Python left this mode's columns unset", field, *got)
	case *got != *want:
		t.Errorf("%s = %d, want %d", field, *got, *want)
	}
}

func assertDatePointer(t *testing.T, field string, got *time.Time, want *string) {
	t.Helper()
	switch {
	case got == nil && want == nil:
	case got == nil:
		t.Errorf("%s = nil, want %s", field, *want)
	case want == nil:
		t.Errorf("%s = %s, want nil", field, got.Format("2006-01-02"))
	case got.Format("2006-01-02") != *want:
		t.Errorf("%s = %s, want %s", field, got.Format("2006-01-02"), *want)
	}
}

// assertFloat compares against Python's repr, which is the shortest string that
// round-trips to the same float64. Go's 'g' with -1 precision has the same
// property, so equal values produce identical text and a divergence of one ULP
// is visible rather than rounded away.
func assertFloat(t *testing.T, field string, got float64, want string) {
	t.Helper()
	formatted := strconv.FormatFloat(got, 'g', -1, 64)
	if formatted == want {
		return
	}
	// Python renders integral floats as "5.0" where Go's 'g' gives "5".
	if !strings.ContainsAny(formatted, ".eE") && want == formatted+".0" {
		return
	}
	t.Errorf("%s = %s, want %s", field, formatted, want)
}

func numericalRepoRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("no go.mod above the test")
		}
		directory = parent
	}
}
