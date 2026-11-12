package aiimpact

import (
	"testing"
	"time"
)

// An incident counts on the UTC day its started_at falls in, the same day the
// raw incidents-started-per-day count uses, whatever zone the time carries.
func TestIncidentCountsOnTheUTCDayItStartedNearTheBoundary(t *testing.T) {
	dayStart := fixtureDay
	dayEnd := dayStart.AddDate(0, 0, 1)
	plusTwo := time.FixedZone("UTC+2", 2*60*60)
	minusTwo := time.FixedZone("UTC-2", -2*60*60)
	cases := []struct {
		name      string
		startedAt time.Time
		counts    bool
	}{
		{"last instant of the day before", dayStart.Add(-time.Nanosecond), false},
		{"first instant of the day", dayStart, true},
		{"last millisecond of the day", dayEnd.Add(-time.Millisecond), true},
		{"first instant of the next day", dayEnd, false},
		{"local date is the day, UTC is the day before", time.Date(2026, 9, 3, 1, 30, 0, 0, plusTwo), false},
		{"local date is the day before, UTC is the day", time.Date(2026, 9, 2, 22, 30, 0, 0, minusTwo), true},
		{"local date is the next day, UTC is the day", time.Date(2026, 9, 4, 1, 30, 0, 0, plusTwo), true},
	}
	key := incidentRowKey{workType: IncidentWorkType, bucket: BucketHuman}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.counts != (!tc.startedAt.UTC().Before(dayStart) && tc.startedAt.UTC().Before(dayEnd)) {
				t.Fatalf("case is wrong: %s is not where it claims", tc.startedAt.UTC())
			}
			want := map[incidentRowKey]float64{}
			if tc.counts {
				want[key] = 1
			}
			params := incidentFixtureParams(IncidentRow{ID: "i1", RepoID: fixtureRepo, StartedAt: tc.startedAt})
			wantIncidents(t, tc.name, Compute(params), want)

			params.Day = dayStart.In(minusTwo)
			wantIncidents(t, tc.name+" (day given in another zone)", Compute(params), want)
		})
	}
}
