package pytime

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNaiveDateTimeWire(t *testing.T) {
	pacific := time.FixedZone("PDT", -7*3600)
	for _, tc := range []struct {
		name string
		at   NaiveDateTime
		want string
	}{
		{"day keeps its own wall-clock date", NaiveDay(time.Date(2026, 9, 23, 23, 0, 0, 0, pacific)), `"2026-09-23T00:00:00"`},
		{"day of a UTC midnight", NaiveDay(time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC)), `"2026-09-23T00:00:00"`},
		{"utc second", NaiveUTC(time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)), `"2026-09-24T12:00:00"`},
		{"utc from another zone", NaiveUTC(time.Date(2026, 9, 24, 5, 0, 0, 0, pacific)), `"2026-09-24T12:00:00"`},
		{"trailing-zero micros kept", NaiveUTC(time.Date(2026, 9, 24, 12, 0, 0, 895620000, time.UTC)), `"2026-09-24T12:00:00.895620"`},
	} {
		got, err := json.Marshal(tc.at)
		if err != nil || string(got) != tc.want {
			t.Errorf("%s: %s, %v; want %s", tc.name, got, err, tc.want)
		}
	}
	var back NaiveDateTime
	if err := json.Unmarshal([]byte(`"2026-09-24T12:00:00.895620"`), &back); err != nil || time.Time(back).Nanosecond() != 895620000 {
		t.Errorf("round trip: %v %v", back, err)
	}
}
