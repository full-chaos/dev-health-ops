package cognitiveload

import (
	"reflect"
	"testing"
)

// Missing is not healthy: a day with team commit-timing rows but no per-developer row has no user load to serve. Today
// the merge serves prInterruptionLoad, contextSpreadCount and reviewRequestLoad as 0 for that day, the same answer as a
// day whose developers carried no load. The WANT is type-neutral: each user field of the team-only day is either a nil
// pointer (a nullable field) or absent, never a number. The control (a day with a user row) keeps today's values.
func TestCognitiveLoadTeamOnlyDayServesNoMeasuredUserLoad(t *testing.T) {
	signals := mergeUserAndTeamRows(
		[]userLoadRow{{day: day("2026-08-20"), prInterruptionLoad: 4, contextSpreadCount: 2, reviewRequestLoad: 1}},
		[]teamRatioRow{{day: day("2026-08-21"), afterHoursCommitRatio: 0.5, weekendCommitRatio: 0}},
	)
	if len(signals) != 2 {
		t.Fatalf("signals = %+v, want 2 days (the union)", signals)
	}
	userFields := []string{"PrInterruptionLoad", "ContextSpreadCount", "ReviewRequestLoad"}
	number := func(field reflect.Value) (float64, bool) {
		switch field.Kind() {
		case reflect.Float64:
			return field.Float(), true
		case reflect.Pointer:
			if field.IsNil() {
				return 0, false
			}
			return field.Elem().Float(), true
		}
		return 0, false
	}

	control := reflect.ValueOf(signals[0])
	for index, want := range []float64{4, 2, 1} {
		got, ok := number(control.FieldByName(userFields[index]))
		if !ok || got != want {
			t.Errorf("control, day with a user row: %s = %v (set %v), want %v", userFields[index], got, ok, want)
		}
	}

	teamOnly := reflect.ValueOf(signals[1])
	for _, name := range userFields {
		field := teamOnly.FieldByName(name)
		if !field.IsValid() {
			continue
		}
		if got, ok := number(field); ok {
			t.Errorf("team-only day %s: %s = %v, want no value (no per-developer row that day: unknown, not 0)", signals[1].Day.String(), name, got)
		}
	}
}
