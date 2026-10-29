package sync

import (
	"errors"
	"testing"
	"time"
)

// CHAOS-11774: the plan is validated at its output, before any unit row is written: a unit whose window is empty or
// inverted (since >= before) is refused as an invalid plan. A zero-width unit fetches nothing, is refused by the
// provider route on every attempt, and renders in the coverage projection as a failed range.
func TestValidateUnitWindowsRefusesAnEmptyOrInvertedWindow(t *testing.T) {
	at := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
	later := at.Add(time.Microsecond)
	earlier := at.Add(-time.Microsecond)
	unit := func(since, before *time.Time) PlannedUnit {
		return PlannedUnit{Provider: "github", Dataset: "work-items", WindowStart: since, WindowEnd: before}
	}
	for name, bad := range map[string]PlannedUnit{
		"zero width": unit(&at, &at), "inverted": unit(&later, &at), "inverted by a microsecond": unit(&at, &earlier),
	} {
		err := validateUnitWindows([]PlannedUnit{unit(&at, &later), bad})
		if !errors.Is(err, ErrInvalidPlan) {
			t.Errorf("%s: err = %v, want ErrInvalidPlan", name, err)
		}
	}
	if err := validateUnitWindows([]PlannedUnit{unit(&at, &later), unit(nil, &at), unit(nil, nil)}); err != nil {
		t.Errorf("valid and open-start windows are accepted: %v", err)
	}
}
