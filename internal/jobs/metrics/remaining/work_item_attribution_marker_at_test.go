package remaining

import (
	"testing"
	"time"
)

func TestWorkItemAttributionMarkerAtHoldsBeforeTheEarliestDeferredRow(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	early := now.Add(-2 * time.Hour)
	late := now.Add(-time.Hour)
	cases := []struct {
		name    string
		covered map[string]time.Time
		want    time.Time
	}{
		{"no deferral keeps now", nil, now},
		{"one deferred row", map[string]time.Time{"a": late}, late.Add(-3 * time.Millisecond)},
		{"the earliest of two rows", map[string]time.Time{"a": late, "b": early}, early.Add(-3 * time.Millisecond)},
		{"never after now", map[string]time.Time{"a": now.Add(time.Hour)}, now},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := workItemAttributionMarkerAt(now, test.covered); !got.Equal(test.want) {
				t.Fatalf("workItemAttributionMarkerAt = %s, want %s", got, test.want)
			}
		})
	}
}
