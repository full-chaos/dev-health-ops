package hotspots

import (
	"context"
	"testing"
	"time"
)

func pinWindowToday(t *testing.T, ts string) {
	t.Helper()
	now, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		t.Fatal(err)
	}
	prev := windowToday
	windowToday = func() time.Time { return now }
	t.Cleanup(func() { windowToday = prev })
}

func untilBound(t *testing.T, until, now string) string {
	t.Helper()
	pinWindowToday(t, now)
	client := &fakeClient{responses: []*fakeRowScanner{{}}}
	if _, err := Resolve(context.Background(), client, "org-1",
		mustTime(t, "2026-10-15T00:00:00Z"), mustTime(t, until), nil, nil); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	for _, b := range client.bindings[0] {
		if b.Name == "until_day" {
			return b.Value.(string)
		}
	}
	t.Fatal("no until_day binding")
	return ""
}

func TestResolve_EndOfTodayOrLaterIsYesterday(t *testing.T) {
	for _, now := range []string{"2026-10-22T00:30:00Z", "2026-10-22T23:30:00Z"} {
		for _, until := range []string{"2026-10-22T23:59:59Z", "2026-10-23T23:59:59Z", "2027-01-01T00:00:00Z"} {
			if got := untilBound(t, until, now); got != "2026-10-21" {
				t.Errorf("clock %s until %s: until_day = %s, want 2026-10-21", now, until, got)
			}
		}
		if got := untilBound(t, "2026-10-21T23:59:59Z", now); got != "2026-10-21" {
			t.Errorf("clock %s: yesterday moved to %s", now, got)
		}
		if got := untilBound(t, "2026-10-18T23:59:59Z", now); got != "2026-10-18" {
			t.Errorf("clock %s: an earlier end moved to %s", now, got)
		}
	}
}

func TestResolve_ClockAfterMidnightMovesTheEndByOneDay(t *testing.T) {
	if got := untilBound(t, "2026-10-22T23:59:59Z", "2026-10-22T23:59:59Z"); got != "2026-10-21" {
		t.Fatalf("23:59:59 = %s", got)
	}
	if got := untilBound(t, "2026-10-22T23:59:59Z", "2026-10-23T00:00:01Z"); got != "2026-10-22" {
		t.Fatalf("next day = %s", got)
	}
}
