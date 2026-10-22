package complexitytimeseries

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
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

func untilBound(t *testing.T, until string, now string) (string, int) {
	t.Helper()
	pinWindowToday(t, now)
	client := &fakeClient{responses: []*fakeRowScanner{{}, {}, {}}}
	if _, err := Resolve(context.Background(), client, "org-1",
		mustDay(t, "2026-10-15T00:00:00Z"), mustDay(t, until),
		model.TimeGranularityDay, model.ComplexityScopeRepo, nil, nil); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(client.bindings) == 0 {
		t.Fatal("no query issued")
	}
	for _, b := range client.bindings[0] {
		if b.Name == "until_day" {
			return b.Value.(string), len(client.bindings)
		}
	}
	t.Fatal("no until_day binding")
	return "", 0
}

func TestResolve_EndOfTodayOrLaterIsYesterday(t *testing.T) {
	for _, now := range []string{"2026-10-22T00:30:00Z", "2026-10-22T23:30:00Z"} {
		for _, until := range []string{"2026-10-22T23:59:59Z", "2026-10-23T23:59:59Z", "2027-01-01T00:00:00Z"} {
			if got, _ := untilBound(t, until, now); got != "2026-10-21" {
				t.Errorf("clock %s until %s: until_day = %s, want 2026-10-21", now, until, got)
			}
		}
		if got, _ := untilBound(t, "2026-10-21T23:59:59Z", now); got != "2026-10-21" {
			t.Errorf("clock %s: yesterday moved to %s", now, got)
		}
		if got, _ := untilBound(t, "2026-10-18T23:59:59Z", now); got != "2026-10-18" {
			t.Errorf("clock %s: an earlier end moved to %s", now, got)
		}
	}
}

func TestResolve_ClockAfterMidnightMovesTheEndByOneDay(t *testing.T) {
	if got, _ := untilBound(t, "2026-10-22T23:59:59Z", "2026-10-22T23:59:59Z"); got != "2026-10-21" {
		t.Fatalf("23:59:59 = %s", got)
	}
	if got, _ := untilBound(t, "2026-10-22T23:59:59Z", "2026-10-23T00:00:01Z"); got != "2026-10-22" {
		t.Fatalf("next day = %s", got)
	}
}
