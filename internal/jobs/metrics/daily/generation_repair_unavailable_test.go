package daily

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
)

type captureLevels struct {
	mu     sync.Mutex
	levels []slog.Level
	attrs  map[string]string
}

func (c *captureLevels) Enabled(context.Context, slog.Level) bool { return true }
func (c *captureLevels) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.levels = append(c.levels, r.Level)
	c.attrs = map[string]string{}
	r.Attrs(func(a slog.Attr) bool { c.attrs[a.Key] = a.Value.String(); return true })
	return nil
}
func (c *captureLevels) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *captureLevels) WithGroup(string) slog.Handler      { return c }

// CHAOS-10139 review r1: only a store error CAUSED by the ended budget is a
// partial; a store fault that races the budget stays a failure, logged at Warn.
func TestRepairUnavailableSeparatesBudgetFromStoreFault(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for name, tc := range map[string]struct {
		ctx        context.Context
		cause      error
		wantBudget bool
	}{
		"cancelled budget, context cause":    {cancelled, context.Canceled, true},
		"cancelled budget, store fault":      {cancelled, errors.New("connection reset"), false},
		"live budget, store fault":           {context.Background(), errors.New("connection reset"), false},
		"live budget, deadline-shaped fault": {context.Background(), context.DeadlineExceeded, false},
	} {
		capture := &captureLevels{}
		previous := slog.Default()
		slog.SetDefault(slog.New(capture))
		err := repairUnavailable(tc.ctx, "step11", tc.cause)
		slog.SetDefault(previous)
		if got := errors.Is(err, errRepairBudget); got != tc.wantBudget {
			t.Errorf("%s: budget=%v err=%v, want budget=%v", name, got, err, tc.wantBudget)
		}
		if !tc.wantBudget {
			if !errors.Is(err, ErrUnavailable) {
				t.Errorf("%s: err=%v, want ErrUnavailable", name, err)
			}
			if len(capture.levels) != 1 || capture.levels[0] != slog.LevelWarn {
				t.Errorf("%s: failure log levels = %v, want one Warn", name, capture.levels)
			}
			if capture.attrs["metric"] != GenerationRepairFailuresCounter || capture.attrs["step"] != "step11" {
				t.Errorf("%s: failure log attrs = %v, want metric %s and step step11 (dho has no exporter: the log line is its telemetry)", name, capture.attrs, GenerationRepairFailuresCounter)
			}
		}
	}
}
