package health

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type transientFake struct{ transient bool }

func (transientFake) Error() string     { return "dependency detail that must not be exposed" }
func (f transientFake) Transient() bool { return f.transient }

func TestRequiredCheckClassifiesTransientSeparatelyFromTimeout(t *testing.T) {
	t.Parallel()
	registry := NewRegistry(time.Second)
	checks := map[string]error{
		"unreachable": fmt.Errorf("wrapped: %w", transientFake{true}),
		"not_flagged": transientFake{false},
		"timeout":     context.DeadlineExceeded,
		"plain":       errors.New("boom"),
	}
	for name, err := range checks {
		if regErr := registry.RegisterRequired(name, func(context.Context) error { return err }); regErr != nil {
			t.Fatal(regErr)
		}
	}
	want := map[string]struct {
		cause     string
		timedOut  bool
		transient bool
	}{
		"unreachable": {CauseUnreachable, false, true},
		"not_flagged": {"error", false, false},
		"timeout":     {"timeout", true, false},
		"plain":       {"error", false, false},
	}
	for _, status := range registry.CheckRequired(context.Background()).Checks {
		w := want[status.Name]
		if !status.Failed || status.Cause != w.cause || status.TimedOut != w.timedOut || status.Transient != w.transient {
			t.Errorf("%s = %+v, want %+v", status.Name, status, w)
		}
	}
}
