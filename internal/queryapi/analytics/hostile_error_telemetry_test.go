package analytics

import (
	"context"
	"errors"
	"testing"
	"time"
)

// CHAOS-7936: the two failure recorders read the ClickHouse exception through a bounded walk: an error whose Unwrap returns itself
// or panics, or whose As panics, must neither stall nor crash a recorder.

type selfUnwrapping struct{}

func (e *selfUnwrapping) Error() string { return "self" }
func (e *selfUnwrapping) Unwrap() error { return e }

type panickingUnwrap struct{}

func (panickingUnwrap) Error() string { return "panicking unwrap" }
func (panickingUnwrap) Unwrap() error { panic("unwrap") }

type panickingAs struct{}

func (panickingAs) Error() string { return "panicking as" }
func (panickingAs) As(any) bool   { panic("as") }

func TestTheFailureRecordersSurviveHostileErrors(t *testing.T) {
	var typedNil *selfUnwrapping
	for name, err := range map[string]error{
		"self unwrap":      &selfUnwrapping{},
		"panicking unwrap": panickingUnwrap{},
		"panicking as":     panickingAs{},
		"typed nil":        typedNil,
		"wrapped hostile":  errors.Join(errors.New("x"), &selfUnwrapping{}),
	} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			defaultRecordDegradation(context.Background(), "flowMatrix", err)
			defaultRecordInvestmentCoverageFailure(context.Background(), "org-1", MeasureCount, true, coverageStageQuery, "", err)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: a failure recorder stalled on a hostile error", name)
		}
	}
}
