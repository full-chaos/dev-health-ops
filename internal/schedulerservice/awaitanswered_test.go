package schedulerservice

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// answered is the stub of the old poll shape: it looks once and gives up.
func answered(_ context.Context, probes ...*postgres.LazyProbeCheck) bool {
	for _, probe := range probes {
		if !probe.Answered() {
			return false
		}
	}
	return true
}

// gatedProbe warms a probe whose first execution blocks until release is closed.
func gatedProbe() (probe *postgres.LazyProbeCheck, started <-chan struct{}, release func()) {
	startedCh := make(chan struct{})
	gate := make(chan struct{})
	probe = &postgres.LazyProbeCheck{}
	probe.Warm("gated_probe", func(context.Context) error {
		close(startedCh)
		<-gate
		return nil
	}, postgres.PostureCheckOptions{})
	return probe, startedCh, func() { close(gate) }
}

func TestAnsweredWaitsForTheWarmedFlightToFinish(t *testing.T) {
	for round := 0; round < 32; round++ {
		probe, started, release := gatedProbe()
		result := make(chan bool, 1)
		go func() { result <- answered(t.Context(), probe) }()
		<-started
		select {
		case <-result:
			t.Fatalf("round %d: answered returned before the warmed flight finished", round)
		default:
		}
		release()
		if !<-result {
			t.Fatalf("round %d: answered said false although the flight finished", round)
		}
	}
}

func TestAnsweredStopsWhenItsContextEndsBeforeAnyAnswer(t *testing.T) {
	probe, started, release := gatedProbe()
	defer release()
	<-started
	guard, stop := context.WithCancel(t.Context())
	stop()
	if answered(guard, probe) {
		t.Fatal("answered said true for a probe that has not answered")
	}
}

func TestAnsweredIsTrueForAProbeThatAlreadyAnswered(t *testing.T) {
	probe, started, release := gatedProbe()
	<-started
	release()
	if !answered(t.Context(), probe) {
		t.Fatal("answered said false for a flight that finished")
	}
	if !answered(t.Context(), probe) {
		t.Fatal("answered said false for a probe that already answered")
	}
}
