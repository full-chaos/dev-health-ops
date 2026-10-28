package reconcilerservice

import (
	"context"
	"testing"
	"time"
)

func TestSyncBufferWaitForSeesALineWrittenBeforeTheWait(t *testing.T) {
	var buffer syncBuffer
	_, _ = buffer.Write([]byte("a readiness check refused\n"))
	if !buffer.waitFor(t.Context(), "readiness check refused") {
		t.Fatal("waitFor missed a line that was already written")
	}
}

func TestSyncBufferWaitForSeesALineWrittenAfterItParked(t *testing.T) {
	buffer := syncBuffer{}
	armed := armedSignal(&buffer)
	guard, stop := context.WithTimeout(t.Context(), 20*time.Second)
	defer stop()
	result := make(chan bool, 1)
	go func() { result <- buffer.waitFor(guard, "readiness check refused") }()
	<-armed
	_, _ = buffer.Write([]byte("a readiness check refused\n"))
	if !<-result {
		t.Fatal("waitFor missed a line written after it parked")
	}
	if guard.Err() != nil {
		t.Fatal("waitFor returned only when its hang guard fired: the write did not wake it")
	}
}

func TestSyncBufferWaitForIgnoresOtherLines(t *testing.T) {
	buffer := syncBuffer{}
	armed := armedSignal(&buffer)
	guard, stop := context.WithCancel(t.Context())
	defer stop()
	result := make(chan bool, 1)
	go func() { result <- buffer.waitFor(guard, "readiness check refused") }()
	<-armed
	_, _ = buffer.Write([]byte("some other line\n"))
	stop()
	if <-result {
		t.Fatal("waitFor returned true for a line that never arrived")
	}
}

func TestSyncBufferWaitForReReadsAfterTheGuardEnds(t *testing.T) {
	var buffer syncBuffer
	guard, stop := context.WithCancel(t.Context())
	stop()
	_, _ = buffer.Write([]byte("a readiness check refused\n"))
	if !buffer.waitFor(guard, "readiness check refused") {
		t.Fatal("waitFor lost a line that was there when its guard ended")
	}
}

// The write and the end of the guard both happen between the waiter's check and its select, so both
// are ready when it selects and select picks one at random: a waiter that does not read the buffer
// again after the guard ends loses the line on some rounds.
func TestSyncBufferWaitForKeepsALineThatRacesTheGuardEnd(t *testing.T) {
	for round := 0; round < 64; round++ {
		var buffer syncBuffer
		guard, stop := context.WithCancel(t.Context())
		buffer.onArmed = func() {
			_, _ = buffer.Write([]byte("a readiness check refused\n"))
			stop()
			buffer.onArmed = nil
		}
		if !buffer.waitFor(guard, "readiness check refused") {
			t.Fatalf("round %d: waitFor lost a line that raced the end of its guard", round)
		}
	}
}

// armedSignal makes the buffer tell the test, once, that a waiter is parked.
func armedSignal(b *syncBuffer) <-chan struct{} {
	armed := make(chan struct{}, 1)
	b.onArmed = func() {
		select {
		case armed <- struct{}{}:
		default:
		}
	}
	return armed
}
