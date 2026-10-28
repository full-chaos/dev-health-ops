package shell

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
	buffer := syncBuffer{armed: make(chan struct{}, 1)}
	guard, stop := context.WithTimeout(t.Context(), 20*time.Second)
	defer stop()
	result := make(chan bool, 1)
	go func() { result <- buffer.waitFor(guard, "readiness check refused") }()
	<-buffer.armed
	_, _ = buffer.Write([]byte("a readiness check refused\n"))
	if !<-result {
		t.Fatal("waitFor missed a line written after it parked")
	}
	if guard.Err() != nil {
		t.Fatal("waitFor returned only when its hang guard fired: the write did not wake it")
	}
}

func TestSyncBufferWaitForIgnoresOtherLines(t *testing.T) {
	buffer := syncBuffer{armed: make(chan struct{}, 1)}
	guard, stop := context.WithCancel(t.Context())
	defer stop()
	result := make(chan bool, 1)
	go func() { result <- buffer.waitFor(guard, "readiness check refused") }()
	<-buffer.armed
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
