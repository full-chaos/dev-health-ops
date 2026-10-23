package session

import (
	"testing"
	"time"
)

func TestSendQueueNeverBlocksAndCountsDrops(t *testing.T) {
	q := newSendQueue(2)
	release := make(chan struct{})
	started := make(chan struct{}, 2)
	for range 2 {
		if !q.submit(func() { started <- struct{}{}; <-release }) {
			t.Fatal("a free slot refused a send")
		}
	}
	<-started
	<-started
	done := make(chan bool)
	go func() { done <- q.submit(func() { t.Error("a dropped send ran") }) }()
	select {
	case accepted := <-done:
		if accepted {
			t.Fatal("a full queue accepted a send")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("submit blocked on a full queue")
	}
	close(release)
	q.wait()
	if queued, dropped := q.outcomes(); queued != 2 || dropped != 1 {
		t.Fatalf("outcomes queued=%d dropped=%d, want 2 and 1", queued, dropped)
	}
	if !q.submit(func() {}) {
		t.Fatal("a freed slot refused a send")
	}
	q.wait()
}
