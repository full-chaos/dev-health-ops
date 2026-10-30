package revisionfeed

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ACP ruling on PR B r1 (P1-1): after a (re)subscribe the replica reads the high-water hash and reaches every mark with no
// further event. Order: subscribe first, then read, then the marks move; a live event is revision-idempotent.

func latest(rig *subscriberRig, typ, id string) int64 {
	n, _ := rig.sub.consumer.Watermarks().Latest(typ, id)
	return n
}

func TestCatchUpReadsTheHashOnlyAfterTheSubscriptionIsConfirmed(t *testing.T) {
	rig := startSubscriber(t)
	s := rig.client.next(t)
	time.Sleep(50 * time.Millisecond)
	if n := rig.client.hgetalls(); n != 0 {
		t.Fatalf("%d HGETALL before the subscription was confirmed: a read before the subscribe leaves a gap", n)
	}
	s.hooks.OnSubscribed()
	waitFor(t, "the catch-up read", func() bool { return rig.client.hgetalls() >= 1 })
}

func TestAReconnectedReplicaReachesTheMarkWithNoFurtherEvent(t *testing.T) {
	rig := startSubscriber(t)
	first := rig.client.next(t)
	first.hooks.OnSubscribed()
	first.hooks.OnMessage("user:u1:1")
	waitFor(t, "mark 1", func() bool { return latest(rig, "user", "u1") == 1 })
	first.done <- errors.New("connection reset")
	rig.client.setHGetAll(map[string]string{"user:u1": "3", "organization:o:9": "4"}, nil)
	second := rig.client.next(t)
	second.hooks.OnSubscribed()
	waitFor(t, "the marks", func() bool { return latest(rig, "user", "u1") == 3 && latest(rig, "organization", "o:9") == 4 })
	waitFor(t, "the invalidation", func() bool {
		for _, c := range rig.rec.snapshot() {
			if !c.all && c.typ == "user" && c.id == "u1" && c.revision == 3 {
				return true
			}
		}
		return false
	})
}

func TestCatchUpIgnoresAFloorAtOrBelowTheMark(t *testing.T) {
	rig := startSubscriber(t)
	rig.client.setHGetAll(map[string]string{"user:u1": "2"}, nil)
	s := rig.client.next(t)
	s.hooks.OnSubscribed()
	s.hooks.OnMessage("user:u1:1")
	s.hooks.OnMessage("user:u1:2")
	waitFor(t, "mark 2", func() bool { return latest(rig, "user", "u1") == 2 })
	waitFor(t, "gauge 1", func() bool { return gaugeValue(t, rig.reader) == 1 })
	n := 0
	for _, c := range rig.rec.snapshot() {
		if !c.all && c.revision == 2 {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("revision 2 invalidated %d times, want 1 (a live event and the floor are idempotent)", n)
	}
}

func TestTheGaugeStaysDownAndTheReadRetriesWhileTheCatchUpFails(t *testing.T) {
	rig := startSubscriber(t)
	rig.client.setHGetAll(nil, errors.New("dial redis://user:SENTINEL_PASSWORD@host:6379/1"))
	s := rig.client.next(t)
	s.hooks.OnSubscribed()
	waitFor(t, "two read attempts", func() bool { return rig.client.hgetalls() >= 2 })
	if got := gaugeValue(t, rig.reader); got != 0 {
		t.Fatalf("gauge %d while the catch-up fails, want 0", got)
	}
	rig.client.setHGetAll(map[string]string{"user:u1": "5"}, nil)
	waitFor(t, "gauge 1", func() bool { return gaugeValue(t, rig.reader) == 1 })
	waitFor(t, "the mark", func() bool { return latest(rig, "user", "u1") == 5 })
	if strings.Contains(rig.logs.String(), "SENTINEL_PASSWORD") || strings.Contains(rig.logs.String(), "redis://") {
		t.Fatalf("a log line holds the connection error:\n%s", rig.logs.String())
	}
}

func TestACorruptFloorKeepsTheGaugeDownAndTheMarksUntouched(t *testing.T) {
	rig := startSubscriber(t)
	rig.client.setHGetAll(map[string]string{"user:u1": "notanumber"}, nil)
	s := rig.client.next(t)
	s.hooks.OnSubscribed()
	waitFor(t, "two read attempts", func() bool { return rig.client.hgetalls() >= 2 })
	if gaugeValue(t, rig.reader) != 0 || latest(rig, "user", "u1") != 0 {
		t.Fatal("a corrupt hash moved the gauge or a mark")
	}
}

var _ = context.Background
