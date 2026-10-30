package revisionfeed

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// A valid revision is 1..9007199254740991 (2^53-1): the largest integer every consumer (a JSON number, a float64) holds
// exactly. Outside it a value is dropped (live and catch-up) or makes the read an error (PublishedFloors); a cursor never
// moves on it.
var outOfRangeRevisions = []string{"9007199254740992", "9007199254740993", "9223372036854775807", "99999999999999999999", "0", "-1", "-9007199254740992"}

func TestPublishedFloorsRefusesARevisionOutsideOneToTwoToTheFiftyThreeMinusOne(t *testing.T) {
	for _, v := range outOfRangeRevisions {
		got, err := PublishedFloors(context.Background(), &fakeClient{hash: map[string]string{"user:u1": "3", "user:u2": v}})
		if !errors.Is(err, ErrCorruptFloors) || got != nil {
			t.Fatalf("value %s: floors %v, err %v, want nil and ErrCorruptFloors", v, got, err)
		}
	}
	for _, v := range []string{"1", "9007199254740991"} {
		got, err := PublishedFloors(context.Background(), &fakeClient{hash: map[string]string{"user:u1": v}})
		if err != nil || len(got) != 1 {
			t.Fatalf("value %s: floors %v, err %v, want it accepted", v, got, err)
		}
	}
}

func TestALiveMessageOutsideTheBoundIsDroppedCountedAndMovesNoCursor(t *testing.T) {
	for _, v := range outOfRangeRevisions {
		t.Run(v, func(t *testing.T) {
			rig := startSubscriber(t)
			s := rig.client.next(t)
			s.hooks.OnSubscribed()
			waitFor(t, "gauge 1", func() bool { return gaugeValue(t, rig.reader) == 1 })
			s.hooks.OnMessage("user:u1:" + v)
			waitFor(t, "the drop is counted", func() bool {
				return counterValue(t, rig.reader, "devhealth_auth_revision_feed_events_total", "disposition", "dropped") == 1
			})
			if got, ok := rig.sub.consumer.Watermarks().Latest("user", "u1"); ok || got != 0 {
				t.Fatalf("the mark moved to %d on a dropped message", got)
			}
			for _, c := range rig.rec.snapshot() {
				if !c.all {
					t.Fatalf("a dropped message invalidated %+v", c)
				}
			}
			// a later valid event still applies
			s.hooks.OnMessage("user:u1:1")
			waitFor(t, "the valid event", func() bool { return latest(rig, "user", "u1") == 1 })
		})
	}
}

func TestALiveMessageAtTheUpperBoundStillApplies(t *testing.T) {
	rig := startSubscriber(t)
	s := rig.client.next(t)
	s.hooks.OnSubscribed()
	s.hooks.OnMessage("user:u1:9007199254740991")
	waitFor(t, "gap to the bound", func() bool { return latest(rig, "user", "u1") == 9007199254740991 })
}

func TestACatchUpFloorOutsideTheBoundIsDroppedCountedAndMovesNoCursor(t *testing.T) {
	for _, v := range []int64{9007199254740992, 9007199254740993, 9223372036854775807, 0, -1} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			c, rec := newTestConsumer(t)
			ctx := context.Background()
			c.Handle(ctx, ev("user", "u1", 1))
			before := len(rec.snapshot())
			c.CatchUp(ctx, map[string]int64{"user:u1": v})
			if got, _ := c.Watermarks().Latest("user", "u1"); got != 1 {
				t.Fatalf("the mark moved to %d on a dropped floor", got)
			}
			if len(rec.snapshot()) != before {
				t.Fatal("a dropped floor invalidated something")
			}
			c.Handle(ctx, ev("user", "u1", 2))
			if got, _ := c.Watermarks().Latest("user", "u1"); got != 2 {
				t.Fatalf("a later valid event did not apply: mark %d", got)
			}
		})
	}
}

func TestACatchUpDropIsCounted(t *testing.T) {
	rig := startSubscriber(t)
	rig.client.setHGetAll(map[string]string{"user:u1": "3"}, nil) // valid through PublishedFloors
	s := rig.client.next(t)
	s.hooks.OnSubscribed()
	waitFor(t, "mark 3", func() bool { return latest(rig, "user", "u1") == 3 })
	rig.sub.consumer.CatchUp(context.Background(), map[string]int64{"user:u2": 9007199254740992})
	if got := counterValue(t, rig.reader, "devhealth_auth_revision_feed_events_total", "disposition", "dropped"); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
}
