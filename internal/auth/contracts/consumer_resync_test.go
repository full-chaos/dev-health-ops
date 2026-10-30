package contracts

import "testing"

func resyncEvent(typ, id string, revision int64) ProvisioningEvent {
	return ProvisioningEvent{AggregateType: typ, AggregateID: id, Payload: ProvisioningPayload{Revision: Revision(revision)}}
}

func TestResyncToMovesTheMarkToTheGapRevision(t *testing.T) {
	c := NewConsumerCursor()
	if got := c.Observe(resyncEvent("principal", "p1", 1)); got != Apply {
		t.Fatalf("1: %q", got)
	}
	if got := c.Observe(resyncEvent("principal", "p1", 3)); got != Gap {
		t.Fatalf("3: %q, want gap", got)
	}
	c.ResyncTo(AggregateKey("principal", "p1"), 3)
	if got := c.Observe(resyncEvent("principal", "p1", 3)); got != Ignore {
		t.Fatalf("3 again after resync: %q, want ignore", got)
	}
	if got := c.Observe(resyncEvent("principal", "p1", 2)); got != Ignore {
		t.Fatalf("2 after resync to 3: %q, want ignore", got)
	}
	if got := c.Observe(resyncEvent("principal", "p1", 4)); got != Apply {
		t.Fatalf("4 after resync to 3: %q, want apply", got)
	}
}

func TestResyncToNeverLowersAMark(t *testing.T) {
	c := NewConsumerCursor()
	for r := int64(1); r <= 5; r++ {
		if got := c.Observe(resyncEvent("principal", "p1", r)); got != Apply {
			t.Fatalf("%d: %q", r, got)
		}
	}
	key := AggregateKey("principal", "p1")
	c.ResyncTo(key, 2)
	if got := c.Observe(resyncEvent("principal", "p1", 3)); got != Ignore {
		t.Fatalf("3 after a resync to 2 below a mark of 5: %q, want ignore (the mark must not move down)", got)
	}
	if got := c.Observe(resyncEvent("principal", "p1", 6)); got != Apply {
		t.Fatalf("6: %q, want apply", got)
	}
}

func TestResyncToTouchesOnlyItsAggregate(t *testing.T) {
	c := NewConsumerCursor()
	c.ResyncTo(AggregateKey("principal", "p1"), 7)
	if got := c.Observe(resyncEvent("principal", "p2", 1)); got != Apply {
		t.Fatalf("another aggregate: %q, want apply", got)
	}
	if got := c.Observe(resyncEvent("user", "p1", 1)); got != Apply {
		t.Fatalf("the same id in another type: %q, want apply", got)
	}
}
