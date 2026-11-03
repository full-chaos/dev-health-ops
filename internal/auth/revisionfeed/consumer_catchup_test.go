package revisionfeed

import (
	"context"
	"testing"
)

func TestCatchUpMovesTheMarkAndInvalidatesOnlyWhatMoved(t *testing.T) {
	c, rec := newTestConsumer(t)
	ctx := context.Background()
	c.Handle(ctx, ev("user", "a", 1))
	c.Handle(ctx, ev("user", "b", 1))
	c.Handle(ctx, ev("user", "b", 2))
	before := len(rec.snapshot())
	c.CatchUp(ctx, map[string]int64{"user:a": 4, "user:b": 2, "organization:o:1": 7, "nocolon": 3})
	if got, _ := c.Watermarks().Latest("user", "a"); got != 4 {
		t.Fatalf("user:a mark %d, want 4", got)
	}
	if got, _ := c.Watermarks().Latest("organization", "o:1"); got != 7 {
		t.Fatalf("organization:o:1 mark %d, want 7 (the id keeps its colons)", got)
	}
	if got, _ := c.Watermarks().Latest("user", "b"); got != 2 {
		t.Fatalf("user:b mark %d, want 2", got)
	}
	moved := rec.snapshot()[before:]
	if len(moved) != 2 {
		t.Fatalf("%d invalidations from the catch-up, want 2 (user:a and organization:o:1): %+v", len(moved), moved)
	}
	if got, _ := c.Watermarks().Latest("nocolon", ""); got != 0 {
		t.Fatal("a field without a colon moved a mark")
	}
}
