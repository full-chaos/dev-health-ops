package remaining

import (
	"fmt"
	"strings"
	"testing"
)

func TestChunkStringsByRenderedBytes(t *testing.T) {
	t.Run("empty input has no chunks", func(t *testing.T) {
		if got := chunkStringsByRenderedBytes(nil, 100); got != nil {
			t.Fatalf("got %v, want nil", got)
		}
	})
	t.Run("input under the cap is one chunk, unchanged", func(t *testing.T) {
		in := []string{"a", "b", "c"}
		got := chunkStringsByRenderedBytes(in, 100)
		if len(got) != 1 || strings.Join(got[0], ",") != "a,b,c" {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("rendered length of every chunk is at most the cap, order and items kept", func(t *testing.T) {
		in := realisticWorkItemIDs(1000)
		const max = 1000
		got := chunkStringsByRenderedBytes(in, max)
		var flat []string
		for _, chunk := range got {
			if n := len(renderedForClickHouse("?", []any{chunk})); n > max {
				t.Fatalf("chunk renders to %d bytes, cap %d", n, max)
			}
			flat = append(flat, chunk...)
		}
		if strings.Join(flat, "|") != strings.Join(in, "|") {
			t.Fatal("chunks do not concatenate back to the input")
		}
		if len(got) < 2 {
			t.Fatalf("1000 ids at cap %d made %d chunk(s)", max, len(got))
		}
	})
	t.Run("the cap is exact: a chunk of exactly cap bytes is allowed, one more splits", func(t *testing.T) {
		// "[" + 'aa' + ", " + 'aa' + "]" = 1+4+2+4+1 = 12
		if got := chunkStringsByRenderedBytes([]string{"aa", "aa"}, 12); len(got) != 1 {
			t.Fatalf("12-byte cap, 12-byte array: %d chunks, want 1", len(got))
		}
		if got := chunkStringsByRenderedBytes([]string{"aa", "aa"}, 11); len(got) != 2 {
			t.Fatalf("11-byte cap, 12-byte array: %d chunks, want 2", len(got))
		}
	})
	t.Run("escaping is counted: quotes and backslashes double", func(t *testing.T) {
		in := []string{`a'b`, `c\d`}
		for _, chunk := range chunkStringsByRenderedBytes(in, 16) {
			if n := len(renderedForClickHouse("?", []any{chunk})); n > 16 {
				t.Fatalf("chunk %q renders to %d bytes, cap 16", chunk, n)
			}
		}
		// each element renders to 6 bytes ('a\'b'), so 2 elements = 1+6+2+6+1 = 16
		if got := chunkStringsByRenderedBytes(in, 16); len(got) != 1 {
			t.Fatalf("got %d chunks, want 1", len(got))
		}
		if got := chunkStringsByRenderedBytes(in, 15); len(got) != 2 {
			t.Fatalf("got %d chunks at cap 15, want 2", len(got))
		}
	})
	t.Run("an item larger than the cap is kept, alone", func(t *testing.T) {
		big := strings.Repeat("x", 50)
		got := chunkStringsByRenderedBytes([]string{"a", big, "b"}, 10)
		if len(got) != 3 || got[1][0] != big {
			t.Fatalf("got %v", got)
		}
	})
}

// The production cap, applied to the realistic id shape, must keep one array
// well under the server limit and leave room for the other arrays and the fixed
// text of the statement that carries it.
func TestProductionCapLeavesRoomUnderTheServerLimit(t *testing.T) {
	if 3*workItemAttributionMaxArrayBytes+8192 >= clickhouseMaxQuerySize {
		t.Fatalf("3 arrays at the cap plus fixed text reach the %d-byte server limit", clickhouseMaxQuerySize)
	}
	chunks := chunkStringsByRenderedBytes(realisticWorkItemIDs(orgSizedIDCount), workItemAttributionMaxArrayBytes)
	if len(chunks) < 2 {
		t.Fatalf("%d org-sized ids made %d chunk(s), want >= 2", orgSizedIDCount, len(chunks))
	}
	t.Log(fmt.Sprintf("%d ids -> %d chunks", orgSizedIDCount, len(chunks)))
}
