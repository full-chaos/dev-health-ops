package querybound

import (
	"fmt"
	"strings"
	"testing"
)

// rendered is the array literal clickhouse-go writes for a list of strings.
func rendered(items []string) string {
	quoted := make([]string, 0, len(items))
	for _, item := range items {
		quoted = append(quoted, "'"+strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(item)+"'")
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func TestChunksKeepEveryItemInOrderUnderTheCap(t *testing.T) {
	items := make([]string, 0, 2000)
	for index := 0; index < 2000; index++ {
		items = append(items, fmt.Sprintf("gh:acme/api#%d", index))
	}
	items = append(items, `it's`, `back\slash`)
	const maxBytes = 500
	chunks := ChunkStringsByRenderedBytes(items, maxBytes)
	if len(chunks) < 2 {
		t.Fatalf("%d items at a cap of %d bytes made %d chunk(s)", len(items), maxBytes, len(chunks))
	}
	var flat []string
	for _, chunk := range chunks {
		if len(chunk) == 0 {
			t.Fatal("an empty chunk")
		}
		if size := len(rendered(chunk)); size > maxBytes {
			t.Fatalf("a chunk renders to %d bytes, cap %d", size, maxBytes)
		}
		flat = append(flat, chunk...)
	}
	if strings.Join(flat, "\x00") != strings.Join(items, "\x00") {
		t.Fatal("the chunks do not join back to the input")
	}
}

func TestTheCapIsExactAndCountsEscapes(t *testing.T) {
	// "[" + 'aa' + ", " + 'aa' + "]" = 1+4+2+4+1 = 12 bytes.
	if got := ChunkStringsByRenderedBytes([]string{"aa", "aa"}, 12); len(got) != 1 {
		t.Fatalf("a 12-byte list at a cap of 12: %d chunks, want 1", len(got))
	}
	if got := ChunkStringsByRenderedBytes([]string{"aa", "aa"}, 11); len(got) != 2 {
		t.Fatalf("a 12-byte list at a cap of 11: %d chunks, want 2", len(got))
	}
	// A quote and a backslash each take one more byte: 'a\'b' is 6 bytes.
	if got, want := RenderedStringLen(`a'b`), len(`'a\'b'`); got != want {
		t.Fatalf("RenderedStringLen(a'b) = %d, want %d", got, want)
	}
	if got, want := RenderedStringLen(`c\d`), len(`'c\\d'`); got != want {
		t.Fatalf(`RenderedStringLen(c\d) = %d, want %d`, got, want)
	}
}

func TestNoItemIsDroppedAndNoInputMakesNoChunk(t *testing.T) {
	if got := ChunkStringsByRenderedBytes(nil, 100); got != nil {
		t.Fatalf("no item: %v, want no chunk", got)
	}
	// An item longer than the cap keeps a chunk of its own.
	long := strings.Repeat("x", 50)
	got := ChunkStringsByRenderedBytes([]string{"a", long, "b"}, 10)
	if len(got) != 3 || got[1][0] != long {
		t.Fatalf("an item above the cap: %v", got)
	}
}

// Two lists at the cap and the fixed text stay under the server's limit.
func TestTwoListsAtTheCapStayUnderTheServerLimit(t *testing.T) {
	const serverMaxQuerySize = 262144
	if 2*MaxArrayBytes+16*1024 > serverMaxQuerySize {
		t.Fatalf("two lists of %d bytes and 16 KiB of text are above max_query_size %d", MaxArrayBytes, serverMaxQuerySize)
	}
}
