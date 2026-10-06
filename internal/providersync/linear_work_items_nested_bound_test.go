package providersync

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

var linearEmbeddedNestedFields = []string{"labels", "attachments", "history", "comments", "relations", "inverseRelations"}

// linearEffectRowCount counts the rows a batch holds for one destination.
func linearEffectRowCount(batch CompleteRouteBatch, destination string) int {
	total := 0
	for _, effect := range batch.Effects {
		if effect.Destination == destination {
			total += len(effect.Rows)
		}
	}
	return total
}

// linearFieldRowCount is the number of persisted rows that one nested field
// feeds. Every node of the fake server is distinct, so each page adds one row.
func linearFieldRowCount(t *testing.T, batch CompleteRouteBatch, field string) int {
	t.Helper()
	switch field {
	case "labels":
		for _, effect := range batch.Effects {
			if effect.Destination == "work_items" {
				var row linearWorkItemRow
				if err := json.Unmarshal(effect.Rows[0], &row); err != nil {
					t.Fatal(err)
				}
				return len(row.Labels)
			}
		}
		return -1
	case "attachments", "relations", "inverseRelations":
		return linearEffectRowCount(batch, "work_item_dependencies")
	case "history":
		return linearEffectRowCount(batch, "work_item_transitions")
	case "comments":
		return linearEffectRowCount(batch, "work_item_interactions")
	default: // cycles
		return linearEffectRowCount(batch, "sprints")
	}
}

// Literal numbers on purpose: a bound that moves with the constant must still
// be caught. 50 total pages pass; 51 fail. For an embedded field the first
// page counts, so 49 follow-up requests serve 50 pages.
func TestLinearWorkItemsNestedBoundIsExactlyFiftyTotalPages(t *testing.T) {
	t.Parallel()
	for _, field := range append(append([]string{}, linearEmbeddedNestedFields...), "cycles") {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			batch, server, err := collectWithLinearNestedServer(t, field, 50)
			if err != nil {
				t.Fatalf("%s: 50 total pages must pass: %v", field, err)
			}
			wantNested := 49
			if field == "cycles" {
				wantNested = 50
			}
			if server.nested != wantNested {
				t.Fatalf("%s: nested requests=%d want %d", field, server.nested, wantNested)
			}
			if got := linearFieldRowCount(t, batch, field); got != 50 {
				t.Fatalf("%s: rows=%d want 50", field, got)
			}

			_, _, err = collectWithLinearNestedServer(t, field, 51)
			if !errors.Is(err, ErrPaginationCapExceeded) {
				t.Fatalf("%s: 51 total pages must fail closed, got %v", field, err)
			}
			for _, want := range []string{field, "after 50 pages", "max 50 pages"} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("%s: error %q lacks %q", field, err, want)
				}
			}
		})
	}
}

// The failure counts the embedded first page in its page and item numbers.
func TestLinearWorkItemsNestedBoundErrorCountsEmbeddedPage(t *testing.T) {
	t.Parallel()
	_, _, err := collectWithLinearNestedServer(t, "attachments", 60)
	if err == nil || !strings.Contains(err.Error(), "(50 items, max 50 pages)") {
		t.Fatalf("error %v: want 50 pages and 50 items counted, embedded page included", err)
	}
}

// Two rows that differ only by their provider id are two rows: the follow-up
// pages hold the same body, time and author, and none may collapse.
func TestLinearWorkItemsNestedRowsWithIdenticalFieldsStayDistinct(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"comments", "labels"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			batch, _, err := collectWithLinearNestedServerVariant(t, field, 12, true)
			if err != nil {
				t.Fatal(err)
			}
			if got := linearFieldRowCount(t, batch, field); got != 12 {
				t.Fatalf("%s: rows=%d want 12", field, got)
			}
		})
	}
}

// The same id on a later page is one row, not two.
func TestLinearAppendUniqueByIDDropsOnlyRepeatedIDs(t *testing.T) {
	t.Parallel()
	rows := appendLinearUniqueByID(linearCommentID,
		[]linearCommentPayload{{ID: "a", Body: "x"}},
		[]linearCommentPayload{{ID: "a", Body: "x"}, {ID: "b", Body: "x"}, {Body: "x"}, {Body: "x"}},
	)
	if len(rows) != 4 {
		t.Fatalf("rows=%d want 4 (a, b and the two id-less rows)", len(rows))
	}
}

// The reported page count includes the embedded first page of every field
// that was followed up.
func TestLinearWorkItemsEvidencePagesCountEmbeddedPage(t *testing.T) {
	t.Parallel()
	batch, server, err := collectWithLinearNestedServer(t, "comments", 12)
	if err != nil {
		t.Fatal(err)
	}
	// 1 team page + 1 issues page + 12 comment pages (11 follow-ups + embedded).
	if server.nested != 11 || batch.Evidence.Pages != 14 {
		t.Fatalf("nested=%d pages=%d want 11 and 14", server.nested, batch.Evidence.Pages)
	}
}
