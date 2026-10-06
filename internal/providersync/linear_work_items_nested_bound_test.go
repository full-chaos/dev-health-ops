package providersync

import (
	"encoding/json"
	"errors"
	"regexp"
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

// A provider that re-serves the previous page's row at a page boundary (same id)
// must not double it. Labels, history and comments show it in the rows; for
// attachments, relations and inverseRelations the row of the downstream
// dependency identity (source, type, target) would hide a doubled node, so the
// per-type id functions are pinned directly below.
func TestLinearWorkItemsNestedRowsRepeatedByIDAreOneRow(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"labels", "history", "comments"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			batch, _, err := collectWithLinearNestedServerOptions(t, &linearNestedServer{field: field, totalPages: 12, overlap: true})
			if err != nil {
				t.Fatal(err)
			}
			if got := linearFieldRowCount(t, batch, field); got != 12 {
				t.Fatalf("%s: rows=%d want 12 (a repeated id is one row)", field, got)
			}
		})
	}
}

// Every nested type de-duplicates by ITS OWN provider id: the same id twice is
// one row, two ids with identical fields are two rows, a row without an id is
// kept. A constant or wrong id function fails here for each type.
func TestLinearAppendUniqueByIDForEveryNestedType(t *testing.T) {
	t.Parallel()
	check := func(t *testing.T, got, want int) {
		t.Helper()
		if got != want {
			t.Fatalf("rows=%d want %d", got, want)
		}
	}
	t.Run("labels", func(t *testing.T) {
		t.Parallel()
		rows := appendLinearUniqueByID(linearLabelID, []linearLabelPayload{{ID: "a", Name: "x"}},
			[]linearLabelPayload{{ID: "a", Name: "x"}, {ID: "b", Name: "x"}, {Name: "x"}})
		check(t, len(rows), 3)
	})
	t.Run("attachments", func(t *testing.T) {
		t.Parallel()
		rows := appendLinearUniqueByID(linearAttachmentID, []linearAttachmentPayload{{ID: "a", URL: "u"}},
			[]linearAttachmentPayload{{ID: "a", URL: "u"}, {ID: "b", URL: "u"}, {URL: "u"}})
		check(t, len(rows), 3)
	})
	t.Run("history", func(t *testing.T) {
		t.Parallel()
		rows := appendLinearUniqueByID(linearHistoryID, []linearHistoryEntry{{ID: "a", CreatedAt: "t"}},
			[]linearHistoryEntry{{ID: "a", CreatedAt: "t"}, {ID: "b", CreatedAt: "t"}, {CreatedAt: "t"}})
		check(t, len(rows), 3)
	})
	t.Run("comments", func(t *testing.T) {
		t.Parallel()
		rows := appendLinearUniqueByID(linearCommentID, []linearCommentPayload{{ID: "a", Body: "x"}},
			[]linearCommentPayload{{ID: "a", Body: "x"}, {ID: "b", Body: "x"}, {Body: "x"}})
		check(t, len(rows), 3)
	})
	t.Run("relations and inverseRelations", func(t *testing.T) {
		t.Parallel()
		rows := appendLinearUniqueByID(linearRelationID, []linearRelationPayload{{ID: "a", Type: "related"}},
			[]linearRelationPayload{{ID: "a", Type: "related"}, {ID: "b", Type: "related"}, {Type: "related"}})
		check(t, len(rows), 3)
	})
}

// Every nested connection the route asks Linear for SELECTS id: de-duplication
// by id is only as good as the id being in the answer. The fake servers return an
// id whatever the query asks, so the assertion is on the query text the route
// sends, not on the reply.
func TestLinearWorkItemsNestedQueriesSelectID(t *testing.T) {
	t.Parallel()
	collapse := func(query string) string { return strings.Join(strings.Fields(query), " ") }
	for _, test := range []struct {
		query      string
		connection string
	}{
		{linearWorkItemsLabelsQuery, "labels"},
		{linearWorkItemsAttachmentsQuery, "attachments"},
		{linearWorkItemsHistoryQuery, "history"},
		{linearWorkItemsCommentsQuery, "comments"},
		{linearWorkItemsRelationsQuery, "relations"},
		{linearWorkItemsInverseRelationsQuery, "inverseRelations"},
		{linearWorkItemsQuery, "labels"},
		{linearWorkItemsQuery, "attachments"},
		{linearWorkItemsQuery, "history"},
		{linearWorkItemsQuery, "comments"},
		{linearWorkItemsQuery, "relations"},
		{linearWorkItemsQuery, "inverseRelations"},
	} {
		pattern := regexp.MustCompile(test.connection + `\(first: [^)]*\) \{ nodes \{ id\b`)
		if !pattern.MatchString(collapse(test.query)) {
			t.Errorf("%s connection of query %.40q does not select id first in nodes", test.connection, collapse(test.query))
		}
	}
	// And what the route really sends: the issue query and every follow-up.
	_, server, err := collectWithLinearNestedServer(t, "comments", 3)
	if err != nil {
		t.Fatal(err)
	}
	sawFollowUp := false
	for _, query := range server.queries {
		if strings.Contains(query, "query LinearWorkItemsComments(") {
			sawFollowUp = true
			if !regexp.MustCompile(`comments\(first: [^)]*\) \{ nodes \{ id\b`).MatchString(collapse(query)) {
				t.Fatalf("comments follow-up sent without id: %s", collapse(query))
			}
		}
	}
	if !sawFollowUp {
		t.Fatal("no comments follow-up was sent")
	}
}
