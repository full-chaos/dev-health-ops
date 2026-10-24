package syncdispatchruntime

import (
	"fmt"
	"testing"
)

type pagedRow struct{ id string }

// fakeKeysetTable serves rows in id order and records the size of every page it returns.
type fakeKeysetTable struct {
	rows  []pagedRow
	pages []int
}

func newFakeKeysetTable(n int) *fakeKeysetTable {
	table := &fakeKeysetTable{}
	for index := 0; index < n; index++ {
		table.rows = append(table.rows, pagedRow{id: fmt.Sprintf("%08d", index)})
	}
	return table
}

func (table *fakeKeysetTable) fetch(after *string, limit int) ([]pagedRow, error) {
	var page []pagedRow
	for _, row := range table.rows {
		if after != nil && row.id <= *after {
			continue
		}
		page = append(page, row)
		if len(page) == limit {
			break
		}
	}
	table.pages = append(table.pages, len(page))
	return page, nil
}

func TestKeysetPagerReadsAtMostKRowsPerCallAndEveryRowOnce(t *testing.T) {
	const k = 500
	for _, n := range []int{0, 1, k - 1, k, k + 1, 10*k + 7} {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			table := newFakeKeysetTable(n)
			var seen []string
			err := forEachKeysetPage(k, func(row pagedRow) string { return row.id }, table.fetch, func(row pagedRow) error {
				seen = append(seen, row.id)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(seen) != n {
				t.Fatalf("folded %d rows, want %d", len(seen), n)
			}
			for index, id := range seen {
				if id != table.rows[index].id {
					t.Fatalf("row %d = %s, want %s (id order, each once)", index, id, table.rows[index].id)
				}
			}
			for _, size := range table.pages {
				if size > k {
					t.Fatalf("a call returned %d rows, want at most %d (pages %v)", size, k, table.pages)
				}
			}
			wantCalls := n/k + 1
			if len(table.pages) != wantCalls {
				t.Fatalf("calls = %d, want %d for N=%d", len(table.pages), wantCalls, n)
			}
		})
	}
}
