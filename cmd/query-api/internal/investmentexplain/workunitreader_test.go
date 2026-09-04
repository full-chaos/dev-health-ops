package investmentexplain

import (
	"strconv"
	"strings"
	"testing"
)

// TestChunkStringsMatchesPythonChunkBoundaries proves chunkStrings splits
// at exactly lookupChunkSize (250), matching work_unit_investments.py's
// _chunks(values, size=_LOOKUP_CHUNK_SIZE) -- the off-by-one this guards
// is real: a chunk boundary at 251 instead of 250 (or vice versa) would
// silently under- or over-batch the FIRST IN-list query issued once real
// data exceeds 250 ids, invisible on any smaller fixture.
func TestChunkStringsMatchesPythonChunkBoundaries(t *testing.T) {
	cases := []struct {
		name       string
		count      int
		wantChunks []int // expected length of each chunk, in order
	}{
		{"empty", 0, nil},
		{"under_one_chunk", 1, []int{1}},
		{"exactly_one_chunk", 250, []int{250}},
		{"one_over", 251, []int{250, 1}},
		{"exactly_two_chunks", 500, []int{250, 250}},
		{"two_chunks_plus_one", 501, []int{250, 250, 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values := make([]string, tc.count)
			for i := range values {
				values[i] = strconv.Itoa(i)
			}
			chunks := chunkStrings(values, lookupChunkSize)
			if len(chunks) != len(tc.wantChunks) {
				t.Fatalf("chunk count: want %d, got %d", len(tc.wantChunks), len(chunks))
			}
			total := 0
			for i, chunk := range chunks {
				if len(chunk) != tc.wantChunks[i] {
					t.Fatalf("chunk %d length: want %d, got %d", i, tc.wantChunks[i], len(chunk))
				}
				total += len(chunk)
			}
			if total != tc.count {
				t.Fatalf("total values across chunks: want %d, got %d", tc.count, total)
			}
		})
	}
}

// TestUniqueNonEmptyDropsBlanksAndDuplicates proves uniqueNonEmpty matches
// work_unit_investments.py's _unique_non_empty: `list(dict.fromkeys(v for
// v in values if v))` -- empty strings dropped, order-preserving dedup.
func TestUniqueNonEmptyDropsBlanksAndDuplicates(t *testing.T) {
	got := uniqueNonEmpty([]string{"b", "", "a", "b", "", "c", "a"})
	want := []string{"b", "a", "c"}
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("want %v, got %v", want, got)
		}
	}
}

// TestFetchWorkUnitInvestmentsQueryShape checks the compiled query text's
// structural fragments (source, filter clauses, ORDER BY, LIMIT) without a
// live ClickHouse -- consistent with the bigboy testcontainer pause. The
// query text itself is captured via a fake conn.
func TestFetchWorkUnitInvestmentsQueryShape(t *testing.T) {
	capture := &capturingClient{}
	reader, err := NewReader(capture)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	if _, err := reader.FetchWorkUnitInvestments(t.Context(), WorkUnitInvestmentsFilter{
		OrgID:      "org-1",
		RepoIDs:    []string{"repo-1", "repo-2"},
		Limit:      50,
		WorkUnitID: "unit-1",
	}); err != nil {
		t.Fatalf("FetchWorkUnitInvestments: %v", err)
	}

	query := capture.lastQuery
	for _, fragment := range []string{
		"ORDER BY effort_value DESC, work_unit_id ASC",
		"LIMIT {limit:UInt64}",
		"work_unit_investments.repo_id IN {repo_ids:Array(String)}",
		"work_unit_investments.work_unit_id = {work_unit_id:String}",
		"SETTINGS max_execution_time",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("query missing fragment %q:\n%s", fragment, query)
		}
	}

	foundLimit := false
	for _, b := range capture.lastBindings {
		if b.Name == "limit" {
			foundLimit = true
			if b.Value != uint64(50) {
				t.Fatalf("limit binding: want uint64(50), got %#v (%T)", b.Value, b.Value)
			}
		}
	}
	if !foundLimit {
		t.Fatal("no limit binding found")
	}
}

// TestFetchWorkUnitInvestmentsOmitsFiltersWhenUnset proves the repo_id/
// work_unit_id clauses are absent (not just empty-valued) when the filter
// isn't set -- matching Python's own conditional `if repo_ids:`/
// `if work_unit_id:` filter-list construction.
func TestFetchWorkUnitInvestmentsOmitsFiltersWhenUnset(t *testing.T) {
	capture := &capturingClient{}
	reader, err := NewReader(capture)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	if _, err := reader.FetchWorkUnitInvestments(t.Context(), WorkUnitInvestmentsFilter{
		OrgID: "org-1",
		Limit: 10,
	}); err != nil {
		t.Fatalf("FetchWorkUnitInvestments: %v", err)
	}

	query := capture.lastQuery
	for _, fragment := range []string{"repo_id IN", "work_unit_id ="} {
		if strings.Contains(query, fragment) {
			t.Fatalf("query unexpectedly contains %q with no filter set:\n%s", fragment, query)
		}
	}
}
