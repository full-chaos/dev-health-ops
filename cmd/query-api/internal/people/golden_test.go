package people

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// fixtureRowScanner replays a fixed slice of pre-built rows -- same shape
// as quadrant/drilldown's own fixtureRowScanner.
type fixtureRowScanner struct {
	rows  [][]any
	index int
}

func (s *fixtureRowScanner) Next() bool {
	if s.index >= len(s.rows) {
		return false
	}
	s.index++
	return true
}

func (s *fixtureRowScanner) Scan(dest ...any) error {
	row := s.rows[s.index-1]
	*dest[0].(*string) = row[0].(string)
	*dest[1].(*time.Time) = row[1].(time.Time)
	return nil
}

func (s *fixtureRowScanner) Err() error   { return nil }
func (s *fixtureRowScanner) Close() error { return nil }

// fakeQueryClient dispatches whatever handler the test supplies, same
// convention as quadrant/drilldown's own fakeQueryClient.
type fakeQueryClient struct {
	t       *testing.T
	handler func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

func (c fakeQueryClient) Query(_ context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return c.handler(c.t, query, bindings)
}

func bindingValue(bindings []dhclickhouse.Binding, name string) (any, bool) {
	for _, b := range bindings {
		if b.Name == name {
			return b.Value, true
		}
	}
	return nil, false
}

func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// loadGolden decodes a testdata JSON file -- captured from the REAL
// Python search_people_response (monkeypatched query_people/
// clickhouse_client/utc_today) via a one-off `uv run python3`
// invocation (see this PR's own TEST-EVIDENCE for the exact script).
// DisallowUnknownFields makes a field-name mismatch a hard test failure,
// not a silent drop.
func loadGolden(t *testing.T, name string) []SearchResult {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	var resp []SearchResult
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode golden %s: %v", name, err)
	}
	return resp
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

// TestGoldenNoAliasMapping replays testdata/no_alias_mapping.json: an
// empty identity_mapping.yaml (this deployment's practical reality --
// see identity.go's own doc comment), two raw identities with no alias
// entry at all, one recently seen (active) and one stale (inactive).
func TestGoldenNoAliasMapping(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", t.TempDir()+"/missing.yaml")

	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if v, _ := bindingValue(bindings, "org_id"); v != "org-acme" {
			t.Fatalf("org_id binding = %v, want org-acme", v)
		}
		if v, _ := bindingValue(bindings, "query"); v != "%ali%" {
			t.Fatalf("query binding = %v, want %%ali%%", v)
		}
		if v, _ := bindingValue(bindings, "limit"); v != 20 {
			t.Fatalf("limit binding = %v, want 20", v)
		}
		return &fixtureRowScanner{rows: [][]any{
			{"alice@example.com", day(2024, 6, 1)},
			{"github:alicia", day(2024, 1, 1)},
		}}, nil
	}}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildSearchResponse(context.Background(), reader, "org-acme", SearchParams{
		Query: "  Ali  ",
		Limit: 20,
		Now:   day(2024, 6, 15),
	})
	if err != nil {
		t.Fatalf("BuildSearchResponse: %v", err)
	}
	want := loadGolden(t, "no_alias_mapping.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestGoldenAliasMapping replays testdata/alias_mapping.json: a populated
// identity_mapping.yaml with one canonical/alias pair, exercised three
// ways in one response -- a raw identity that IS an alias (reverse-map to
// canonical), a raw identity that already equals the canonical (no
// duplicate append), and a raw identity with no alias-map entry at all
// (falls back to itself), including a NULL/zero last_seen (active stays
// the Python default True).
func TestGoldenAliasMapping(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/identity_mapping.yaml"
	if err := os.WriteFile(path, []byte(`
version: 1
identities:
  - canonical: bob.jones@example.com
    aliases:
      - "github:bjones"
      - "Bob J"
`), 0o600); err != nil {
		t.Fatalf("write identity mapping: %v", err)
	}
	t.Setenv("IDENTITY_MAPPING_PATH", path)

	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		return &fixtureRowScanner{rows: [][]any{
			{"github:bjones", day(2024, 5, 1)},
			{"bob.jones@example.com", day(2024, 6, 10)},
			{"carol@example.com", time.Time{}},
		}}, nil
	}}

	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildSearchResponse(context.Background(), reader, "org-acme", SearchParams{
		Query: "bo",
		Limit: 20,
		Now:   day(2024, 6, 15),
	})
	if err != nil {
		t.Fatalf("BuildSearchResponse: %v", err)
	}
	want := loadGolden(t, "alias_mapping.json")
	if gotJSON, wantJSON := mustMarshal(t, got), mustMarshal(t, want); gotJSON != wantJSON {
		t.Fatalf("response mismatch\n got:  %s\nwant: %s", gotJSON, wantJSON)
	}
}

// TestEmptyQueryShortCircuitsWithoutTouchingClickHouse pins `if not
// trimmed: return []` (services/people.py:405-406): a blank/whitespace-only
// q never calls the query client at all.
func TestEmptyQueryShortCircuitsWithoutTouchingClickHouse(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Fatalf("query client must not be called for an empty query")
		return nil, nil
	}}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got, err := BuildSearchResponse(context.Background(), reader, "org-acme", SearchParams{Query: "   ", Limit: 20, Now: day(2024, 6, 15)})
	if err != nil {
		t.Fatalf("BuildSearchResponse: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("BuildSearchResponse(empty query) = %v, want empty", got)
	}
}

// TestBoundedSearchLimit pins _bounded_limit(limit, _MAX_SEARCH_LIMIT)
// (services/people.py:267-271) as specialized by boundedSearchLimit.
func TestBoundedSearchLimit(t *testing.T) {
	cases := map[int]int{
		0:   50,
		-5:  50,
		1:   1,
		20:  20,
		50:  50,
		999: 50,
	}
	for in, want := range cases {
		if got := boundedSearchLimit(in); got != want {
			t.Fatalf("boundedSearchLimit(%d) = %d, want %d", in, got, want)
		}
	}
}
