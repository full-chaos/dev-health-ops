package edges

import (
	"encoding/json"
	"fmt"
	"time"
)

// GoldenDocument decodes tests/fixtures/workgraph_issue_edges_python_golden.json.
//
// Every string field is an index into Strings — the fixture interns ids so it
// stays checkable-in (1.0 MB for 6,531 dependency rows and 3,548 edges). The
// encoding is lossless: the decode below reconstructs the exact rows the deployed
// Python producer read and wrote.
type GoldenDocument struct {
	Schema    string   `json:"schema"`
	OrgID     string   `json:"org_id"`
	FrozenNow string   `json:"frozen_now"`
	Strings   []string `json:"strings"`

	// [source*, target*, relationship_type*, relationship_type_raw*, semantics*, last_synced*]
	Dependencies [][6]int `json:"dependencies"`
	// One list per cursor page, in page order. Page BOUNDARIES are part of the
	// contract: the cleanup step pages work_graph_edges on an `edge_id > cursor`
	// cursor, and an unpaged port works on this org and truncates on a larger one.
	ExistingEdgeIDs [][]int      `json:"existing_edge_ids"`
	Edges           []GoldenEdge `json:"edges"`
	// [org_id*, projection_name*, scope_repo_id*, rule_version*, watermark*, row_count, completed_at*]
	ProjectionRuns [][7]int     `json:"projection_runs"`
	Mutations      []Mutation   `json:"mutations"`
	Config         GoldenConfig `json:"config"`
	Queries        []Query      `json:"queries"`

	Counts map[string]int `json:"counts"`
}

// Mutation is one recorded `ALTER TABLE ... DELETE`. The generator records these
// without executing them: they are part of the behaviour a port must reproduce
// (a rewrite that skips them leaves the historical orientation alive under its
// own edge_id), but a golden generator that mutated the data it was capturing
// would corrupt the shared stack and could not be re-run.
type Mutation struct {
	Statement  string         `json:"statement"`
	Parameters map[string]any `json:"parameters"`
}

// GoldenEdge keeps the one non-interned position (confidence) typed, so a JSON
// number can never silently become a string index.
type GoldenEdge struct {
	EdgeID       int
	SourceType   int
	SourceID     int
	TargetType   int
	TargetID     int
	EdgeType     int
	Provenance   int
	Confidence   float64
	Evidence     int
	DiscoveredAt int
	LastSynced   int
	EventTs      int
	Day          int
	OrgID        int
}

func (edge *GoldenEdge) UnmarshalJSON(data []byte) error {
	var raw []json.Number
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) != 14 {
		return fmt.Errorf("golden edge row has %d fields, want 14", len(raw))
	}
	targets := []*int{
		&edge.EdgeID, &edge.SourceType, &edge.SourceID, &edge.TargetType,
		&edge.TargetID, &edge.EdgeType, &edge.Provenance,
	}
	for index, target := range targets {
		value, err := raw[index].Int64()
		if err != nil {
			return fmt.Errorf("golden edge field %d: %w", index, err)
		}
		*target = int(value)
	}
	confidence, err := raw[7].Float64()
	if err != nil {
		return fmt.Errorf("golden edge confidence: %w", err)
	}
	edge.Confidence = confidence
	tail := []*int{
		&edge.Evidence, &edge.DiscoveredAt, &edge.LastSynced,
		&edge.EventTs, &edge.Day, &edge.OrgID,
	}
	for offset, target := range tail {
		value, err := raw[8+offset].Int64()
		if err != nil {
			return fmt.Errorf("golden edge field %d: %w", 8+offset, err)
		}
		*target = int(value)
	}
	return nil
}

// String resolves an interned index.
func (document *GoldenDocument) String(index int) (string, error) {
	if index < 0 || index >= len(document.Strings) {
		return "", fmt.Errorf("intern index %d out of range (%d strings)", index, len(document.Strings))
	}
	return document.Strings[index], nil
}

// Instant resolves an interned index and parses it as an RFC3339 instant.
//
// Timestamps are compared as INSTANTS, never as strings: work_graph_edges
// carries DateTime64(3, 'UTC') while work_item_dependencies carries a bare
// DateTime64(3), and a port that compared rendered text would either paper over
// that difference or invent one.
func (document *GoldenDocument) Instant(index int) (time.Time, error) {
	value, err := document.String(index)
	if err != nil {
		return time.Time{}, err
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse instant %q: %w", value, err)
	}
	return parsed.UTC(), nil
}

// EdgeRow reconstructs one work_graph_edges row from the golden, applying the
// Float32 narrowing the column performs. The confidence is the value PYTHON
// wrote; applying the variant-C policy on top is the caller's business, so the
// exception list stays visible at the comparison site.
func (document *GoldenDocument) EdgeRow(edge GoldenEdge) (Row, error) {
	var row Row
	// Positional, deliberately not a map keyed by the intern index: interning is
	// value-based, so source_type and target_type share an index whenever both
	// endpoints are issues -- which is every row this producer writes. A
	// map[index]*field silently drops one of each colliding pair.
	fields := []struct {
		index int
		into  *string
	}{
		{edge.EdgeID, &row.EdgeID},
		{edge.SourceType, &row.SourceType},
		{edge.SourceID, &row.SourceID},
		{edge.TargetType, &row.TargetType},
		{edge.TargetID, &row.TargetID},
		{edge.EdgeType, &row.EdgeType},
		{edge.Provenance, &row.Provenance},
		{edge.Evidence, &row.Evidence},
		{edge.OrgID, &row.OrgID},
	}
	for _, field := range fields {
		value, err := document.String(field.index)
		if err != nil {
			return Row{}, err
		}
		*field.into = value
	}
	row.Confidence = Quantize(edge.Confidence)
	var err error
	if row.DiscoveredAt, err = document.Instant(edge.DiscoveredAt); err != nil {
		return Row{}, err
	}
	if row.LastSynced, err = document.Instant(edge.LastSynced); err != nil {
		return Row{}, err
	}
	if row.EventTs, err = document.Instant(edge.EventTs); err != nil {
		return Row{}, err
	}
	day, err := document.String(edge.Day)
	if err != nil {
		return Row{}, err
	}
	parsedDay, err := time.Parse("2006-01-02", day)
	if err != nil {
		return Row{}, fmt.Errorf("parse day %q: %w", day, err)
	}
	row.Day = parsedDay.UTC()
	return row, nil
}

// GoldenConfig is the FULL producer input, not just its rows.
//
// Freezing only the rows leaves every other input dimension outside the oracle
// while it still looks exhaustive — the blind spot lane-pathb-go's review found,
// where a generator passed only DSN and org and the build WINDOW was never
// captured at all.
type GoldenConfig struct {
	OrgID               string  `json:"org_id"`
	FromTs              string  `json:"from_ts"`
	ToTs                string  `json:"to_ts"`
	RepoID              *string `json:"repo_id"`
	HeuristicDaysWindow int     `json:"heuristic_days_window"`
	HeuristicConfidence float64 `json:"heuristic_confidence"`
	// ClickHouseBounds is what _format_datetime_for_clickhouse rendered from the
	// two instants above, frozen alongside them so the second-truncation contract
	// is testable against Python's own output rather than a Go constant.
	ClickHouseBounds struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"clickhouse_bounds"`
}

// Query is one read the producer issued, statement and parameters.
//
// The TEXT is frozen, not just the rows it returned, because rows alone cannot
// show which input dimensions a producer consults. It is what makes "this
// sub-builder does not filter by window" an asserted structural fact instead of
// a reading of the source that decays the moment Python changes.
type Query struct {
	Statement  string         `json:"statement"`
	Parameters map[string]any `json:"parameters"`
}
