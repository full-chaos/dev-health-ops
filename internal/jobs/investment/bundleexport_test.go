package investment

// CHAOS-8712 experiment: export the REAL text bundles of an org, one JSON line
// per work unit, with no LLM call and no write.
//
// This file is _test.go on purpose. It needs the unexported
// Materializer.fetchEntities (so the bundle is built by the exact production
// chain, not a copy), and a _test.go file is not compiled into any production
// binary. The live entry point is gated by EXPORT_BUNDLES_* env vars and
// skips when they are absent:
//
//	EXPORT_BUNDLES_CH_DSN=clickhouse://user:pass@host:9000/db \
//	EXPORT_BUNDLES_ORG=<org uuid> \
//	EXPORT_BUNDLES_OUT=/path/out.jsonl \
//	  go test ./internal/jobs/investment -run TestExportBundlesLive -count=1
//
// Everything on the path is a SELECT. The reader and the incumbent query go
// through a conn that has only Query; no chwrite type is touched.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

// exportConn is the read-only capability the export uses.
type exportConn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// BundleHandle is one evidence handle (E<n>) of a bundle.
type BundleHandle struct {
	Handle     string `json:"handle"`
	SourceType string `json:"source_type"`
	SourceID   string `json:"source_id"`
	TextChars  int    `json:"text_chars"`
}

// EntityCounts counts entities by type.
type EntityCounts struct {
	Issues  int `json:"issues"`
	PRs     int `json:"prs"`
	Commits int `json:"commits"`
}

// IncumbentQuote is one persisted evidence quote of the incumbent.
type IncumbentQuote struct {
	Quote      string `json:"quote"`
	SourceType string `json:"source_type"`
	SourceID   string `json:"source_id"`
}

// Incumbent is the persisted outcome of the production categorizer for a unit.
type Incumbent struct {
	Status          string             `json:"status"`
	ModelVersion    string             `json:"model_version"`
	InputHash       string             `json:"input_hash"`
	InputHashMatch  bool               `json:"input_hash_match"`
	RunID           string             `json:"run_id"`
	ComputedAt      time.Time          `json:"computed_at"`
	Subcategories   map[string]float64 `json:"subcategories"`
	Themes          map[string]float64 `json:"themes"`
	TopSubcategory  string             `json:"top_subcategory"`
	EvidenceQuality float64            `json:"evidence_quality"`
	QualityBand     string             `json:"evidence_quality_band"`
	ErrorsJSON      string             `json:"errors_json"`
	Quotes          []IncumbentQuote   `json:"quotes"`
	RowsForUnit     int                `json:"rows_for_unit"`
}

// BundleRecord is one exported line.
type BundleRecord struct {
	BundleID        string         `json:"bundle_id"`
	WorkUnitID      string         `json:"work_unit_id"`
	InputHash       string         `json:"input_hash"`
	SourceBlock     string         `json:"source_block"`
	SourceBlockLen  int            `json:"source_block_len"`
	TextCharCount   int            `json:"text_char_count"`
	TextSourceCount int            `json:"text_source_count"`
	Handles         []BundleHandle `json:"handles"`
	Entities        EntityCounts   `json:"entities"`
	Selected        EntityCounts   `json:"selected"`
	// LabelsPresent: a selected issue carries at least one non-empty label.
	LabelsPresent bool `json:"labels_present"`
	// LabelsInBlock: the first label of such an issue survives into the text the
	// model sees (the 280/900 char caps can cut it).
	LabelsInBlock bool `json:"labels_in_block"`
	// PassesGate mirrors the production pre-LLM gate (materialize.go Run:
	// TextCharCount < minEvidenceChars, then TextSourceCount == 0).
	PassesGate bool   `json:"passes_gate"`
	GateReason string `json:"gate_reason"`
	RepoID     string `json:"repo_id"`
	Provider   string `json:"provider"`
	UnitType   string `json:"work_unit_type"`
	// BoundsFrom/BoundsTo are the unit's computed time bounds.
	BoundsFrom time.Time  `json:"bounds_from"`
	BoundsTo   time.Time  `json:"bounds_to"`
	Incumbent  *Incumbent `json:"incumbent"`
}

// ExportConfig scopes one export.
type ExportConfig struct {
	OrgID string
	// MaxComponentNodes nil keeps the production default.
	MaxComponentNodes *int
}

// ExportSummary counts what the export saw.
type ExportSummary struct {
	Components int
	Exported   int
	Skipped    map[string]int
}

// wideWindow keeps every component in scope: the export wants all units, not
// only the ones a production run would touch inside its lookback.
var (
	wideFrom = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	wideTo   = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
)

// ExportBundles runs the production chain (edges -> components -> entities ->
// MaterializeComponent) and calls emit once per unit, in component order. It
// never calls a provider and never writes.
func ExportBundles(ctx context.Context, connection exportConn, cfg ExportConfig, emit func(BundleRecord) error) (ExportSummary, error) {
	summary := ExportSummary{Skipped: map[string]int{}}
	if strings.TrimSpace(cfg.OrgID) == "" {
		return summary, fmt.Errorf("export: org id is required")
	}
	reader, err := chquery.NewReader(connection)
	if err != nil {
		return summary, err
	}
	edgeRows, err := reader.FetchWorkGraphEdges(ctx, chquery.EdgeQueryOptions{OrganizationID: cfg.OrgID})
	if err != nil {
		return summary, fmt.Errorf("fetch work graph edges: %w", err)
	}
	components := units.BuildComponents(
		chquery.ComponentEdges(edgeRows), cfg.MaxComponentNodes, partitionOversizedHubs, &units.BuildStats{},
	)
	summary.Components = len(components)
	if len(components) == 0 {
		return summary, nil
	}
	// Only the reader is set: fetchEntities touches nothing else.
	materializer := &Materializer{reader: reader}
	entities, err := materializer.fetchEntities(ctx, Config{OrgID: cfg.OrgID}, components)
	if err != nil {
		return summary, err
	}
	edgeRepoIDs := make(map[string]string, len(edgeRows))
	for _, row := range edgeRows {
		if row.RepoID != "" {
			edgeRepoIDs[row.Edge.EdgeID] = row.RepoID
		}
	}
	incumbents, err := fetchIncumbentRows(ctx, connection, cfg.OrgID)
	if err != nil {
		return summary, err
	}

	for index, component := range components {
		result, err := MaterializeComponent(MaterializeComponentInput{
			Component: component, WorkItems: entities.WorkItems, PRs: entities.PRs, Commits: entities.Commits,
			EdgeRepoIDs: edgeRepoIDs, PRChurn: entities.PRChurn, CommitChurn: entities.CommitChurn,
			ActiveHours: entities.ActiveHours, ParentTitles: entities.ParentTitles, EpicTitles: entities.EpicTitles,
			FromTS: wideFrom, ToTS: wideTo,
		})
		if err != nil {
			return summary, fmt.Errorf("assemble component %d: %w", index, err)
		}
		if result.Skipped != "" {
			summary.Skipped[result.Skipped]++
			continue
		}
		record := buildBundleRecord(component, result, entities.WorkItems)
		record.Incumbent = pickIncumbent(incumbents[record.WorkUnitID], record.InputHash)
		if err := emit(record); err != nil {
			return summary, err
		}
		summary.Exported++
	}
	return summary, nil
}

// buildBundleRecord is the pure half: it only reshapes what the producer
// already built.
func buildBundleRecord(component units.Component, result MaterializeComponentResult, workItems map[string]chquery.WorkItem) BundleRecord {
	bundle := result.Bundle
	record := BundleRecord{
		BundleID:        "bnd_" + shortHash(bundle.InputHash),
		WorkUnitID:      result.Investment.WorkUnitID,
		InputHash:       bundle.InputHash,
		SourceBlock:     bundle.SourceBlock,
		SourceBlockLen:  utf8.RuneCountInString(bundle.SourceBlock),
		TextCharCount:   bundle.TextCharCount,
		TextSourceCount: bundle.TextSourceCount,
		Handles:         []BundleHandle{},
		BoundsFrom:      result.Investment.FromTS,
		BoundsTo:        result.Investment.ToTS,
	}
	switch {
	case bundle.TextCharCount < minEvidenceChars:
		record.GateReason = "insufficient_evidence"
	case bundle.TextSourceCount == 0:
		record.GateReason = "no_text_sources"
	default:
		record.PassesGate = true
	}
	if result.Investment.RepoID != nil {
		record.RepoID = result.Investment.RepoID.String()
	}
	if result.Investment.Provider != nil {
		record.Provider = *result.Investment.Provider
	}
	if result.Investment.WorkUnitType != nil {
		record.UnitType = *result.Investment.WorkUnitType
	}

	for _, node := range dedupeNodeKeys(component.Nodes) {
		switch node.Type {
		case "issue":
			record.Entities.Issues++
		case "pr":
			record.Entities.PRs++
		case "commit":
			record.Entities.Commits++
		}
	}

	handles := make([]string, 0, len(bundle.HandleMap))
	for handle := range bundle.HandleMap {
		handles = append(handles, handle)
	}
	sort.Slice(handles, func(i, j int) bool { return handleOrdinal(handles[i]) < handleOrdinal(handles[j]) })
	for _, handle := range handles {
		ref := bundle.HandleMap[handle]
		text := bundle.SourceTexts[ref.SourceType][ref.SourceID]
		record.Handles = append(record.Handles, BundleHandle{
			Handle: handle, SourceType: ref.SourceType, SourceID: ref.SourceID,
			TextChars: utf8.RuneCountInString(text),
		})
		switch ref.SourceType {
		case "issue":
			record.Selected.Issues++
			if label := firstLabel(workItems[ref.SourceID].Labels); label != "" {
				record.LabelsPresent = true
				if strings.Contains(text, "Labels: "+label) {
					record.LabelsInBlock = true
				}
			}
		case "pr":
			record.Selected.PRs++
		case "commit":
			record.Selected.Commits++
		}
	}
	return record
}

func firstLabel(labels []string) string {
	for _, label := range labels {
		if label != "" {
			return label
		}
	}
	return ""
}

func shortHash(hash string) string {
	if len(hash) > 16 {
		return hash[:16]
	}
	return hash
}

// handleOrdinal orders "E2" before "E10".
func handleOrdinal(handle string) int {
	var n int
	_, _ = fmt.Sscanf(strings.TrimPrefix(handle, "E"), "%d", &n)
	return n
}

// incumbentRow is one raw work_unit_investments row for a unit.
type incumbentRow struct {
	Status, ModelVersion, InputHash, RunID, ErrorsJSON, Band string
	ComputedAt                                               time.Time
	Subcategories, Themes                                    map[string]float64
	Quality                                                  float64
	Quotes                                                   []IncumbentQuote
}

// fetchIncumbentRows reads the persisted rows and quotes of an org. Raw reads,
// no FINAL: the ReplacingMergeTree may hold unmerged duplicates, which
// pickIncumbent resolves by computed_at.
func fetchIncumbentRows(ctx context.Context, connection exportConn, orgID string) (map[string][]incumbentRow, error) {
	rows, err := connection.Query(ctx, `
        SELECT work_unit_id, categorization_status, categorization_model_version,
               categorization_input_hash, categorization_run_id, categorization_errors_json,
               evidence_quality_band, evidence_quality, computed_at,
               subcategory_distribution_json, theme_distribution_json
        FROM work_unit_investments
        WHERE org_id = {org_id:String}`, clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, fmt.Errorf("query incumbent rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string][]incumbentRow{}
	for rows.Next() {
		var id string
		var r incumbentRow
		if err := rows.Scan(&id, &r.Status, &r.ModelVersion, &r.InputHash, &r.RunID, &r.ErrorsJSON,
			&r.Band, &r.Quality, &r.ComputedAt, &r.Subcategories, &r.Themes); err != nil {
			return nil, fmt.Errorf("scan incumbent row: %w", err)
		}
		out[id] = append(out[id], r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	quoteRows, err := connection.Query(ctx, `
        SELECT work_unit_id, categorization_run_id, quote, source_type, source_id
        FROM work_unit_investment_quotes
        WHERE org_id = {org_id:String}`, clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, fmt.Errorf("query incumbent quotes: %w", err)
	}
	defer func() { _ = quoteRows.Close() }()
	type quoteKey struct{ unit, run string }
	quotes := map[quoteKey][]IncumbentQuote{}
	for quoteRows.Next() {
		var unit, run string
		var q IncumbentQuote
		if err := quoteRows.Scan(&unit, &run, &q.Quote, &q.SourceType, &q.SourceID); err != nil {
			return nil, fmt.Errorf("scan incumbent quote: %w", err)
		}
		quotes[quoteKey{unit, run}] = append(quotes[quoteKey{unit, run}], q)
	}
	if err := quoteRows.Err(); err != nil {
		return nil, err
	}
	for id := range out {
		for i := range out[id] {
			q := quotes[quoteKey{id, out[id][i].RunID}]
			sort.Slice(q, func(a, b int) bool {
				if q[a].SourceID != q[b].SourceID {
					return q[a].SourceID < q[b].SourceID
				}
				return q[a].Quote < q[b].Quote
			})
			out[id][i].Quotes = q
		}
	}
	return out, nil
}

// pickIncumbent chooses the row to compare against: the newest row whose input
// hash equals the exported bundle's and whose status is a real answer
// (ok/repaired); else the newest row with a matching hash; else the newest
// row, flagged InputHashMatch=false. Nil when the unit has no row.
func pickIncumbent(rows []incumbentRow, inputHash string) *Incumbent {
	if len(rows) == 0 {
		return nil
	}
	rank := func(r incumbentRow) int {
		switch {
		case r.InputHash == inputHash && (r.Status == "ok" || r.Status == "repaired"):
			return 2
		case r.InputHash == inputHash:
			return 1
		default:
			return 0
		}
	}
	best := rows[0]
	for _, r := range rows[1:] {
		if rank(r) > rank(best) || (rank(r) == rank(best) && r.ComputedAt.After(best.ComputedAt)) {
			best = r
		}
	}
	incumbent := &Incumbent{
		Status: best.Status, ModelVersion: best.ModelVersion, InputHash: best.InputHash,
		InputHashMatch: best.InputHash == inputHash, RunID: best.RunID, ComputedAt: best.ComputedAt.UTC(),
		Subcategories: best.Subcategories, Themes: best.Themes,
		EvidenceQuality: best.Quality, QualityBand: best.Band, ErrorsJSON: best.ErrorsJSON,
		Quotes: best.Quotes, RowsForUnit: len(rows),
	}
	if incumbent.Quotes == nil {
		incumbent.Quotes = []IncumbentQuote{}
	}
	top, topWeight := "", -1.0
	keys := make([]string, 0, len(best.Subcategories))
	for key := range best.Subcategories {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if best.Subcategories[key] > topWeight {
			top, topWeight = key, best.Subcategories[key]
		}
	}
	incumbent.TopSubcategory = top
	return incumbent
}

// TestExportBundlesLive is the explicit entry point. It skips unless the
// EXPORT_BUNDLES_* env vars are set, so it never runs in CI.
func TestExportBundlesLive(t *testing.T) {
	dsn := os.Getenv("EXPORT_BUNDLES_CH_DSN")
	org := os.Getenv("EXPORT_BUNDLES_ORG")
	out := os.Getenv("EXPORT_BUNDLES_OUT")
	if dsn == "" || org == "" || out == "" {
		t.Skip("EXPORT_BUNDLES_CH_DSN, EXPORT_BUNDLES_ORG and EXPORT_BUNDLES_OUT not all set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	cfg := clickhousestore.DefaultConfig(dsn)
	cfg.ReadTimeout = 10 * time.Minute
	connection, err := clickhousestore.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer func() { _ = connection.Close() }()

	file, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	defer func() { _ = file.Close() }()
	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)

	summary, err := ExportBundles(ctx, connection, ExportConfig{OrgID: org}, func(record BundleRecord) error {
		return encoder.Encode(record)
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	t.Logf("components=%d exported=%d skipped=%v", summary.Components, summary.Exported, summary.Skipped)
}
