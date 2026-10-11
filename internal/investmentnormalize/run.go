package investmentnormalize

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
)

// Conn is the ClickHouse connection the script needs.
type Conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	Exec(ctx context.Context, query string, args ...any) error
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// ErrUnmappedKeys is returned by Apply when the window holds a legacy key the mapping table does not hold.
var ErrUnmappedKeys = errors.New("investmentnormalize: the window holds legacy keys the mapping table does not hold; nothing was written")

// Options select what to read. From is inclusive, To exclusive, both whole days (UTC).
type Options struct {
	Org   string
	From  time.Time
	To    time.Time
	RunID string
	Now   func() time.Time
}

// Report is what a run counted: counts only, never a title or an id of an item.
type Report struct {
	Items            int            // completed work items of the window with a legacy classification
	SkippedNoRepo    int            // no repository: not placeable (P3)
	SkippedCanonical int            // the repository-day already holds a categorized unit: the Jev row wins
	Unmatched        int            // product/general items no estimate rule matched: stay out (no data)
	Unmapped         map[string]int // legacy keys the table does not hold, by "area/stream"
	ByTheme          map[string]int // rows that would be (were) written, by theme
	BySubcategory    map[string]int
	ByRule           map[string]int // "map:<area>/<stream>" or the estimate rule name
	Rows             int
}

func newReport() Report {
	return Report{Unmapped: map[string]int{}, ByTheme: map[string]int{}, BySubcategory: map[string]int{}, ByRule: map[string]int{}}
}

// UnmappedTotal is the number of items with an unmapped key.
func (r Report) UnmappedTotal() int {
	n := 0
	for _, c := range r.Unmapped {
		n += c
	}
	return n
}

// Lines renders the report as the dry-run table (stable order).
func (r Report) Lines() []string {
	out := []string{
		fmt.Sprintf("items read: %d", r.Items),
		fmt.Sprintf("rows (normalized): %d", r.Rows),
		fmt.Sprintf("skipped, no repository: %d", r.SkippedNoRepo),
		fmt.Sprintf("skipped, repository-day already categorized: %d", r.SkippedCanonical),
		fmt.Sprintf("unmatched (product/general, no estimate rule): %d", r.Unmatched),
		fmt.Sprintf("unmapped keys (items): %d", r.UnmappedTotal()),
	}
	for _, section := range []struct {
		title string
		m     map[string]int
	}{{"by theme", r.ByTheme}, {"by subcategory", r.BySubcategory}, {"by rule", r.ByRule}, {"unmapped", r.Unmapped}} {
		keys := make([]string, 0, len(section.m))
		for k := range section.m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			out = append(out, fmt.Sprintf("%s: %s = %d", section.title, k, section.m[k]))
		}
	}
	return out
}

type sourceRow struct {
	repo        *uuid.UUID
	artifactID  string
	provider    string
	area        string
	stream      string
	legacyRule  string
	title       string
	description *string
	labels      []string
	createdAt   time.Time
	completedAt time.Time
	points      *float64
}

const itemsQuery = `
SELECT c.repo_id, c.artifact_id, c.provider, c.investment_area, c.project_stream, c.rule_id,
       w.title, w.description, w.labels, w.created_at, w.completed_at, w.story_points
FROM (
    SELECT repo_id, artifact_id, provider, day,
           argMax(investment_area, computed_at) AS investment_area,
           argMax(project_stream, computed_at) AS project_stream,
           argMax(rule_id, computed_at) AS rule_id
    FROM investment_classifications_daily
    WHERE org_id = {org:String} AND artifact_type = 'work_item'
      AND day >= toDate({from:String}) AND day < toDate({to:String})
    GROUP BY repo_id, artifact_id, provider, day
) AS c
INNER JOIN (
    SELECT repo_id, work_item_id,
           argMax(title, last_synced) AS title, argMax(description, last_synced) AS description,
           argMax(labels, last_synced) AS labels, argMax(created_at, last_synced) AS created_at,
           argMax(completed_at, last_synced) AS completed_at, argMax(story_points, last_synced) AS story_points
    FROM work_items WHERE org_id = {org:String}
    GROUP BY repo_id, work_item_id
) AS w ON w.repo_id = c.repo_id AND w.work_item_id = c.artifact_id
WHERE w.completed_at IS NOT NULL AND toDate(w.completed_at) = c.day
ORDER BY c.day, c.artifact_id
SETTINGS max_execution_time = 600`

const coverageQuery = `
SELECT DISTINCT repo_id, toDate(from_ts) AS d0, toDate(to_ts) AS d1
FROM work_unit_investments FINAL
WHERE org_id = {org:String} AND categorization_status != '` + Status + `'
  AND repo_id IS NOT NULL
  AND to_ts >= toDate({from:String}) AND from_ts < toDate({to:String})
SETTINGS max_execution_time = 600`

type dayRange struct{ from, to time.Time }

func args(o Options) []any {
	return []any{
		clickhouse.Named("org", o.Org),
		clickhouse.Named("from", o.From.Format("2006-01-02")),
		clickhouse.Named("to", o.To.Format("2006-01-02")),
	}
}

func readCoverage(ctx context.Context, conn Conn, o Options) (map[uuid.UUID][]dayRange, error) {
	rows, err := conn.Query(ctx, coverageQuery, args(o)...)
	if err != nil {
		return nil, fmt.Errorf("read categorized coverage: %w", err)
	}
	defer rows.Close()
	out := map[uuid.UUID][]dayRange{}
	for rows.Next() {
		var repo *uuid.UUID
		var d0, d1 time.Time
		if err := rows.Scan(&repo, &d0, &d1); err != nil {
			return nil, fmt.Errorf("scan categorized coverage: %w", err)
		}
		if repo != nil {
			out[*repo] = append(out[*repo], dayRange{from: d0, to: d1})
		}
	}
	return out, rows.Err()
}

func covered(cov map[uuid.UUID][]dayRange, repo uuid.UUID, day time.Time) bool {
	for _, r := range cov[repo] {
		if !day.Before(r.from) && !day.After(r.to) {
			return true
		}
	}
	return false
}

// Plan reads the window and decides every row. It writes nothing.
func Plan(ctx context.Context, conn Conn, o Options) ([]chwrite.InvestmentRecord, Report, error) {
	if strings.TrimSpace(o.Org) == "" {
		return nil, Report{}, errors.New("investmentnormalize: an organization id is required")
	}
	if !o.From.Before(o.To) {
		return nil, Report{}, errors.New("investmentnormalize: --from must be before --to")
	}
	now := time.Now
	if o.Now != nil {
		now = o.Now
	}
	cov, err := readCoverage(ctx, conn, o)
	if err != nil {
		return nil, Report{}, err
	}
	rows, err := conn.Query(ctx, itemsQuery, args(o)...)
	if err != nil {
		return nil, Report{}, fmt.Errorf("read legacy classifications: %w", err)
	}
	defer rows.Close()
	report := newReport()
	var records []chwrite.InvestmentRecord
	computedAt := now().UTC()
	for rows.Next() {
		var s sourceRow
		var completed *time.Time
		if err := rows.Scan(&s.repo, &s.artifactID, &s.provider, &s.area, &s.stream, &s.legacyRule,
			&s.title, &s.description, &s.labels, &s.createdAt, &completed, &s.points); err != nil {
			return nil, Report{}, fmt.Errorf("scan legacy classification: %w", err)
		}
		if completed == nil {
			continue
		}
		s.completedAt = completed.UTC()
		report.Items++
		if s.repo == nil {
			report.SkippedNoRepo++
			continue
		}
		day := time.Date(s.completedAt.Year(), s.completedAt.Month(), s.completedAt.Day(), 0, 0, 0, 0, time.UTC)
		if covered(cov, *s.repo, day) {
			report.SkippedCanonical++
			continue
		}
		var subcategory, ruleName string
		switch {
		case IsDefaultFallback(s.area, s.stream):
			sub, name, ok := Estimate(s.title, s.description, s.labels)
			if !ok {
				report.Unmatched++
				continue
			}
			subcategory, ruleName = sub, name
		default:
			sub, ok := MapSpecific(s.area, s.stream)
			if !ok {
				report.Unmapped[s.area+"/"+s.stream]++
				continue
			}
			subcategory, ruleName = sub, "map:"+s.area+"/"+s.stream
		}
		theme := Theme(subcategory)
		report.Rows++
		report.ByTheme[theme]++
		report.BySubcategory[subcategory]++
		report.ByRule[ruleName]++
		records = append(records, record(o, s, subcategory, theme, ruleName, computedAt))
	}
	if err := rows.Err(); err != nil {
		return nil, Report{}, fmt.Errorf("read legacy classifications: %w", err)
	}
	return records, report, nil
}

func record(o Options, s sourceRow, subcategory, theme, ruleName string, computedAt time.Time) chwrite.InvestmentRecord {
	units := 1.0
	if s.points != nil && *s.points != 0 && !math.IsNaN(*s.points) && !math.IsInf(*s.points, 0) {
		units = *s.points
	}
	units = math.Trunc(units) // the legacy delivery_units (workitemengine.ComputeInvestmentDaily)
	evidence, _ := json.Marshal(map[string]any{
		"issues": []string{}, "prs": []string{}, "commits": []string{}, "edges": []string{},
		"normalized_legacy": map[string]any{
			"source": "investment_classifications_daily", "investment_area": s.area, "project_stream": s.stream,
			"legacy_rule_id": s.legacyRule, "rule": ruleName, "mapping_version": MappingVersion,
		},
	})
	hash := sha256.Sum256([]byte(strings.Join([]string{o.Org, s.provider, s.artifactID, s.area, s.stream, s.title}, "\x00")))
	workUnitType, name, provider := "work_item", s.title, s.provider
	quotes := uint32(0)
	return chwrite.InvestmentRecord{
		WorkUnitID:   fmt.Sprintf("%s:%s:%s:%s", Status, o.Org, s.provider, s.artifactID),
		WorkUnitType: &workUnitType, WorkUnitName: &name,
		FromTS: s.createdAt.UTC(), ToTS: s.completedAt, RepoID: s.repo, Provider: &provider,
		EffortMetric: EffortMetric, EffortValue: units,
		ThemeDistribution: map[string]float64{theme: 1.0}, SubcategoryDistribution: map[string]float64{subcategory: 1.0},
		StructuralEvidenceJSON: string(evidence),
		EvidenceQuality:        QualityScore, EvidenceQualityBand: QualityBand,
		CategorizationStatus: Status, CategorizationErrorsJSON: "[]",
		CategorizationModelVersion: "normalized_legacy;mapping=" + MappingVersion + ";source=investment_classifications_daily",
		CategorizationInputHash:    hex.EncodeToString(hash[:]),
		CategorizationRunID:        o.RunID, ComputedAt: computedAt, EvidenceQuoteCount: &quotes,
	}
}

// Apply writes the planned rows in batches. It refuses a window that holds an unmapped key.
func Apply(ctx context.Context, conn Conn, o Options, records []chwrite.InvestmentRecord, report Report) error {
	if report.UnmappedTotal() > 0 {
		return ErrUnmappedKeys
	}
	writer, err := chwrite.NewWriter(conn)
	if err != nil {
		return err
	}
	const batch = 2000
	for start := 0; start < len(records); start += batch {
		end := start + batch
		if end > len(records) {
			end = len(records)
		}
		if _, err := writer.WriteInvestments(ctx, o.Org, records[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// RollbackCount counts the normalized rows an org holds (all, or one run).
func RollbackCount(ctx context.Context, conn Conn, org, runID string) (uint64, error) {
	query, bind := rollbackWhere(org, runID)
	rows, err := conn.Query(ctx, "SELECT count() FROM work_unit_investments FINAL WHERE "+query, bind...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var n uint64
	for rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return 0, err
		}
	}
	return n, rows.Err()
}

// Rollback deletes the normalized rows of an org (all, or one run): only rows with the provenance status.
func Rollback(ctx context.Context, conn Conn, org, runID string) error {
	query, bind := rollbackWhere(org, runID)
	return conn.Exec(ctx, "ALTER TABLE work_unit_investments DELETE WHERE "+query+" SETTINGS mutations_sync = 2", bind...)
}

func rollbackWhere(org, runID string) (string, []any) {
	query := "org_id = {org:String} AND categorization_status = '" + Status + "'"
	bind := []any{clickhouse.Named("org", org)}
	if runID != "" {
		query += " AND categorization_run_id = {run:String}"
		bind = append(bind, clickhouse.Named("run", runID))
	}
	return query, bind
}

func countRows(ctx context.Context, conn Conn, query string) (uint64, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var n uint64
	for rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return 0, err
		}
	}
	return n, rows.Err()
}
