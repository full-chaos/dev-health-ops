package investmentexplain

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// CacheKeyInput bundles _compute_cache_key's parameters
// (investment_mix_explain.py:147-148): Filters is whatever
// filters.model_dump(mode="json") would have produced -- a JSON-safe
// value tree (nil, bool, string, int, int64, float64, []any,
// map[string]any), built by the caller from the resolved MetricFilter
// rather than re-derived here, since this port has no MetricFilter type
// of its own.
type CacheKeyInput struct {
	Filters     any
	Theme       *string
	Subcategory *string
	OrgID       string
}

// ComputeCacheKey ports _compute_cache_key (investment_mix_explain.py:
// 147-174) exactly:
//
//	key_parts = {"filters": filter_data, "theme": theme,
//	             "subcategory": subcategory, "org_id": org_id}
//	key_json = json.dumps(key_parts, sort_keys=True, default=str)
//	return hashlib.sha256(key_json.encode()).hexdigest()[:32]
//
// org_id is part of the key so two tenants with identical filters/theme/
// subcategory never collide on the same SHA256 and read each other's
// cached explanation (CHAOS-2393); ReadInvestmentExplanation's own
// org_id predicate is defence in depth on top of that, matching
// read_investment_explanation's own doc comment.
func ComputeCacheKey(input CacheKeyInput) (string, error) {
	keyParts := map[string]any{
		"filters":     input.Filters,
		"theme":       optionalStringToAny(input.Theme),
		"subcategory": optionalStringToAny(input.Subcategory),
		"org_id":      input.OrgID,
	}
	keyJSON, err := pythonparity.MarshalPythonJSONSorted(keyParts)
	if err != nil {
		return "", fmt.Errorf("compute cache key: %w", err)
	}
	sum := sha256.Sum256(keyJSON)
	return hex.EncodeToString(sum[:])[:32], nil
}

func optionalStringToAny(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

// InvestmentExplanationRecord ports metrics/schemas.py's
// InvestmentExplanationRecord.
type InvestmentExplanationRecord struct {
	CacheKey        string
	ExplanationJSON string
	LLMProvider     string
	LLMModel        *string
	ComputedAt      time.Time
	OrgID           string
}

// ReadInvestmentExplanation ports
// ClickHouseMetricsSink.read_investment_explanation
// (metrics/sinks/clickhouse/investment.py:340-383): a cache lookup by
// cache_key, org_id-scoped, using FINAL to resolve the
// ReplacingMergeTree(computed_at) version.
//
// See investmentexplain's package doc / CHAOS-4977 status notes: this
// service's only ClickHouse client (analytics.QueryClient, dev-health-go's
// Client) exposes Query only -- no Insert/Exec -- so this port covers the
// READ side. The WRITE side (write_investment_explanation) needs either a
// write-capable client this service doesn't currently have, or a
// different mechanism entirely; flagged to team-lead rather than guessed
// at, since query-api has been read-only throughout (grep confirms zero
// existing INSERT/write call sites anywhere in cmd/query-api).
func (reader *Reader) ReadInvestmentExplanation(ctx context.Context, cacheKey, orgID string) (*InvestmentExplanationRecord, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}

	query := fmt.Sprintf(`
SELECT
    cache_key,
    explanation_json,
    llm_provider,
    llm_model,
    computed_at,
    org_id
FROM investment_explanations FINAL
WHERE cache_key = {cache_key:String} AND org_id = {org_id:String}
LIMIT 1
%s
`, settingsMaxExecutionTime())

	bindings := []dhclickhouse.Binding{
		{Name: "cache_key", Value: cacheKey},
		{Name: "org_id", Value: orgID},
	}

	rows, err := reader.client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("query investment explanation cache: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate investment explanation cache rows: %w", err)
		}
		return nil, nil
	}

	var record InvestmentExplanationRecord
	if err := rows.Scan(
		&record.CacheKey, &record.ExplanationJSON, &record.LLMProvider,
		&record.LLMModel, &record.ComputedAt, &record.OrgID,
	); err != nil {
		return nil, fmt.Errorf("scan investment explanation cache row: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate investment explanation cache rows: %w", err)
	}
	return &record, nil
}
