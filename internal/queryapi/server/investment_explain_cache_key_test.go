package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

// A request that names the page's dateRange must still get a cache key, and the key is the
// sha256 of what Python's json.dumps(sort_keys=True) writes for the same value.
func TestExplainCacheKeyWithADateRangeEncodesLikePythonJSON(t *testing.T) {
	reader, err := investmentexplain.NewReader(unusedQueryClient{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, errs := decodeExplainBodyForTest(t, `{"date_range":{"start_date":"2026-09-30","end_date":"2026-10-14"},"filters":{"scope":{"level":"org","ids":[]}}}`)
	if len(errs) > 0 {
		t.Fatalf("errors: %+v", errs)
	}
	opts, err := buildExplainOptions(context.Background(), reader, "org-1", decoded, "auto", false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := investmentexplain.ComputeCacheKey(investmentexplain.CacheKeyInput{Filters: opts.FiltersForCacheKey, OrgID: "org-1"})
	if err != nil {
		t.Fatalf("cache key for a request with date_range: %v", err)
	}
	const python = `{"filters": {"date_range": ["2026-09-30", "2026-10-14"], "filters": {"scope": {"ids": [], "level": "org"}}}, "org_id": "org-1", "subcategory": null, "theme": null}`
	sum := sha256.Sum256([]byte(python))
	if want := hex.EncodeToString(sum[:])[:32]; got != want {
		t.Fatalf("cache key = %s, want %s (sha256 of Python json.dumps sort_keys)", got, want)
	}
}

// Every shape the route hands the cache key must be a tree the encoder accepts.
func TestExplainCacheKeyEncodesEveryRequestShape(t *testing.T) {
	reader, err := investmentexplain.NewReader(unusedQueryClient{})
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"empty body":                   `{}`,
		"empty filters":                `{"filters":{}}`,
		"date_range and no filters":    `{"date_range":{"start_date":"2026-09-30","end_date":"2026-10-14"}}`,
		"date_range, measure, filters": `{"measure":"COUNT","date_range":{"start_date":"2026-09-30","end_date":"2026-10-14"},"filters":{"scope":{"level":"team","ids":[]}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			decoded, errs := decodeExplainBodyForTest(t, body)
			if len(errs) > 0 {
				t.Fatalf("errors: %+v", errs)
			}
			opts, err := buildExplainOptions(context.Background(), reader, "org-1", decoded, "auto", false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := investmentexplain.ComputeCacheKey(investmentexplain.CacheKeyInput{Filters: opts.FiltersForCacheKey, OrgID: "org-1"}); err != nil {
				t.Fatalf("cache key: %v", err)
			}
		})
	}
}
