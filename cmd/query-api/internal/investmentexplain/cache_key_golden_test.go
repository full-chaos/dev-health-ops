package investmentexplain

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type cacheKeyGolden struct {
	Case  string `json:"case"`
	Input struct {
		Filters     taggedValue `json:"filters"`
		Theme       taggedValue `json:"theme"`
		Subcategory taggedValue `json:"subcategory"`
		OrgID       string      `json:"org_id"`
	} `json:"input"`
	CacheKey string `json:"cache_key"`
}

func TestComputeCacheKeyMatchesPythonGolden(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "cache_key__*.json"))
	if err != nil {
		t.Fatalf("glob goldens: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no cache_key__*.json goldens found -- run generate_cache_key_golden.py")
	}

	for _, path := range files {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			var golden cacheKeyGolden
			if err := json.Unmarshal(data, &golden); err != nil {
				t.Fatalf("decode golden: %v", err)
			}

			filters, err := golden.Input.Filters.decode()
			if err != nil {
				t.Fatalf("decode filters: %v", err)
			}
			theme, err := golden.Input.Theme.decode()
			if err != nil {
				t.Fatalf("decode theme: %v", err)
			}
			subcategory, err := golden.Input.Subcategory.decode()
			if err != nil {
				t.Fatalf("decode subcategory: %v", err)
			}

			got, err := ComputeCacheKey(CacheKeyInput{
				Filters:     filters,
				Theme:       anyToOptionalString(theme),
				Subcategory: anyToOptionalString(subcategory),
				OrgID:       golden.Input.OrgID,
			})
			if err != nil {
				t.Fatalf("ComputeCacheKey: %v", err)
			}
			if got != golden.CacheKey {
				t.Fatalf("case %q: want %s, got %s", golden.Case, golden.CacheKey, got)
			}
		})
	}
}

func anyToOptionalString(value any) *string {
	if value == nil {
		return nil
	}
	s, ok := value.(string)
	if !ok {
		return nil
	}
	return &s
}
