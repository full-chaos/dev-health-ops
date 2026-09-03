package investmentexplain

import "sort"

// topItems ports investment_mix_explain.py's _top_items (lines 112-121):
//
//	sorted([(k, float(v or 0.0)) for k, v in distribution.items() if v > 0],
//	       key=lambda item: item[1], reverse=True)[: max(1, limit)]
//
// Python's sorted(reverse=True) is a STABLE descending sort -- ties keep
// their ORIGINAL relative order (the order was never reversed, only the
// comparison direction), never reversed -- so sort.SliceStable with a
// plain ">" comparator is the exact match; items must already be in the
// same order Python's dict.items() would iterate (see keyValue's
// producers: orderedFloatMap.Items, parseDistributionOrdered).
func topItems(items []keyValue, limit int) []keyValue {
	filtered := make([]keyValue, 0, len(items))
	for _, kv := range items {
		if kv.Value > 0 {
			filtered = append(filtered, kv)
		}
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		return filtered[i].Value > filtered[j].Value
	})
	if limit < 1 {
		limit = 1
	}
	if len(filtered) > limit {
		filtered = filtered[:limit]
	}
	return filtered
}

// dominantSubcategory ports investment_mix_explain.py's
// _dominant_subcategory (lines 124-132): a streaming max over
// subcategories.items(), STRICT ">" so the FIRST-encountered key wins any
// tie (a later equal value never overwrites) -- items must be in JSON/
// dict insertion order (parseDistributionOrdered), not map iteration
// order. found is false when every value is <= 0 (Python returns None in
// that case, since best_key starts as None and only updates on a strict
// improvement over the 0.0 floor).
func dominantSubcategory(items []keyValue) (key string, found bool) {
	bestValue := 0.0
	for _, kv := range items {
		if kv.Value > bestValue {
			bestValue = kv.Value
			key = kv.Key
			found = true
		}
	}
	return key, found
}

// determineConfidenceLevel ports investment_mix_explain.py's
// _determine_confidence_level (lines 135-144).
func determineConfidenceLevel(qualityMean, qualityStddev *float64) string {
	if qualityMean == nil {
		return "unknown"
	}
	if *qualityMean >= 0.7 && (qualityStddev == nil || *qualityStddev < 0.15) {
		return "high"
	}
	if *qualityMean >= 0.5 {
		return "moderate"
	}
	return "low"
}
