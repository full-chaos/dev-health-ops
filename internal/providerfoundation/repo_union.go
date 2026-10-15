package providerfoundation

// UnionByKey is the union rule of a scoped GitHub namespace listing: primary
// keeps its order and its items, extra adds the items whose key primary does
// not hold. A key is a lowercased full_name; an item with an empty key cannot
// be compared and is kept. Scheduler discovery and the credential repo browse
// share this rule so they cannot disagree about what the union of a token's
// repositories and a namespace's repositories is.
func UnionByKey[T any](primary, extra []T, key func(T) string) []T {
	seen := make(map[string]struct{}, len(primary))
	for _, item := range primary {
		seen[key(item)] = struct{}{}
	}
	merged := primary
	for _, item := range extra {
		if k := key(item); k != "" {
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
		}
		merged = append(merged, item)
	}
	return merged
}
