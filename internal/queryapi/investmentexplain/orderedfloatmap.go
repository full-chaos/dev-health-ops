package investmentexplain

// orderedFloatMap is a string->float64 map that preserves FIRST-INSERTION
// order, matching CPython dict semantics -- needed because
// _top_items (investment_mix_explain.py:112-121) is a STABLE sort
// (Python's sorted(..., reverse=True): descending by value, ties keep
// their ORIGINAL relative order, never reversed), so which theme/
// subcategory wins a tie depends on the insertion order of the dict
// `_top_items` was built from, not just its final key/value set. A plain
// Go map has no deterministic iteration order at all, so it cannot
// reproduce this even approximately.
type orderedFloatMap struct {
	keys   []string
	values map[string]float64
}

func newOrderedFloatMap() *orderedFloatMap {
	return &orderedFloatMap{values: map[string]float64{}}
}

// Add ports `d[key] = d.get(key, 0.0) + value`: first-sighting order is
// recorded once, on the FIRST add for a key; subsequent adds for the same
// key only accumulate the value.
func (m *orderedFloatMap) Add(key string, value float64) {
	if _, exists := m.values[key]; !exists {
		m.keys = append(m.keys, key)
	}
	m.values[key] += value
}

// Get reports (value, exists) for key, matching dict.get with an
// explicit presence flag rather than Python's `.get(key, 0.0)` default
// (callers that want the Python default should do `v, _ := m.Get(key)`).
func (m *orderedFloatMap) Get(key string) (float64, bool) {
	v, ok := m.values[key]
	return v, ok
}

// Items returns (key, value) pairs in insertion order, matching Python's
// dict.items() iteration order.
func (m *orderedFloatMap) Items() []keyValue {
	items := make([]keyValue, len(m.keys))
	for i, key := range m.keys {
		items[i] = keyValue{Key: key, Value: m.values[key]}
	}
	return items
}

// ToMap returns a plain map[string]float64 snapshot -- for callers (like
// parseFindingOptions.themeSharesPct) that only need value lookups, never
// order.
func (m *orderedFloatMap) ToMap() map[string]float64 {
	out := make(map[string]float64, len(m.values))
	for k, v := range m.values {
		out[k] = v
	}
	return out
}

type keyValue struct {
	Key   string
	Value float64
}

// keyValueGet ports a dict.get(key, 0.0) lookup over an ordered
// []keyValue -- linear, but these slices are always small (a work
// unit's own theme/subcategory distribution, at most a few dozen
// entries).
func keyValueGet(items []keyValue, key string) (float64, bool) {
	for _, kv := range items {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return 0, false
}
