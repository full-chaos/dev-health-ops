package routing

// Test-only views of the generator's internals, which the Python tests also reached by name.

// Raised runs f and returns the message of the Python-style error it raised ("" when it raised none).
func Raised(f func()) (msg string) {
	defer func() {
		if r := recover(); r != nil {
			e, ok := r.(pyRaise)
			if !ok {
				panic(r)
			}
			msg = e.msg
		}
	}()
	f()
	return ""
}

func LoadYAML(text []byte, composeTags bool) (any, error) { return loadYAML(text, composeTags) }

func PyStr(v any) string { return pyStr(v) }

func PyRepr(v any) string { return pyRepr(v) }

func Lookup(v any, keys ...any) any {
	for _, k := range keys {
		v = item(v, k)
	}
	return v
}

func Paths(doc any) ([]string, []string) { return pathsFromDoc(doc) }

func toPairs(es []entry) [][2]any {
	out := [][2]any{}
	for _, e := range es {
		out = append(out, [2]any{e.path, e.pathType})
	}
	return out
}

func fromPairs(ps [][2]any) []entry {
	out := []entry{}
	for _, p := range ps {
		out = append(out, entry{p[0], p[1]})
	}
	return out
}

func PythonAllow(doc any) [][2]any { return toPairs(pythonAllowListFromDoc(doc)) }

func EmitDynamic(goRegex, queryRegex string, allow [][2]any) string {
	return EmitDynamicConfig(goRegex, queryRegex, fromPairs(allow))
}

func TraefikPathRule(entries [][2]any) string { return traefikPathRule(fromPairs(entries)) }

func PathsOnTwoPlanes(doc any) []string { return pathsOnTwoPlanes(doc) }

func QueryAPIPathsThatDifferByHost(doc any) []string { return queryAPIPathsThatDifferByHost(doc) }

func PathsInTwoObjects(doc any) []string { return pathsInTwoObjects(doc) }

type NamedPairs struct {
	Name    string
	Entries [][2]any
}

func PythonSources(doc any) []NamedPairs {
	var out []NamedPairs
	for _, s := range pythonSources(doc) {
		out = append(out, NamedPairs{s.name, toPairs(s.entries)})
	}
	return out
}

func GoPlaneSegments(doc any) []Segments {
	var out []Segments
	for _, c := range goPlanePaths(doc) {
		out = append(out, c.segments)
	}
	return out
}

func SegmentsRegex(s Segments) string { return segmentsRegex(s) }

func PythonEntryOverlaps(path, pathType string, s Segments) bool {
	return pythonEntryOverlaps(path, pathType, s)
}

func QueryAPIEntrySegments(path, pathType string) Segments {
	return queryapiEntrySegments(path, pathType)
}

func QueryAPIEntryRegex(path, pathType string) string {
	return segmentsRegex(queryapiEntrySegments(path, pathType))
}

func PyEscape(s string) string { return pyEscape(s) }

func MapKeys(v any) []any {
	if mp, ok := v.(*pyMap); ok {
		return append([]any(nil), mp.keys...)
	}
	return nil
}
