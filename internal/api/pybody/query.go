package pybody

import "net/url"

// LastQueryValue is how FastAPI reads a scalar query parameter: the LAST of
// repeated values, nil when the name is absent. A present, empty value
// ("?limit=") is "" and is validated, never treated as absent.
func LastQueryValue(values url.Values, name string) *string {
	raw, present := values[name]
	if !present || len(raw) == 0 {
		return nil
	}
	last := raw[len(raw)-1]
	return &last
}
