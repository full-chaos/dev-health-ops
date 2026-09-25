package pushcli

import (
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// dumps writes value as json.dumps does with ensure_ascii (its default),
// sort_keys as given, and either its default separators (", " and ": ") or, with
// indent, two-space indentation. Strings, numbers, booleans and null are
// written by pyjson (Python's float repr among them).
func dumps(value pyjson.Value, sortKeys, indent bool) (string, error) {
	var out strings.Builder
	if err := writeValue(&out, value, sortKeys, indent, 0); err != nil {
		return "", err
	}
	return out.String(), nil
}

func writeValue(out *strings.Builder, value pyjson.Value, sortKeys, indent bool, depth int) error {
	newline := func(level int) {
		if indent {
			out.WriteString("\n")
			out.WriteString(strings.Repeat("  ", level))
		}
	}
	switch typed := value.(type) {
	case *pyjson.Object:
		if typed.Len() == 0 {
			out.WriteString("{}")
			return nil
		}
		keys := typed.Keys()
		if sortKeys {
			sortStrings(keys)
		}
		out.WriteString("{")
		for index, key := range keys {
			if index > 0 {
				out.WriteString(",")
				if !indent {
					out.WriteString(" ")
				}
			}
			newline(depth + 1)
			text, err := pyjson.Dumps(key)
			if err != nil {
				return err
			}
			out.WriteString(text + ": ")
			item, _ := typed.Get(key)
			if err := writeValue(out, item, sortKeys, indent, depth+1); err != nil {
				return err
			}
		}
		newline(depth)
		out.WriteString("}")
	case []pyjson.Value:
		if len(typed) == 0 {
			out.WriteString("[]")
			return nil
		}
		out.WriteString("[")
		for index, item := range typed {
			if index > 0 {
				out.WriteString(",")
				if !indent {
					out.WriteString(" ")
				}
			}
			newline(depth + 1)
			if err := writeValue(out, item, sortKeys, indent, depth+1); err != nil {
				return err
			}
		}
		newline(depth)
		out.WriteString("]")
	default:
		text, err := pyjson.Dumps(value)
		if err != nil {
			return err
		}
		out.WriteString(text)
	}
	return nil
}

func sortStrings(keys []string) { sort.Strings(keys) }
