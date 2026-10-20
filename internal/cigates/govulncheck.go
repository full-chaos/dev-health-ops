package cigates

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

// jsonStream decodes a concatenated JSON value stream into maps.
func jsonStream(r io.Reader) ([]map[string]any, error) {
	dec := json.NewDecoder(r)
	var out []map[string]any
	for {
		var v any
		if err := dec.Decode(&v); err == io.EOF {
			return out, nil
		} else if err != nil {
			return out, err
		}
		if m, ok := v.(map[string]any); ok {
			out = append(out, m)
		}
	}
}

func str(m map[string]any, key, def string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprint(v)
	}
	return def
}

func truthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case string:
		return t != ""
	case float64:
		return t != 0
	case []any:
		return len(t) > 0
	case map[string]any:
		return len(t) > 0
	}
	return true
}

// AuthGovulncheck gates `govulncheck -format json` output read from in. It returns the exit code.
func AuthGovulncheck(in io.Reader, stdout, stderr io.Writer) int {
	messages, err := jsonStream(in)
	if err != nil {
		fmt.Fprintf(stderr, "auth govulncheck: unreadable scan output: %v\n", err)
		return 1
	}
	sawConfig := false
	reachable := map[string]string{}
	for _, message := range messages {
		if _, ok := message["config"]; ok {
			sawConfig = true
		}
		finding, ok := message["finding"].(map[string]any)
		if !ok || len(finding) == 0 {
			continue
		}
		trace, _ := finding["trace"].([]any)
		if len(trace) == 0 {
			continue
		}
		first, _ := trace[0].(map[string]any)
		if !truthy(first["function"]) {
			continue
		}
		osv, ok := finding["osv"].(string)
		if !ok {
			fmt.Fprintln(stderr, "auth govulncheck: finding without an osv id")
			return 1
		}
		reachable[osv] = fmt.Sprintf("%s@%s fixed in %s", str(first, "module", "?"), str(first, "version", "?"), str(finding, "fixed_version", "?"))
	}
	if !sawConfig {
		fmt.Fprintln(stderr, "auth govulncheck: no scan configuration in the output; the scan did not run")
		return 1
	}
	ids := make([]string, 0, len(reachable))
	for id := range reachable {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		fmt.Fprintf(stderr, "auth govulncheck: reachable %s %s\n", id, reachable[id])
	}
	fmt.Fprintf(stdout, "auth govulncheck: %d reachable\n", len(reachable))
	if len(reachable) > 0 {
		return 1
	}
	return 0
}
