package cigates

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// VulnException is one row of ci/auth-vuln-exceptions.tsv. It excepts exactly
// one advisory in exactly one module at exactly one version. It has no date:
// it ends when the advisory gets a fixed version.
type VulnException struct {
	OSV, Module, Version, Ticket, Reason string
}

// ReadVulnExceptions parses the exceptions file. Blank lines and lines that
// start with # are skipped. Any other row must have exactly five non-empty
// tab-separated columns (osv, module, version, ticket, reason) and a unique
// osv+module+version key; anything else is an error, so a bad row fails closed.
func ReadVulnExceptions(r io.Reader) ([]VulnException, error) {
	var out []VulnException
	seen := map[string]bool{}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimRight(sc.Text(), "\r")
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cols := strings.Split(line, "\t")
		if len(cols) != 5 {
			return nil, fmt.Errorf("line %d: want 5 tab-separated columns (osv, module, version, ticket, reason), got %d", n, len(cols))
		}
		for i, c := range cols {
			if strings.TrimSpace(c) == "" || c != strings.TrimSpace(c) {
				return nil, fmt.Errorf("line %d: column %d is empty or has surrounding space", n, i+1)
			}
		}
		e := VulnException{cols[0], cols[1], cols[2], cols[3], cols[4]}
		if !strings.HasPrefix(e.OSV, "GO-") {
			return nil, fmt.Errorf("line %d: %q is not a GO- advisory id", n, e.OSV)
		}
		key := e.OSV + " " + e.Module + " " + e.Version
		if seen[key] {
			return nil, fmt.Errorf("line %d: duplicate row for %s", n, key)
		}
		seen[key] = true
		out = append(out, e)
	}
	return out, sc.Err()
}

type vulnFinding struct{ module, version, fixed string }

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
// exceptions may be nil. A reachable finding passes only when a row matches its
// advisory id, module and version exactly AND the finding has no fixed version.
func AuthGovulncheck(in io.Reader, exceptions []VulnException, stdout, stderr io.Writer) int {
	messages, err := jsonStream(in)
	if err != nil {
		fmt.Fprintf(stderr, "auth govulncheck: unreadable scan output: %v\n", err)
		return 1
	}
	sawConfig := false
	reachable := map[string]vulnFinding{}
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
		reachable[osv] = vulnFinding{str(first, "module", "?"), str(first, "version", "?"), str(finding, "fixed_version", "?")}
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
	failing, excepted := 0, 0
	for _, id := range ids {
		f := reachable[id]
		var row *VulnException
		for i := range exceptions {
			if exceptions[i].OSV == id && exceptions[i].Module == f.module && exceptions[i].Version == f.version {
				row = &exceptions[i]
				break
			}
		}
		switch {
		case row == nil:
			failing++
			fmt.Fprintf(stderr, "auth govulncheck: reachable %s %s@%s fixed in %s\n", id, f.module, f.version, f.fixed)
		case f.fixed != "?" && f.fixed != "":
			failing++
			fmt.Fprintf(stderr, "auth govulncheck: reachable %s %s@%s fixed in %s: the exception (%s) ends now that a fixed version exists; remove its row from ci/auth-vuln-exceptions.tsv and bump\n", id, f.module, f.version, f.fixed, row.Ticket)
		default:
			excepted++
			fmt.Fprintf(stderr, "auth govulncheck: WARN excepted %s %s@%s (%s): %s\n", id, f.module, f.version, row.Ticket, row.Reason)
		}
	}
	fmt.Fprintf(stdout, "auth govulncheck: %d reachable\n", failing)
	if excepted > 0 {
		fmt.Fprintf(stdout, "auth govulncheck: %d excepted\n", excepted)
	}
	if failing > 0 {
		return 1
	}
	return 0
}
