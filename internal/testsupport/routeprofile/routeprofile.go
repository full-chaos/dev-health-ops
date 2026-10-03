// Package routeprofile compares the (method, path) pairs the Go services serve with the
// endpoint-authentication-profile rows (contracts/auth/v1) in both directions
// (CHAOS-8305, S2 of CHAOS-7526). It is test support: the route set is passed in by the
// caller, who walks it from the production router constructors; nothing here lists a
// route by hand.
//
// A served route with no profile row fails, and a profile row with no served route
// fails, except for the two closed lists the caller loads: routes a wildcard handler
// serves by comparing a literal path value in code (a registered pattern cannot show
// them), and rows for routes no Go service serves, each with the ticket or decision
// that explains it. A list entry that no longer applies fails too.
package routeprofile

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// Pair is one (method, path) with every path parameter written {}.
type Pair struct {
	Method string
	Path   string
}

func (p Pair) String() string { return p.Method + " " + p.Path }

// Parameter matches one {name} path parameter.
var Parameter = regexp.MustCompile(`\{[^}/]*\}`)

// Normalize writes every {parameter} of a path as {}.
func Normalize(path string) string { return Parameter.ReplaceAllString(path, "{}") }

// Served is one route a Go service serves.
type Served struct {
	Plane string
	Pair
}

// Row is the part of an endpoint-profile row the comparison reads.
type Row struct {
	ID             string
	SurfaceKind    string
	Method         string
	Route          string
	Classification string
	Service        string
	// Classes are the row's accepted credential classes.
	Classes []string
	// Raw is the row exactly as it is in the file (canonical JSON), for byte pins.
	Raw json.RawMessage
}

// Pairs is the row's (method, path) pairs: a row written "GET,HEAD" is two pairs.
func (r Row) Pairs() []Pair {
	var pairs []Pair
	for _, method := range strings.Split(r.Method, ",") {
		pairs = append(pairs, Pair{strings.ToUpper(strings.TrimSpace(method)), Normalize(r.Route)})
	}
	return pairs
}

// LoadRows reads the rows of an endpoint-profiles file.
func LoadRows(path string) ([]Row, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // repo-relative contract file
	if err != nil {
		return nil, err
	}
	var document struct {
		Rows []json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	rows := make([]Row, 0, len(document.Rows))
	for index, item := range document.Rows {
		var row struct {
			ID             string   `json:"id"`
			SurfaceKind    string   `json:"surface_kind"`
			Method         string   `json:"method"`
			Route          string   `json:"route"`
			Classification string   `json:"classification"`
			Service        string   `json:"service"`
			Classes        []string `json:"accepted_credential_classes"`
		}
		if err := json.Unmarshal(item, &row); err != nil {
			return nil, fmt.Errorf("%s: row %d: %w", path, index, err)
		}
		rows = append(rows, Row{ID: row.ID, SurfaceKind: row.SurfaceKind, Method: row.Method, Route: row.Route,
			Classification: row.Classification, Service: row.Service, Classes: row.Classes, Raw: item})
	}
	return rows, nil
}

// Wildcard is a profile row's route that a Go wildcard handler serves by comparing a
// literal path value in code (internal/apiservice/admin/ipallowlist.go: entry_id ==
// "check"): the registered pattern is the wildcard, the literal is not in the route set.
type Wildcard struct {
	Method   string // the row's method
	Route    string // the row's route, parameters as {}
	Wildcard string // the registered pattern that serves it, parameters as {}
	Literal  string // the path value the handler compares
	Site     string // file:line of that comparison
	Proof    string // "unit" or "integration"
}

// Stub is a registration that only refuses: net/http would answer a different status or
// Allow header without it (HEAD /billing/plans/pull-stripe, whose 405 must name POST). It
// serves nothing, so it has no profile row; the gate runs Probe and requires Status.
type Stub struct {
	Method  string
	Pattern string // the registered pattern, parameters as {}
	Probe   string // a concrete path the registration answers
	Status  int    // the refusal's status
	Site    string // file:line of the registration
}

// PythonOnly is a profile row for a route no Go service serves, with what explains it.
type PythonOnly struct {
	Method string
	Route  string
	Ref    string // ticket or decision id
	Reason string
}

// Report is the outcome of a comparison.
type Report struct {
	Matched         int
	GoOnly          []Pair // served, no row
	ProfileOnly     []Pair // row, not served, not explained
	StaleWildcard   []string
	StalePythonOnly []string
	StaleStubs      []string
	Problems        []string
}

// OK says the comparison found nothing wrong.
func (r Report) OK() bool { return len(r.Problems) == 0 }

// Compare matches the served pairs with the profile pairs of rows.
//
// Automatic methods are one rule, not rows: a HEAD pair needs no row when the same
// path has a GET, and an OPTIONS pair needs none when the path has any other method
// (net/http answers them from the explicit route, they carry the GET/other route's
// class).
func Compare(served []Served, rows []Row, wildcards []Wildcard, pythonOnly []PythonOnly, stubs []Stub) Report {
	var report Report
	go_ := map[Pair]bool{}
	for _, route := range served {
		go_[route.Pair] = true
	}
	profile := map[Pair]bool{}
	for _, row := range rows {
		if row.SurfaceKind != "rest" {
			continue
		}
		for _, pair := range row.Pairs() {
			profile[pair] = true
		}
	}
	wildcardServes := map[Pair]Wildcard{}
	usedWildcard := map[Pair]bool{}
	for _, entry := range wildcards {
		key := Pair{strings.ToUpper(entry.Method), Normalize(entry.Route)}
		wildcardServes[key] = entry
	}
	pythonOnlyKeys := map[Pair]PythonOnly{}
	usedPythonOnly := map[Pair]bool{}
	for _, entry := range pythonOnly {
		pythonOnlyKeys[Pair{strings.ToUpper(entry.Method), Normalize(entry.Route)}] = entry
	}
	stubKeys := map[Pair]Stub{}
	for _, stub := range stubs {
		stubKeys[Pair{strings.ToUpper(stub.Method), Normalize(stub.Pattern)}] = stub
	}
	hasOtherMethod := func(path string, except string) bool {
		for pair := range go_ {
			if pair.Path == path && pair.Method != except {
				return true
			}
		}
		return false
	}
	// Direction 1: every served pair has a row, a wildcard cover, or is an automatic method.
	for _, route := range served {
		pair := route.Pair
		if profile[pair] {
			continue
		}
		if _, ok := stubKeys[pair]; ok {
			continue
		}
		if pair.Method == "HEAD" && go_[Pair{"GET", pair.Path}] {
			continue
		}
		if pair.Method == "OPTIONS" && hasOtherMethod(pair.Path, "OPTIONS") {
			continue
		}
		covered := false
		for key, entry := range wildcardServes {
			if key.Method == pair.Method && Normalize(entry.Wildcard) == pair.Path {
				covered = true
			}
		}
		if covered {
			continue
		}
		report.GoOnly = append(report.GoOnly, pair)
	}
	// Direction 2: every profile pair is served, wildcard-served, or explained.
	for pair := range profile {
		if go_[pair] {
			report.Matched++
			continue
		}
		if entry, ok := wildcardServes[pair]; ok {
			if go_[Pair{pair.Method, Normalize(entry.Wildcard)}] {
				usedWildcard[pair] = true
				continue
			}
		}
		if _, ok := pythonOnlyKeys[pair]; ok {
			usedPythonOnly[pair] = true
			continue
		}
		report.ProfileOnly = append(report.ProfileOnly, pair)
	}
	for key, entry := range wildcardServes {
		switch {
		case !profile[key]:
			report.StaleWildcard = append(report.StaleWildcard, fmt.Sprintf("%s has no profile row (%s)", key, entry.Site))
		case go_[key]:
			report.StaleWildcard = append(report.StaleWildcard, fmt.Sprintf("%s is a registered route now: drop its wildcard entry (%s)", key, entry.Site))
		case !go_[Pair{key.Method, Normalize(entry.Wildcard)}]:
			report.StaleWildcard = append(report.StaleWildcard, fmt.Sprintf("%s names wildcard %s, which no Go service registers (%s)", key, entry.Wildcard, entry.Site))
		}
	}
	for key, entry := range pythonOnlyKeys {
		switch {
		case !profile[key]:
			report.StalePythonOnly = append(report.StalePythonOnly, fmt.Sprintf("%s has no profile row (%s)", key, entry.Ref))
		case go_[key]:
			report.StalePythonOnly = append(report.StalePythonOnly, fmt.Sprintf("%s is served by Go now: drop its entry (%s)", key, entry.Ref))
		case entry.Ref == "":
			report.StalePythonOnly = append(report.StalePythonOnly, fmt.Sprintf("%s has no ticket or decision id", key))
		}
	}
	for key, stub := range stubKeys {
		switch {
		case !go_[key]:
			report.StaleStubs = append(report.StaleStubs, fmt.Sprintf("%s is not a registered route any more (%s)", key, stub.Site))
		case profile[key]:
			report.StaleStubs = append(report.StaleStubs, fmt.Sprintf("%s has a profile row: drop its stub entry (%s)", key, stub.Site))
		}
	}
	sortPairs(report.GoOnly)
	sort.Strings(report.StaleStubs)
	sortPairs(report.ProfileOnly)
	sort.Strings(report.StaleWildcard)
	sort.Strings(report.StalePythonOnly)
	for _, pair := range report.GoOnly {
		report.Problems = append(report.Problems, fmt.Sprintf("served by Go with NO profile row: %s", pair))
	}
	for _, pair := range report.ProfileOnly {
		report.Problems = append(report.Problems, fmt.Sprintf("profile row with NO served route and no closed-list entry: %s", pair))
	}
	report.Problems = append(report.Problems, report.StaleWildcard...)
	report.Problems = append(report.Problems, report.StalePythonOnly...)
	report.Problems = append(report.Problems, report.StaleStubs...)
	return report
}

func sortPairs(pairs []Pair) {
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].Path != pairs[j].Path {
			return pairs[i].Path < pairs[j].Path
		}
		return pairs[i].Method < pairs[j].Method
	})
}

// tsvRows reads a tab-separated closed list: '#' comment lines and blank lines are skipped,
// every other line must have exactly want columns.
func tsvRows(path string, want int) ([][]string, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // repo-relative contract file
	if err != nil {
		return nil, err
	}
	var rows [][]string
	for number, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		columns := strings.Split(line, "\t")
		if len(columns) != want {
			return nil, fmt.Errorf("%s:%d: %d columns, want %d", path, number+1, len(columns), want)
		}
		for index := range columns {
			columns[index] = strings.TrimSpace(columns[index])
			if columns[index] == "" {
				return nil, fmt.Errorf("%s:%d: column %d is empty", path, number+1, index+1)
			}
		}
		rows = append(rows, columns)
	}
	return rows, nil
}

// LoadWildcards reads ci/go_wildcard_dispatch.tsv: method, row route, wildcard pattern,
// literal, site (file:line), proof.
func LoadWildcards(path string) ([]Wildcard, error) {
	rows, err := tsvRows(path, 6)
	if err != nil {
		return nil, err
	}
	out := make([]Wildcard, 0, len(rows))
	for _, row := range rows {
		if row[5] != "unit" && row[5] != "integration" {
			return nil, fmt.Errorf("%s: proof %q is neither unit nor integration", path, row[5])
		}
		out = append(out, Wildcard{Method: row[0], Route: row[1], Wildcard: row[2], Literal: row[3], Site: row[4], Proof: row[5]})
	}
	return out, nil
}

// LoadPythonOnly reads ci/endpoint_profiles_python_only.tsv: method, route, ticket or
// decision id, reason.
func LoadPythonOnly(path string) ([]PythonOnly, error) {
	rows, err := tsvRows(path, 4)
	if err != nil {
		return nil, err
	}
	out := make([]PythonOnly, 0, len(rows))
	for _, row := range rows {
		out = append(out, PythonOnly{Method: row[0], Route: row[1], Ref: row[2], Reason: row[3]})
	}
	return out, nil
}

// LoadStubs reads ci/go_refusal_stubs.tsv: method, registered pattern, probe path, status,
// file:line.
func LoadStubs(path string) ([]Stub, error) {
	rows, err := tsvRows(path, 5)
	if err != nil {
		return nil, err
	}
	out := make([]Stub, 0, len(rows))
	for _, row := range rows {
		status := 0
		if _, err := fmt.Sscanf(row[3], "%d", &status); err != nil || status < 100 || status > 599 {
			return nil, fmt.Errorf("%s: %q is not an HTTP status", path, row[3])
		}
		out = append(out, Stub{Method: row[0], Pattern: row[1], Probe: row[2], Status: status, Site: row[4]})
	}
	return out, nil
}

// ClassException records a profile row whose class the Go code does NOT enforce as the
// row says, found by the executed class probe.
type ClassException struct {
	Method   string
	Route    string
	Profile  string // the row's classification
	Status   int    // the status the unauthenticated probe answers today
	Ref      string // ticket
	Evidence string
}

// LoadClassExceptions reads ci/endpoint_profile_class_exceptions.tsv: method, route, the
// row's class, the probe status, ticket, evidence.
func LoadClassExceptions(path string) ([]ClassException, error) {
	rows, err := tsvRows(path, 6)
	if err != nil {
		return nil, err
	}
	out := make([]ClassException, 0, len(rows))
	for _, row := range rows {
		status := 0
		if _, err := fmt.Sscanf(row[3], "%d", &status); err != nil {
			return nil, fmt.Errorf("%s: %q is not an HTTP status", path, row[3])
		}
		out = append(out, ClassException{Method: row[0], Route: row[1], Profile: row[2], Status: status, Ref: row[4], Evidence: row[5]})
	}
	return out, nil
}

// Canonical is the row's canonical JSON (keys sorted, no insignificant space): the form
// the byte pins hash.
func Canonical(raw json.RawMessage) ([]byte, error) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}
