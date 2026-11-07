// Package shadowvenue drives go-api's shadow decision point on the bigboy compose venue, one row and caller at a
// time, and judges each row from go-api's own log: the per-request "api shadow pdp: result" line and the mismatch
// WARN line. The output is a per-row match table, the venue path of the per-row bar for the members, org, settings,
// billing, admin, integrations and teams flips.
//
// The driver sends to a loopback origin only, reads passwords from owner-only files by path, and keeps tokens in
// memory. Destructive rows run last, in the throwaway organization only. Two control cells must mismatch, or the
// run is void: they prove the readout can see a mismatch.
package shadowvenue

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/accessrouting"
)

const rowsHeader = "row\tmethod\troute\torg\tdestructive\text\tprincipal\texpect\tpath\tbody"

const (
	expectMatch    = "match"
	expectMismatch = "mismatch"
	none           = "-"
)

var (
	rowIDPattern = regexp.MustCompile(`^(C?[1-9][0-9]*)$`)
	extPattern   = regexp.MustCompile(`^(-|[a-z][a-z0-9]*)$`)
	methods      = []string{"GET", "POST", "PUT", "PATCH", "DELETE"}
)

// Row is one line of the row table: a go-api shadow row, or a control cell (ID C<n>) of one named principal that
// must mismatch.
type Row struct {
	ID, Method, Route, Org string
	Destructive            bool
	// Ext names the outside service a legacy-allowed request can reach, or "-".
	Ext string
	// Principal is the control cell's principal; "-" on a row, which runs every caller kind.
	Principal string
	Expect    string
	// Path and Body hold placeholders (see resolve); Body "-" sends no body.
	Path, Body string
}

// Key is the shadow row key "METHOD /route".
func (r Row) Key() string { return r.Method + " " + r.Route }

// Control reports whether r is a control cell.
func (r Row) Control() bool { return strings.HasPrefix(r.ID, "C") }

// ParseRows reads a row table; comment lines start with '#'.
func ParseRows(raw []byte) ([]Row, error) {
	var rows []Row
	header := false
	seen := map[string]bool{}
	for n, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !header {
			if line != rowsHeader {
				return nil, fmt.Errorf("rows: line %d is not the header %q", n+1, rowsHeader)
			}
			header = true
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 10 {
			return nil, fmt.Errorf("rows: line %d has %d columns, want 10", n+1, len(f))
		}
		row := Row{ID: f[0], Method: f[1], Route: f[2], Org: f[3], Ext: f[5], Principal: f[6], Expect: f[7], Path: f[8], Body: f[9]}
		if err := row.check(f[4]); err != nil {
			return nil, fmt.Errorf("rows: line %d: %w", n+1, err)
		}
		row.Destructive = f[4] == "yes"
		if seen[row.ID] {
			return nil, fmt.Errorf("rows: line %d: row %s twice", n+1, row.ID)
		}
		seen[row.ID] = true
		rows = append(rows, row)
	}
	if !header || len(rows) == 0 {
		return nil, errors.New("rows: no header or no rows")
	}
	return rows, nil
}

func (r Row) check(destructive string) error {
	switch {
	case !rowIDPattern.MatchString(r.ID):
		return fmt.Errorf("row id %q is not <n> or C<n>", r.ID)
	case !slices.Contains(methods, r.Method):
		return fmt.Errorf("row %s: method %q", r.ID, r.Method)
	case !strings.HasPrefix(r.Route, "/api/v1/"):
		return fmt.Errorf("row %s: route %q is not under /api/v1/", r.ID, r.Route)
	case r.Org == "":
		return fmt.Errorf("row %s: no org", r.ID)
	case destructive != "yes" && destructive != "no":
		return fmt.Errorf("row %s: destructive %q is not yes or no", r.ID, destructive)
	case !extPattern.MatchString(r.Ext):
		return fmt.Errorf("row %s: ext %q", r.ID, r.Ext)
	case !strings.HasPrefix(r.Path, "/"):
		return fmt.Errorf("row %s: path %q is not absolute", r.ID, r.Path)
	case r.Body == "":
		return fmt.Errorf("row %s: empty body column (use -)", r.ID)
	}
	if r.Control() {
		switch {
		case r.Expect != expectMismatch:
			return fmt.Errorf("control %s must expect mismatch", r.ID)
		case r.Principal == none || r.Principal == "":
			return fmt.Errorf("control %s names no principal", r.ID)
		case destructive != "no":
			return fmt.Errorf("control %s is destructive", r.ID)
		}
		return nil
	}
	if r.Expect != expectMatch {
		return fmt.Errorf("row %s must expect match", r.ID)
	}
	if r.Principal != none {
		return fmt.Errorf("row %s names a principal; rows run every caller kind", r.ID)
	}
	return nil
}

// CheckRows holds the rows to go-api's shadow rows in profiles (an endpoint-profiles.go-api.json), read by the
// shadow's own parser: every shadow row is in the table exactly once, the table has no other row, and every
// control is a shadow row.
func CheckRows(rows []Row, profiles []byte) error {
	shadowRows, err := policy.ParseShadowRows(profiles, accessrouting.Service)
	if err != nil {
		return err
	}
	have := map[string]string{}
	var problems []string
	for _, row := range rows {
		if _, ok := shadowRows[row.Key()]; !ok {
			if row.Control() {
				problems = append(problems, fmt.Sprintf("control %s: %s is no shadow row", row.ID, row.Key()))
			} else {
				problems = append(problems, fmt.Sprintf("row %s: %s is no shadow row", row.ID, row.Key()))
			}
			continue
		}
		if row.Control() {
			continue
		}
		if first, dup := have[row.Key()]; dup {
			problems = append(problems, fmt.Sprintf("%s is in the table twice (rows %s and %s)", row.Key(), first, row.ID))
			continue
		}
		have[row.Key()] = row.ID
	}
	var keys []string
	for key := range shadowRows {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if _, ok := have[key]; !ok {
			problems = append(problems, fmt.Sprintf("shadow row %s is not in the table", key))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("rows: the table differs from the profile shadow rows:\n  %s", strings.Join(problems, "\n  "))
	}
	return nil
}

// validJSON reports whether body is one JSON value.
func validJSON(body string) bool { return json.Valid([]byte(body)) }
