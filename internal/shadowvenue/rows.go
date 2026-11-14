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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"

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

// rowClass is a route's class: its outside service (ext) and whether it is destructive.
type rowClass struct {
	ext         string
	destructive bool
}

// embeddedRows is the embedded row table as the package was built, copied at init so no later change of DefaultRows
// can move a class.
var embeddedRows = append([]byte(nil), DefaultRows...)

// ClassTableSHA256 pins the class table, the one canonical source of each route's class (ADDENDUM-13267-1; acp): the
// sha256 of the sorted lines "METHOD /route<TAB>ext<TAB>destructive" of the embedded table's rows. A change of any
// row's class changes it, and classes refuses to serve a table that does not hash to it.
const ClassTableSHA256 = "99f0875b27fec8072f52489de730f58ef6855b2c1c8c3b7268478193d0b8e8ec"

// classDigest is the sha256 of a class table in the ClassTableSHA256 form.
func classDigest(table map[string]rowClass) string {
	lines := make([]string, 0, len(table))
	for key, c := range table {
		lines = append(lines, fmt.Sprintf("%s\t%s\t%v", key, c.ext, c.destructive))
	}
	slices.Sort(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n") + "\n"))
	return hex.EncodeToString(sum[:])
}

// classes are the classes of the embedded table's rows by route key: a custom row table may change a row's path or
// body, never its class (the row table is an input; the classes are code, pinned by ClassTableSHA256).
var classes = sync.OnceValue(func() map[string]rowClass {
	rows, err := ParseRows(embeddedRows)
	if err != nil {
		panic("shadowvenue: the embedded row table does not parse: " + err.Error())
	}
	out := map[string]rowClass{}
	for _, row := range rows {
		if !row.Control() {
			out[row.Key()] = rowClass{ext: row.Ext, destructive: row.Destructive}
		}
	}
	if got := classDigest(out); got != ClassTableSHA256 {
		panic("shadowvenue: the embedded class table hashes to " + got + ", not the pinned ClassTableSHA256")
	}
	return out
})

// classOf is the class of a route key, ok false for a route the embedded table does not hold.
func classOf(key string) (rowClass, bool) {
	c, ok := classes()[key]
	return c, ok
}

// CheckRows holds the rows to go-api's shadow rows in profiles (an endpoint-profiles.go-api.json), read by the
// shadow's own parser: every shadow row is in the table exactly once, the table has no other row, and every
// control is a shadow row. Each row keeps its route's class (classOf): its ext and destructive columns are the
// embedded table's, and a control names no outside service.
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
	if _, err := CheckRowClasses(rows); err != nil {
		problems = append(problems, err.Error())
	}
	if len(problems) > 0 {
		return fmt.Errorf("rows: the table differs from the profile shadow rows:\n  %s", strings.Join(problems, "\n  "))
	}
	return nil
}

// CheckRowClasses is the one class check of a row table, which plan, judge and judge-enforced all call first. Every row,
// data or control, must carry its route's pinned class (classOf: ext AND destructive), and a control must sit on a plain
// route (no outside service, not destructive). A route the embedded table does not hold has the zero class, which no
// parsed row has, so it is refused too. It returns each row id's class.
func CheckRowClasses(rows []Row) (map[string]rowClass, error) {
	out := map[string]rowClass{}
	var problems []string
	for _, row := range rows {
		c, _ := classOf(row.Key())
		got := rowClass{ext: row.Ext, destructive: row.Destructive}
		switch {
		case c != got:
			problems = append(problems, fmt.Sprintf("row %s: %s has the class ext %s, destructive %v; the table names ext %s, destructive %v",
				row.ID, row.Key(), c.ext, c.destructive, row.Ext, row.Destructive))
		case row.Control() && (c.ext != none || c.destructive):
			problems = append(problems, fmt.Sprintf("control %s: %s has the class ext %s, destructive %v; a control stays on a plain route",
				row.ID, row.Key(), c.ext, c.destructive))
		}
		out[row.ID] = c
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("class: the row table differs from the pinned route classes:\n  %s", strings.Join(problems, "\n  "))
	}
	return out, nil
}

// validJSON reports whether body is one JSON value.
func validJSON(body string) bool { return json.Valid([]byte(body)) }
