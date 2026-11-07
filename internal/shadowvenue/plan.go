package shadowvenue

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

const principalsHeader = "kind\tname\temail\tuuid\torg\trole\tpassword_file"

// MissingID is the id placeholder {missing} sends: a valid UUID no venue row has, so the handler answers 404 after
// the guard.
const MissingID = "6f1e2d3c-0000-4000-8000-000000013148"

// Principal is a venue sign-in: Org is an org label, "-" for the superuser.
type Principal struct {
	Name, Email, UUID, Org, Role, PasswordFile string
}

// Caller is the shadow caller kind the legacy guard gives p (policy.ShadowCaller).
func (p Principal) Caller() string {
	return policy.ShadowCaller(strings.HasPrefix(p.Role, "platform_admin"), p.Role)
}

type seed struct{ Name, UUID, Org string }

// Venue is the principals file: the proof orgs by label, the principals, and the seed rows by kind.
type Venue struct {
	Orgs       map[string]string
	Principals []Principal
	seeds      map[string]seed
}

// Principal returns the principal called name.
func (v Venue) Principal(name string) (Principal, bool) {
	for _, p := range v.Principals {
		if p.Name == name {
			return p, true
		}
	}
	return Principal{}, false
}

// ParsePrincipals reads the venue principals file. It holds no secret: password_file is a path.
func ParsePrincipals(raw []byte) (Venue, error) {
	v := Venue{Orgs: map[string]string{}, seeds: map[string]seed{}}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) == 0 || lines[0] != principalsHeader {
		return Venue{}, fmt.Errorf("principals: the first line is not %q", principalsHeader)
	}
	names := map[string]bool{}
	for n, line := range lines[1:] {
		f := strings.Split(line, "\t")
		if len(f) != 7 {
			return Venue{}, fmt.Errorf("principals: line %d has %d columns, want 7", n+2, len(f))
		}
		kind, name, email, id, org, role, pw := f[0], f[1], f[2], f[3], f[4], f[5], f[6]
		switch kind {
		case "org":
			if _, dup := v.Orgs[org]; dup || org == none {
				return Venue{}, fmt.Errorf("principals: line %d: org label %q twice or empty", n+2, org)
			}
			if !isUUID(id) {
				return Venue{}, fmt.Errorf("principals: line %d: org %s has no uuid", n+2, org)
			}
			v.Orgs[org] = strings.ToLower(id)
		case "principal":
			if names[name] {
				return Venue{}, fmt.Errorf("principals: line %d: principal %s twice", n+2, name)
			}
			names[name] = true
			p := Principal{Name: name, Email: email, UUID: strings.ToLower(id), Org: org, Role: role, PasswordFile: pw}
			if !strings.HasPrefix(role, "platform_admin") && role != "member" && role != "admin" && role != "owner" {
				return Venue{}, fmt.Errorf("principals: line %d: role %q", n+2, role)
			}
			if !filepath.IsAbs(pw) || !isUUID(id) || email == none {
				return Venue{}, fmt.Errorf("principals: line %d: principal %s needs an email, a uuid and an absolute password file", n+2, name)
			}
			v.Principals = append(v.Principals, p)
		default:
			if _, dup := v.seeds[kind]; !dup {
				v.seeds[kind] = seed{Name: name, UUID: strings.ToLower(id), Org: org}
			}
		}
	}
	for _, p := range v.Principals {
		if _, ok := v.Orgs[p.Org]; !ok && p.Org != none {
			return Venue{}, fmt.Errorf("principals: principal %s names org %q, which the file does not list", p.Name, p.Org)
		}
		if p.Org == none && p.Caller() != policy.ShadowCallerSuperuser {
			return Venue{}, fmt.Errorf("principals: principal %s has no org and is no superuser", p.Name)
		}
	}
	return v, nil
}

// Slot is one request of a run, and after the run its timing and HTTP status. A gap slot (Note "gap: ...") is a cell
// the driver did not send.
type Slot struct {
	Seq                            int
	Row, Caller, Principal, Expect string
	K                              int
	CrossOrg                       bool
	Method, Route, Path, Body      string
	Start, End                     time.Time
	Status                         int
	Note                           string
	Destructive                    bool
}

// Gap reports whether s was not sent.
func (s Slot) Gap() bool { return strings.HasPrefix(s.Note, "gap:") }

// Key is the shadow row key of s.
func (s Slot) Key() string { return s.Method + " " + s.Route }

// Plan is the ordered slots of one run.
type Plan struct{ Slots []Slot }

var callerOrder = []string{policy.ShadowCallerMember, policy.ShadowCallerAdmin, policy.ShadowCallerSuperuser}

var placeholder = regexp.MustCompile(`\{([a-z_]+)(?::([A-Za-z0-9_-]+))?\}`)

// PlanOptions: Throwaway is the org label destructive rows act in; AllowExternal the outside services a request may
// reach; K the requests per cell; Destructive sends the destructive rows, which are gaps without it.
type PlanOptions struct {
	Pool          []string
	Frozen        ThrowawayIDs
	OnlyRows      []string
	Throwaway     string
	AllowExternal []string
	K             int
	Destructive   bool
}

// MakePlan orders rows into slots, K per cell: the controls first, then the rows, then the destructive rows, each in
// table order, with the callers member, admin, superuser inside a row. A destructive row must act in the throwaway
// org only, by its org and by every org or user it names; a row with an outside service not allowed is a gap.
func MakePlan(rows []Row, v Venue, o PlanOptions) (Plan, error) {
	throwaway, allowExternal, k := o.Throwaway, o.AllowExternal, o.K
	if _, ok := v.Orgs[throwaway]; !ok || throwaway == "" {
		return Plan{}, fmt.Errorf("plan: the throwaway org %q is not in the principals file", throwaway)
	}
	if k < 1 {
		return Plan{}, errors.New("plan: k must be at least 1")
	}
	var ordered []Row
	for _, phase := range []func(Row) bool{
		func(r Row) bool { return r.Control() },
		func(r Row) bool { return !r.Control() && !r.Destructive },
		func(r Row) bool { return r.Destructive },
	} {
		for _, row := range rows {
			if phase(row) {
				ordered = append(ordered, row)
			}
		}
	}
	var plan Plan
	for _, row := range ordered {
		if _, ok := v.Orgs[row.Org]; !ok {
			return Plan{}, fmt.Errorf("plan: row %s: org %q is not in the principals file", row.ID, row.Org)
		}
		if row.Destructive {
			if err := v.throwawayOnly(row, throwaway); err != nil {
				return Plan{}, err
			}
		}
		path, err := v.resolve(row, row.Path)
		if err != nil {
			return Plan{}, err
		}
		if !pathFits(row.Route, path) {
			return Plan{}, fmt.Errorf("plan: row %s: path %q does not fit route %q", row.ID, path, row.Route)
		}
		body := none
		if row.Body != none {
			if body, err = v.resolve(row, row.Body); err != nil {
				return Plan{}, err
			}
			if !validJSON(body) {
				return Plan{}, fmt.Errorf("plan: row %s: the body is not JSON", row.ID)
			}
		}
		for _, cell := range v.cells(row) {
			base := Slot{Row: row.ID, Caller: cell.caller, Principal: cell.principal, Expect: row.Expect, CrossOrg: cell.crossOrg,
				Method: row.Method, Route: row.Route, Path: path, Body: body}
			gap := cell.gap
			if gap == "" && row.Destructive && !o.Destructive {
				gap = "gap: destructive rows off (-destructive)"
			}
			if gap == "" && row.Ext != none && !slices.Contains(allowExternal, row.Ext) {
				gap = "gap: ext " + row.Ext + " not allowed (-allow-external " + row.Ext + ")"
			}
			if gap != "" {
				base.Seq, base.Note = len(plan.Slots)+1, gap
				plan.Slots = append(plan.Slots, base)
				continue
			}
			for i := 1; i <= k; i++ {
				slot := base
				slot.K, slot.Seq = i, len(plan.Slots)+1
				plan.Slots = append(plan.Slots, slot)
			}
		}
	}
	return plan, nil
}

type cell struct {
	caller, principal, gap string
	crossOrg               bool
}

// cells are the callers of row: the control's principal, or one principal per caller kind. A row whose org has no
// member borrows another org's member as a cross-org caller; a missing admin or superuser is a gap.
func (v Venue) cells(row Row) []cell {
	if row.Control() {
		p, _ := v.Principal(row.Principal)
		return []cell{{caller: p.Caller(), principal: p.Name}}
	}
	var out []cell
	for _, kind := range callerOrder {
		var same, other *Principal
		for i := range v.Principals {
			p := &v.Principals[i]
			if p.Caller() != kind {
				continue
			}
			if kind == policy.ShadowCallerSuperuser || p.Org == row.Org {
				same = p
				break
			}
			if other == nil && kind == policy.ShadowCallerMember {
				other = p
			}
		}
		switch {
		case same != nil:
			out = append(out, cell{caller: kind, principal: same.Name})
		case other != nil:
			out = append(out, cell{caller: kind, principal: other.Name, crossOrg: true})
		default:
			out = append(out, cell{caller: kind, principal: none, gap: "gap: no " + kind + " principal for org " + row.Org})
		}
	}
	return out
}

// resolve fills the placeholders of text: {org} (the row's org), {org:<label>}, {user:<principal>},
// {uuid:<seed kind>}, {name:<seed kind>} and {missing}.
func (v Venue) resolve(row Row, text string) (string, error) {
	var failure error
	out := placeholder.ReplaceAllStringFunc(text, func(match string) string {
		m := placeholder.FindStringSubmatch(match)
		value, err := v.value(row, m[1], m[2])
		if err != nil && failure == nil {
			failure = fmt.Errorf("plan: row %s: %s: %w", row.ID, match, err)
		}
		return value
	})
	if failure != nil {
		return "", failure
	}
	return out, nil
}

func (v Venue) value(row Row, name, arg string) (string, error) {
	switch {
	case name == "org" && arg == "":
		return v.Orgs[row.Org], nil
	case name == "org":
		if id, ok := v.Orgs[arg]; ok {
			return id, nil
		}
		return "", errors.New("no such org label")
	case name == "user":
		if p, ok := v.Principal(arg); ok {
			return p.UUID, nil
		}
		return "", errors.New("no such principal")
	case name == "uuid":
		if s, ok := v.seeds[arg]; ok && isUUID(s.UUID) {
			return s.UUID, nil
		}
		return "", errors.New("no seed of that kind with a uuid")
	case name == "name":
		if s, ok := v.seeds[arg]; ok && s.Name != "" && !strings.HasPrefix(s.Name, "MISSING") {
			return s.Name, nil
		}
		return "", errors.New("no seed of that kind with a name")
	case name == "missing" && arg == "":
		return MissingID, nil
	}
	return "", errors.New("unknown placeholder")
}

// throwawayOnly refuses a destructive row that acts outside the throwaway org: its own org, an {org:<label>} or a
// {user:<principal>} of another org.
func (v Venue) throwawayOnly(row Row, throwaway string) error {
	if row.Org != throwaway {
		return fmt.Errorf("plan: destructive row %s acts in org %s, not in the throwaway org %s", row.ID, row.Org, throwaway)
	}
	for _, text := range []string{row.Path, row.Body} {
		for _, m := range placeholder.FindAllStringSubmatch(text, -1) {
			switch m[1] {
			case "org":
				if m[2] != "" && m[2] != throwaway {
					return fmt.Errorf("plan: destructive row %s names org %s, not the throwaway org %s", row.ID, m[2], throwaway)
				}
			case "user":
				if p, ok := v.Principal(m[2]); !ok || p.Org != throwaway {
					return fmt.Errorf("plan: destructive row %s names user %s, who is not in the throwaway org %s", row.ID, m[2], throwaway)
				}
			case "uuid", "name":
				if s, ok := v.seeds[m[2]]; !ok || s.Org != throwaway {
					return fmt.Errorf("plan: destructive row %s names seed %s, which is not a seed of the throwaway org %s", row.ID, m[2], throwaway)
				}
			}
		}
	}
	return nil
}

// pathFits holds path to the shape of route: the same segment count, and every literal segment equal.
func pathFits(route, path string) bool {
	rs, ps := strings.Split(route, "/"), strings.Split(path, "/")
	if len(rs) != len(ps) {
		return false
	}
	for i := range rs {
		if strings.HasPrefix(rs[i], "{") && strings.HasSuffix(rs[i], "}") {
			if ps[i] == "" {
				return false
			}
			continue
		}
		if rs[i] != ps[i] {
			return false
		}
	}
	return true
}

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func isUUID(s string) bool { return uuidPattern.MatchString(s) }
