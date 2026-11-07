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
	// Destructive marks a slot of a destructive row; the driver holds it to the frozen throwaway ids before it sends.
	// The slots file does not carry it.
	Destructive bool
}

// Gap reports whether s was not sent.
func (s Slot) Gap() bool { return strings.HasPrefix(s.Note, "gap:") }

// Key is the shadow row key of s.
func (s Slot) Key() string { return s.Method + " " + s.Route }

// Plan is the ordered slots of one run.
type Plan struct{ Slots []Slot }

var callerOrder = []string{policy.ShadowCallerMember, policy.ShadowCallerAdmin, policy.ShadowCallerSuperuser}

var placeholder = regexp.MustCompile(`\{([a-z_]+)(?::([A-Za-z0-9_-]+))?\}`)

// anyUUID finds every UUID in a text, wherever it stands.
var anyUUID = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

// PlanOptions: Throwaway is the row-table org label of the destructive rows, a pool label and never an org of the
// principals file. Pool names, with Destructive, the K principals-file orgs destructive slot k acts in (Pool[k-1]), and
// Frozen the throwaway ids (ParseThrowawayIDs) frozen before the run: every pool org and its principals must be in it,
// no other org or principal may be, and every UUID a destructive slot sends must be in it. OnlyRows, when set, names
// the rows the run sends (the controls always run); AllowExternal the outside services a request may reach; K the
// requests per cell; Destructive sends the destructive rows, which are gaps without it. States holds the minted
// single-use state of each slot of a row whose body names {state}.
type PlanOptions struct {
	Throwaway     string
	Pool          []string
	Frozen        ThrowawayIDs
	States        States
	OnlyRows      []string
	AllowExternal []string
	K             int
	Destructive   bool
}

// MakePlan orders rows into slots, K per cell: the controls first, then the rows, each in table order with the callers
// member, admin, superuser inside a row; then the destructive rows k-major: for each k, every destructive row in table
// order, in pool org Pool[k-1], so each pool org sees the destructive rows once, from its seeded state. A row with an
// outside service not allowed is a gap.
func MakePlan(rows []Row, v Venue, o PlanOptions) (Plan, error) {
	if o.K < 1 {
		return Plan{}, errors.New("plan: k must be at least 1")
	}
	if o.Throwaway == "" {
		return Plan{}, errors.New("plan: no throwaway label")
	}
	if _, clash := v.Orgs[o.Throwaway]; clash {
		return Plan{}, fmt.Errorf("plan: the throwaway label %q is an org of the principals file; it must be a pool label only", o.Throwaway)
	}
	only, err := onlyRows(rows, o.OnlyRows)
	if err != nil {
		return Plan{}, err
	}
	if o.Destructive {
		if err := v.checkPool(o); err != nil {
			return Plan{}, err
		}
	}
	var plain, destructive []Row
	for _, phase := range []func(Row) bool{
		func(r Row) bool { return r.Control() },
		func(r Row) bool { return !r.Control() && !r.Destructive },
	} {
		for _, row := range rows {
			if phase(row) && (row.Control() || only == nil || only[row.ID]) {
				plain = append(plain, row)
			}
		}
	}
	for _, row := range rows {
		if row.Destructive && (only == nil || only[row.ID]) {
			if err := throwawayOnly(row, o.Throwaway); err != nil {
				return Plan{}, err
			}
			destructive = append(destructive, row)
		}
	}
	var plan Plan
	add := func(s Slot) {
		s.Seq = len(plan.Slots) + 1
		plan.Slots = append(plan.Slots, s)
	}
	extGap := func(row Row) string {
		if row.Ext != none && !slices.Contains(o.AllowExternal, row.Ext) {
			return "gap: ext " + row.Ext + " not allowed (-allow-external " + row.Ext + ")"
		}
		return ""
	}
	for _, row := range plain {
		path, body, err := v.resolveRow(row, row.Org)
		if err != nil {
			return Plan{}, err
		}
		for _, cell := range v.cells(row, row.Org, true) {
			base := Slot{Row: row.ID, Caller: cell.caller, Principal: cell.principal, Expect: row.Expect, CrossOrg: cell.crossOrg,
				Method: row.Method, Route: row.Route, Path: path, Body: body}
			gap := cell.gap
			if gap == "" {
				gap = extGap(row)
			}
			if gap != "" {
				base.Note = gap
				add(base)
				continue
			}
			for i := 1; i <= o.K; i++ {
				slot := base
				slot.K = i
				if slot.Body, err = o.States.fill(body, row.ID, cell.caller, i); err != nil {
					return Plan{}, err
				}
				add(slot)
			}
		}
	}
	var live []Row
	for _, row := range destructive {
		gap := extGap(row)
		if !o.Destructive {
			gap = "gap: destructive rows off (-destructive)"
		}
		if gap == "" {
			live = append(live, row)
			continue
		}
		for _, caller := range callerOrder {
			add(Slot{Row: row.ID, Caller: caller, Principal: none, Expect: row.Expect, Method: row.Method, Route: row.Route,
				Path: row.Path, Body: row.Body, Note: gap})
		}
	}
	for k := 1; k <= o.K && len(live) > 0; k++ {
		org := o.Pool[k-1]
		for _, row := range live {
			path, body, err := v.resolveRow(row, org)
			if err != nil {
				return Plan{}, err
			}
			for _, cell := range v.cells(row, org, false) {
				if cell.gap != "" {
					return Plan{}, fmt.Errorf("plan: destructive row %s: %s", row.ID, strings.TrimPrefix(cell.gap, "gap: "))
				}
				add(Slot{Row: row.ID, Caller: cell.caller, Principal: cell.principal, Expect: row.Expect, K: k, Method: row.Method,
					Route: row.Route, Path: path, Body: body, Destructive: true})
			}
		}
	}
	return plan, nil
}

// onlyRows is the set of row ids the run sends, nil for every row. An id that is no row of the table, a control (the
// controls always run) or an id named twice is refused.
func onlyRows(rows []Row, ids []string) (map[string]bool, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	table := map[string]Row{}
	for _, row := range rows {
		table[row.ID] = row
	}
	out := map[string]bool{}
	for _, id := range ids {
		row, ok := table[id]
		switch {
		case !ok:
			return nil, fmt.Errorf("plan: -only-rows names %q, which is no row of the table", id)
		case row.Control():
			return nil, fmt.Errorf("plan: -only-rows names control %s; the controls always run", id)
		case out[id]:
			return nil, fmt.Errorf("plan: -only-rows names %s twice", id)
		}
		out[id] = true
	}
	return out, nil
}

// checkPool holds the pool to the frozen throwaway ids: K distinct pool labels, each an org of the principals file;
// every pool org and every principal of a pool org frozen; no other org and no other principal frozen, so a pool org
// can be no alias of a seeded org.
func (v Venue) checkPool(o PlanOptions) error {
	if len(o.Pool) != o.K {
		return fmt.Errorf("plan: the throwaway pool has %d orgs, want one per k (%d)", len(o.Pool), o.K)
	}
	pool := map[string]bool{}
	for _, label := range o.Pool {
		id, ok := v.Orgs[label]
		switch {
		case !ok:
			return fmt.Errorf("plan: pool org %q is not in the principals file", label)
		case pool[label]:
			return fmt.Errorf("plan: pool org %s twice", label)
		case !o.Frozen.ids[id]:
			return fmt.Errorf("plan: pool org %s (%s) is not in the frozen throwaway ids (-throwaway-ids)", label, id)
		}
		pool[label] = true
	}
	labels := make([]string, 0, len(v.Orgs))
	for label := range v.Orgs {
		labels = append(labels, label)
	}
	slices.Sort(labels)
	for _, label := range labels {
		if !pool[label] && o.Frozen.ids[v.Orgs[label]] {
			return fmt.Errorf("plan: org %s is not in the pool but its id is in the frozen throwaway ids", label)
		}
	}
	for _, p := range v.Principals {
		switch {
		case pool[p.Org] && !o.Frozen.ids[p.UUID]:
			return fmt.Errorf("plan: principal %s of pool org %s is not in the frozen throwaway ids", p.Name, p.Org)
		case !pool[p.Org] && o.Frozen.ids[p.UUID]:
			return fmt.Errorf("plan: principal %s is not of a pool org but its id is in the frozen throwaway ids", p.Name)
		}
	}
	return nil
}

type cell struct {
	caller, principal, gap string
	crossOrg               bool
}

// cells are the callers of row in org: the control's principal, or one principal per caller kind. With borrow, an
// org with no member borrows another org's member as a cross-org caller; a missing admin or superuser is a gap.
func (v Venue) cells(row Row, org string, borrow bool) []cell {
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
			if kind == policy.ShadowCallerSuperuser || p.Org == org {
				same = p
				break
			}
			if other == nil && borrow && kind == policy.ShadowCallerMember {
				other = p
			}
		}
		switch {
		case same != nil:
			out = append(out, cell{caller: kind, principal: same.Name})
		case other != nil:
			out = append(out, cell{caller: kind, principal: other.Name, crossOrg: true})
		default:
			out = append(out, cell{caller: kind, principal: none, gap: "gap: no " + kind + " principal for org " + org})
		}
	}
	return out
}

// resolveRow resolves row's path and body in org and holds them to the route and to JSON.
func (v Venue) resolveRow(row Row, org string) (path, body string, err error) {
	if _, ok := v.Orgs[org]; !ok {
		return "", "", fmt.Errorf("plan: row %s: org %q is not in the principals file", row.ID, org)
	}
	if path, err = v.resolve(row, org, row.Path); err != nil {
		return "", "", err
	}
	if strings.Contains(path, statePlaceholder) {
		return "", "", fmt.Errorf("plan: row %s: {state} may stand in the body only", row.ID)
	}
	if !pathFits(row.Route, path) {
		return "", "", fmt.Errorf("plan: row %s: path %q does not fit route %q", row.ID, path, row.Route)
	}
	body = none
	if row.Body != none {
		if body, err = v.resolve(row, org, row.Body); err != nil {
			return "", "", err
		}
		if !validJSON(body) {
			return "", "", fmt.Errorf("plan: row %s: the body is not JSON", row.ID)
		}
	}
	return path, body, nil
}

// resolve fills the placeholders of text: {org} (org), {org:<label>}, {owner} (the one owner of org),
// {user:<principal>}, {uuid:<seed kind>}, {name:<seed kind>} and {missing}; {state} stays for the slot (States.fill).
func (v Venue) resolve(row Row, org, text string) (string, error) {
	var failure error
	out := placeholder.ReplaceAllStringFunc(text, func(match string) string {
		m := placeholder.FindStringSubmatch(match)
		value, err := v.value(org, m[1], m[2])
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

func (v Venue) value(org, name, arg string) (string, error) {
	switch {
	case name == "org" && arg == "":
		return v.Orgs[org], nil
	case name == "org":
		if id, ok := v.Orgs[arg]; ok {
			return id, nil
		}
		return "", errors.New("no such org label")
	case name == "owner" && arg == "":
		var owners []string
		for _, p := range v.Principals {
			if p.Org == org && p.Role == "owner" {
				owners = append(owners, p.UUID)
			}
		}
		if len(owners) != 1 {
			return "", fmt.Errorf("org %s has %d owner principals, want 1", org, len(owners))
		}
		return owners[0], nil
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
	case name == "state" && arg == "":
		return statePlaceholder, nil
	}
	return "", errors.New("unknown placeholder")
}

// throwawayOnly refuses a destructive row that could act outside its pool org: its org must be the throwaway label,
// and its path and body may hold no literal UUID and no placeholder but {org}, {owner} and {missing}, which resolve
// in the slot's pool org (or to MissingID, which no row has).
func throwawayOnly(row Row, throwaway string) error {
	if row.Org != throwaway {
		return fmt.Errorf("plan: destructive row %s acts in org %s, not in the throwaway pool %s", row.ID, row.Org, throwaway)
	}
	for _, text := range []string{row.Path, row.Body} {
		if anyUUID.MatchString(text) {
			return fmt.Errorf("plan: destructive row %s holds a literal UUID; only {org}, {owner} and {missing} may name an id in the throwaway pool", row.ID)
		}
		for _, m := range placeholder.FindAllStringSubmatch(text, -1) {
			if !slices.Contains([]string{"{org}", "{owner}", "{missing}"}, m[0]) {
				return fmt.Errorf("plan: destructive row %s names %s; only {org}, {owner} and {missing} may name an id in the throwaway pool", row.ID, m[0])
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
