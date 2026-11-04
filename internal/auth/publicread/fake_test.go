package publicread

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fOrg struct {
	ID               uuid.UUID
	Slug, Name       string
	Active           bool
	Created, Updated time.Time
}

type fMem struct {
	ID, OrgID, UserID uuid.UUID
	Role              string
	Joined            *time.Time
	Created, Updated  time.Time
}

type fUser struct {
	ID     uuid.UUID
	Active bool
}

type fPrincipal struct {
	ID     uuid.UUID
	Kind   string
	Active bool
	OrgID  uuid.UUID
}

type dataset struct {
	orgs       []fOrg
	mems       []fMem
	users      []fUser
	principals []fPrincipal
}

// fakeDB answers each statement from the dataset of the schema its SQL names:
// a statement that names an auth copy of organizations, memberships or users
// reads the auth dataset, any other the public one.
type fakeDB struct {
	public, auth dataset
	revs         map[string]int64
	sql          []string
}

var (
	markerRE   = regexp.MustCompile(`^/\* publicread\.([a-z_]+) \*/`)
	authCopyRE = regexp.MustCompile(`"?auth\w*"?\.(organizations|memberships|users)\b`)
)

func (f *fakeDB) source(sql string) *dataset {
	if authCopyRE.MatchString(sql) {
		return &f.auth
	}
	return &f.public
}

func (f *fakeDB) rev(kind, prefix string, id uuid.UUID) int64 {
	return f.revs[kind+":"+prefix+id.String()]
}

type fakeRows struct {
	rows [][]any
	at   int
	err  error
}

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return r.err }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Values() ([]any, error)                       { return r.rows[r.at-1], nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }
func (r *fakeRows) Next() bool {
	if r.at >= len(r.rows) {
		return false
	}
	r.at++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	row := r.rows[r.at-1]
	if len(dest) != len(row) {
		return fmt.Errorf("fake: %d destinations for %d columns", len(dest), len(row))
	}
	for i := range dest {
		if err := assign(dest[i], row[i]); err != nil {
			return fmt.Errorf("column %d: %w", i, err)
		}
	}
	return nil
}

func assign(dest, v any) error {
	switch d := dest.(type) {
	case *uuid.UUID:
		*d = v.(uuid.UUID)
	case *string:
		*d = v.(string)
	case **uuid.UUID:
		*d = v.(*uuid.UUID)
	case **string:
		if s, ok := v.(*string); ok {
			*d = s
		} else {
			s := v.(string)
			*d = &s
		}
	case *bool:
		*d = v.(bool)
	case *int64:
		*d = v.(int64)
	case *time.Time:
		*d = v.(time.Time)
	case **time.Time:
		*d = v.(*time.Time)
	default:
		return fmt.Errorf("unsupported destination %T", dest)
	}
	return nil
}

type fakeRow struct{ rows *fakeRows }

func (r fakeRow) Scan(dest ...any) error {
	if !r.rows.Next() {
		return pgx.ErrNoRows
	}
	return r.rows.Scan(dest...)
}

func (f *fakeDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	rows, err := f.Query(ctx, sql, args...)
	if err != nil {
		return fakeRow{&fakeRows{err: err}}
	}
	return fakeRow{rows.(*fakeRows)}
}

func uuidArg(a any) *uuid.UUID {
	switch v := a.(type) {
	case uuid.UUID:
		return &v
	case *uuid.UUID:
		return v
	}
	return nil
}

func (f *fakeDB) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.sql = append(f.sql, sql)
	m := markerRE.FindStringSubmatch(sql)
	if m == nil {
		return nil, fmt.Errorf("fake: statement has no publicread marker: %.60s", sql)
	}
	src := f.source(sql)
	out := &fakeRows{}
	limit := func(i int) int { return args[i].(int) }
	after := func(i int) *uuid.UUID { return uuidArg(args[i]) }
	switch m[1] {
	case "org_get":
		id := *uuidArg(args[0])
		for _, o := range src.orgs {
			if o.ID == id {
				out.rows = append(out.rows, []any{o.ID, o.Slug, o.Name, o.Active, o.Created, o.Updated, f.rev("organization", "org_", o.ID)})
			}
		}
	case "orgs_of":
		user := *uuidArg(args[0])
		var picked []fMem
		for _, mem := range src.mems {
			if mem.UserID == user {
				picked = append(picked, mem)
			}
		}
		orgByID := map[uuid.UUID]fOrg{}
		for _, o := range src.orgs {
			orgByID[o.ID] = o
		}
		sort.Slice(picked, func(i, j int) bool { return picked[i].OrgID.String() < picked[j].OrgID.String() })
		for _, mem := range picked {
			o, ok := orgByID[mem.OrgID]
			if !ok || (after(1) != nil && o.ID.String() <= after(1).String()) || len(out.rows) >= limit(2) {
				continue
			}
			out.rows = append(out.rows, []any{o.ID, o.Slug, o.Name, o.Active, o.Created, o.Updated, f.rev("organization", "org_", o.ID),
				mem.ID, mem.Role, mem.Joined, mem.Created, mem.Updated, f.rev("membership", "mbr_", mem.ID)})
		}
	case "membership_get":
		org, user := *uuidArg(args[0]), *uuidArg(args[1])
		for _, mem := range src.mems {
			if mem.OrgID == org && mem.UserID == user {
				out.rows = append(out.rows, f.memRow(mem))
			}
		}
	case "memberships_list":
		org := *uuidArg(args[0])
		var picked []fMem
		for _, mem := range src.mems {
			if mem.OrgID == org {
				picked = append(picked, mem)
			}
		}
		sort.Slice(picked, func(i, j int) bool { return picked[i].ID.String() < picked[j].ID.String() })
		for _, mem := range picked {
			if (after(1) != nil && mem.ID.String() <= after(1).String()) || len(out.rows) >= limit(2) {
				continue
			}
			out.rows = append(out.rows, f.memRow(mem))
		}
	case "principal_get_user":
		id := *uuidArg(args[0])
		for _, u := range src.users {
			if u.ID == id {
				out.rows = append(out.rows, []any{u.ID, u.Active, f.rev("principal", "prn_", u.ID)})
			}
		}
	case "principal_get_other":
		id := *uuidArg(args[0])
		for _, p := range f.auth.principals {
			if p.ID == id && p.Kind != "user" {
				out.rows = append(out.rows, []any{p.ID, p.Kind, p.Active, f.rev("principal", "prn_", p.ID)})
			}
		}
	case "principals_list":
		org := *uuidArg(args[0])
		kind, _ := args[1].(*string)
		type listed struct {
			Principal
			mem *fMem
		}
		var all []listed
		for i, mem := range src.mems {
			for _, u := range src.users {
				if mem.OrgID == org && u.ID == mem.UserID {
					all = append(all, listed{Principal{ID: u.ID, Kind: "user", Active: u.Active}, &src.mems[i]})
				}
			}
		}
		for _, p := range f.auth.principals {
			if p.OrgID == org && p.Kind != "user" {
				all = append(all, listed{Principal: Principal{ID: p.ID, Kind: p.Kind, Active: p.Active}})
			}
		}
		sort.Slice(all, func(i, j int) bool { return all[i].ID.String() < all[j].ID.String() })
		for _, p := range all {
			if (kind != nil && p.Kind != *kind) || (after(2) != nil && p.ID.String() <= after(2).String()) || len(out.rows) >= limit(3) {
				continue
			}
			row := []any{p.ID, p.Kind, p.Active, f.rev("principal", "prn_", p.ID), (*uuid.UUID)(nil), (*string)(nil), (*time.Time)(nil), (*time.Time)(nil), (*time.Time)(nil), int64(0)}
			if m := p.mem; m != nil {
				role, created, updated := m.Role, m.Created, m.Updated
				row[4], row[5], row[6], row[7], row[8], row[9] = &m.ID, &role, m.Joined, &created, &updated, f.rev("membership", "mbr_", m.ID)
			}
			out.rows = append(out.rows, row)
		}
	default:
		return nil, fmt.Errorf("fake: no statement %q", m[1])
	}
	return out, nil
}

func (f *fakeDB) memRow(mem fMem) []any {
	return []any{mem.ID, mem.OrgID, mem.UserID, mem.Role, mem.Joined, mem.Created, mem.Updated, f.rev("membership", "mbr_", mem.ID)}
}

func ids(n int) []uuid.UUID {
	out := make([]uuid.UUID, n)
	for i := range out {
		out[i] = uuid.MustParse(fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1))
	}
	return out
}

func mustContain(sql string, parts ...string) []string {
	var missing []string
	flat := strings.Join(strings.Fields(sql), " ")
	for _, p := range parts {
		if !strings.Contains(flat, p) {
			missing = append(missing, p)
		}
	}
	return missing
}
