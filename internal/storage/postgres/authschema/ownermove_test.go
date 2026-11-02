package authschema

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// presentQuerier answers every catalog question of the grant builders with "the table and the column exist".
type presentQuerier struct{}

type presentRow struct{}

func (presentRow) Scan(dest ...any) error {
	for _, d := range dest {
		switch v := d.(type) {
		case *string:
			*v = "r"
		case *bool:
			*v = true
		}
	}
	return nil
}

func (presentQuerier) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("presentQuerier: Exec")
}
func (presentQuerier) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("presentQuerier: Query")
}
func (presentQuerier) QueryRow(context.Context, string, ...any) pgx.Row { return presentRow{} }
func (presentQuerier) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("presentQuerier: Begin")
}

var publicGrantStatement = regexp.MustCompile(`^GRANT (SELECT|INSERT|UPDATE|DELETE|TRUNCATE|REFERENCES|TRIGGER)(?: \((.*)\))? ON TABLE "public"\."([a-z_]+)" TO `)

// emittedPublicGrants runs every public-schema statement builder of the grants pass with every switch on and returns
// the privileges they GRANT, one "table|privilege|column" key each ("" column for a table-level privilege).
func emittedPublicGrants(t *testing.T) map[string]bool {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.DiscardHandler)
	options := Options{Schema: "auth", RuntimeRole: "rt", UsersOwned: true, RequirePublicTables: true}
	builders := map[string]func() ([]string, error){
		"publicRead": func() ([]string, error) { return publicReadStatements(ctx, presentQuerier{}, options, logger) },
		"usersWrite": func() ([]string, error) { return usersWriteStatements(ctx, presentQuerier{}, options, logger) },
		"register": func() ([]string, error) {
			return registerOrganizationStatements(ctx, presentQuerier{}, options, logger)
		},
		"linkPurge":     func() ([]string, error) { return legacyLinkPurgeStatements(ctx, presentQuerier{}, options, logger) },
		"membership":    func() ([]string, error) { return membershipWriteStatements(ctx, presentQuerier{}, options, logger) },
		"invite":        func() ([]string, error) { return inviteWriteStatements(ctx, presentQuerier{}, options, logger) },
		"audit":         func() ([]string, error) { return publicAuditWriteStatements(ctx, presentQuerier{}, options, logger) },
		"ssoProviderUp": func() ([]string, error) { return ssoProviderWriteStatements(ctx, presentQuerier{}, options, logger) },
		"impersonation": func() ([]string, error) { return impersonationWriteStatements(ctx, presentQuerier{}, options, logger) },
	}
	emitted := map[string]bool{}
	for name, build := range builders {
		statements, err := build()
		if err != nil {
			t.Fatalf("%s statements: %v", name, err)
		}
		for _, statement := range statements {
			if !strings.HasPrefix(statement, "GRANT ") {
				continue
			}
			m := publicGrantStatement.FindStringSubmatch(statement)
			if m == nil {
				t.Fatalf("%s emitted a GRANT this test cannot read: %s", name, statement)
			}
			if m[2] == "" {
				emitted[m[3]+"|"+m[1]+"|"] = true
				continue
			}
			for _, column := range strings.Split(m[2], ", ") {
				emitted[m[3]+"|"+m[1]+"|"+strings.Trim(column, `"`)] = true
			}
		}
	}
	return emitted
}

func keysOfPrivilegeSet(set []PublicPrivilege) map[string]bool {
	keys := map[string]bool{}
	for _, entry := range set {
		if entry.Columns == nil {
			keys[entry.Table+"|"+entry.Privilege+"|"] = true
			continue
		}
		for _, column := range entry.Columns {
			keys[entry.Table+"|"+entry.Privilege+"|"+column] = true
		}
	}
	return keys
}

func sortedMissing(have, want map[string]bool) []string {
	var out []string
	for key := range want {
		if !have[key] {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// The migrator holds exactly what the grants pass can give the runtime role, both ways: a privilege the builders
// emit and the set lacks fails the grants Job at its first GRANT; a privilege the set holds and no builder emits is
// authority the migrator keeps for nothing.
func TestPublicPrivilegeSetEqualsWhatTheGrantsPassCanGrant(t *testing.T) {
	emitted := emittedPublicGrants(t)
	if len(emitted) == 0 {
		t.Fatal("the grant builders emitted nothing; the test reads nothing")
	}
	set := keysOfPrivilegeSet(PublicPrivilegeSet())
	if missing := sortedMissing(set, emitted); len(missing) > 0 {
		t.Errorf("the grants pass can grant %d privileges the set lacks: %v", len(missing), missing)
	}
	if extra := sortedMissing(emitted, set); len(extra) > 0 {
		t.Errorf("the set holds %d privileges no builder grants: %v", len(extra), extra)
	}
}

func TestPublicPrivilegeSetIsClosed(t *testing.T) {
	set := PublicPrivilegeSet()
	if len(set) == 0 {
		t.Fatal("PublicPrivilegeSet is empty")
	}
	seen := map[string]bool{}
	for _, entry := range set {
		key := entry.Table + "|" + entry.Privilege
		if seen[key] {
			t.Errorf("%s is listed twice; one entry per table and privilege", key)
		}
		seen[key] = true
		switch entry.Privilege {
		case "SELECT", "INSERT", "UPDATE", "DELETE":
		default:
			t.Errorf("%s: privilege %q is outside SELECT, INSERT, UPDATE, DELETE", entry.Table, entry.Privilege)
		}
		if entry.Privilege == "DELETE" && entry.Columns != nil {
			t.Errorf("%s: DELETE has no column form", entry.Table)
		}
		if entry.Privilege != "DELETE" && len(entry.Columns) == 0 {
			t.Errorf("%s %s: a column privilege names no column; a table-level SELECT, INSERT or UPDATE is never granted", entry.Table, entry.Privilege)
		}
		if err := ValidateIdentifier(entry.Table); err != nil {
			t.Errorf("table %q: %v", entry.Table, err)
		}
	}
	for _, table := range []string{"memberships", "email_verification_tokens", "password_reset_tokens"} {
		if !seen[table+"|DELETE"] {
			t.Errorf("%s: the table-level DELETE the grants pass gives is not in the set", table)
		}
	}
}

func TestOwnerMoveOptionsValidate(t *testing.T) {
	good := OwnerMoveOptions{Schema: "auth", From: "devhealth", To: "auth_migrator", RuntimeRole: "devhealth_auth"}
	if err := good.Validate(); err != nil {
		t.Fatalf("a well formed move is refused: %v", err)
	}
	cases := map[string]func(*OwnerMoveOptions){
		"no from":           func(o *OwnerMoveOptions) { o.From = "" },
		"no to":             func(o *OwnerMoveOptions) { o.To = "" },
		"from equals to":    func(o *OwnerMoveOptions) { o.To = o.From },
		"to is the runtime": func(o *OwnerMoveOptions) { o.To = o.RuntimeRole },
		"from is runtime":   func(o *OwnerMoveOptions) { o.From = o.RuntimeRole },
		"no runtime role":   func(o *OwnerMoveOptions) { o.RuntimeRole = "" },
		"bad schema":        func(o *OwnerMoveOptions) { o.Schema = "Auth; DROP" },
		"bad to":            func(o *OwnerMoveOptions) { o.To = "x\"y" },
		"reserved to":       func(o *OwnerMoveOptions) { o.To = "select" },
		"negative lock":     func(o *OwnerMoveOptions) { o.LockTimeout = -1 },
	}
	for name, mutate := range cases {
		o := good
		mutate(&o)
		err := o.Validate()
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if !errors.Is(err, ErrOwnerMove) && !errors.Is(err, ErrInvalidOptions) {
			t.Errorf("%s: error %v is neither ErrOwnerMove nor ErrInvalidOptions", name, err)
		}
	}
}

func TestOptionsSchemaOwnerRoleValidates(t *testing.T) {
	o := Options{Schema: "auth", RuntimeRole: "rt", SchemaOwnerRole: "rt"}
	if err := o.Validate("migrator"); err == nil {
		t.Error("a pinned owner equal to the runtime role is accepted")
	}
	o = Options{Schema: "auth", RuntimeRole: "rt", SchemaOwnerRole: "Bad;"}
	if err := o.Validate("migrator"); err == nil {
		t.Error("a pinned owner that is not an identifier is accepted")
	}
	o = Options{Schema: "auth", RuntimeRole: "rt", SchemaOwnerRole: "auth_migrator"}
	if err := o.Validate("migrator"); err != nil {
		t.Errorf("a valid pinned owner is refused: %v", err)
	}
}

// The anchor is the one table-level privilege the migrator holds without grant option; no grant builder gives it to
// the runtime role, so it stays out of the set the builders are compared with.
func TestPublicAnchorPrivilegeIsOutsideTheSetAndCoversEveryTable(t *testing.T) {
	if PublicAnchorPrivilege != "REFERENCES" {
		t.Fatalf("anchor privilege = %q, want REFERENCES (the weakest table-level privilege)", PublicAnchorPrivilege)
	}
	tables := map[string]bool{}
	for _, entry := range PublicPrivilegeSet() {
		if entry.Privilege == PublicAnchorPrivilege {
			t.Errorf("%s: the anchor privilege is in the grant set", entry.Table)
		}
		tables[entry.Table] = true
	}
	anchors := PublicAnchorTables()
	if len(anchors) != len(tables) {
		t.Fatalf("anchor tables = %v, want one per table of the set %v", anchors, tables)
	}
	for _, table := range anchors {
		if !tables[table] {
			t.Errorf("anchor table %s is not a table of the set", table)
		}
	}
}
