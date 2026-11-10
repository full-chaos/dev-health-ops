package admin

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// storedUserLookups read the admin users projection (userColumns): is_superuser is the stored holding.
var storedUserLookups = map[string]bool{"fullUserByID": true, "userByEmail": true, "userByUsername": true}

// actorUserLookups read only what an acting user's lookup needs, with no is_superuser.
var actorUserLookups = map[string]bool{"actorByID": true, "actorByEmail": true}

// storedUserLookupClass is every call of a stored lookup in the package, by file:func:callee(id arg).
// A target read shows the stored holding; a uniqueness check reads only nil or not nil.
var storedUserLookupClass = map[string]string{
	"operator.go:CreateUser:userByEmail(in.Email)":                                           "uniqueness",
	"operator.go:CreateUser:userByUsername(*in.Username)":                                    "uniqueness",
	"operator.go:findUser:fullUserByID(parsed)":                                              "target",
	"operator.go:findUser:userByEmail(*email)":                                               "target",
	"operator.go:findUser:userByUsername(*username)":                                         "target",
	"operator.go:CreateOrg:userByEmail(*in.OwnerEmail)":                                      "target",
	"userstore.go:insertUser:userByEmail(pythonparity.Strip(pythonparity.Lower(*in.Email)))": "uniqueness",
	"userstore.go:insertUser:userByUsername(*in.Username)":                                   "uniqueness",
	"userstore.go:updateUser:fullUserByID(id)":                                               "target",
	"userstore.go:updateUser:userByEmail(*patch.Email)":                                      "uniqueness",
	"userstore.go:updateUser:userByUsername(*patch.Username)":                                "uniqueness",
	"users.go:getUser:fullUserByID(targetID)":                                                "target",
	"users.go:updateUser:fullUserByID(targetID)":                                             "target",
	"users.go:setUserPassword:fullUserByID(targetID)":                                        "target",
	"users.go:deleteUser:fullUserByID(targetID)":                                             "target",
	"users_owned_forward.go:writeUserReadBack:fullUserByID(id)":                              "target",
	"users_owned_forward.go:refuseIfTaken:userByEmail(emailLookup)":                          "uniqueness",
	"users_owned_forward.go:refuseIfTaken:userByUsername(username)":                          "uniqueness",
	"users_owned_forward.go:updateUserInAuth:fullUserByID(existing.ID)":                      "target",
}

// actorOnlyLookupSites are the lookups of the acting user: each calls an actor lookup, never a stored one.
var actorOnlyLookupSites = map[string]string{
	"users.go:setUserPassword:actingID":           "actorByID",
	"operator_membership.go:resolveActorOn:actor": "actorByEmail",
}

// actorArgNames are the id args that name the acting user, never a target.
var actorArgNames = map[string]bool{"actingID": true, "actor": true, "user.UserID": true}

type userLookupCall struct {
	file, fn, callee, arg string
	pos                   token.Position
}

func (c userLookupCall) site() string { return c.file + ":" + c.fn + ":" + c.arg }
func (c userLookupCall) key() string {
	return c.file + ":" + c.fn + ":" + c.callee + "(" + c.arg + ")"
}

func adminUserLookupCalls(t *testing.T) []userLookupCall {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var calls []userLookupCall
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !(storedUserLookups[sel.Sel.Name] || actorUserLookups[sel.Sel.Name]) || len(call.Args) != 2 {
					return true
				}
				arg := call.Args[1]
				calls = append(calls, userLookupCall{file: name, fn: fn.Name.Name, callee: sel.Sel.Name,
					arg: string(src[fset.Position(arg.Pos()).Offset:fset.Position(arg.End()).Offset]), pos: fset.Position(call.Pos())})
				return true
			})
		}
	}
	return calls
}

// C1: an actor-only lookup never reads the stored projection, and every stored lookup has a class.
func TestActorOnlyAdminUserLookupsDoNotUseTheStoredProjection(t *testing.T) {
	seen := map[string]bool{}
	actorSeen := map[string]string{}
	for _, c := range adminUserLookupCalls(t) {
		if want, ok := actorOnlyLookupSites[c.site()]; ok {
			actorSeen[c.site()] = c.callee
			if c.callee != want {
				t.Errorf("%s: actor-only lookup %s calls %s, want %s (no stored is_superuser)", c.pos, c.site(), c.callee, want)
			}
			continue
		}
		if actorUserLookups[c.callee] {
			t.Errorf("%s: %s is not an actor-only lookup site; name it in actorOnlyLookupSites", c.pos, c.key())
			continue
		}
		seen[c.key()] = true
		if _, ok := storedUserLookupClass[c.key()]; !ok {
			t.Errorf("%s: stored users lookup %s has no class in storedUserLookupClass", c.pos, c.key())
		}
	}
	for site, want := range actorOnlyLookupSites {
		if actorSeen[site] != want {
			t.Errorf("actor-only lookup site %s: calls %q, want %s", site, actorSeen[site], want)
		}
	}
	keys := make([]string, 0, len(storedUserLookupClass))
	for key := range storedUserLookupClass {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		class := storedUserLookupClass[key]
		if class != "target" && class != "uniqueness" {
			t.Errorf("%s: class %q, want target or uniqueness", key, class)
		}
		arg := key[strings.LastIndex(key, "(")+1 : len(key)-1]
		if actorArgNames[arg] {
			t.Errorf("%s: the id arg names the acting user; an actor lookup is not a %s read", key, class)
		}
		if !seen[key] {
			t.Errorf("%s: no such call in the package", key)
		}
	}
}

// lookupRecordDB answers every users lookup with one row (the id only) and records the SQL text.
type lookupRecordDB struct {
	sql []string
}

func (db *lookupRecordDB) Begin(context.Context) (pgx.Tx, error) { return lookupRecordTx{db}, nil }
func (db *lookupRecordDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (db *lookupRecordDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &notSuperuserRows{}, nil
}

// notSuperuserRows answers the superuser guard's target read: one target, not a superuser.
type notSuperuserRows struct{ read bool }

func (r *notSuperuserRows) Close()                                       {}
func (r *notSuperuserRows) Err() error                                   { return nil }
func (r *notSuperuserRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *notSuperuserRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *notSuperuserRows) Next() bool {
	next := !r.read
	r.read = true
	return next
}
func (r *notSuperuserRows) Scan(dest ...any) error {
	if len(dest) != 1 {
		return errors.New("notSuperuserRows: want one column")
	}
	if b, ok := dest[0].(*bool); ok {
		*b = false
		return nil
	}
	return errors.New("notSuperuserRows: want a bool")
}
func (r *notSuperuserRows) Values() ([]any, error) { return []any{false}, nil }
func (r *notSuperuserRows) RawValues() [][]byte    { return nil }
func (r *notSuperuserRows) Conn() *pgx.Conn        { return nil }
func (db *lookupRecordDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	db.sql = append(db.sql, sql)
	id := uuid.New()
	if len(args) > 0 {
		if given, ok := args[0].(uuid.UUID); ok {
			id = given
		}
	}
	return lookupRecordRow{id: id}
}

type lookupRecordTx struct{ db *lookupRecordDB }

func (tx lookupRecordTx) Begin(context.Context) (pgx.Tx, error) { return tx, nil }
func (tx lookupRecordTx) Commit(context.Context) error          { return nil }
func (tx lookupRecordTx) Rollback(context.Context) error        { return nil }
func (tx lookupRecordTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.db.Exec(ctx, sql, args...)
}
func (tx lookupRecordTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return tx.db.Query(ctx, sql, args...)
}
func (tx lookupRecordTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.db.QueryRow(ctx, sql, args...)
}
func (tx lookupRecordTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("unused")
}
func (tx lookupRecordTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (tx lookupRecordTx) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (tx lookupRecordTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("unused")
}
func (tx lookupRecordTx) Conn() *pgx.Conn { return nil }

// lookupRecordRow scans the id into the first column and leaves the rest zero (password_hash NULL).
type lookupRecordRow struct{ id uuid.UUID }

func (row lookupRecordRow) Scan(dest ...any) error {
	if len(dest) > 0 {
		if id, ok := dest[0].(*uuid.UUID); ok {
			*id = row.id
		}
	}
	return nil
}

// storedSuperuserReads are the texts by which a users read carries is_superuser, in both platform role modes.
func storedSuperuserReads(t *testing.T, store pgStore) []string {
	t.Helper()
	expr, err := store.platformRoles().StoredSuperuser("is_superuser", "users.id")
	if err != nil {
		t.Fatal(err)
	}
	if expr == "is_superuser" {
		return []string{expr}
	}
	return []string{"is_superuser", expr}
}

func platformRoleModes() map[string]pgStore {
	return map[string]pgStore{"public users": {}, "platform roles owned": {PlatformRolesOwned: true}}
}

func setPasswordActingLookup(t *testing.T, store pgStore) (*lookupRecordDB, int, string) {
	t.Helper()
	db := &lookupRecordDB{}
	store.Pool = db
	h := &handlers{store: store, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	target := uuid.New()
	rec := answerWith(h.setUserPassword, http.MethodPost, func(req *http.Request) *http.Request {
		req.SetPathValue("user_id", target.String())
		return req.WithContext(context.WithValue(req.Context(), setPasswordInputKey{},
			setPasswordInput{AdminPassword: "Admin-current-pass-1", Password: "New-strong-pass-1"}))
	})
	return db, rec.Code, rec.Body.String()
}

// C2a: the operator --actor email lookup reads no stored is_superuser.
func TestResolveActorEmailLookupReadsNoStoredSuperuser(t *testing.T) {
	for mode, store := range platformRoleModes() {
		db := &lookupRecordDB{}
		store.Pool = db
		if _, err := resolveActorOn(context.Background(), store, "actor@example.com"); err != nil {
			t.Fatalf("%s: resolveActorOn: %v", mode, err)
		}
		if len(db.sql) != 1 {
			t.Fatalf("%s: %d reads, want 1", mode, len(db.sql))
		}
		for _, read := range storedSuperuserReads(t, store) {
			if strings.Contains(db.sql[0], read) {
				t.Errorf("%s: the --actor lookup reads %q: %s", mode, read, db.sql[0])
			}
		}
	}
}

// C2b: the set-password acting-admin lookup reads no stored is_superuser; the target read keeps it.
func TestSetUserPasswordActingAdminLookupReadsNoStoredSuperuser(t *testing.T) {
	for mode, store := range platformRoleModes() {
		db, _, _ := setPasswordActingLookup(t, store)
		if len(db.sql) != 2 {
			t.Fatalf("%s: %d reads, want 2 (target, acting admin): %q", mode, len(db.sql), db.sql)
		}
		reads := storedSuperuserReads(t, store)
		if !strings.Contains(db.sql[0], reads[len(reads)-1]) {
			t.Errorf("%s: the target read lost the stored is_superuser: %s", mode, db.sql[0])
		}
		for _, read := range reads {
			if strings.Contains(db.sql[1], read) {
				t.Errorf("%s: the acting-admin lookup reads %q: %s", mode, read, db.sql[1])
			}
		}
	}
}

// C3: an acting admin with no password hash still gets the same 403 bytes.
func TestSetUserPasswordActingAdminWithoutPasswordHashKeepsThe403(t *testing.T) {
	for mode, store := range platformRoleModes() {
		_, code, body := setPasswordActingLookup(t, store)
		if code != http.StatusForbidden || body != `{"detail":"Admin password verification failed"}` {
			t.Errorf("%s: %d %s, want 403 {\"detail\":\"Admin password verification failed\"}", mode, code, body)
		}
	}
}
