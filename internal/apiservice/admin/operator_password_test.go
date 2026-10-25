package admin

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const elevenCharacterPassword = "Aa1-xyzabcd"

// passwordFakeDB answers the operator's lookups without a database: no user by
// email or username, or (withUser) one user by any lookup. It records every
// write so a test sees that a refused password stored nothing.
type passwordFakeDB struct {
	withUser bool
	writes   []string
}

func (db *passwordFakeDB) Begin(context.Context) (pgx.Tx, error) { return passwordFakeTx{db}, nil }
func (db *passwordFakeDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	db.writes = append(db.writes, sql)
	return pgconn.CommandTag{}, nil
}
func (db *passwordFakeDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("passwordFakeDB: unexpected Query")
}
func (db *passwordFakeDB) QueryRow(context.Context, string, ...any) pgx.Row {
	return passwordFakeRow{found: db.withUser}
}

type passwordFakeTx struct{ db *passwordFakeDB }

func (tx passwordFakeTx) Begin(ctx context.Context) (pgx.Tx, error) { return tx, nil }
func (tx passwordFakeTx) Commit(context.Context) error              { return nil }
func (tx passwordFakeTx) Rollback(context.Context) error            { return nil }
func (tx passwordFakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.db.Exec(ctx, sql, args...)
}
func (tx passwordFakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return tx.db.Query(ctx, sql, args...)
}
func (tx passwordFakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return tx.db.QueryRow(ctx, sql, args...)
}
func (tx passwordFakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("unused")
}
func (tx passwordFakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }
func (tx passwordFakeTx) LargeObjects() pgx.LargeObjects                         { return pgx.LargeObjects{} }
func (tx passwordFakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("unused")
}
func (tx passwordFakeTx) Conn() *pgx.Conn { return nil }

type passwordFakeRow struct{ found bool }

func (row passwordFakeRow) Scan(dest ...any) error {
	if !row.found {
		return pgx.ErrNoRows
	}
	*dest[0].(*uuid.UUID) = uuid.MustParse("00000000-0000-4000-8000-000000000001")
	*dest[1].(*string) = "someone@example.com"
	return nil
}

func assertPolicyRefusal(t *testing.T, err error, password string) {
	t.Helper()
	var refusal *OperatorError
	if !errors.As(err, &refusal) {
		t.Fatalf("err = %v, want an OperatorError refusal", err)
	}
	if !strings.Contains(refusal.Message, "Password must be at least 12 characters long") {
		t.Errorf("refusal %q lacks the policy violation", refusal.Message)
	}
	if strings.Contains(refusal.Message, password) {
		t.Errorf("refusal %q carries the password", refusal.Message)
	}
}

func TestOperatorCreateUserRefusesElevenCharacterPassword(t *testing.T) {
	db := &passwordFakeDB{}
	_, err := Operator{db: db}.CreateUser(context.Background(), CreateUserInput{Email: "new@example.com", Password: elevenCharacterPassword})
	assertPolicyRefusal(t, err, elevenCharacterPassword)
	if len(db.writes) != 0 {
		t.Errorf("a refused create wrote: %v", db.writes)
	}
}

func TestOperatorUpdateUserRefusesElevenCharacterPassword(t *testing.T) {
	db := &passwordFakeDB{withUser: true}
	password := elevenCharacterPassword
	email := "someone@example.com"
	_, err := Operator{db: db}.UpdateUser(context.Background(), UpdateUserInput{Email: &email, Password: &password})
	assertPolicyRefusal(t, err, password)
	if len(db.writes) != 0 {
		t.Errorf("a refused update wrote: %v", db.writes)
	}
}

func TestOperatorPasswordRefusalCoversThePolicy(t *testing.T) {
	refused := map[string]string{
		"empty":        "",
		"eleven":       elevenCharacterPassword,
		"no digit":     "Abcdefghijkl",
		"no letter":    "123456789012",
		"common":       "password1234",
		"73 bytes":     strings.Repeat("a", 72) + "1",
		"far too long": strings.Repeat("a", 200) + "1",
	}
	for name, password := range refused {
		if err := operatorPasswordRefusal(password); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for name, password := range map[string]string{
		"twelve":   "Aa1-xyzabcde",
		"72 bytes": strings.Repeat("a", 71) + "1",
	} {
		if err := operatorPasswordRefusal(password); err != nil {
			t.Errorf("%s: refused: %v", name, err)
		}
	}
}

func TestOperatorPasswordPathsCallTheSharedPolicy(t *testing.T) {
	for _, fn := range []string{"CreateUser", "UpdateUser"} {
		if !declCalls(t, "operator.go", fn, "operatorPasswordRefusal") {
			t.Errorf("%s does not call operatorPasswordRefusal", fn)
		}
	}
	if !declCalls(t, "operator.go", "operatorPasswordRefusal", "passwordpolicy.ValidateForHash") {
		t.Error("operatorPasswordRefusal does not call passwordpolicy.ValidateForHash")
	}
	files, _ := filepath.Glob("*.go")
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "PasswordMinLength") {
			t.Errorf("%s still names PasswordMinLength: a second length rule", file)
		}
	}
}

func declCalls(t *testing.T, file, fn, callee string) bool {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, decl := range parsed.Decls {
		d, ok := decl.(*ast.FuncDecl)
		if !ok || d.Name.Name != fn || d.Body == nil {
			continue
		}
		ast.Inspect(d.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch f := call.Fun.(type) {
			case *ast.Ident:
				found = found || f.Name == callee
			case *ast.SelectorExpr:
				if x, ok := f.X.(*ast.Ident); ok {
					found = found || x.Name+"."+f.Sel.Name == callee
				}
			}
			return true
		})
	}
	return found
}
