package postgres

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

func usersTableOf(t *testing.T, posture RolePosture) TablePrivilege {
	t.Helper()
	var found []TablePrivilege
	for _, table := range posture.RequiredTables {
		if table.TableName == "users" {
			found = append(found, table)
		}
	}
	if len(found) != 1 {
		t.Fatalf("users is declared %d times, want once", len(found))
	}
	return found[0]
}

func usersColumnsOf(posture RolePosture, privilege string) []string {
	var columns []string
	for _, column := range posture.ColumnScoped {
		if column.TableName == "users" && column.Privilege == privilege {
			columns = append(columns, column.ColumnName)
		}
	}
	sort.Strings(columns)
	return columns
}

var (
	wantUsersInsertWhileOwned = []string{"auth_provider", "auth_provider_id", "avatar_url", "created_at", "email", "full_name", "id",
		"is_active", "is_verified", "last_login_at", "password_hash", "saml_nameid_format", "saml_sp_name_qualifier", "sso_provider_id",
		"token_version", "updated_at", "username"}
	wantUsersUpdateWhileOwned = []string{"avatar_url", "email", "full_name", "is_active", "is_verified", "last_login_at", "password_hash",
		"saml_nameid_format", "saml_sp_name_qualifier", "token_version", "updated_at", "username"}
)

// While auth-service owns the platform roles the api role writes no
// public.users.is_superuser: INSERT and UPDATE move from the table to the
// columns go-api's writers name, SELECT and DELETE stay table-wide.
func TestAPIPosturePlatformRolesOwnedRevokesSuperuserWrites(t *testing.T) {
	for _, sessions := range []bool{false, true} {
		owned, off := APIPostureFor(sessions, true), APIPosture(sessions)
		if got := usersTableOf(t, owned); got != (TablePrivilege{"users", false, false, true}) {
			t.Errorf("sessions=%v users = %+v, want select and delete at table level", sessions, got)
		}
		if got := usersColumnsOf(owned, "INSERT"); !reflect.DeepEqual(got, wantUsersInsertWhileOwned) {
			t.Errorf("sessions=%v users INSERT columns = %v, want %v", sessions, got, wantUsersInsertWhileOwned)
		}
		if got := usersColumnsOf(owned, "UPDATE"); !reflect.DeepEqual(got, wantUsersUpdateWhileOwned) {
			t.Errorf("sessions=%v users UPDATE columns = %v, want %v", sessions, got, wantUsersUpdateWhileOwned)
		}
		for _, column := range owned.ColumnScoped {
			if column.TableName == "users" && column.ColumnName == "is_superuser" {
				t.Errorf("sessions=%v: the api role keeps %s (is_superuser) while owned", sessions, column.Privilege)
			}
		}
		if len(owned.RequiredTables) != len(off.RequiredTables) {
			t.Fatalf("sessions=%v: the switch adds or drops a table", sessions)
		}
		for i := range off.RequiredTables {
			if off.RequiredTables[i].TableName != "users" && owned.RequiredTables[i] != off.RequiredTables[i] {
				t.Errorf("sessions=%v: the switch changes %+v to %+v", sessions, off.RequiredTables[i], owned.RequiredTables[i])
			}
		}
		var others []ColumnPrivilege
		for _, column := range owned.ColumnScoped {
			if column.TableName != "users" {
				others = append(others, column)
			}
		}
		if !reflect.DeepEqual(others, off.ColumnScoped) {
			t.Errorf("sessions=%v: the switch changes column grants of other tables: %+v, want %+v", sessions, others, off.ColumnScoped)
		}
	}
}

// The switch off, the manifest is the one APIPosture always gave.
func TestAPIPosturePlatformRolesFlagOffIsUnchanged(t *testing.T) {
	for _, sessions := range []bool{false, true} {
		if !reflect.DeepEqual(APIPostureFor(sessions, false), APIPosture(sessions)) {
			t.Errorf("sessions=%v: platformRolesOwned=false changes the manifest", sessions)
		}
		off := APIPosture(sessions)
		if got := usersTableOf(t, off); got != (TablePrivilege{"users", true, true, true}) {
			t.Errorf("sessions=%v users off = %+v, want insert, update and delete at table level", sessions, got)
		}
		if len(usersColumnsOf(off, "INSERT"))+len(usersColumnsOf(off, "UPDATE")) != 0 {
			t.Errorf("sessions=%v: users has column grants with the switch off", sessions)
		}
	}
}

// usersDDLColumns is every column of public.users: the baseline plus any ADD
// COLUMN of a later public revision.
func usersDDLColumns(t *testing.T) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile("../../pgmigrate/baseline/head.json")
	if err != nil {
		t.Fatal(err)
	}
	var baseline struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(raw, &baseline); err != nil {
		t.Fatal(err)
	}
	body := regexp.MustCompile(`(?s)CREATE TABLE public\.users \((.*?)\n\);`).FindStringSubmatch(baseline.Schema)
	if body == nil {
		t.Fatal("users is not in the baseline")
	}
	columns := map[string]bool{}
	for _, match := range regexp.MustCompile(`(?m)^\s+([a-z_]+) `).FindAllStringSubmatch(body[1], -1) {
		columns[match[1]] = true
	}
	files, err := os.ReadDir("../../pgmigrate/sql")
	if err != nil {
		t.Fatal(err)
	}
	add := regexp.MustCompile(`(?is)ALTER TABLE\s+(?:ONLY\s+)?(?:public\.)?users\s+ADD COLUMN\s+(?:IF NOT EXISTS\s+)?([a-z_]+)`)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		text, err := os.ReadFile(filepath.Join("../../pgmigrate/sql", file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range add.FindAllStringSubmatch(string(text), -1) {
			columns[match[1]] = true
		}
	}
	return columns
}

// Every column of public.users is classified for INSERT and for UPDATE while
// owned, so a new column fails this cell until someone decides whether go-api
// may still write it. is_superuser is refused for both.
func TestAPIPostureUsersColumnsCoverTheDDL(t *testing.T) {
	columns := usersDDLColumns(t)
	if !columns["is_superuser"] || !columns["email"] {
		t.Fatalf("the DDL read is wrong: %v", columns)
	}
	refused := map[string]map[string]bool{
		"INSERT": {"is_superuser": true},
		"UPDATE": {"id": true, "auth_provider": true, "auth_provider_id": true, "sso_provider_id": true, "created_at": true, "is_superuser": true},
	}
	for privilege, refusedSet := range refused {
		granted := map[string]bool{}
		for _, column := range usersColumnsOf(APIPostureFor(false, true), privilege) {
			granted[column] = true
			if !columns[column] {
				t.Errorf("granted %s column %s is not in the DDL", privilege, column)
			}
		}
		for column := range columns {
			if refusedSet[column] == granted[column] {
				t.Errorf("users.%s is not classified for %s: refused=%v granted=%v", column, privilege, refusedSet[column], granted[column])
			}
		}
		for column := range refusedSet {
			if !columns[column] {
				t.Errorf("refused %s column %s is not in the DDL", privilege, column)
			}
		}
	}
}

// The old entry points hold no logic of their own: each is one return of the
// For-form with the platform roles switch off.
func TestAPIPostureOldEntryPointsAreOneLineWrappers(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "api_authorization.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for name, callee := range map[string]string{"APIPosture": "APIPostureFor", "CheckAPIAuthorization": "CheckAPIAuthorizationFor"} {
		var decl *ast.FuncDecl
		for _, d := range file.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
				decl = fn
			}
		}
		if decl == nil {
			t.Fatalf("%s is not declared", name)
		}
		if len(decl.Body.List) != 1 {
			t.Errorf("%s has %d statements, want one return of %s", name, len(decl.Body.List), callee)
			continue
		}
		ret, ok := decl.Body.List[0].(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 1 {
			t.Errorf("%s: the one statement is not a single-value return", name)
			continue
		}
		call, ok := ret.Results[0].(*ast.CallExpr)
		if !ok {
			t.Errorf("%s: the return is not a call", name)
			continue
		}
		if ident, ok := call.Fun.(*ast.Ident); !ok || ident.Name != callee {
			t.Errorf("%s returns a call of %v, want %s", name, call.Fun, callee)
		}
		if last, ok := call.Args[len(call.Args)-1].(*ast.Ident); !ok || last.Name != "false" {
			t.Errorf("%s does not pass platformRolesOwned=false", name)
		}
	}
}

var (
	goAPIUsersWrite  = regexp.MustCompile(`(?is)\b(?:insert\s+into|update)\s+(?:only\s+)?(?:public\s*\.\s*)?users\b`)
	goAPIInsertHead  = regexp.MustCompile(`(?is)^\s*insert\s+into\s+(?:public\s*\.\s*)?users\s*\(`)
	goAPIUpdateHead  = regexp.MustCompile(`(?is)^\s*update\s+(?:only\s+)?(?:public\s*\.\s*)?users(?:\s+(?:as\s+)?[a-z_]+)?\s+set\b`)
	goAPIConflictSet = regexp.MustCompile(`(?is)\bon\s+conflict\b.*?\bdo\s+update\s+set\b`)
	goAPIClauseEnd   = regexp.MustCompile(`(?is)^(?:where|from|returning)\b`)
)

// usersStatementColumns is the INSERT and UPDATE target columns of one users
// write, lowered; ok is false when the statement shape is not one it parses.
func usersStatementColumns(sql string) (insert, update []string, ok bool) {
	sql = strings.ReplaceAll(sql, `"`, "")
	if loc := goAPIInsertHead.FindStringIndex(sql); loc != nil {
		open := loc[1] - 1
		end := matchParenAt(sql, open)
		if end < 0 {
			return nil, nil, false
		}
		for _, column := range splitTop(sql[open+1 : end]) {
			insert = append(insert, bareColumn(column))
		}
		if head := strings.ToLower(strings.TrimSpace(sql[end+1:])); !strings.HasPrefix(head, "values") {
			return nil, nil, false
		}
		if conflict := goAPIConflictSet.FindStringIndex(sql); conflict != nil {
			set, ok := setTargets(sql[conflict[1]:])
			if !ok {
				return nil, nil, false
			}
			update = set
		}
		return insert, update, true
	}
	if loc := goAPIUpdateHead.FindStringIndex(sql); loc != nil {
		set, ok := setTargets(sql[loc[1]:])
		return nil, set, ok
	}
	return nil, nil, false
}

func matchParenAt(s string, open int) int {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func splitTop(list string) []string {
	var parts []string
	depth, start := 0, 0
	for i, r := range list {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(list[start:i]))
				start = i + 1
			}
		}
	}
	return append(parts, strings.TrimSpace(list[start:]))
}

func bareColumn(target string) string {
	target = strings.ToLower(strings.TrimSpace(target))
	return target[strings.LastIndex(target, ".")+1:]
}

// setTargets is the target columns of a SET clause, tuple targets included.
func setTargets(clause string) ([]string, bool) {
	depth, end := 0, len(clause)
scan:
	for i := 0; i < len(clause); i++ {
		switch clause[i] {
		case '(':
			depth++
		case ')':
			depth--
		default:
			word := i == 0 || !(clause[i-1] == '_' || clause[i-1] >= 'a' && clause[i-1] <= 'z' || clause[i-1] >= 'A' && clause[i-1] <= 'Z' || clause[i-1] >= '0' && clause[i-1] <= '9')
			if depth == 0 && word && goAPIClauseEnd.MatchString(clause[i:]) {
				end = i
				break scan
			}
		}
	}
	var targets []string
	for _, assignment := range splitTop(clause[:end]) {
		eq := strings.Index(assignment, "=")
		if eq < 0 {
			return nil, false
		}
		lhs := strings.TrimSpace(assignment[:eq])
		if strings.HasPrefix(lhs, "(") && strings.HasSuffix(lhs, ")") {
			for _, column := range splitTop(lhs[1 : len(lhs)-1]) {
				targets = append(targets, bareColumn(column))
			}
			continue
		}
		if strings.ContainsAny(lhs, " ()") {
			return nil, false
		}
		targets = append(targets, bareColumn(lhs))
	}
	return targets, true
}

func funcNameOf(decl *ast.FuncDecl) string {
	if decl.Recv == nil || len(decl.Recv.List) == 0 {
		return decl.Name.Name
	}
	recv := decl.Recv.List[0].Type
	if star, ok := recv.(*ast.StarExpr); ok {
		recv = star.X
	}
	if ident, ok := recv.(*ast.Ident); ok {
		return ident.Name + "." + decl.Name.Name
	}
	return decl.Name.Name
}

// goAPISuperuserWriters are the only go-api functions whose users statements
// may name is_superuser. Neither runs it while the platform roles are owned:
// insertUser refuses a true value and omits a false one, updateUser takes its
// owned branch (internal/auth/platformrole/testdata/superuser_writes.tsv holds
// the proof tests).
var goAPISuperuserWriters = map[string]bool{
	"internal/apiservice/admin/userstore.go\tpgStore.insertUser": true,
	"internal/apiservice/admin/userstore.go\tpgStore.updateUser": true,
}

// goAPIUsersWriteSites is the count of users INSERT and UPDATE statements per
// go-api file, so a probe that goes blind fails, and a new statement in a known
// file is looked at here once.
var goAPIUsersWriteSites = map[string]int{
	"internal/api/session/changepassword.go":  1,
	"internal/api/session/register.go":        1,
	"internal/api/session/social.go":          1,
	"internal/api/session/store.go":           1,
	"internal/api/session/verify.go":          2,
	"internal/api/sso/oauth.go":               1,
	"internal/api/sso/provision.go":           2,
	"internal/apiservice/admin/userdelete.go": 1,
	"internal/apiservice/admin/userstore.go":  5,
}

// Every users INSERT and UPDATE that go-api (internal/api, internal/apiservice)
// runs as the api role names only columns the owned posture grants, so no
// go-api write fails 42501 after the REVOKE. is_superuser is named only by the
// two admin functions that never run it while owned.
func TestGoAPIUsersWritesStayWithinTheOwnedGrant(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	owned := APIPostureFor(true, true)
	granted := map[string]map[string]bool{"INSERT": {}, "UPDATE": {}}
	for privilege := range granted {
		for _, column := range usersColumnsOf(owned, privilege) {
			granted[privilege][column] = true
		}
	}
	sites := map[string]int{}
	fset := token.NewFileSet()
	for _, dir := range []string{"internal/api", "internal/apiservice"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			rel := filepath.ToSlash(strings.TrimPrefix(path, root+string(filepath.Separator)))
			for _, decl := range file.Decls {
				scope := "-"
				if fn, ok := decl.(*ast.FuncDecl); ok {
					scope = funcNameOf(fn)
				}
				ast.Inspect(decl, func(n ast.Node) bool {
					lit, ok := n.(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						return true
					}
					sql, err := strconv.Unquote(lit.Value)
					if err != nil || !goAPIUsersWrite.MatchString(sql) {
						return true
					}
					sites[rel]++
					insert, update, ok := usersStatementColumns(sql)
					if !ok {
						t.Errorf("%s %s: a users write this census cannot parse: %q", rel, scope, sql)
						return true
					}
					for privilege, columns := range map[string][]string{"INSERT": insert, "UPDATE": update} {
						for _, column := range columns {
							if column == "is_superuser" {
								if !goAPISuperuserWriters[rel+"\t"+scope] {
									t.Errorf("%s %s: %s names is_superuser; the owned posture refuses it", rel, scope, privilege)
								}
								continue
							}
							if !granted[privilege][column] {
								t.Errorf("%s %s: %s (%s) is not in the owned users grant", rel, scope, privilege, column)
							}
						}
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(sites, goAPIUsersWriteSites) {
		t.Errorf("go-api users write statements per file = %v, want %v: check the new statement's columns, then update the count", sites, goAPIUsersWriteSites)
	}
}

// The census parser reads the statement shapes go-api uses and refuses the rest.
func TestUsersStatementColumnsReadsItsShapes(t *testing.T) {
	for sql, want := range map[string][2][]string{
		"INSERT INTO users\n\t(id, email, is_active)\nVALUES ($1, $2, true)":                       {{"id", "email", "is_active"}, nil},
		`UPDATE users SET full_name = $2, last_login_at = $3, updated_at = $3 WHERE id = $1::uuid`: {nil, {"full_name", "last_login_at", "updated_at"}},
		`UPDATE users SET a = COALESCE($4::text, a), b = b + 1 WHERE id = $1`:                      {nil, {"a", "b"}},
		`UPDATE public.users u SET (email, "is_superuser") = ($2, $3) WHERE u.id = $1`:             {nil, {"email", "is_superuser"}},
		`INSERT INTO users (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET is_superuser = true`:    {{"id"}, {"is_superuser"}},
	} {
		insert, update, ok := usersStatementColumns(sql)
		if !ok || !reflect.DeepEqual(insert, want[0]) || !reflect.DeepEqual(update, want[1]) {
			t.Errorf("%q: %v %v %v, want %v %v", sql, insert, update, ok, want[0], want[1])
		}
	}
	for _, sql := range []string{
		`INSERT INTO users (id, is_superuser) SELECT id, true FROM staging`,
		`INSERT INTO users VALUES ($1, true)`,
		`UPDATE users WHERE id = $1`,
	} {
		if _, _, ok := usersStatementColumns(sql); ok {
			t.Errorf("%q: parsed, want refused", sql)
		}
	}
}

// No other runtime role's manifest gives a write of public.users: the domain,
// queue and coordinator roles and query-api read it at most.
func TestNoOtherRuntimePostureWritesUsers(t *testing.T) {
	for name, posture := range map[string]RolePosture{
		"domain": DomainPosture(), "queue": QueuePosture(), "coordinator": CoordinatorPosture(), "query-api": QueryAPIPosture(),
	} {
		for _, table := range posture.RequiredTables {
			if table.TableName == "users" && (table.AllowInsert || table.AllowUpdate || table.AllowDelete) {
				t.Errorf("%s: users = %+v, want read only", name, table)
			}
		}
		for _, column := range posture.ColumnScoped {
			if column.TableName == "users" && column.Privilege != "SELECT" {
				t.Errorf("%s: %s (%s) on users", name, column.Privilege, column.ColumnName)
			}
		}
	}
}
