// Package passwordwriters is the census of the module's SQL that writes a user's
// password hash. The reset-link binding census (internal/auth/identity/local) and
// the shared-policy census (internal/auth/passwordpolicy) read the same matches.
package passwordwriters

import (
	"go/ast"
	"go/token"
	"regexp"
	"strconv"
	"strings"
)

// Table names the users table a site writes.
type Table string

const (
	// Public is go-api's users table: `users` or `public.users`.
	Public Table = "public"
	// Auth is auth-service's users table: `auth.users`, or `<schema>.users` with
	// the schema name joined in at run time (`UPDATE `+s.q+`.users ...`).
	Auth Table = "auth"
)

// Kind is the statement of a site.
type Kind string

const (
	Update Kind = "update"
	Insert Kind = "insert"
)

// A Site is one SQL text that writes password_hash of a users table. File is
// the path the caller gave; Line is the line of the literal
// (or of the joined expression) in that file. SQL is the text as matched; a part
// joined in at run time reads as RuntimePart.
type Site struct {
	File  string
	Line  int
	Table Table
	Kind  Kind
	SQL   string
}

// RuntimePart stands in SQL for an operand of a + join that is not a string literal.
const RuntimePart = "\x00"

var (
	PublicUpdate = regexp.MustCompile(`(?is)^\s*UPDATE\s+(public\.)?users\s+SET\b.*\bpassword_hash\s*=`)
	PublicInsert = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+(public\.)?users\s*\(.*\bpassword_hash\b`)
	AuthUpdate   = regexp.MustCompile(`(?is)^\s*UPDATE\s+(auth|\x00)\.users\s+SET\b.*\bpassword_hash\s*=`)
	AuthInsert   = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+(auth|\x00)\.users\s*\(.*\bpassword_hash\b`)
)

// Match reports the table and kind of one SQL text, or ok=false.
func Match(sql string) (table Table, kind Kind, ok bool) {
	switch {
	case PublicUpdate.MatchString(sql):
		return Public, Update, true
	case PublicInsert.MatchString(sql):
		return Public, Insert, true
	case AuthUpdate.MatchString(sql):
		return Auth, Update, true
	case AuthInsert.MatchString(sql):
		return Auth, Insert, true
	}
	return "", "", false
}

// FileSites returns the sites of one parsed file; rel is the slash-separated path
// a Site carries. Each string literal is matched alone; each outermost + join that
// holds a string literal is matched once more as one text, a non-literal part
// read as RuntimePart. The caller walks the files (the read stays in its test).
func FileSites(fset *token.FileSet, file *ast.File, rel string) []Site {
	var sites []Site
	add := func(pos token.Pos, sql string) {
		if table, kind, ok := Match(sql); ok {
			sites = append(sites, Site{File: rel, Line: fset.Position(pos).Line, Table: table, Kind: kind, SQL: sql})
		}
	}
	var visit func(n ast.Node) bool
	visit = func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BasicLit:
			if x.Kind == token.STRING {
				if sql, err := strconv.Unquote(x.Value); err == nil {
					add(x.Pos(), sql)
				}
			}
		case *ast.BinaryExpr:
			if x.Op != token.ADD {
				return true
			}
			if sql, joined := join(x); joined {
				add(x.Pos(), sql)
			}
			var operands []ast.Expr
			flatten(x, &operands)
			for _, o := range operands {
				ast.Inspect(o, visit)
			}
			return false
		}
		return true
	}
	ast.Inspect(file, visit)
	return sites
}

func flatten(e ast.Expr, out *[]ast.Expr) {
	if b, ok := e.(*ast.BinaryExpr); ok && b.Op == token.ADD {
		flatten(b.X, out)
		flatten(b.Y, out)
		return
	}
	if p, ok := e.(*ast.ParenExpr); ok {
		flatten(p.X, out)
		return
	}
	*out = append(*out, e)
}

// join renders a + join; joined is false when it holds no string literal.
func join(b *ast.BinaryExpr) (sql string, joined bool) {
	var operands []ast.Expr
	flatten(b, &operands)
	var text strings.Builder
	literal := false
	for _, o := range operands {
		if lit, ok := o.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			s, err := strconv.Unquote(lit.Value)
			if err != nil {
				return "", false
			}
			text.WriteString(s)
			literal = true
			continue
		}
		text.WriteString(RuntimePart)
	}
	return text.String(), literal
}
