package chartgates_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"regexp"
	"strconv"
	"strings"
)

// A guard that matches raw file text passes on a comment, an echo or a quoted string that merely names the
// thing it looks for. These helpers give the guards a parsed field to read instead: Go source through
// go/parser (comments are not nodes), Dockerfiles as instruction lists, templates with their comments removed.

func (g *guard) goFile(rel string) *ast.File {
	f, err := parser.ParseFile(token.NewFileSet(), g.path(rel), nil, 0)
	if err != nil {
		g.failf("cannot parse %s: %v", rel, err)
		return nil
	}
	return f
}

func strLit(e ast.Expr) (string, bool) {
	bl, ok := e.(*ast.BasicLit)
	if !ok || bl.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(bl.Value)
	if err != nil {
		return "", false
	}
	return s, true
}

// keyedFields returns the key: value pairs of a composite literal, keyed by identifier.
func keyedFields(cl *ast.CompositeLit) map[string]ast.Expr {
	out := map[string]ast.Expr{}
	for _, e := range cl.Elts {
		if kv, ok := e.(*ast.KeyValueExpr); ok {
			if id, ok := kv.Key.(*ast.Ident); ok {
				out[id.Name] = kv.Value
			}
		}
	}
	return out
}

func isTrue(e ast.Expr) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == "true"
}

// stringLiterals returns every string literal in a file, with its value.
func stringLiterals(f *ast.File) []string {
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		if e, ok := n.(ast.Expr); ok {
			if s, ok := strLit(e); ok {
				out = append(out, s)
			}
		}
		return true
	})
	return out
}

// funcDecl finds a method or function by name; recv, when non-empty, must be the receiver's type name.
func funcDecl(f *ast.File, name, recv string) *ast.FuncDecl {
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != name || fd.Body == nil {
			continue
		}
		if recv == "" {
			return fd
		}
		if fd.Recv == nil || len(fd.Recv.List) == 0 {
			continue
		}
		t := fd.Recv.List[0].Type
		if st, ok := t.(*ast.StarExpr); ok {
			t = st.X
		}
		if id, ok := t.(*ast.Ident); ok && id.Name == recv {
			return fd
		}
	}
	return nil
}

// reachableStmts are the function body's top-level statements up to the first unconditional return: code
// after it never runs, so a metric written there is not emitted.
func reachableStmts(fd *ast.FuncDecl) []ast.Stmt {
	var out []ast.Stmt
	for _, s := range fd.Body.List {
		if _, ret := s.(*ast.ReturnStmt); ret {
			break
		}
		out = append(out, s)
	}
	return out
}

func stmtsLiterals(stmts []ast.Stmt) []string {
	var out []string
	for _, s := range stmts {
		ast.Inspect(s, func(n ast.Node) bool {
			if e, ok := n.(ast.Expr); ok {
				if v, ok := strLit(e); ok {
					out = append(out, v)
				}
			}
			return true
		})
	}
	return out
}

// emitsMetric reports whether the reachable part of fd writes the metric as a sample line or hands its
// exact name to a writer helper.
func emitsMetric(fd *ast.FuncDecl, metric string) bool {
	sample := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(metric) + `[ {]`)
	for _, lit := range stmtsLiterals(reachableStmts(fd)) {
		if lit == metric || sample.MatchString(lit) {
			return true
		}
	}
	return false
}

// --- Dockerfile ---

type dockerInstr struct {
	stage string
	cmd   string
	args  string
}

func parseDockerfile(text string) []dockerInstr {
	var out []dockerInstr
	var logical []string
	var cur strings.Builder
	for _, line := range strings.Split(text, "\n") {
		t := strings.TrimSpace(line)
		if cur.Len() == 0 && (t == "" || strings.HasPrefix(t, "#")) {
			continue
		}
		if strings.HasPrefix(t, "#") {
			continue
		}
		if strings.HasSuffix(t, `\`) {
			cur.WriteString(strings.TrimSuffix(t, `\`) + " ")
			continue
		}
		cur.WriteString(t)
		logical = append(logical, cur.String())
		cur.Reset()
	}
	stage := ""
	for _, l := range logical {
		cmd, args, _ := strings.Cut(l, " ")
		cmd = strings.ToUpper(cmd)
		args = strings.TrimSpace(args)
		if cmd == "FROM" {
			fields := strings.Fields(args)
			stage = ""
			for i, f := range fields {
				if strings.EqualFold(f, "as") && i+1 < len(fields) {
					stage = fields[i+1]
				}
			}
		}
		out = append(out, dockerInstr{stage: stage, cmd: cmd, args: args})
	}
	return out
}

// runCommands splits every RUN instruction into its shell commands (on ; and &&).
func runCommands(ins []dockerInstr) []string {
	var out []string
	for _, i := range ins {
		if i.cmd != "RUN" {
			continue
		}
		for _, part := range strings.FieldsFunc(strings.ReplaceAll(i.args, "&&", ";"), func(r rune) bool { return r == ';' }) {
			if c := strings.TrimSpace(part); c != "" {
				out = append(out, strings.Join(strings.Fields(c), " "))
			}
		}
	}
	return out
}

func hasInstr(ins []dockerInstr, cmd, args string) bool {
	for _, i := range ins {
		if i.cmd == cmd && strings.Join(strings.Fields(i.args), " ") == strings.Join(strings.Fields(args), " ") {
			return true
		}
	}
	return false
}

func hasRunCommand(ins []dockerInstr, command string) bool {
	for _, c := range runCommands(ins) {
		if c == command {
			return true
		}
	}
	return false
}

func hasStageInstr(ins []dockerInstr, stage, cmd, args string) bool {
	for _, i := range ins {
		if i.stage == stage && i.cmd == cmd && strings.Join(strings.Fields(i.args), " ") == args {
			return true
		}
	}
	return false
}

// --- templates ---

var tplComment = regexp.MustCompile(`(?s)\{\{-?\s*/\*.*?\*/\s*-?\}\}`)

// tplCode is a template's text with its {{/* */}} comments and its # comment lines removed.
func tplCode(text string) string {
	text = tplComment.ReplaceAllString(text, "")
	var keep []string
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "#") {
			continue
		}
		keep = append(keep, l)
	}
	return strings.Join(keep, "\n")
}
