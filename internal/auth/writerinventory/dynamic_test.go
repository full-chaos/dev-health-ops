package writerinventory

import (
	"go/ast"
	"go/token"
	"sort"
	"strings"
)

// DynamicSite is a call site whose SQL writes a table the scan cannot name: the target of a watched verb is a format
// directive (%s) or a runtime value. Key() is File|Func|Verb. Named holds the literal table names that follow a runtime
// schema (%s.table), which the site's rows must cover.
type DynamicSite struct {
	File, Func, Verb string
	Named            []string
}

func (d DynamicSite) Key() string { return d.File + "|" + d.Func + "|" + d.Verb }

// dynHit is one statement with an unresolved target; table is the literal name after a runtime schema, or "".
type dynHit struct {
	verb, table string
}

// unresolvedTarget reports whether ts[j] starts a table name the scan cannot read: a format directive (%s, %[1]s), or a
// runtime value that is not the auth-schema prefix form qname resolves (\x00.table). It returns the index after the
// target and the literal table name that follows a runtime schema ("" if none).
func unresolvedTarget(ts []tok, j int) (next int, table string, ok bool) {
	if j >= len(ts) || ts[j].q {
		return j, "", false
	}
	switch ts[j].s {
	case unresolved:
		if j+2 < len(ts) && kw(ts, j+1, ".") && isIdent(ts[j+2]) {
			return j, "", false
		}
		return j + 1, "", true
	case "%":
		k := j + 1
		if kw(ts, k, "[") {
			for k < len(ts) && !kw(ts, k, "]") {
				k++
			}
			k++
		}
		if k >= len(ts) || ts[k].q || !isIdent(ts[k]) {
			return j, "", false
		}
		k++
		if kw(ts, k, ".") && k+1 < len(ts) && isIdent(ts[k+1]) {
			return k + 2, ts[k+1].s, true
		}
		return k, "", true
	}
	return j, "", false
}

// skipAlias skips AS and one alias name.
func skipAlias(ts []tok, j int) int {
	if kw(ts, j, "as") {
		j++
	}
	if j < len(ts) && isIdent(ts[j]) && !kw(ts, j, "set") && !kw(ts, j, "where") && !kw(ts, j, "using") && !kw(ts, j, "returning") {
		j++
	}
	return j
}

func endsStatement(ts []tok, j int, words ...string) bool {
	if j >= len(ts) || kw(ts, j, ";") || kw(ts, j, ")") {
		return true
	}
	for _, w := range words {
		if kw(ts, j, w) {
			return true
		}
	}
	return false
}

// unresolvedVerbs returns the statements in sql whose target table the scan cannot name. Prose such as an error text
// ("delete from %s: %w") is not a statement: the target must be followed by what that statement takes next.
func unresolvedVerbs(sql string) []dynHit {
	ts := sqlTokens(sql)
	var out []dynHit
	skipOnly := func(i int) int {
		if kw(ts, i, "only") {
			return i + 1
		}
		return i
	}
	for i := range ts {
		if ts[i].q {
			continue
		}
		switch ts[i].s {
		case "insert":
			if !kw(ts, i+1, "into") {
				continue
			}
			j, table, ok := unresolvedTarget(ts, skipOnly(i+2))
			if !ok {
				continue
			}
			if kw(ts, j, "as") && j+1 < len(ts) && isIdent(ts[j+1]) {
				j += 2
			}
			if !endsStatement(ts, j, "(", "values", "select", "default", "overriding", "with") {
				continue
			}
			verb := "INSERT"
			conflict := false
			for k := j; k < len(ts); k++ {
				if kw(ts, k, ";") || (kw(ts, k, "insert") && kw(ts, k+1, "into")) {
					break
				}
				if kw(ts, k, "on") && kw(ts, k+1, "conflict") {
					conflict = true
				}
				if conflict && kw(ts, k, "do") && kw(ts, k+1, "update") {
					verb = "UPSERT"
					break
				}
			}
			out = append(out, dynHit{verb, table})
		case "update":
			if kw(ts, i-1, "do") || kw(ts, i-1, "for") {
				continue
			}
			j, table, ok := unresolvedTarget(ts, skipOnly(i+1))
			if ok && kw(ts, skipAlias(ts, j), "set") {
				out = append(out, dynHit{"UPDATE", table})
			}
		case "delete":
			if !kw(ts, i+1, "from") {
				continue
			}
			j, table, ok := unresolvedTarget(ts, skipOnly(i+2))
			if ok && endsStatement(ts, skipAlias(ts, j), "where", "using", "returning") {
				out = append(out, dynHit{"DELETE", table})
			}
		case "truncate":
			j := i + 1
			if kw(ts, j, "table") {
				j++
			}
			for {
				j = skipOnly(j)
				if next, table, ok := unresolvedTarget(ts, j); ok {
					if endsStatement(ts, next, ",", "cascade", "restrict", "restart", "continue") {
						out = append(out, dynHit{"TRUNCATE", table})
					}
					break
				}
				_, _, next, ok := qname(ts, j)
				if !ok || !kw(ts, next, ",") {
					break
				}
				j = next + 1
			}
		}
	}
	return out
}

// walkDynamic is walkStrings for whole statements only: the operands of a sum are fragments of it (a prefix that ends at a
// runtime table name is not a statement of its own), so only the calls, map fields and named values inside them start new
// strings.
func walkDynamic(root ast.Node, ev *evaluator, fn func(string)) {
	ast.Inspect(root, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.BasicLit:
			if x.Kind == token.STRING {
				fn(ev.eval(x))
			}
			return false
		case *ast.BinaryExpr:
			if x.Op != token.ADD {
				return true
			}
			fn(ev.eval(x))
			for _, op := range []ast.Expr{x.X, x.Y} {
				ast.Inspect(op, func(m ast.Node) bool {
					switch m.(type) {
					case *ast.BasicLit:
						return false
					case *ast.BinaryExpr, *ast.ParenExpr:
						return true
					case ast.Expr:
						walkDynamic(m, ev, fn)
						return false
					}
					return true
				})
			}
			return false
		case *ast.CallExpr:
			fn(ev.eval(x))
		case *ast.SelectorExpr:
			base := x.X
			for {
				if ix, ok := base.(*ast.IndexExpr); ok {
					base = ix.X
				} else if pe, ok := base.(*ast.ParenExpr); ok {
					base = pe.X
				} else {
					break
				}
			}
			id, ok := base.(*ast.Ident)
			if !ok {
				return true
			}
			d, isLocal, ok := ev.lookup(id)
			if !ok || isStringExpr(d.expr) {
				return true
			}
			var found bool
			ast.Inspect(d.expr, func(m ast.Node) bool {
				if kv, ok := m.(*ast.KeyValueExpr); ok {
					if k, ok := kv.Key.(*ast.Ident); ok && k.Name == x.Sel.Name {
						found = true
						walkDynamic(kv.Value, ev, fn)
						return false
					}
				}
				return true
			})
			if !found {
				return true
			}
			if ev.inFunc && !isLocal {
				ev.pkg.used[id.Name] = true
			}
			return false
		case *ast.Ident:
			d, isLocal, ok := ev.lookup(x)
			if !ok || ev.visiting[x.Name] {
				return true
			}
			if !isLocal && ev.inFunc {
				ev.pkg.used[x.Name] = true
			}
			ev.visiting[x.Name] = true
			walkDynamic(d.expr, ev, fn)
			delete(ev.visiting, x.Name)
		}
		return true
	})
}

// ScanDynamic returns every call site in the root module whose SQL has a watched verb on a table the scan cannot name.
func ScanDynamic(root string) ([]DynamicSite, error) {
	seen := map[string]*DynamicSite{}
	named := map[string]map[string]bool{}
	_, err := scanWalk(root, map[string]bool{}, nil, walkDynamic, func(file, fn, sql string) {
		for _, h := range unresolvedVerbs(sql) {
			d := DynamicSite{File: file, Func: fn, Verb: h.verb}
			if seen[d.Key()] == nil {
				seen[d.Key()] = &d
				named[d.Key()] = map[string]bool{}
			}
			if h.table != "" {
				named[d.Key()][h.table] = true
			}
		}
	})
	if err != nil {
		return nil, err
	}
	out := make([]DynamicSite, 0, len(seen))
	for k, d := range seen {
		for t := range named[k] {
			d.Named = append(d.Named, t)
		}
		sort.Strings(d.Named)
		out = append(out, *d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out, nil
}

// dynamicProblems compares the found sites with the rows (File, Func, Table, Verb: one row per watched table a site
// writes) and the ClickHouse sites (no Postgres table). A site with no row or ClickHouse entry, a row or entry whose site is
// gone, and a site whose runtime-schema table (%s.table) has no row are each one problem.
func dynamicProblems(found []DynamicSite, rows []Entry, clickhouse []DynamicSite) []string {
	tables := map[string]map[string]bool{}
	for _, e := range rows {
		k := DynamicSite{File: e.File, Func: e.Func, Verb: e.Verb}.Key()
		if tables[k] == nil {
			tables[k] = map[string]bool{}
		}
		tables[k][e.Table] = true
	}
	ch := map[string]bool{}
	for _, d := range clickhouse {
		ch[d.Key()] = true
	}
	var problems []string
	sites := map[string]bool{}
	for _, d := range found {
		k := d.Key()
		sites[k] = true
		if tables[k] == nil && !ch[k] {
			problems = append(problems, "unresolved table name in a watched-verb statement, site not listed with its table set: "+k)
			continue
		}
		for _, n := range d.Named {
			if !tables[k]["auth."+n] && !tables[k]["public."+n] {
				problems = append(problems, "site "+k+" writes %s."+n+" but no row names that table")
			}
		}
	}
	for k := range tables {
		if !sites[k] {
			problems = append(problems, "stale dynamic row (no such site in the tree): "+k)
		}
	}
	for k := range ch {
		if !sites[k] {
			problems = append(problems, "stale ClickHouse site (no such site in the tree): "+k)
		}
		if tables[k] != nil {
			problems = append(problems, "site "+k+" is both a ClickHouse site and a Postgres row")
		}
	}
	sort.Strings(problems)
	return problems
}

// siteRows returns the Table|Verb pairs the rows hold for one site file and function.
func siteRows(rows []Entry, file, fn string) []string {
	var out []string
	for _, e := range rows {
		if e.File == file && e.Func == fn {
			out = append(out, e.Table+"|"+e.Verb)
		}
	}
	sort.Strings(out)
	return out
}

func joinSorted(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sameList(a, b []string) bool { return strings.Join(a, "\n") == strings.Join(b, "\n") }
