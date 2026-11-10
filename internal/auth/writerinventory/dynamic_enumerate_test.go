package writerinventory

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// The table sets of the three dynamic writers, read from the data that names their tables.

const (
	restoreFenceSpecs   = "internal/auth/restorefence/specs.go"
	restoreFenceApply   = "section.applySQL"
	orgDeletionFile     = "internal/apiservice/admin/orgdeletion.go"
	orgDeletionTargets  = "internal/apiservice/admin/orgdeletion_targets.go"
	orgDeletionDelete   = "deletionTarget.deleteSQL"
	userDeleteFile      = "internal/apiservice/admin/userdelete.go"
	userDeleteForce     = "pgStore.forceDeleteUser"
	pgBaselineHead      = "internal/pgmigrate/baseline/head.json"
	pgPostBaselineSQLs  = "internal/pgmigrate/sql"
	restoreFenceSection = "sections"
)

// packageVarElems returns the elements of the composite literal assigned to the package-level var name in file.
func packageVarElems(root, file, name string) ([]*ast.CompositeLit, error) {
	af, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, file), nil, 0)
	if err != nil {
		return nil, err
	}
	for _, d := range af.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, sp := range gd.Specs {
			vs := sp.(*ast.ValueSpec)
			for i, n := range vs.Names {
				if n.Name != name || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.CompositeLit)
				if !ok {
					return nil, fmt.Errorf("%s: %s is not a composite literal", file, name)
				}
				var out []*ast.CompositeLit
				for _, el := range lit.Elts {
					c, ok := el.(*ast.CompositeLit)
					if !ok {
						return nil, fmt.Errorf("%s: an element of %s is not a composite literal", file, name)
					}
					out = append(out, c)
				}
				return out, nil
			}
		}
	}
	return nil, fmt.Errorf("%s: no package-level var %s", file, name)
}

// field returns the value of key in a keyed composite literal, or nil.
func field(c *ast.CompositeLit, key string) ast.Expr {
	for _, el := range c.Elts {
		if kv, ok := el.(*ast.KeyValueExpr); ok {
			if k, ok := kv.Key.(*ast.Ident); ok && k.Name == key {
				return kv.Value
			}
		}
	}
	return nil
}

func stringField(c *ast.CompositeLit, key string) (string, error) {
	lit, ok := field(c, key).(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", fmt.Errorf("field %s is not a string literal", key)
	}
	return strconv.Unquote(lit.Value)
}

// restoreFenceVerb is the verb section.applySQL runs for a section kind; "" for a kind that writes nothing.
var restoreFenceVerb = map[string]string{
	"kindRevoke": "UPDATE", "kindCounter": "UPDATE", "kindRetire": "UPDATE", "kindTombstone": "UPDATE",
	"kindInsert": "INSERT", "kindPresence": "DELETE", "kindReport": "",
}

// restoreFenceSectionWrites reads restorefence's sections: each writes <schema>.<table> (public when legacy, else auth)
// with its kind's verb.
func restoreFenceSectionWrites(root string) (map[string]bool, error) {
	elems, err := packageVarElems(root, restoreFenceSpecs, restoreFenceSection)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, c := range elems {
		table, err := stringField(c, "table")
		if err != nil {
			return nil, fmt.Errorf("%s: %w", restoreFenceSpecs, err)
		}
		k, ok := field(c, "kind").(*ast.Ident)
		if !ok {
			return nil, fmt.Errorf("%s: section %s has no kind identifier", restoreFenceSpecs, table)
		}
		verb, known := restoreFenceVerb[k.Name]
		if !known {
			return nil, fmt.Errorf("%s: section %s has kind %s, which restoreFenceVerb does not map", restoreFenceSpecs, table, k.Name)
		}
		schema := "auth"
		switch l := field(c, "legacy").(type) {
		case nil:
		case *ast.Ident:
			if l.Name == "true" {
				schema = "public"
			} else if l.Name != "false" {
				return nil, fmt.Errorf("%s: section %s legacy is %s", restoreFenceSpecs, table, l.Name)
			}
		default:
			return nil, fmt.Errorf("%s: section %s legacy is not a literal", restoreFenceSpecs, table)
		}
		if verb != "" {
			out[schema+"."+table+"|"+verb] = true
		}
	}
	return out, nil
}

// restoreFenceSectionTexts returns the source text, whitespace collapsed, of every section of schema.table (public when
// legacy, else auth), whatever its kind.
func restoreFenceSectionTexts(root, qualified string) ([]string, error) {
	path := filepath.Join(root, restoreFenceSpecs)
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	af, err := parser.ParseFile(fset, path, src, 0)
	if err != nil {
		return nil, err
	}
	var out []string
	var walkErr error
	ast.Inspect(af, func(n ast.Node) bool {
		vs, ok := n.(*ast.ValueSpec)
		if !ok || len(vs.Names) != 1 || vs.Names[0].Name != restoreFenceSection || len(vs.Values) != 1 {
			return true
		}
		lit, ok := vs.Values[0].(*ast.CompositeLit)
		if !ok {
			walkErr = fmt.Errorf("%s: %s is not a composite literal", restoreFenceSpecs, restoreFenceSection)
			return false
		}
		for _, el := range lit.Elts {
			c, ok := el.(*ast.CompositeLit)
			if !ok {
				walkErr = fmt.Errorf("%s: an element of %s is not a composite literal", restoreFenceSpecs, restoreFenceSection)
				return false
			}
			table, err := stringField(c, "table")
			if err != nil {
				walkErr = err
				return false
			}
			schema := "auth"
			if l, ok := field(c, "legacy").(*ast.Ident); ok && l.Name == "true" {
				schema = "public"
			}
			if schema+"."+table == qualified {
				out = append(out, strings.Join(strings.Fields(string(src[fset.Position(c.Pos()).Offset:fset.Position(c.End()).Offset])), " "))
			}
		}
		return false
	})
	return out, walkErr
}

// orgDeletionTargetWrites reads orgDeletionTargets: deleteSQL deletes from each public table.
func orgDeletionTargetWrites(root string) (map[string]bool, error) {
	elems, err := packageVarElems(root, orgDeletionTargets, "orgDeletionTargets")
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, c := range elems {
		table, err := stringField(c, "Table")
		if err != nil {
			return nil, fmt.Errorf("%s: %w", orgDeletionTargets, err)
		}
		if strings.Contains(table, ".") {
			return nil, fmt.Errorf("%s: target %s is schema-qualified; the enumeration reads bare public names", orgDeletionTargets, table)
		}
		out["public."+table+"|DELETE"] = true
	}
	return out, nil
}

var (
	userFKRe        = regexp.MustCompile(`ALTER TABLE ONLY ([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)\s+ADD CONSTRAINT \S+ FOREIGN KEY \(([^)]*)\) REFERENCES public\.users\(id\)([^;]*);`)
	userFKActionRe  = regexp.MustCompile(`ON DELETE (CASCADE|SET NULL|SET DEFAULT|RESTRICT|NO ACTION)`)
	anyUsersRefRe   = regexp.MustCompile(`(?i)REFERENCES\s+("?public"?\s*\.\s*)?"?users"?\s*\(`)
	userFKBaseCheck = regexp.MustCompile(`REFERENCES\s+\S*users`)
)

// userDeleteReferenceWrites reads the go-api schema baseline: forceDeleteUser sets to NULL every single-column foreign key
// to users(id) whose delete action is NO ACTION or RESTRICT and whose column takes NULL (CASCADE and SET NULL are the
// database's, a NOT NULL or SET DEFAULT key refuses the delete). A migration after the baseline that adds a users
// reference fails the read: the enumeration must learn it first.
func userDeleteReferenceWrites(root string) (map[string]bool, error) {
	raw, err := os.ReadFile(filepath.Join(root, "internal", "pgmigrate", "baseline", "head.json"))
	if err != nil {
		return nil, err
	}
	var head struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, err
	}
	matches := userFKRe.FindAllStringSubmatch(head.Schema, -1)
	if len(matches) == 0 || len(matches) != len(userFKBaseCheck.FindAllString(head.Schema, -1)) {
		return nil, fmt.Errorf("%s: the foreign keys to users read %d of %d references", pgBaselineHead, len(matches), len(userFKBaseCheck.FindAllString(head.Schema, -1)))
	}
	out := map[string]bool{}
	for _, m := range matches {
		schema, table, cols, rest := m[1], m[2], m[3], m[4]
		action := "NO ACTION"
		if a := userFKActionRe.FindStringSubmatch(rest); a != nil {
			action = a[1]
		}
		if strings.Contains(cols, ",") || (action != "NO ACTION" && action != "RESTRICT") {
			continue
		}
		notNull, err := baselineColumnNotNull(head.Schema, schema, table, strings.TrimSpace(cols))
		if err != nil {
			return nil, err
		}
		if !notNull {
			out[schema+"."+table+"|UPDATE"] = true
		}
	}
	files, err := filepath.Glob(filepath.Join(root, "internal", "pgmigrate", "sql", "*.sql"))
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		if anyUsersRefRe.Match(b) {
			return nil, fmt.Errorf("%s/%s adds a reference to users after the baseline; teach userDeleteReferenceWrites to read it", pgPostBaselineSQLs, filepath.Base(f))
		}
	}
	return out, nil
}

// baselineColumnNotNull reports whether column of schema.table is NOT NULL in the pg_dump text.
func baselineColumnNotNull(dump, schema, table, column string) (bool, error) {
	start := strings.Index(dump, "CREATE TABLE "+schema+"."+table+" (\n")
	if start < 0 {
		return false, fmt.Errorf("%s: no CREATE TABLE %s.%s", pgBaselineHead, schema, table)
	}
	end := strings.Index(dump[start:], "\n);")
	if end < 0 {
		return false, fmt.Errorf("%s: CREATE TABLE %s.%s does not end", pgBaselineHead, schema, table)
	}
	for _, line := range strings.Split(dump[start:start+end], "\n")[1:] {
		f := strings.Fields(line)
		if len(f) > 0 && strings.Trim(f[0], `"`) == column {
			return strings.Contains(line, "NOT NULL"), nil
		}
	}
	return false, fmt.Errorf("%s: %s.%s has no column %s", pgBaselineHead, schema, table, column)
}

// watchedOnly keeps the Table|Verb pairs whose table is watched.
func watchedOnly(set map[string]bool, watched map[string]bool) map[string]bool {
	out := map[string]bool{}
	for k := range set {
		if watched[strings.SplitN(k, "|", 2)[0]] {
			out[k] = true
		}
	}
	return out
}
