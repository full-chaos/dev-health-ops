package chdatetime_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// column is a DateTime64 column of one table and whether the table keeps it in a partition key, sort key, primary key or
// skip index; those are the places where ClickHouse prunes granules by the range of the column.
type column struct {
	Table string
	Name  string
	Keys  []string
}

type catalog struct {
	Tables  int
	Columns []column
}

var keyClauses = [][]string{
	{"PARTITION", "BY"}, {"ORDER", "BY"}, {"PRIMARY", "KEY"}, {"SAMPLE", "BY"}, {"TTL"}, {"SETTINGS"}, {"COMMENT"},
}

// clauseAt reports the name of the engine clause that starts at toks[i], and its length in tokens.
func clauseAt(toks []tok, i int) (string, int) {
	for _, words := range keyClauses {
		if i+len(words) > len(toks) {
			continue
		}
		match := true
		for k, w := range words {
			if !toks[i+k].isKeyword(w) {
				match = false
				break
			}
		}
		if match {
			return strings.Join(words, " "), len(words)
		}
	}
	return "", 0
}

func references(toks []tok, column string) bool {
	for _, t := range toks {
		if (t.Kind == tIdent || t.Kind == tQuoted) && t.Text == column {
			return true
		}
	}
	return false
}

func isDateTime64Type(toks []tok) bool {
	if len(toks) > 0 && toks[0].isKeyword("DateTime64") {
		return true
	}
	return len(toks) > 2 && toks[0].isKeyword("Nullable") && toks[1].isPunct("(") && toks[2].isKeyword("DateTime64")
}

// parseTable reads one CREATE TABLE statement with the SQL lexer, so a quoted keyword or a name inside a string or
// comment is never a clause or a reference. It fails loudly on a shape it cannot measure.
func parseTable(name, create string) ([]column, error) {
	toks := lex(create)
	if len(toks) < 4 || !toks[0].isKeyword("CREATE") || !toks[1].isKeyword("TABLE") {
		return nil, fmt.Errorf("%s: not a CREATE TABLE statement", name)
	}
	open := -1
	for i, t := range toks {
		if t.isPunct("(") {
			open = i
			break
		}
	}
	if open < 0 {
		return nil, fmt.Errorf("%s: no column list", name)
	}
	closeAt := matchClose(toks, open)
	if closeAt < 0 {
		return nil, fmt.Errorf("%s: unbalanced column list", name)
	}
	engine := -1
	for i := closeAt + 1; i < len(toks); i++ {
		if toks[i].isKeyword("ENGINE") {
			engine = i
			break
		}
	}
	if engine < 0 {
		return nil, fmt.Errorf("%s: no ENGINE clause", name)
	}
	clauses := map[string][]tok{}
	current, depth := "", 0
	for i := engine; i < len(toks); i++ {
		switch {
		case toks[i].isPunct("("):
			depth++
		case toks[i].isPunct(")"):
			depth--
		}
		if depth == 0 {
			if clause, n := clauseAt(toks, i); n > 0 {
				current, i = clause, i+n-1
				continue
			}
		}
		if current != "" {
			clauses[current] = append(clauses[current], toks[i])
		}
	}
	var indexes [][]tok
	var defs [][]tok
	for _, def := range splitDepth0(toks[open+1:closeAt], func(t tok) bool { return t.isPunct(",") }) {
		switch {
		case len(def) == 0:
			return nil, fmt.Errorf("%s: empty column definition", name)
		case def[0].isKeyword("INDEX"):
			parts := splitDepth0(def[2:], func(t tok) bool { return t.isKeyword("TYPE") })
			if len(def) < 3 || len(parts) < 2 {
				return nil, fmt.Errorf("%s: cannot read index definition", name)
			}
			indexes = append(indexes, parts[0])
		case def[0].isKeyword("CONSTRAINT", "PROJECTION"):
		default:
			defs = append(defs, def)
		}
	}
	var out []column
	for _, def := range defs {
		if def[0].Kind != tIdent && def[0].Kind != tQuoted || len(def) < 2 {
			return nil, fmt.Errorf("%s: cannot read column definition at offset %d", name, def[0].Pos)
		}
		if !isDateTime64Type(def[1:]) {
			continue
		}
		col := column{Table: name, Name: def[0].Text}
		for _, kind := range []string{"PARTITION BY", "ORDER BY", "PRIMARY KEY"} {
			if references(clauses[kind], col.Name) {
				col.Keys = append(col.Keys, kind)
			}
		}
		for _, expr := range indexes {
			if references(expr, col.Name) {
				col.Keys = append(col.Keys, "INDEX")
				break
			}
		}
		out = append(out, col)
	}
	return out, nil
}

// buildCatalog reads head.json; extra holds additional CREATE TABLE statements by table name (fixtures).
func buildCatalog(headJSON []byte, extra map[string]string) (catalog, error) {
	var head struct {
		Objects []struct {
			Name   string `json:"name"`
			Create string `json:"create"`
		} `json:"objects"`
	}
	if err := json.Unmarshal(headJSON, &head); err != nil {
		return catalog{}, fmt.Errorf("head.json: %w", err)
	}
	creates := map[string]string{}
	for _, object := range head.Objects {
		if object.Name == "" {
			return catalog{}, fmt.Errorf("head.json: an object has no name")
		}
		kind := strings.Join(strings.Fields(object.Create), " ")
		switch {
		case strings.HasPrefix(kind, "CREATE TABLE "):
			creates[object.Name] = object.Create
		case strings.HasPrefix(kind, "CREATE MATERIALIZED VIEW "), strings.HasPrefix(kind, "CREATE VIEW "),
			strings.HasPrefix(kind, "CREATE DICTIONARY "):
		default:
			return catalog{}, fmt.Errorf("head.json: object %s has no readable CREATE statement", object.Name)
		}
	}
	for name, create := range extra {
		creates[name] = create
	}
	names := make([]string, 0, len(creates))
	for name := range creates {
		names = append(names, name)
	}
	sort.Strings(names)
	var cat catalog
	for _, name := range names {
		cols, err := parseTable(name, creates[name])
		if err != nil {
			return catalog{}, err
		}
		cat.Tables++
		cat.Columns = append(cat.Columns, cols...)
	}
	return cat, nil
}

func (c catalog) names() map[string]bool {
	out := map[string]bool{}
	for _, col := range c.Columns {
		out[col.Name] = true
	}
	return out
}

// keyNames are the DateTime64 column names that at least one table keeps in a key or index. A name is the unit, not a
// table, because a statement text does not say its table; a collision over-flags and never under-flags.
func (c catalog) keyNames() map[string][]string {
	out := map[string][]string{}
	for _, col := range c.Columns {
		if len(col.Keys) > 0 {
			out[col.Name] = append(out[col.Name], col.Table+":"+strings.Join(col.Keys, "+"))
		}
	}
	return out
}

func (c catalog) has(table, name string) (column, bool) {
	for _, col := range c.Columns {
		if col.Table == table && col.Name == name {
			return col, true
		}
	}
	return column{}, false
}

func loadHead(root string) ([]byte, error) {
	return os.ReadFile(filepath.Join(root, "internal", "chmigrate", "baseline", "head.json"))
}
