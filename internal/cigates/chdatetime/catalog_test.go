package chdatetime_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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

var (
	clauseRe     = regexp.MustCompile(`\b(PARTITION BY|ORDER BY|PRIMARY KEY|SAMPLE BY|TTL|SETTINGS|COMMENT)\b`)
	identRe      = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)
	columnDefRe  = regexp.MustCompile("^`(\\w+)` (.+)$")
	indexDefRe   = regexp.MustCompile(`^INDEX (\w+) (.+) TYPE (\w+)`)
	nonColumnRe  = regexp.MustCompile(`^(INDEX|CONSTRAINT|PROJECTION)\b`)
	dateTime64Re = regexp.MustCompile(`^(Nullable\()?DateTime64\(`)
)

// splitTop splits text on commas that sit outside parentheses and quotes.
func splitTop(text string) []string {
	var parts []string
	depth, start, quote := 0, 0, false
	for i := 0; i < len(text); i++ {
		switch c := text[i]; {
		case quote && c == '\\':
			i++
		case c == '\'':
			quote = !quote
		case quote:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			parts = append(parts, strings.TrimSpace(text[start:i]))
			start = i + 1
		}
	}
	return append(parts, strings.TrimSpace(text[start:]))
}

// matchParen returns the index of the parenthesis that closes the one at open.
func matchParen(text string, open int) int {
	depth, quote := 0, false
	for i := open; i < len(text); i++ {
		switch c := text[i]; {
		case quote && c == '\\':
			i++
		case c == '\'':
			quote = !quote
		case quote:
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// parseTable reads one CREATE TABLE statement. It fails loudly on a shape it cannot measure.
func parseTable(name, create string) ([]column, error) {
	open := strings.Index(create, "(")
	if !strings.HasPrefix(create, "CREATE TABLE ") || open < 0 {
		return nil, fmt.Errorf("%s: not a CREATE TABLE statement", name)
	}
	closeAt := matchParen(create, open)
	if closeAt < 0 {
		return nil, fmt.Errorf("%s: unbalanced column list", name)
	}
	rest := create[closeAt+1:]
	engineAt := strings.Index(rest, "ENGINE = ")
	if engineAt < 0 {
		return nil, fmt.Errorf("%s: no ENGINE clause", name)
	}
	engine := rest[engineAt:]
	clauses := map[string]string{}
	locs := clauseRe.FindAllStringIndex(engine, -1)
	for i, loc := range locs {
		end := len(engine)
		if i+1 < len(locs) {
			end = locs[i+1][0]
		}
		clauses[engine[loc[0]:loc[1]]] += " " + engine[loc[1]:end]
	}
	keyText := map[string]string{
		"PARTITION BY": clauses["PARTITION BY"],
		"ORDER BY":     clauses["ORDER BY"],
		"PRIMARY KEY":  clauses["PRIMARY KEY"],
	}
	var defs, indexes []string
	for _, def := range splitTop(create[open+1 : closeAt]) {
		switch {
		case indexDefRe.MatchString(def):
			indexes = append(indexes, indexDefRe.FindStringSubmatch(def)[2])
		case nonColumnRe.MatchString(def):
		default:
			defs = append(defs, def)
		}
	}
	var out []column
	for _, def := range defs {
		m := columnDefRe.FindStringSubmatch(def)
		if m == nil {
			return nil, fmt.Errorf("%s: cannot read column definition %q", name, def)
		}
		if !dateTime64Re.MatchString(m[2]) {
			continue
		}
		col := column{Table: name, Name: m[1]}
		for _, kind := range []string{"PARTITION BY", "ORDER BY", "PRIMARY KEY"} {
			if mentions(keyText[kind], m[1]) {
				col.Keys = append(col.Keys, kind)
			}
		}
		for _, expr := range indexes {
			if mentions(expr, m[1]) {
				col.Keys = append(col.Keys, "INDEX")
				break
			}
		}
		out = append(out, col)
	}
	if got, want := len(out), strings.Count(create[open:closeAt], "DateTime64("); got > want {
		return nil, fmt.Errorf("%s: parsed %d DateTime64 columns from %d DateTime64 types", name, got, want)
	}
	return out, nil
}

func mentions(expr, column string) bool {
	for _, id := range identRe.FindAllString(expr, -1) {
		if id == column {
			return true
		}
	}
	return false
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
		if strings.HasPrefix(object.Create, "CREATE TABLE ") {
			creates[object.Name] = object.Create
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
