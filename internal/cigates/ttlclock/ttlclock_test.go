// Package ttlclock is the CI guard that an integration test writing a ClickHouse table with a TTL takes its
// dates from the clock, not from a literal. A row whose TTL column + interval is before the server's now() is
// deleted by the next merge, so a frozen date in such a test turns into a short read on the day it ages out.
//
// The TTL tables come from the migrations (baseline/head.json, then sql/*.sql in order). A finding is a date
// literal before 2090 (time.Date with a year literal, or a YYYY-MM-DD string literal; the Go layout 2006-01-02 is
// not a date) in a //go:build integration test file whose string literals name a TTL table. A finding needs an
// allow.tsv row: file, date, table, expiry, names, where, reason. A row naming a TTL table carries the expiry the
// guard computes (date + TTL, through the UInt32 wrap of toDateTime) and FAILS once that expiry is within 180 days
// of today. A row with table "-" and expiry "never" is a literal the reason says never reaches a TTL table. names
// (the TTL tables the file names, comma-separated) and where (each occurrence as line:function) pin what was
// reviewed: a change to either makes the row STALE. An unlisted finding, a row without a reason, a wrong expiry, a
// duplicate row and a row that matches nothing FAIL. Only dates are compared; nothing measures time.
package ttlclock

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	allowFile    = "allow.tsv"
	headFile     = "internal/chmigrate/baseline/head.json"
	sqlDir       = "internal/chmigrate/sql"
	futureYear   = 2090
	shelfDays    = 180
	goLayoutDate = "2006-01-02"
	dateLayout   = "2006-01-02"
	never        = "never"
)

// ttlTable is one table's retention; wrap is true when the TTL column passes through toDateTime (UInt32 seconds).
type ttlTable struct {
	days int
	wrap bool
}

var (
	ttlPrecise   = regexp.MustCompile("(?is)\\bTTL\\s+(toDateTime\\(\\s*)?`?(\\w+)`?\\s*(\\))?\\s*\\+\\s*(?:toIntervalDay\\(\\s*(\\d+)\\s*\\)|INTERVAL\\s+(\\d+)\\s+DAY)(\\s+DELETE)?")
	ttlWord      = regexp.MustCompile(`(?i)\bTTL\b`)
	ttlTail      = regexp.MustCompile(`(?i)^\s*(?:SETTINGS\b|COMMENT\b|$)`)
	engineWord   = regexp.MustCompile(`(?i)\bENGINE\b`)
	createTable  = regexp.MustCompile("(?is)^\\s*CREATE\\s+(?:OR\\s+REPLACE\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(?:`?\\w+`?\\.)?`?(\\w+)`?")
	dropTable    = regexp.MustCompile("(?is)^\\s*DROP\\s+TABLE\\s+(?:IF\\s+EXISTS\\s+)?(?:`?\\w+`?\\.)?`?(\\w+)`?")
	lineComment  = regexp.MustCompile(`--[^\n]*`)
	dateInString = regexp.MustCompile(`(?:^|[^0-9])((1[0-9]{3}|20[0-9]{2})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]))`)
)

// parseTTL reads the table-level TTL of one CREATE statement. A TTL it cannot model (column TTL, TTL ... WHERE,
// TO DISK, several expressions) is a problem, so a new form never passes unread.
func parseTTL(table, statement string) (ttlTable, bool, string) {
	words := ttlWord.FindAllStringIndex(statement, -1)
	if len(words) == 0 {
		return ttlTable{}, false, ""
	}
	match := ttlPrecise.FindStringSubmatchIndex(statement)
	engine := engineWord.FindStringIndex(statement)
	if len(words) != 1 || match == nil || match[0] != words[0][0] || engine == nil || match[0] < engine[0] ||
		(match[2] >= 0) != (match[6] >= 0) || !ttlTail.MatchString(statement[match[1]:]) {
		return ttlTable{}, false, fmt.Sprintf("%s: a TTL this guard does not model: %q (extend parseTTL)", table, strings.TrimSpace(statement[words[0][0]:]))
	}
	digits := ""
	if match[8] >= 0 {
		digits = statement[match[8]:match[9]]
	} else {
		digits = statement[match[10]:match[11]]
	}
	days, err := strconv.Atoi(digits)
	if err != nil || days < 1 {
		return ttlTable{}, false, fmt.Sprintf("%s: bad TTL interval %q", table, digits)
	}
	return ttlTable{days: days, wrap: match[2] >= 0}, true, ""
}

// ttlTables replays the migrations: the baseline objects, then every sql/ statement in file order.
func ttlTables(head []byte, sqlFiles map[string][]byte) (map[string]ttlTable, []string) {
	var problems []string
	tables := map[string]ttlTable{}
	var baseline struct {
		Objects []struct {
			Name   string `json:"name"`
			Create string `json:"create"`
		} `json:"objects"`
	}
	if err := json.Unmarshal(head, &baseline); err != nil {
		return nil, []string{"baseline: " + err.Error()}
	}
	if len(baseline.Objects) == 0 {
		problems = append(problems, "baseline: no object")
	}
	for _, object := range baseline.Objects {
		ttl, ok, problem := parseTTL(object.Name, object.Create)
		if problem != "" {
			problems = append(problems, "baseline "+problem)
		}
		if ok {
			tables[object.Name] = ttl
		}
	}
	names := make([]string, 0, len(sqlFiles))
	for name := range sqlFiles {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		text := lineComment.ReplaceAllString(string(sqlFiles[name]), "")
		for _, statement := range strings.Split(text, ";") {
			if m := createTable.FindStringSubmatch(statement); m != nil {
				delete(tables, m[1])
				ttl, ok, problem := parseTTL(m[1], statement)
				if problem != "" {
					problems = append(problems, name+" "+problem)
				}
				if ok {
					tables[m[1]] = ttl
				}
				continue
			}
			if m := dropTable.FindStringSubmatch(statement); m != nil {
				delete(tables, m[1])
				continue
			}
			if ttlWord.MatchString(statement) {
				problems = append(problems, fmt.Sprintf("%s: a TTL statement this guard does not model: %q (extend ttlTables)", name, strings.TrimSpace(statement)))
			}
		}
	}
	return tables, problems
}

// finding is one date of one file: where lists each occurrence as line:function ("-" outside a function), and
// tables the TTL tables the file names.
type finding struct {
	file, date string
	where      []string
	tables     []string
}

func (f finding) names() string {
	if len(f.tables) == 0 {
		return "-"
	}
	return strings.Join(f.tables, ",")
}

// funcName names a function declaration, with its receiver type for a method.
func funcName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	recv := fn.Recv.List[0].Type
	for {
		switch node := recv.(type) {
		case *ast.StarExpr:
			recv = node.X
			continue
		case *ast.IndexExpr:
			recv = node.X
			continue
		case *ast.IndexListExpr:
			recv = node.X
			continue
		case *ast.Ident:
			return node.Name + "." + fn.Name.Name
		}
		return fn.Name.Name
	}
}

// isIntegration is true when the file's //go:build line needs the integration tag.
func isIntegration(file *ast.File) bool {
	for _, group := range file.Comments {
		if group.Pos() > file.Package {
			break
		}
		for _, comment := range group.List {
			if !constraint.IsGoBuild(comment.Text) {
				continue
			}
			expr, err := constraint.Parse(comment.Text)
			if err != nil {
				return false
			}
			return expr.Eval(func(tag string) bool { return tag == "integration" }) && !expr.Eval(func(string) bool { return false })
		}
	}
	return false
}

var months = map[string]int{"January": 1, "February": 2, "March": 3, "April": 4, "May": 5, "June": 6, "July": 7,
	"August": 8, "September": 9, "October": 10, "November": 11, "December": 12}

func intArg(expr ast.Expr, timeName string) (int, bool) {
	switch node := expr.(type) {
	case *ast.BasicLit:
		if node.Kind == token.INT {
			value, err := strconv.Atoi(node.Value)
			return value, err == nil
		}
	case *ast.SelectorExpr:
		if ident, ok := node.X.(*ast.Ident); ok && ident.Name == timeName {
			value, ok := months[node.Sel.Name]
			return value, ok
		}
	}
	return 0, false
}

// scanFile returns the date findings of one test file, or nil when it is not an integration test naming a TTL table.
func scanFile(rel string, source []byte, tables map[string]ttlTable) ([]finding, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, rel, source, parser.ParseComments)
	if err != nil {
		return nil, err
	}
	if !isIntegration(file) {
		return nil, nil
	}
	timeName := ""
	for _, spec := range file.Imports {
		if path, _ := strconv.Unquote(spec.Path.Value); path == "time" {
			timeName = "time"
			if spec.Name != nil {
				timeName = spec.Name.Name
			}
		}
	}
	words := map[string]*regexp.Regexp{}
	for table := range tables {
		words[table] = regexp.MustCompile(`\b` + regexp.QuoteMeta(table) + `\b`)
	}
	named := map[string]bool{}
	type occurrence struct {
		line  int
		owner string
	}
	dates := map[string][]occurrence{}
	owner := "-"
	add := func(date string, pos token.Pos) {
		if date != goLayoutDate {
			dates[date] = append(dates[date], occurrence{fset.Position(pos).Line, owner})
		}
	}
	inspect := func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.BasicLit:
			if node.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(node.Value)
			if err != nil {
				return true
			}
			for table, word := range words {
				if word.MatchString(text) {
					named[table] = true
				}
			}
			for _, m := range dateInString.FindAllStringSubmatch(text, -1) {
				if year, _ := strconv.Atoi(m[2]); year < futureYear {
					add(m[1], node.Pos())
				}
			}
		case *ast.CallExpr:
			selector, ok := node.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Date" || timeName == "" || len(node.Args) < 3 {
				return true
			}
			if ident, ok := selector.X.(*ast.Ident); !ok || ident.Name != timeName {
				return true
			}
			year, ok := intArg(node.Args[0], timeName)
			if !ok || year >= futureYear {
				return true
			}
			month, okMonth := intArg(node.Args[1], timeName)
			day, okDay := intArg(node.Args[2], timeName)
			if !okMonth || !okDay || month < 1 || month > 12 || day < 1 || day > 31 {
				month, day = 1, 1
			}
			add(time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC).Format(dateLayout), node.Pos())
		}
		return true
	}
	for _, decl := range file.Decls {
		owner = "-"
		if fn, ok := decl.(*ast.FuncDecl); ok {
			owner = funcName(fn)
		}
		ast.Inspect(decl, inspect)
	}
	if len(named) == 0 || len(dates) == 0 {
		return nil, nil
	}
	var tableNames []string
	for table := range named {
		tableNames = append(tableNames, table)
	}
	sort.Strings(tableNames)
	var out []finding
	for date, occurrences := range dates {
		sort.SliceStable(occurrences, func(i, j int) bool { return occurrences[i].line < occurrences[j].line })
		where := make([]string, len(occurrences))
		for i, o := range occurrences {
			where[i] = fmt.Sprintf("%d:%s", o.line, o.owner)
		}
		out = append(out, finding{file: rel, date: date, where: where, tables: tableNames})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].date < out[j].date })
	return out, nil
}

// expiry is the day a row stamped at date stops existing: date + TTL, through toDateTime's UInt32 seconds.
func expiry(date time.Time, ttl ttlTable) time.Time {
	if !ttl.wrap {
		return date.AddDate(0, 0, ttl.days)
	}
	seconds := uint32(date.Unix()) + uint32(ttl.days*86400)
	return time.Unix(int64(seconds), 0).UTC().Truncate(24 * time.Hour)
}

type allowRow struct {
	file, date, table, expiry, names, where, reason string
	line                                            int
}

const allowFields = "file, date, table, expiry, names, where, reason"

func parseAllow(text string) ([]allowRow, []string) {
	var rows []allowRow
	var problems []string
	scanner := bufio.NewScanner(strings.NewReader(text))
	number := 0
	for scanner.Scan() {
		number++
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 7 {
			problems = append(problems, fmt.Sprintf("%s:%d: want 7 tab-separated fields (%s), got %d", allowFile, number, allowFields, len(fields)))
			continue
		}
		rows = append(rows, allowRow{file: fields[0], date: fields[1], table: fields[2], expiry: fields[3], names: fields[4], where: fields[5],
			reason: strings.TrimSpace(fields[6]), line: number})
	}
	return rows, problems
}

// check compares the findings with the allowlist. today is a date (UTC midnight); exists reports a repo file.
func check(findings []finding, rows []allowRow, tables map[string]ttlTable, today time.Time, exists func(string) bool) []string {
	var problems []string
	byKey := map[[2]string]finding{}
	for _, f := range findings {
		byKey[[2]string{f.file, f.date}] = f
	}
	listed := map[[2]string]bool{}
	for _, row := range rows {
		where := fmt.Sprintf("%s:%d (%s %s)", allowFile, row.line, row.file, row.date)
		key := [2]string{row.file, row.date}
		if listed[key] {
			problems = append(problems, where+": duplicate row")
			continue
		}
		listed[key] = true
		if row.reason == "" {
			problems = append(problems, where+": row without a reason")
		}
		date, err := time.Parse(dateLayout, row.date)
		if err != nil {
			problems = append(problems, where+": date is not YYYY-MM-DD")
			continue
		}
		switch {
		case row.table == "-":
			if row.expiry != never {
				problems = append(problems, where+": table \"-\" (never reaches a TTL table) needs expiry \"never\"")
			}
		default:
			ttl, ok := tables[row.table]
			if !ok {
				problems = append(problems, fmt.Sprintf("%s: %s has no TTL at the migration head", where, row.table))
				break
			}
			want := expiry(date, ttl).Format(dateLayout)
			if row.expiry != want {
				problems = append(problems, fmt.Sprintf("%s: expiry %s, computed %s (%s + %d d)", where, row.expiry, want, row.date, ttl.days))
				break
			}
			if expiry(date, ttl).Before(today.AddDate(0, 0, shelfDays)) {
				problems = append(problems, fmt.Sprintf("%s: the row expires %s, within %d days of %s: take the date from the clock", where, want, shelfDays, today.Format(dateLayout)))
			}
		}
		f, found := byKey[key]
		switch {
		case !found && !exists(row.file):
			problems = append(problems, where+": the file no longer exists")
		case !found:
			problems = append(problems, where+": the literal is no longer a finding in the file")
		case row.names != f.names():
			problems = append(problems, fmt.Sprintf("%s: STALE, the file now names TTL table(s) %s (row: %s): re-review the row", where, f.names(), row.names))
		case row.where != strings.Join(f.where, ","):
			problems = append(problems, fmt.Sprintf("%s: STALE, the literal is now at %s (row: %s): re-review the row", where, strings.Join(f.where, ","), row.where))
		}
	}
	for _, f := range findings {
		if !listed[[2]string{f.file, f.date}] {
			problems = append(problems, fmt.Sprintf("%s at %s: date literal %s in an integration test that names TTL table(s) %s; a TTL merge deletes such a row once %s + TTL is past",
				f.file, strings.Join(f.where, ","), f.date, f.names(), f.date))
		}
	}
	sort.Strings(problems)
	return problems
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func repoTTLTables(t *testing.T, root string) map[string]ttlTable {
	t.Helper()
	head, err := os.ReadFile(filepath.Join(root, headFile))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(root, sqlDir))
	if err != nil {
		t.Fatal(err)
	}
	sqlFiles := map[string][]byte{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(root, sqlDir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		sqlFiles[entry.Name()] = body
	}
	tables, problems := ttlTables(head, sqlFiles)
	for _, problem := range problems {
		t.Error(problem)
	}
	if len(tables) == 0 {
		t.Fatal("the migrations hold no TTL table: the scan did not run")
	}
	return tables
}

func TestIntegrationTestsWriteTTLTablesWithClockDates(t *testing.T) {
	root := moduleRoot(t)
	tables := repoTTLTables(t, root)
	var findings []finding
	scanned := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := entry.Name()
		if entry.IsDir() {
			if name == ".git" || name == "testdata" || name == "node_modules" || name == "vendor" || name == "third_party" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, "_test.go") {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Contains(source, []byte("integration")) {
			return nil
		}
		mentions := false
		for table := range tables {
			if bytes.Contains(source, []byte(table)) {
				mentions = true
				break
			}
		}
		if !mentions {
			return nil
		}
		scanned++
		rel, _ := filepath.Rel(root, path)
		found, err := scanFile(filepath.ToSlash(rel), source, tables)
		if err != nil {
			return err
		}
		findings = append(findings, found...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned == 0 {
		t.Fatal("no integration test names a TTL table: the walk did not run")
	}
	allow, err := os.ReadFile(allowFile)
	if err != nil {
		t.Fatal(err)
	}
	rows, problems := parseAllow(string(allow))
	today := time.Now().UTC().Truncate(24 * time.Hour)
	problems = append(problems, check(findings, rows, tables, today, func(rel string) bool {
		_, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel)))
		return err == nil
	})...)
	for _, problem := range problems {
		t.Error(problem)
	}
	if len(problems) > 0 {
		t.Logf("fix: derive the seeded dates from time.Now() (or now64()/today() in SQL) inside the TTL; a literal that never reaches a TTL table, or a deliberate edge case, gets an %s row with its reason and computed expiry", allowFile)
	}
}

// checkSites is the entry point of the site-comment check (CHAOS-11715); red stub: it delegates to the allow.tsv
// check with no row, so every literal is unlisted.
func checkSites(rel string, source []byte, tables map[string]ttlTable, today time.Time) []string {
	found, err := scanFile(rel, source, tables)
	if err != nil {
		return []string{err.Error()}
	}
	return check(found, nil, tables, today, func(string) bool { return true })
}
