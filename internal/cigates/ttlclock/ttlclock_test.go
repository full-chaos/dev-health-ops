// Package ttlclock is the CI guard that an integration test writing a ClickHouse table with a TTL takes its
// dates from the clock, not from a literal. A row whose TTL column + interval is before the server's now() is
// deleted by the next merge, so a frozen date in such a test turns into a short read on the day it ages out.
//
// The TTL tables come from the migrations (baseline/head.json, then sql/*.sql in order). A finding is a date
// literal before 2090 (time.Date with a year literal, or a YYYY-MM-DD string literal; the Go layout 2006-01-02 is
// not a date) in a //go:build integration test file whose string literals name a TTL table. A string literal with
// several dates is one literal. A finding needs a //ttlclock:allow <reason> site comment on the literal's own line
// or the line directly above; the comment covers that one literal. The reason either says "never reaches a TTL
// table" and ends with tables=<the TTL tables the file names> (a new table makes it STALE), or names the earliest
// expiry the guard computes (date + TTL over the file's TTL tables, through the UInt32 wrap of toDateTime), which
// FAILS once it is within 180 days of today. A literal with no comment, a comment with no literal, an empty reason,
// a second literal on one comment and a wrong expiry FAIL. Only dates are compared; nothing measures time.
package ttlclock

import (
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
	markerPrefix = "//ttlclock:allow"
	neverPhrase  = "never reaches a TTL table"
	headFile     = "internal/chmigrate/baseline/head.json"
	sqlDir       = "internal/chmigrate/sql"
	futureYear   = 2090
	shelfDays    = 180
	goLayoutDate = "2006-01-02"
	dateLayout   = "2006-01-02"
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

// literal is one date literal: a time.Date call with a year literal, or one string literal holding one or more
// YYYY-MM-DD dates (several dates in one string are one literal). line is the literal's first line.
type literal struct {
	line  int
	dates []string
}

// marker is one //ttlclock:allow comment and its reason ("" when the reason is missing).
type marker struct {
	line   int
	reason string
}

// scanned is what one test file holds: its date literals (only when it is an integration test whose string
// literals name a TTL table), the TTL tables it names (sorted), and every marker it carries.
type scanned struct {
	literals []literal
	tables   []string
	markers  []marker
}

// scanFile parses one test file. Markers are collected from any file; literals and tables only from an integration
// test, and literals only when a TTL table is named.
func scanFile(rel string, source []byte, tables map[string]ttlTable) (scanned, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, rel, source, parser.ParseComments)
	if err != nil {
		return scanned{}, err
	}
	var out scanned
	for _, group := range file.Comments {
		for _, c := range group.List {
			rest, ok := strings.CutPrefix(c.Text, markerPrefix)
			if !ok || (rest != "" && rest[0] != ' ' && rest[0] != '\t') {
				continue
			}
			out.markers = append(out.markers, marker{line: fset.Position(c.Slash).Line, reason: strings.TrimSpace(rest)})
		}
	}
	if !isIntegration(file) {
		return out, nil
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
	var literals []literal
	ast.Inspect(file, func(node ast.Node) bool {
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
			var dates []string
			for _, m := range dateInString.FindAllStringSubmatch(text, -1) {
				if year, _ := strconv.Atoi(m[2]); year < futureYear && m[1] != goLayoutDate {
					dates = append(dates, m[1])
				}
			}
			if len(dates) > 0 {
				literals = append(literals, literal{line: fset.Position(node.Pos()).Line, dates: dates})
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
			date := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC).Format(dateLayout)
			literals = append(literals, literal{line: fset.Position(node.Pos()).Line, dates: []string{date}})
		}
		return true
	})
	if len(named) == 0 {
		return out, nil
	}
	for table := range named {
		out.tables = append(out.tables, table)
	}
	sort.Strings(out.tables)
	sort.SliceStable(literals, func(i, j int) bool { return literals[i].line < literals[j].line })
	out.literals = literals
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

// checkSites scans one test file and checks its literals against its markers. today is a date (UTC midnight).
func checkSites(rel string, source []byte, tables map[string]ttlTable, today time.Time) []string {
	found, err := scanFile(rel, source, tables)
	if err != nil {
		return []string{err.Error()}
	}
	return checkScanned(rel, found, tables, today)
}

// checkScanned pairs each literal with the marker on its own line, else the marker on the line directly above.
// A marker covers one literal; a marker no literal claims is an orphan.
func checkScanned(rel string, found scanned, tables map[string]ttlTable, today time.Time) []string {
	var problems []string
	markerAt := map[int]int{}
	for i, m := range found.markers {
		markerAt[m.line] = i
	}
	claims := make([][]literal, len(found.markers))
	names := strings.Join(found.tables, ",")
	for _, l := range found.literals {
		i, ok := markerAt[l.line]
		if !ok {
			i, ok = markerAt[l.line-1]
		}
		if !ok {
			problems = append(problems, fmt.Sprintf("%s:%d: date literal %s in an integration test that names TTL table(s) %s; a TTL merge deletes such a row once the date + TTL is past: take the date from the clock, or put %s <reason> on or directly above the line",
				rel, l.line, strings.Join(l.dates, ","), names, markerPrefix))
			continue
		}
		claims[i] = append(claims[i], l)
	}
	for i, m := range found.markers {
		where := fmt.Sprintf("%s:%d", rel, m.line)
		if m.reason == "" {
			problems = append(problems, fmt.Sprintf("%s: %s needs a reason: name the computed expiry, or say \"never reaches a TTL table\"", where, markerPrefix))
		}
		if len(claims[i]) == 0 {
			problems = append(problems, fmt.Sprintf("%s: no date literal on this line or the next (orphan %s comment): delete it", where, markerPrefix))
			continue
		}
		for _, extra := range claims[i][1:] {
			problems = append(problems, fmt.Sprintf("%s:%d: second literal on the marker at line %d: a marker covers one literal; give this one its own %s", rel, extra.line, m.line, markerPrefix))
		}
		if m.reason != "" {
			problems = append(problems, checkReason(where, m.reason, claims[i][0], found.tables, tables, today)...)
		}
	}
	sort.Strings(problems)
	return problems
}

// checkReason reads one reason. The never form says "never reaches a TTL table" and ends with tables=<the TTL
// tables the file names>; a new table forces a re-review. Otherwise the reason names the EARLIEST expiry over the
// literal's dates x the file's TTL tables (through the UInt32 wrap) and that expiry is at least shelfDays away.
func checkReason(where, reason string, lit literal, named []string, tables map[string]ttlTable, today time.Time) []string {
	if strings.Contains(reason, neverPhrase) {
		want := strings.Join(named, ",")
		if want == "" {
			want = "none"
		}
		fields := strings.Fields(reason)
		list, ok := strings.CutPrefix(fields[len(fields)-1], "tables=")
		switch {
		case !ok:
			return []string{fmt.Sprintf("%s: a %q reason ends with tables=<the TTL tables the file names> (now tables=%s)", where, neverPhrase, want)}
		case list != want:
			return []string{fmt.Sprintf("%s: STALE, the file now names TTL table(s) %s (reason: %s): re-review the literal", where, want, list)}
		}
		return nil
	}
	var earliest time.Time
	var from string
	for _, text := range lit.dates {
		date, err := time.Parse(dateLayout, text)
		if err != nil {
			return []string{fmt.Sprintf("%s: date %s is not YYYY-MM-DD", where, text)}
		}
		for _, name := range named {
			ttl, ok := tables[name]
			if !ok {
				continue
			}
			if e := expiry(date, ttl); earliest.IsZero() || e.Before(earliest) {
				earliest, from = e, fmt.Sprintf("%s + %d d (%s)", text, ttl.days, name)
			}
		}
	}
	want := earliest.Format(dateLayout)
	if !strings.Contains(reason, want) {
		return []string{fmt.Sprintf("%s: the reason must name the expiry %s (the earliest over the literal's dates and the file's TTL tables: %s), or say %q", where, want, from, neverPhrase)}
	}
	if earliest.Before(today.AddDate(0, 0, shelfDays)) {
		return []string{fmt.Sprintf("%s: the expiry %s is within %d days of %s: take the date from the clock", where, want, shelfDays, today.Format(dateLayout))}
	}
	return nil
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

// scanTree walks root: it checks every integration test that names a TTL table, and every
// *_test.go holding the marker, so an orphan comment cannot hide in a file that is not a finding. scanned counts the table-naming integration tests.
func scanTree(root string, tables map[string]ttlTable, today time.Time) (problems []string, scannedFiles int, err error) {
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
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
		marked := bytes.Contains(source, []byte(markerPrefix))
		candidate := false
		if bytes.Contains(source, []byte("integration")) {
			for table := range tables {
				if bytes.Contains(source, []byte(table)) {
					candidate = true
					break
				}
			}
		}
		if !marked && !candidate {
			return nil
		}
		if candidate {
			scannedFiles++
		}
		rel, _ := filepath.Rel(root, path)
		problems = append(problems, checkSites(filepath.ToSlash(rel), source, tables, today)...)
		return nil
	})
	return problems, scannedFiles, err
}

func TestIntegrationTestsWriteTTLTablesWithClockDates(t *testing.T) {
	root := moduleRoot(t)
	tables := repoTTLTables(t, root)
	problems, scannedFiles, err := scanTree(root, tables, time.Now().UTC().Truncate(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if scannedFiles == 0 {
		t.Fatal("no integration test names a TTL table: the walk did not run")
	}
	for _, problem := range problems {
		t.Error(problem)
	}
	if len(problems) > 0 {
		t.Logf("fix: derive the seeded dates from time.Now() (or now64()/today() in SQL) inside the TTL; a literal that never reaches a TTL table, or a deliberate edge case, gets a %s <reason> comment on or directly above it", markerPrefix)
	}
}
