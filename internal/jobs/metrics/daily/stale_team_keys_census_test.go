package daily

import (
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

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The census of the team-keyed tables.
//
// A table whose sorting key holds a team id keeps one row for each team id, so
// a row under an id that is no longer produced stays the newest row of its
// key (stale_team_keys.go). The census reads the schema and the source and
// fails when:
//
//   - a table with a team id in its sorting key (a team_id or a scope_id
//     column) has no decision: the shared rule, its own rule, or a written
//     reason why a row of zeros is wrong for it;
//   - a declaration of the shared rule does not agree with the table: a key
//     column, a measure or a Nullable measure that the schema holds and the
//     declaration does not, or the wrong kind for a key column;
//   - a declared table has no call of the rule in the family that writes it;
//   - a file writes one of the tables and is not a declared writer.

// teamKeyColumns are the sorting-key columns that hold a team id.
// compounding_risk_daily holds the team id of its team rows in scope_id.
var teamKeyColumns = map[string]bool{"team_id": true, "scope_id": true}

// teamKeyOwnRule are the tables whose writer holds the rule itself, with the
// function that adds the rows of zeros. They are plain MergeTree tables: a row
// of zeros replaces nothing by itself, and the rule holds because every reader
// takes the newest row of a key.
var teamKeyOwnRule = map[string]string{
	"issue_type_metrics_daily": "withIssueTypeMetricsZeroRows",
	"investment_metrics_daily": "withInvestmentMetricsZeroRows",
}

// teamKeyExempt are the tables that hold a team id (or a scope id) in the
// sorting key and take no row of zeros, each with the reason.
var teamKeyExempt = map[string]string{
	"ai_policy_events": "a row is one policy event of one subject, keyed by the event id; " +
		"it is not a count of a team and a day, and a row of zeros would be an event that did not occur",
	"recommendations_daily": "a row is the result of one rule for one team and window (fired or not fired), not a count; " +
		"the family writes a not-fired row itself, and the reader reads one team id and shows only fired rows",
	"work_item_team_attributions": "a fact table of the attribution of one work item, not a daily count; " +
		"a superseded attribution is replaced by the row of the next attribution run of the item",
	"manual_attribution_fallbacks":               "admin records keyed by a scope, not a derived daily table; scope_id is a project or repository scope",
	"team_memberships":                           "a dimension of the team itself, with its own validity interval; not a derived daily table",
	"team_project_ownership":                     "a dimension of the team itself, with its own validity interval; not a derived daily table",
	"team_repo_ownership":                        "a dimension of the team itself, with its own validity interval; not a derived daily table",
	"team_sync_policies":                         "configuration of one team; not a derived daily table",
	"work_item_attribution_backstop_scoped_runs": "a ledger of runs; scope_id is the scope of a run, not a team id",
	"work_unit_membership_scoped_runs":           "a ledger of runs; scope_id is the scope of a run, not a team id",
}

// Reasons why a file that writes a team-keyed table is not a writer that
// needs the rule.
const (
	teamKeyWriterUndispatched = "provider sync holds an adapter for the table that no destination reaches: the daily job owns the table"
	teamKeyWriterFixture      = "a fixture of a test-support package"
)

// teamKeyTableWriters is every file with an INSERT of a table that takes a
// rule: "" for the writer of the family, or the reason why the file needs no
// rule. A new writer fails the census until it is declared here, which is the
// point where its author decides how it keeps the rule.
var teamKeyTableWriters = map[string]map[string]string{
	"work_item_metrics_daily": {
		"internal/jobs/metrics/daily/work_item_native_clickhouse.go":                  "",
		"internal/providersync/github_work_item_metric_triplet_effects_clickhouse.go": teamKeyWriterUndispatched,
		"internal/testsupport/sessionscenario/sessionscenario.go":                     teamKeyWriterFixture,
	},
	"work_item_state_durations_daily": {
		"internal/jobs/metrics/daily/work_item_state_native_clickhouse.go":     "",
		"internal/providersync/github_work_item_derived_effects_clickhouse.go": teamKeyWriterUndispatched,
	},
	"estimate_coverage_metrics_daily": {
		"internal/jobs/metrics/daily/work_item_native_clickhouse.go":           "",
		"internal/providersync/github_work_item_derived_effects_clickhouse.go": teamKeyWriterUndispatched,
	},
	"team_metrics_daily":           {"internal/jobs/metrics/daily/wellbeing_native_clickhouse.go": ""},
	"ai_impact_metrics_daily":      {"internal/jobs/metrics/daily/ai_impact_native_clickhouse.go": ""},
	"ai_governance_coverage_daily": {"internal/jobs/metrics/daily/ai_governance_native_clickhouse.go": ""},
	"team_cognitive_load_daily":    {"internal/jobs/metrics/daily/team_cognitive_load_clickhouse.go": ""},
	"team_complexity_daily":        {"internal/jobs/metrics/daily/team_complexity_clickhouse.go": ""},
	"ic_landscape_rolling_30d":     {"internal/jobs/metrics/daily/icfinalize/executor.go": ""},
	"compounding_risk_daily":       {"internal/jobs/metrics/daily/compoundingrisk/clickhouse.go": ""},
	"issue_type_metrics_daily":     {"internal/jobs/metrics/workitemengine/inserts.go": ""},
	"investment_metrics_daily":     {"internal/jobs/metrics/workitemengine/inserts.go": ""},
}

// censusColumn is one column of a table of the schema.
type censusColumn struct {
	name, typ string
	// defaulted is the DEFAULT expression ("" for none).
	defaulted string
}

// censusTable is one table of the schema.
type censusTable struct {
	name, engine string
	columns      []censusColumn
	sortingKey   []string // the column of each element of ORDER BY
}

// splitTopLevel splits on commas that are outside parentheses and quotes.
func splitTopLevel(text string) []string {
	var parts []string
	depth, start, quoted := 0, 0, false
	for index := 0; index < len(text); index++ {
		switch character := text[index]; {
		case character == '\'' && (index == 0 || text[index-1] != '\\'):
			quoted = !quoted
		case quoted:
		case character == '(':
			depth++
		case character == ')':
			depth--
		case character == ',' && depth == 0:
			parts = append(parts, strings.TrimSpace(text[start:index]))
			start = index + 1
		}
	}
	return append(parts, strings.TrimSpace(text[start:]))
}

// balanced returns the text inside the parentheses that open at text[open].
func balanced(t *testing.T, text string, open int) string {
	t.Helper()
	depth, quoted := 0, false
	for index := open; index < len(text); index++ {
		switch character := text[index]; {
		case character == '\'' && text[index-1] != '\\':
			quoted = !quoted
		case quoted:
		case character == '(':
			depth++
		case character == ')':
			depth--
			if depth == 0 {
				return text[open+1 : index]
			}
		}
	}
	t.Fatalf("unbalanced parentheses in %q", text[open:min(len(text), open+80)])
	return ""
}

var (
	censusCreateTable = regexp.MustCompile(`(?is)CREATE TABLE\s+(?:IF NOT EXISTS\s+)?` + "`?" + `(\w+)` + "`?" + `\s*\(`)
	censusEngine      = regexp.MustCompile(`(?i)\)\s*ENGINE\s*=\s*(\w+(?:\([^)]*\))?)`)
	censusOrderBy     = regexp.MustCompile(`(?i)\bORDER BY\s+`)
	censusKeyColumn   = regexp.MustCompile(`^(?:\w+\()*` + "`?" + `(\w+)` + "`?")
	censusColumnHead  = regexp.MustCompile("^`?(\\w+)`?\\s+(.*)$")
)

// parseCensusTables reads every CREATE TABLE of a piece of SQL.
func parseCensusTables(t *testing.T, sql string) []censusTable {
	t.Helper()
	var tables []censusTable
	for _, match := range censusCreateTable.FindAllStringSubmatchIndex(sql, -1) {
		table := censusTable{name: sql[match[2]:match[3]]}
		body := balanced(t, sql, match[1]-1)
		for _, definition := range splitTopLevel(body) {
			head := censusColumnHead.FindStringSubmatch(definition)
			if head == nil {
				t.Fatalf("table %s: cannot read the column definition %q", table.name, definition)
			}
			column := censusColumn{name: head[1], typ: head[2]}
			if at := strings.Index(column.typ, " DEFAULT "); at >= 0 {
				column.typ, column.defaulted = strings.TrimSpace(column.typ[:at]), strings.TrimSpace(column.typ[at+len(" DEFAULT "):])
			}
			table.columns = append(table.columns, column)
		}
		rest := sql[match[1]-1+len(body)+1:]
		if next := censusCreateTable.FindStringIndex(rest); next != nil {
			rest = rest[:next[0]]
		}
		if engine := censusEngine.FindStringSubmatch(rest); engine != nil {
			table.engine = engine[1]
		}
		if order := censusOrderBy.FindStringIndex(rest); order != nil {
			expression := rest[order[1]:]
			if strings.HasPrefix(expression, "(") {
				expression = balanced(t, expression, 0)
			} else {
				expression = strings.Fields(expression)[0]
			}
			for _, element := range splitTopLevel(expression) {
				column := censusKeyColumn.FindStringSubmatch(element)
				if column == nil {
					t.Fatalf("table %s: cannot read the sorting-key element %q", table.name, element)
				}
				table.sortingKey = append(table.sortingKey, column[1])
			}
		}
		tables = append(tables, table)
	}
	return tables
}

// loadCensusTables reads the tables of the head and of the migrations after
// it. A measurement that found no table fails.
func loadCensusTables(t *testing.T) map[string]censusTable {
	t.Helper()
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatalf("load the schema baseline: %v", err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatalf("load the migrations after the head: %v", err)
	}
	tables := map[string]censusTable{}
	for _, object := range baseline.Objects {
		if object.IsView() {
			continue
		}
		parsed := parseCensusTables(t, object.Create)
		if len(parsed) != 1 || parsed[0].name != object.Name {
			t.Fatalf("baseline object %s: read %d table(s) from its CREATE statement", object.Name, len(parsed))
		}
		tables[object.Name] = parsed[0]
	}
	for _, file := range chain {
		for _, table := range parseCensusTables(t, file.SQL) {
			tables[table.name] = table
		}
	}
	if len(tables) < 50 {
		t.Fatalf("read %d tables from the schema: the census did not measure", len(tables))
	}
	for _, name := range []string{"work_item_metrics_daily", "compounding_risk_daily", "teams"} {
		if len(tables[name].sortingKey) == 0 || len(tables[name].columns) == 0 {
			t.Fatalf("table %s has no sorting key or no column in the census read: the census did not measure", name)
		}
	}
	return tables
}

func (table censusTable) teamKeyed() bool {
	for _, column := range table.sortingKey {
		if teamKeyColumns[column] {
			return true
		}
	}
	return false
}

func TestEveryTeamKeyedTableCensusHasADecision(t *testing.T) {
	tables := loadCensusTables(t)
	shared := map[string]bool{}
	for _, table := range StaleTeamKeyTables() {
		if shared[table.Table] {
			t.Errorf("table %s is declared twice in StaleTeamKeyTables", table.Table)
		}
		shared[table.Table] = true
	}
	teamKeyed := 0
	for name, table := range tables {
		if !table.teamKeyed() {
			continue
		}
		teamKeyed++
		decisions := 0
		if shared[name] {
			decisions++
		}
		if _, own := teamKeyOwnRule[name]; own {
			decisions++
		}
		if reason, exempt := teamKeyExempt[name]; exempt {
			decisions++
			if len(strings.TrimSpace(reason)) < 40 {
				t.Errorf("table %s is exempt with no written reason", name)
			}
		}
		if decisions != 1 {
			t.Errorf("table %s holds a team id in its sorting key %v and has %d decision(s), want exactly 1: "+
				"declare it in StaleTeamKeyTables (the shared rule), in teamKeyOwnRule, or in teamKeyExempt with the reason",
				name, table.sortingKey, decisions)
		}
	}
	if teamKeyed < len(shared)+len(teamKeyOwnRule) {
		t.Fatalf("the schema read holds %d team-keyed table(s): the census did not measure", teamKeyed)
	}
	// A decision for a table that the schema does not hold as team-keyed is
	// stale.
	for _, name := range decidedTeamKeyTables() {
		if table, known := tables[name]; !known || !table.teamKeyed() {
			t.Errorf("table %s has a decision in the census and is not a team-keyed table of the schema", name)
		}
	}
}

func decidedTeamKeyTables() []string {
	var names []string
	for _, table := range StaleTeamKeyTables() {
		names = append(names, table.Table)
	}
	for name := range teamKeyOwnRule {
		names = append(names, name)
	}
	for name := range teamKeyExempt {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// The declaration of each table of the shared rule against the schema: the
// rule builds its read and its row of zeros from the declaration alone, so a
// column that the declaration does not hold is a column the rule gets wrong.
func TestEveryStaleKeyTableCensusDeclarationAgreesWithTheSchema(t *testing.T) {
	tables := loadCensusTables(t)
	for _, declared := range StaleTeamKeyTables() {
		t.Run(declared.Table, func(t *testing.T) {
			if err := declared.valid(); err != nil {
				t.Fatal(err)
			}
			table, known := tables[declared.Table]
			if !known {
				t.Fatalf("the schema holds no table %s", declared.Table)
			}
			if table.engine != "ReplacingMergeTree(computed_at)" {
				t.Errorf("engine %q: the shared rule needs ReplacingMergeTree(computed_at), where the row of zeros replaces the row it supersedes",
					table.engine)
			}
			columns := map[string]censusColumn{}
			for _, column := range table.columns {
				columns[column.name] = column
			}
			// The keys are the sorting key without org_id and the day, in the
			// order of the sorting key.
			var wantKeys, gotKeys []string
			for _, column := range table.sortingKey {
				if column != "org_id" && column != declared.DayColumn {
					wantKeys = append(wantKeys, column)
				}
			}
			for _, key := range declared.Keys {
				gotKeys = append(gotKeys, key.Name)
				kind := map[string]staleKeyKind{
					"Nullable(String)": staleKeyNullableString, "UUID": staleKeyUUID, "Nullable(UUID)": staleKeyNullableUUID,
				}[columns[key.Name].typ] // every other type is read and stored as text
				if key.Kind != kind {
					t.Errorf("key column %s has type %s and is declared as kind %d, want kind %d",
						key.Name, columns[key.Name].typ, key.Kind, kind)
				}
			}
			if !reflect.DeepEqual(gotKeys, wantKeys) {
				t.Errorf("declared keys %v, the sorting key holds %v", gotKeys, wantKeys)
			}
			if !contains(table.sortingKey, "org_id") || !contains(table.sortingKey, declared.DayColumn) ||
				columns[declared.DayColumn].typ != "Date" {
				t.Errorf("the sorting key %v must hold org_id and the Date column %s", table.sortingKey, declared.DayColumn)
			}
			if !teamKeyColumns[declared.TeamColumn] || !contains(gotKeys, declared.TeamColumn) {
				t.Errorf("team column %q is not a team-id key column of the table", declared.TeamColumn)
			}
			for _, scope := range declared.Scope {
				if !contains(gotKeys, scope) || scope == declared.TeamColumn {
					t.Errorf("scope column %q must be a key column other than the team column", scope)
				}
			}
			// Every column has one role.
			roles := map[string]string{"org_id": "organization", declared.DayColumn: "day", "computed_at": "version"}
			claim := func(role string, names []string) {
				for _, name := range names {
					if previous, taken := roles[name]; taken {
						t.Errorf("column %s is declared as %s and as %s", name, previous, role)
					}
					roles[name] = role
				}
			}
			claim("key", gotKeys)
			claim("measure", declared.Measures)
			claim("nullable measure", declared.NullableMeasures)
			claim("label", declared.Labels)
			claim("configuration", declared.Configuration)
			for name, role := range roles {
				column, exists := columns[name]
				if !exists {
					t.Errorf("declared %s column %s is not a column of the table", role, name)
					continue
				}
				nullable := strings.HasPrefix(column.typ, "Nullable(")
				switch role {
				case "measure":
					if nullable {
						t.Errorf("measure %s is %s: declare it in NullableMeasures, where a row of zeros holds NULL", name, column.typ)
					}
				case "nullable measure":
					if !nullable {
						t.Errorf("nullable measure %s is %s: declare it in Measures", name, column.typ)
					}
				}
				// A row of zeros leaves every measure, label and
				// configuration column to its default: that must be 0, the
				// empty text or NULL.
				if role != "organization" && role != "day" && role != "version" && role != "key" {
					switch strings.TrimSuffix(column.defaulted, ".") {
					case "", "0", "''", "NULL":
					default:
						t.Errorf("%s column %s has DEFAULT %s: a row of zeros would hold that value", role, name, column.defaulted)
					}
				}
			}
			for _, column := range table.columns {
				if _, declaredRole := roles[column.name]; !declaredRole {
					t.Errorf("column %s (%s) has no role in the declaration: declare it as a measure, a Nullable measure, "+
						"a label or configuration, so that the rule reads it and a row of zeros is right for it", column.name, column.typ)
				}
			}
			if columns["computed_at"].name == "" {
				t.Errorf("the table has no computed_at column")
			}
		})
	}
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

// parseDailyPackage parses the non-test files of this package.
func parseDailyPackage(t *testing.T) map[string]*ast.File {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	files := map[string]*ast.File{}
	fileSet := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fileSet, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files[name] = file
	}
	if len(files) < 50 || files["stale_team_keys.go"] == nil {
		t.Fatalf("parsed %d file(s) of the package: the census did not measure", len(files))
	}
	return files
}

// Every table of the shared rule has a call of the rule in a family of this
// package, and every table with its own rule has a call of its function.
func TestEveryStaleKeyTableCensusHasACallOfTheRule(t *testing.T) {
	files := parseDailyPackage(t)

	// The declarations: the variable of each StaleKeyTable literal, by table.
	tableOfVariable := map[string]string{}
	ast.Inspect(files["stale_team_keys.go"], func(node ast.Node) bool {
		spec, isSpec := node.(*ast.ValueSpec)
		if !isSpec || len(spec.Names) != 1 || len(spec.Values) != 1 {
			return true
		}
		literal, isLiteral := spec.Values[0].(*ast.CompositeLit)
		if !isLiteral {
			return true
		}
		if typ, isIdent := literal.Type.(*ast.Ident); !isIdent || typ.Name != "StaleKeyTable" {
			return true
		}
		for _, element := range literal.Elts {
			pair, isPair := element.(*ast.KeyValueExpr)
			if !isPair {
				continue
			}
			if key, isIdent := pair.Key.(*ast.Ident); isIdent && key.Name == "Table" {
				if value, isBasic := pair.Value.(*ast.BasicLit); isBasic {
					name, _ := strconv.Unquote(value.Value)
					tableOfVariable[spec.Names[0].Name] = name
				}
			}
		}
		return true
	})
	listed := map[string]bool{}
	for _, table := range StaleTeamKeyTables() {
		listed[table.Table] = true
	}
	if len(tableOfVariable) != len(listed) {
		t.Errorf("stale_team_keys.go declares %d StaleKeyTable variable(s) and StaleTeamKeyTables lists %d: every declaration must be listed",
			len(tableOfVariable), len(listed))
	}

	// The calls, outside the file of the rule.
	called := map[string][]string{}
	calledFunctions := map[string]bool{}
	for name, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			call, isCall := node.(*ast.CallExpr)
			if !isCall {
				return true
			}
			function, isIdent := call.Fun.(*ast.Ident)
			if !isIdent {
				return true
			}
			calledFunctions[function.Name] = true
			if name == "stale_team_keys.go" || (function.Name != "supersedeStaleTeamKeys" && function.Name != "supersedeStaleTeamKeysAt") {
				return true
			}
			if len(call.Args) < 3 {
				t.Errorf("%s: a call of %s with %d argument(s)", name, function.Name, len(call.Args))
				return true
			}
			variable, isVariable := call.Args[2].(*ast.Ident)
			if !isVariable || tableOfVariable[variable.Name] == "" {
				t.Errorf("%s: a call of %s must name a declared StaleKeyTable variable as its table", name, function.Name)
				return true
			}
			called[tableOfVariable[variable.Name]] = append(called[tableOfVariable[variable.Name]], name)
			return true
		})
	}
	for table := range listed {
		if len(called[table]) == 0 {
			t.Errorf("table %s is declared for the shared rule and no family calls the rule for it: "+
				"its rows under a superseded team id stay the newest rows of their keys", table)
		}
	}
	for table, function := range teamKeyOwnRule {
		if !calledFunctions[function] {
			t.Errorf("table %s holds its own rule in %s, and no file of the package calls it", table, function)
		}
	}
}

// Every file that writes one of the tables is a declared writer.
func TestEveryWriterOfATeamKeyedTableCensusIsDeclared(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatalf("module root: %v", err)
	}
	wanted := map[string]bool{}
	for _, table := range StaleTeamKeyTables() {
		wanted[table.Table] = true
	}
	for table := range teamKeyOwnRule {
		wanted[table] = true
	}
	for table := range wanted {
		if len(teamKeyTableWriters[table]) == 0 {
			t.Errorf("table %s has no declared writer", table)
		}
	}
	for table := range teamKeyTableWriters {
		if !wanted[table] {
			t.Errorf("table %s has declared writers and takes no rule", table)
		}
	}
	patterns := map[string]*regexp.Regexp{}
	for table := range wanted {
		patterns[table] = regexp.MustCompile(`(?i)\bINSERT\s+INTO\s+` + "`?" + regexp.QuoteMeta(table) + "`?" + `(\s|\()`)
	}
	found := map[string]map[string]bool{}
	walked := 0
	for _, tree := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, tree), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			source, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			walked++
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			for table, pattern := range patterns {
				if pattern.Match(source) {
					if found[table] == nil {
						found[table] = map[string]bool{}
					}
					found[table][filepath.ToSlash(relative)] = true
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", tree, err)
		}
	}
	if walked < 1000 {
		t.Fatalf("walked %d source file(s): the census did not measure", walked)
	}
	for table := range wanted {
		familyWriters := 0
		for file, reason := range teamKeyTableWriters[table] {
			if !found[table][file] {
				t.Errorf("%s is a declared writer of %s and holds no INSERT of it", file, table)
			}
			if reason == "" {
				familyWriters++
			}
		}
		if familyWriters != 1 {
			t.Errorf("table %s has %d declared family writer(s), want 1", table, familyWriters)
		}
		for file := range found[table] {
			if _, declared := teamKeyTableWriters[table][file]; !declared {
				t.Errorf("%s writes %s and is not a declared writer: a second writer of a team-keyed table must keep the "+
					"stale-key rule (stale_team_keys.go) or be declared in teamKeyTableWriters with the reason it needs none",
					file, table)
			}
		}
	}
}
