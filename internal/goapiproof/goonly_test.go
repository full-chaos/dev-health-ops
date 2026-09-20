package goapiproof

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func defaultLedgerForTest(t *testing.T) *GoServedLedger {
	t.Helper()
	ledger, err := DefaultGoServedLedger()
	if err != nil {
		t.Fatalf("DefaultGoServedLedger: %v", err)
	}
	return ledger
}

// The ledger's guard tests exist in the tree. A renamed or deleted guard
// turns the ledger red, so a go-only receipt never cites a test that is gone.
func TestEveryLedgerGuardResolvesInTheTree(t *testing.T) {
	root := repoRootFromTest(t)
	ledger := defaultLedgerForTest(t)
	for _, entry := range ledger.Entries {
		for _, guard := range entry.Guards {
			path := filepath.Join(root, filepath.FromSlash(guard.File))
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Errorf("%s: guard file %s: %v", entry.Operation, guard.File, err)
				continue
			}
			switch filepath.Ext(guard.File) {
			case ".go":
				if !strings.HasSuffix(guard.File, "_test.go") {
					t.Errorf("%s: guard %s is not in a _test.go file", entry.Operation, guard.File)
				}
				if !goTestFuncDeclared(t, path, guard.Test) {
					t.Errorf("%s: %s declares no func %s", entry.Operation, guard.File, guard.Test)
				}
			case ".py":
				if !regexp.MustCompile(`(?m)^(async )?def ` + regexp.QuoteMeta(guard.Test) + `\(`).Match(raw) {
					t.Errorf("%s: %s declares no def %s", entry.Operation, guard.File, guard.Test)
				}
			default:
				t.Errorf("%s: guard file %s is neither Go nor Python", entry.Operation, guard.File)
			}
		}
	}
}

func goTestFuncDeclared(t *testing.T, path, name string) bool {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Recv == nil && function.Name.Name == name && strings.HasPrefix(name, "Test") {
			return true
		}
	}
	return false
}

// The ledger names exactly the operations whose Strawberry field body raises
// the deletion error, and its message template is the text that field body
// raises. A ledger entry with a live Python path, or a deleted path with no
// ledger entry, turns this red.
func TestLedgerMatchesTheDeletedFieldBodiesInSchemaPy(t *testing.T) {
	root := repoRootFromTest(t)
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash("src/dev_health_ops/api/graphql/schema.py")))
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)

	callers := map[string]bool{}
	for _, match := range regexp.MustCompile(`_raise_served_by_query_api\(\s*"([A-Za-z0-9_]+)"`).FindAllStringSubmatch(source, -1) {
		callers[match[1]] = true
	}
	ledger := defaultLedgerForTest(t)
	var fromSchema []string
	for operation := range callers {
		fromSchema = append(fromSchema, operation)
	}
	sort.Strings(fromSchema)
	// A ledger operation is either a root field whose body raises, or a named
	// document over one (its response root is the root field that raises).
	roots := map[string]bool{}
	for _, operation := range ledger.Operations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("ledger operation %s: %v", operation, err)
		}
		roots[spec.ResponseRoot] = true
	}
	var fromLedger []string
	for root := range roots {
		fromLedger = append(fromLedger, root)
	}
	sort.Strings(fromLedger)
	if strings.Join(fromSchema, ",") != strings.Join(fromLedger, ",") {
		t.Fatalf("schema.py raises the deletion error for %v; the ledger's response roots are %v", fromSchema, fromLedger)
	}

	if got := pythonRaisedMessage(t, source); got != ledger.MessageTemplate {
		t.Fatalf("schema.py raises\n%q\nthe ledger expects\n%q", got, ledger.MessageTemplate)
	}
}

// pythonRaisedMessage concatenates the string literals of the
// GoServedOperationUnavailableError raise in _raise_served_by_query_api,
// keeping the f-string placeholder for the operation as "{operation}".
func pythonRaisedMessage(t *testing.T, source string) string {
	t.Helper()
	marker := "raise GoServedOperationUnavailableError("
	start := strings.Index(source, marker)
	if start < 0 {
		t.Fatal("schema.py has no GoServedOperationUnavailableError raise")
	}
	rest := source[start+len(marker):]
	literal := regexp.MustCompile(`^\s*f?"((?:[^"\\]|\\.)*)"`)
	var message strings.Builder
	for {
		match := literal.FindStringSubmatch(rest)
		if match == nil {
			break
		}
		unquoted, err := strconv.Unquote(`"` + match[1] + `"`)
		if err != nil {
			t.Fatalf("unquote %q: %v", match[1], err)
		}
		message.WriteString(unquoted)
		rest = rest[len(match[0]):]
	}
	if message.Len() == 0 {
		t.Fatal("no string literal found in the raise")
	}
	return message.String()
}

func TestLedgerRejectsMalformedDocuments(t *testing.T) {
	valid := string(goServedLedgerJSON)
	var base map[string]any
	if err := json.Unmarshal(goServedLedgerJSON, &base); err != nil {
		t.Fatal(err)
	}
	mutate := func(change func(entry map[string]any, doc map[string]any)) []byte {
		var doc map[string]any
		_ = json.Unmarshal(goServedLedgerJSON, &doc)
		entry := doc["entries"].([]any)[0].(map[string]any)
		change(entry, doc)
		encoded, _ := json.Marshal(doc)
		return encoded
	}
	for name, raw := range map[string][]byte{
		"empty":                 nil,
		"trailing bytes":        []byte(valid + " {}"),
		"closing brace after":   []byte(valid + "}"),
		"closing bracket after": []byte(valid + "]"),
		"second document":       []byte(valid + valid),
		"unknown field":         []byte(strings.Replace(valid, `"entries"`, `"extra": 1, "entries"`, 1)),
		"no placeholder":        mutate(func(_, doc map[string]any) { doc["deletion_error_message"] = "fixed text" }),
		"two placeholders":      mutate(func(_, doc map[string]any) { doc["deletion_error_message"] = "{operation} {operation}" }),
		"no entries":            mutate(func(_, doc map[string]any) { doc["entries"] = []any{} }),
		"short sha":             mutate(func(e, _ map[string]any) { e["two_plane_ops_sha"] = "33ebad6b" }),
		"uppercase sha":         mutate(func(e, _ map[string]any) { e["two_plane_ops_sha"] = strings.ToUpper(e["two_plane_ops_sha"].(string)) }),
		"no guards":             mutate(func(e, _ map[string]any) { e["guards"] = []any{} }),
		"guard with a separator": mutate(func(e, _ map[string]any) {
			e["guards"] = []any{map[string]any{"file": "a_test.go", "test": "TestA,TestB"}}
		}),
		"guard with no file": mutate(func(e, _ map[string]any) { e["guards"] = []any{map[string]any{"file": " ", "test": "TestA"}} }),
		"duplicate guard": mutate(func(e, _ map[string]any) {
			g := map[string]any{"file": "a_test.go", "test": "TestA"}
			e["guards"] = []any{g, g}
		}),
		"operation with a separator": mutate(func(e, _ map[string]any) { e["operation"] = "a;b" }),
		"duplicate operation": mutate(func(e, doc map[string]any) {
			doc["entries"] = append(doc["entries"].([]any), e)
		}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseGoServedLedger(raw); err == nil {
				t.Fatal("a malformed ledger was accepted")
			}
		})
	}
	if _, err := ParseGoServedLedger([]byte(valid)); err != nil {
		t.Fatalf("the shipped ledger is refused: %v", err)
	}
}

func TestCitationConstructorRefusesAnythingTheLedgerDoesNotName(t *testing.T) {
	ledger := defaultLedgerForTest(t)
	for _, operation := range []string{"", "testopsRisk", "capacityforecast", "capacityForecast ", "GO-ONLY:x"} {
		if _, err := NewGoOnlyCitation(ledger, operation); err == nil {
			t.Errorf("a citation was built for %q", operation)
		}
	}
	if _, err := NewGoOnlyCitation(nil, "capacityForecast"); err == nil {
		t.Error("a citation was built from a nil ledger")
	}
	for _, operation := range ledger.Operations() {
		citation, err := NewGoOnlyCitation(ledger, operation)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		parsed, err := ParseGoOnlyCitation(citation)
		if err != nil {
			t.Fatalf("%s: parse own citation %q: %v", operation, citation, err)
		}
		entry, _ := ledger.Entry(operation)
		if parsed.Operation != operation || parsed.TwoPlaneOpsSHA != entry.TwoPlaneOpsSHA || len(parsed.Guards) != len(entry.Guards) {
			t.Fatalf("%s: round trip lost a field: %+v", operation, parsed)
		}
		if !HasGoOnlyPrefix(citation) {
			t.Fatalf("%s: HasGoOnlyPrefix is false for its own citation", operation)
		}
	}
}

func TestCitationParserAcceptsOnlyTheCanonicalText(t *testing.T) {
	ledger := defaultLedgerForTest(t)
	good, _ := NewGoOnlyCitation(ledger, "capacityForecast")
	sha := strings.Repeat("a", 40)
	for name, citation := range map[string]string{
		"empty":            "",
		"no prefix":        strings.TrimPrefix(good, GoOnlyCitationPrefix),
		"lowercase prefix": strings.ToLower(GoOnlyCitationPrefix) + strings.TrimPrefix(good, GoOnlyCitationPrefix),
		"missing field":    GoOnlyCitationPrefix + "op=x;two_plane=" + sha,
		"extra field":      good + ";more=1",
		"reordered":        GoOnlyCitationPrefix + "two_plane=" + sha + ";op=x;guards=TestA",
		"short sha":        GoOnlyCitationPrefix + "op=x;two_plane=abc;guards=TestA",
		"no guard":         GoOnlyCitationPrefix + "op=x;two_plane=" + sha + ";guards=",
		"empty guard":      GoOnlyCitationPrefix + "op=x;two_plane=" + sha + ";guards=TestA,,TestB",
		"duplicate guard":  GoOnlyCitationPrefix + "op=x;two_plane=" + sha + ";guards=TestA,TestA",
		"blank guard":      GoOnlyCitationPrefix + "op=x;two_plane=" + sha + ";guards= ",
		"trailing blank":   good + " ",
		"leading blank":    " " + good,
	} {
		if _, err := ParseGoOnlyCitation(citation); err == nil {
			t.Errorf("%s: %q was parsed", name, citation)
		}
	}
	if _, err := ParseGoOnlyCitation(good); err != nil {
		t.Fatalf("own citation refused: %v", err)
	}
}

// The prefix is reserved in both directions. An ordinary citation can never
// carry it -- a declared baseline defect is refused before a request is sent,
// the writer refuses any prefixed value that is not the ledger's own, and the
// REST writer refuses it outright. The go-only arm can only emit the
// constructor's value.
func TestThePrefixIsReservedForTheLedgersOwnCitation(t *testing.T) {
	ledger := defaultLedgerForTest(t)
	own, _ := NewGoOnlyCitation(ledger, "capacityForecast")

	for _, ticket := range []string{"GO-ONLY:x", "go-only:x", " GO-ONLY:x", "\tgo-only:x", own} {
		err := validateBaselineDefects([]BaselineDefect{{Ticket: ticket}})
		if err == nil {
			t.Errorf("a declared defect %q was accepted", ticket)
		}
	}
	if err := validateBaselineDefects([]BaselineDefect{{Ticket: "ABC-123"}}); err != nil {
		t.Fatalf("an ordinary ticket was refused: %v", err)
	}

	otherOp, _ := NewGoOnlyCitation(ledger, "throughputForecast")
	forged := strings.Replace(own, "guards=", "guards=TestForged,", 1)
	for name, c := range map[string]struct {
		operation, state string
		citations        []string
		ok               bool
	}{
		"own citation, mismatch":                 {"capacityForecast", TerminalStateMismatch, []string{own}, true},
		"no prefix anywhere":                     {"capacityForecast", TerminalStateMismatch, []string{"ABC-123"}, true},
		"no citations":                           {"capacityForecast", TerminalStateMatch, nil, true},
		"own citation on a match receipt":        {"capacityForecast", TerminalStateMatch, []string{own}, false},
		"own citation on unsupported":            {"capacityForecast", TerminalStateUnsupported, []string{own}, false},
		"another operation's citation":           {"capacityForecast", TerminalStateMismatch, []string{otherOp}, false},
		"forged guards":                          {"capacityForecast", TerminalStateMismatch, []string{forged}, false},
		"duplicated own guard":                   {"capacityForecast", TerminalStateMismatch, []string{own + "," + strings.Split(strings.SplitN(own, "guards=", 2)[1], ",")[0]}, false},
		"unparseable claim":                      {"capacityForecast", TerminalStateMismatch, []string{GoOnlyCitationPrefix + "x"}, false},
		"own citation plus an ordinary citation": {"capacityForecast", TerminalStateMismatch, []string{own, "ABC-123"}, false},
		"own citation twice":                     {"capacityForecast", TerminalStateMismatch, []string{own, own}, false},
		"lowercase claim":                        {"capacityForecast", TerminalStateMismatch, []string{strings.ToLower(own)}, false},
		"blank-led claim":                        {"capacityForecast", TerminalStateMismatch, []string{" " + own}, false},
		"operation not in the ledger":            {"testopsRisk", TerminalStateMismatch, []string{own}, false},
	} {
		err := ValidateGoOnlyReceiptCitations(ledger, c.operation, c.state, c.citations)
		if (err == nil) != c.ok {
			t.Errorf("%s: err=%v, want ok=%v", name, err, c.ok)
		}
	}
}

// The deletion-error matcher, over every way the baseline body can differ
// from exactly Python's answer.
func TestDeletionErrorMatcherDomain(t *testing.T) {
	ledger := defaultLedgerForTest(t)
	const op = "capacityForecast"
	message := ledger.ExpectedMessage(op)
	errorObject := func(overrides map[string]any) map[string]any {
		object := map[string]any{"message": message, "locations": []any{map[string]any{"line": 1, "column": 9}}, "path": []any{op}}
		for key, value := range overrides {
			if value == nil {
				delete(object, key)
				continue
			}
			object[key] = value
		}
		return object
	}
	body := func(data any, errs ...any) string {
		document := map[string]any{"data": data}
		if errs != nil {
			document["errors"] = errs
		}
		encoded, _ := json.Marshal(document)
		return string(encoded)
	}
	for name, c := range map[string]struct {
		raw   string
		match bool
	}{
		"data null":               {body(nil, errorObject(nil)), true},
		"root null":               {body(map[string]any{op: nil}, errorObject(nil)), true},
		"no locations":            {body(nil, errorObject(map[string]any{"locations": nil})), true},
		"no data key":             {`{"errors":[` + mustJSON(errorObject(nil)) + `]}`, true},
		"no errors":               {body(map[string]any{op: map[string]any{"a": 1}}), false},
		"empty errors":            {body(nil, []any{}...), false},
		"two errors":              {body(nil, errorObject(nil), errorObject(nil)), false},
		"other operation message": {body(nil, errorObject(map[string]any{"message": ledger.ExpectedMessage("throughputForecast")})), false},
		"suffix appended":         {body(nil, errorObject(map[string]any{"message": message + " "})), false},
		"prefix only":             {body(nil, errorObject(map[string]any{"message": op + " is served by query-api and has no Python implementation."})), false},
		"unrelated error":         {body(nil, errorObject(map[string]any{"message": "boom"})), false},
		"message absent":          {body(nil, errorObject(map[string]any{"message": nil})), false},
		"message not a string":    {body(nil, errorObject(map[string]any{"message": 7})), false},
		"path other root":         {body(nil, errorObject(map[string]any{"path": []any{"capacityForecasts"}})), false},
		"path nested":             {body(nil, errorObject(map[string]any{"path": []any{op, "forecastId"}})), false},
		"path empty":              {body(nil, errorObject(map[string]any{"path": []any{}})), false},
		"path absent":             {body(nil, errorObject(map[string]any{"path": nil})), false},
		"path not a list":         {body(nil, errorObject(map[string]any{"path": op})), false},
		"extensions key":          {body(nil, errorObject(map[string]any{"extensions": map[string]any{"code": "X"}})), false},
		"data with a value":       {body(map[string]any{op: map[string]any{"a": 1}}, errorObject(nil)), false},
		"data with a second root": {body(map[string]any{op: nil, "other": nil}, errorObject(nil)), false},
		"data other root":         {body(map[string]any{"other": nil}, errorObject(nil)), false},
		"data empty object":       {body(map[string]any{}, errorObject(nil)), false},
		"data a list":             {body([]any{}, errorObject(nil)), false},
		"trailing bytes":          {body(nil, errorObject(nil)) + "\n{}", false},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot, err := DecodeSnapshot([]byte(c.raw))
			if err != nil {
				t.Fatalf("DecodeSnapshot: %v", err)
			}
			got, why := ledger.DeletionErrorMatches(op, op, snapshot)
			if got != c.match {
				t.Fatalf("matched=%v (%s), want %v", got, why, c.match)
			}
		})
	}
	if got, _ := (*GoServedLedger)(nil).DeletionErrorMatches(op, op, Snapshot{}); got {
		t.Fatal("a nil ledger matched")
	}
	if got, _ := ledger.DeletionErrorMatches(op, "", Snapshot{}); got {
		t.Fatal("an empty root matched")
	}
}

func mustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}
