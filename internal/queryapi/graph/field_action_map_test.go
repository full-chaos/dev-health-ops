package graph

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const (
	fieldActionMapPath = "../../../contracts/auth/v1/graphql-field-actions.json"
	declaredGapTicket  = "CHAOS-11042"
	unknownValue       = "unknown"
	actionMapTicket    = "CHAOS-12657"
)

type fieldActionRow struct {
	Type             string   `json:"type"`
	Field            string   `json:"field"`
	Action           string   `json:"action"`
	ResourceResolver string   `json:"resource_resolver"`
	Disclosure       string   `json:"disclosure_behavior"`
	Resolver         string   `json:"resolver"`
	Gaps             []string `json:"gaps"`
}

type fieldActionDoc struct {
	SchemaVersion  string           `json:"schema_version"`
	CatalogVersion int              `json:"catalog_version"`
	Rows           []fieldActionRow `json:"rows"`
}

func servedRootFieldsWithOrgArg(t *testing.T) ([]string, map[string]bool) {
	t.Helper()
	schema := NewExecutableSchema(Config{}).Schema()
	orgArg := map[string]bool{}
	var served []string
	for _, root := range []string{"Query", "Mutation", "Subscription"} {
		def := schema.Types[root]
		if def == nil {
			continue
		}
		for _, field := range def.Fields {
			if strings.HasPrefix(field.Name, "__") {
				continue
			}
			served = append(served, root+"."+field.Name)
			for _, arg := range field.Arguments {
				if arg.Name == "orgId" {
					orgArg[root+"."+field.Name] = true
				}
			}
		}
	}
	sort.Strings(served)
	return served, orgArg
}

func loadFieldActionDoc(t *testing.T) fieldActionDoc {
	t.Helper()
	raw, err := os.ReadFile(fieldActionMapPath)
	if err != nil {
		t.Fatalf("the GraphQL per-field action map: %v", err)
	}
	var doc fieldActionDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("%s: %v", fieldActionMapPath, err)
	}
	return doc
}

func resolverPointer(row fieldActionRow) string {
	return strings.ToLower(row.Type[:1]) + row.Type[1:] + "Resolver." + strings.ToUpper(row.Field[:1]) + row.Field[1:] + " in internal/queryapi/graph/schema.resolvers.go"
}

// declText is the comment-free, gofmt-printed text of one declaration of a Go
// file: a function (a method is "Type.Name"), or a var or const spec by name.
// A proof needle is searched in this text only, so a mention in a comment or
// elsewhere in the file never proves a statement.
func declText(src, name string) (string, bool) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "", src, 0)
	if err != nil {
		return "", false
	}
	print := func(node any) string {
		var buf bytes.Buffer
		if err := format.Node(&buf, fset, node); err != nil {
			return ""
		}
		return buf.String()
	}
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			got := d.Name.Name
			if d.Recv != nil && len(d.Recv.List) == 1 {
				recv := d.Recv.List[0].Type
				if star, ok := recv.(*ast.StarExpr); ok {
					recv = star.X
				}
				if id, ok := recv.(*ast.Ident); ok {
					got = id.Name + "." + got
				}
			}
			if got == name {
				return print(d), true
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				if v, ok := spec.(*ast.ValueSpec); ok {
					for _, id := range v.Names {
						if id.Name == name {
							return print(v), true
						}
					}
				}
			}
		}
	}
	return "", false
}

// proofDefects returns what is missing for a proof to hold: the declaration,
// a needle, or (when anyOf is set) every alternative.
func proofDefects(who string, proof claimProof, read func(file string) string, decl string) []string {
	text, found := declText(read(proof.file), decl)
	if !found {
		return []string{fmt.Sprintf("%s: %s holds no declaration %q to prove the statement", who, proof.file, decl)}
	}
	var defects []string
	for _, needle := range proof.needles {
		if !strings.Contains(text, needle) {
			defects = append(defects, fmt.Sprintf("%s: the body of %s in %s does not contain %q", who, decl, proof.file, needle))
		}
	}
	if len(proof.anyOf) > 0 {
		hit := false
		for _, needle := range proof.anyOf {
			hit = hit || strings.Contains(text, needle)
		}
		if !hit {
			defects = append(defects, fmt.Sprintf("%s: the body of %s in %s contains none of %q", who, decl, proof.file, proof.anyOf))
		}
	}
	return defects
}

func hasActionGap(row fieldActionRow) bool {
	for _, gap := range row.Gaps {
		if strings.HasPrefix(gap, "action:") {
			return true
		}
	}
	return false
}

// mapClasses names the registered statements a field's row must carry, from
// the served schema: a field with an orgId argument resolves by argument
// (mutations add requireMutationOrg), the two platform and operator fields
// carry their own, every other field resolves from the credential.
func mapClasses(row fieldActionRow, orgArg map[string]bool) (resource, disclosure string) {
	key := row.Type + "." + row.Field
	switch {
	case key == "Query.productTelemetryPlatformDashboard":
		return "platform", "dplatform"
	case key == "Query.dataHealth":
		return "cred", "ddata"
	case orgArg[key] && row.Type == "Mutation":
		return "arg", "dmut"
	case orgArg[key]:
		return "arg", "darg"
	}
	return "cred", "dcred"
}

// checkFieldActionMap returns every defect of doc: a served field with no row
// or with more than one, a row for a field nothing serves, a catalog version
// that is not the catalog's, a field the field action table (field_actions.go)
// has no entry for or an entry for a field nothing serves, an action that is
// empty, "unknown", not in the catalog or not the table's, a platform-scope action
// on a field whose resource_resolver is not "none" (or the reverse), an action
// row that still carries an "action:" gap, a resource_resolver or disclosure_behavior that is not the statement
// registered for the field's class or whose proof is missing from the
// declaration it names, and a resolver pointer the field name does not derive.
func checkFieldActionMap(served []string, orgArg map[string]bool, doc fieldActionDoc, cat *policycatalog.Catalog, actions map[string]string, claims map[string]mapClaim, read func(file string) string) []string {
	var defects []string
	count := map[string]int{}
	for _, row := range doc.Rows {
		count[row.Type+"."+row.Field]++
	}
	servedSet := map[string]bool{}
	for _, key := range served {
		servedSet[key] = true
		switch n := count[key]; {
		case n == 0:
			defects = append(defects, "served field with no action row: "+key)
		case n > 1:
			defects = append(defects, fmt.Sprintf("served field with %d action rows: %s", n, key))
		}
		if _, listed := actions[key]; !listed {
			defects = append(defects, "served field with no entry in the field action table: "+key)
		}
	}
	for key := range actions {
		if !servedSet[key] {
			defects = append(defects, "field action table entry for a field nothing serves: "+key)
		}
	}
	for key := range count {
		if !servedSet[key] {
			defects = append(defects, "action row for a field nothing serves: "+key)
		}
	}
	if doc.CatalogVersion != cat.Version() {
		defects = append(defects, fmt.Sprintf("map catalog_version %d, catalog %d", doc.CatalogVersion, cat.Version()))
	}
	for _, row := range doc.Rows {
		key := row.Type + "." + row.Field
		switch {
		case row.Action == "":
			defects = append(defects, key+": action is empty or null")
		case row.Action == unknownValue:
			defects = append(defects, fmt.Sprintf("%s: action is unknown; every query-api field names a catalog action (%s)", key, actionMapTicket))
		default:
			action, err := cat.Action(row.Action)
			switch {
			case err != nil:
				defects = append(defects, fmt.Sprintf("%s: action %q is not in the catalog", key, row.Action))
			case row.Action != actions[key]:
				defects = append(defects, fmt.Sprintf("%s: action %q is not the field action table's %q", key, row.Action, actions[key]))
			case (action.Scope == policycatalog.ScopePlatform) != strings.HasPrefix(row.ResourceResolver, "none"):
				defects = append(defects, fmt.Sprintf("%s: action %q has scope %s but resource_resolver is %.40q (a platform action needs resolver none, an organization action an organization resolver)", key, row.Action, action.Scope, row.ResourceResolver))
			}
			if hasActionGap(row) {
				defects = append(defects, fmt.Sprintf("%s: the row names action %q and still carries an \"action:\" gap", key, row.Action))
			}
		}
		resClass, discClass := mapClasses(row, orgArg)
		statement := func(field, value, class string) {
			claim := claims[class]
			switch {
			case value == "":
				defects = append(defects, fmt.Sprintf("%s: %s is empty or null", key, field))
			case value != claim.value:
				defects = append(defects, fmt.Sprintf("%s: %s is not the statement proven for class %s: %.80q", key, field, class, value))
			default:
				for _, proof := range claim.proofs {
					decl := strings.ReplaceAll(proof.decl, "{resolver}", strings.SplitN(row.Resolver, " in ", 2)[0])
					defects = append(defects, proofDefects(key+" "+field, proof, read, decl)...)
				}
			}
		}
		statement("resource_resolver", row.ResourceResolver, resClass)
		statement("disclosure_behavior", row.Disclosure, discClass)
		if row.Resolver != resolverPointer(row) {
			defects = append(defects, fmt.Sprintf("%s: resolver %q is not %q", key, row.Resolver, resolverPointer(row)))
		}
	}
	return defects
}

func readSource(t *testing.T) func(file string) string {
	return func(file string) string {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
}

func TestEveryServedGraphQLFieldHasExactlyOneRowAndEveryStatementIsProvenOrAGap(t *testing.T) {
	served, orgArg := servedRootFieldsWithOrgArg(t)
	if len(served) < 40 {
		t.Fatalf("only %d served root fields: the schema walk stopped seeing them", len(served))
	}
	doc := loadFieldActionDoc(t)
	if doc.SchemaVersion != "graphql-field-actions.v1" {
		t.Fatalf("schema_version %q", doc.SchemaVersion)
	}
	for _, defect := range checkFieldActionMap(served, orgArg, doc, policycatalog.Default(), FieldActionTable(), mapClaims, readSource(t)) {
		t.Error(defect)
	}
}

func TestFieldActionRowsNameTheirResolverMethod(t *testing.T) {
	text := readSource(t)("schema.resolvers.go")
	for _, row := range loadFieldActionDoc(t).Rows {
		want := "func (r *" + strings.ToLower(row.Type[:1]) + row.Type[1:] + "Resolver) " + strings.ToUpper(row.Field[:1]) + row.Field[1:] + "("
		if !strings.Contains(text, want) {
			t.Errorf("%s.%s: schema.resolvers.go holds no %q", row.Type, row.Field, want)
		}
	}
}

func TestFieldActionMapGuardCatchesPlantedDefects(t *testing.T) {
	served := []string{"Mutation.write", "Query.byArg", "Query.noArg"}
	orgArg := map[string]bool{"Query.byArg": true, "Mutation.write": true}
	src := "package p\nfunc f() { _ = \"needle-a\" /* needle-c */ }\nfunc g() { _ = \"other\" }\nfunc queryResolver_x() {}\n"
	claims := map[string]mapClaim{
		"arg":   {value: "by argument", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"needle-a"}}}},
		"darg":  {value: "disclosure by argument", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"needle-a"}}}},
		"dmut":  {value: "disclosure mutation", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"needle-a"}}}},
		"cred":  {value: "from credential", proofs: []claimProof{{file: "a.go", decl: "g", anyOf: []string{"zzz", "other"}}}},
		"dcred": {value: "disclosure credential", proofs: []claimProof{{file: "a.go", decl: "g", needles: []string{"other"}}}},
	}
	read := func(string) string { return src }
	cat := policycatalog.Default()
	mk := func(typ, field, act, res, disc string, gaps ...string) fieldActionRow {
		r := fieldActionRow{Type: typ, Field: field, Action: act, ResourceResolver: res, Disclosure: disc, Gaps: gaps}
		r.Resolver = resolverPointer(r)
		return r
	}
	table := func() map[string]string {
		return map[string]string{"Mutation.write": "reports.write", "Query.byArg": "analytics.read", "Query.noArg": "metrics.read"}
	}
	good := func() fieldActionDoc {
		return fieldActionDoc{SchemaVersion: "graphql-field-actions.v1", CatalogVersion: cat.Version(), Rows: []fieldActionRow{
			mk("Mutation", "write", "reports.write", "by argument", "disclosure mutation"),
			mk("Query", "byArg", "analytics.read", "by argument", "disclosure by argument"),
			mk("Query", "noArg", "metrics.read", "from credential", "disclosure credential"),
		}}
	}
	if defects := checkFieldActionMap(served, orgArg, good(), cat, table(), claims, read); len(defects) != 0 {
		t.Fatalf("the clean fixture is refused: %v", defects)
	}
	swap := func(to string) func(string) string { return func(string) string { return to } }
	cases := []struct {
		name   string
		mutate func(*fieldActionDoc)
		read   func(string) string
		want   string
		table  func(map[string]string)
	}{
		{"orphan row", func(d *fieldActionDoc) {
			d.Rows = append(d.Rows, mk("Query", "gone", "metrics.read", "from credential", "disclosure credential"))
		}, nil, "nothing serves: Query.gone", nil},
		{"missing row", func(d *fieldActionDoc) { d.Rows = d.Rows[1:] }, nil, "no action row: Mutation.write", nil},
		{"duplicate row", func(d *fieldActionDoc) { d.Rows = append(d.Rows, d.Rows[1]) }, nil, "2 action rows: Query.byArg", nil},
		{"null action", func(d *fieldActionDoc) { d.Rows[2].Action = "" }, nil, "action is empty or null", nil},
		{"action set back to unknown", func(d *fieldActionDoc) { d.Rows[2].Action = unknownValue }, nil, "action is unknown", nil},
		{"action not in the catalog", func(d *fieldActionDoc) { d.Rows[2].Action = "x.nothing" }, nil, "is not in the catalog", nil},
		{"action that is not the table's", func(d *fieldActionDoc) { d.Rows[2].Action = "analytics.read" }, nil, "is not the field action table's \"metrics.read\"", nil},
		{"row with an action and a stale action gap", func(d *fieldActionDoc) { d.Rows[2].Gaps = []string{"action: stale; " + declaredGapTicket} }, nil, "still carries an \"action:\" gap", nil},
		{"platform action on an organization resolver", func(d *fieldActionDoc) { d.Rows[2].Action = "admin.telemetry" }, nil, "has scope platform but resource_resolver", func(m map[string]string) { m["Query.noArg"] = "admin.telemetry" }},
		{"served field with no table entry", func(d *fieldActionDoc) {}, nil, "no entry in the field action table: Query.noArg", func(m map[string]string) { delete(m, "Query.noArg") }},
		{"table entry for a field nothing serves", func(d *fieldActionDoc) {}, nil, "nothing serves: Query.ghost", func(m map[string]string) { m["Query.ghost"] = "metrics.read" }},
		{"false disclosure", func(d *fieldActionDoc) { d.Rows[1].Disclosure = "any caller may read" }, nil, "not the statement proven for class darg", nil},
		{"disclosure of another registered class", func(d *fieldActionDoc) { d.Rows[2].Disclosure = "disclosure by argument" }, nil, "not the statement proven for class dcred", nil},
		{"resource of another registered class", func(d *fieldActionDoc) { d.Rows[2].ResourceResolver = "by argument" }, nil, "not the statement proven for class cred", nil},
		{"empty disclosure", func(d *fieldActionDoc) { d.Rows[2].Disclosure = "" }, nil, "disclosure_behavior is empty or null", nil},
		{"needle gone from the declaration", func(d *fieldActionDoc) {}, swap("package p\nfunc f() {}\nfunc g() { _ = \"other\" }\n"), "does not contain", nil},
		{"needle only in a comment", func(d *fieldActionDoc) {}, swap("package p\nfunc f() { /* needle-a */ }\nfunc g() { _ = \"other\" }\n"), "does not contain", nil},
		{"declaration absent", func(d *fieldActionDoc) {}, swap("package p\nfunc f() { _ = \"needle-a\" }\n"), "holds no declaration", nil},
		{"wrong resolver pointer", func(d *fieldActionDoc) { d.Rows[2].Resolver = "queryResolver.Missing in x" }, nil, "is not", nil},
		{"stale catalog version", func(d *fieldActionDoc) { d.CatalogVersion++ }, nil, "catalog_version", nil},
	}
	for _, c := range cases {
		doc := good()
		c.mutate(&doc)
		reader := read
		if c.read != nil {
			reader = c.read
		}
		actions := table()
		if c.table != nil {
			c.table(actions)
		}
		joined := strings.Join(checkFieldActionMap(served, orgArg, doc, cat, actions, claims, reader), "\n")
		if !strings.Contains(joined, c.want) {
			t.Errorf("%s: the guard did not report %q, it reported %q", c.name, c.want, joined)
		}
	}
}

// TestMapGuardGoesRedOnTheVerifierPlants plants, in the real source and rows,
// the defects a verifier named: the superuser branch deleted from the guard,
// the same text kept only in a comment, and a false disclosure on a real field.
func TestMapGuardGoesRedOnTheVerifierPlants(t *testing.T) {
	cat := policycatalog.Default()
	served, orgArg := servedRootFieldsWithOrgArg(t)
	doc := loadFieldActionDoc(t)
	real := readSource(t)
	if defects := checkFieldActionMap(served, orgArg, doc, cat, FieldActionTable(), mapClaims, real); len(defects) != 0 {
		t.Fatalf("the real map is refused before any plant: %v", defects[0])
	}
	swapped := func(file, old, repl string) func(string) string {
		return func(f string) string {
			text := real(f)
			if f == file {
				if !strings.Contains(text, old) {
					t.Fatalf("plant anchor %q is not in %s", old, file)
				}
				text = strings.Replace(text, old, repl, 1)
			}
			return text
		}
	}
	branch := "claims.Holds(rule.ActionAdminOrgs) && !claims.ImpersonationActive"
	deleted := swapped("operation_org_guard.go", "if "+branch+" {", "if false {")
	if joined := strings.Join(checkFieldActionMap(served, orgArg, doc, cat, FieldActionTable(), mapClaims, deleted), "\n"); !strings.Contains(joined, "does not contain") {
		t.Errorf("the superuser branch deleted from operationOrgDecision stayed green")
	}
	commented := swapped("operation_org_guard.go", "if "+branch+" {", "if false { /* "+branch+" */")
	if joined := strings.Join(checkFieldActionMap(served, orgArg, doc, cat, FieldActionTable(), mapClaims, commented), "\n"); !strings.Contains(joined, "does not contain") {
		t.Errorf("the superuser branch kept only in a comment stayed green")
	}
	var planted fieldActionDoc
	planted.CatalogVersion, planted.SchemaVersion = doc.CatalogVersion, doc.SchemaVersion
	for _, row := range doc.Rows {
		if row.Type == "Query" && row.Field == "securityAlerts" {
			row.Disclosure = "any authenticated caller reads every organization's alerts"
		}
		planted.Rows = append(planted.Rows, row)
	}
	if joined := strings.Join(checkFieldActionMap(served, orgArg, planted, cat, FieldActionTable(), mapClaims, real), "\n"); !strings.Contains(joined, "Query.securityAlerts: disclosure_behavior is not the statement proven") {
		t.Errorf("a false disclosure on Query.securityAlerts stayed green: %q", joined)
	}
	otherClass := planted
	otherClass.Rows = append([]fieldActionRow(nil), planted.Rows...)
	for i, row := range otherClass.Rows {
		if row.Type == "Query" && row.Field == "securityAlerts" {
			row.Disclosure = mapClaims["dplatform"].value
			otherClass.Rows[i] = row
		}
	}
	if joined := strings.Join(checkFieldActionMap(served, orgArg, otherClass, cat, FieldActionTable(), mapClaims, real), "\n"); !strings.Contains(joined, "not the statement proven for class darg") {
		t.Errorf("another class's registered disclosure on Query.securityAlerts stayed green")
	}
}

// TestMapGuardGoesRedWhenARealRowIsSetBackOrChanged plants, in the real rows,
// the two defects this map exists to refuse: a row set back to "unknown", and
// a row whose action is not the field action table's.
func TestMapGuardGoesRedWhenARealRowIsSetBackOrChanged(t *testing.T) {
	cat := policycatalog.Default()
	served, orgArg := servedRootFieldsWithOrgArg(t)
	doc := loadFieldActionDoc(t)
	plant := func(key, action string) fieldActionDoc {
		planted := doc
		planted.Rows = append([]fieldActionRow(nil), doc.Rows...)
		for i, row := range planted.Rows {
			if row.Type+"."+row.Field == key {
				row.Action = action
				planted.Rows[i] = row
			}
		}
		return planted
	}
	back := strings.Join(checkFieldActionMap(served, orgArg, plant("Query.home", unknownValue), cat, FieldActionTable(), mapClaims, readSource(t)), "\n")
	if !strings.Contains(back, "Query.home: action is unknown") {
		t.Errorf("a row set back to unknown stayed green: %q", back)
	}
	changed := strings.Join(checkFieldActionMap(served, orgArg, plant("Query.home", "git.read"), cat, FieldActionTable(), mapClaims, readSource(t)), "\n")
	if !strings.Contains(changed, "Query.home: action \"git.read\" is not the field action table's") {
		t.Errorf("a row whose action is not the table's stayed green: %q", changed)
	}
}
