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

func hasGap(row fieldActionRow, field string) bool {
	for _, gap := range row.Gaps {
		if strings.HasPrefix(gap, field+":") && strings.Contains(gap, declaredGapTicket) {
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
// that is not the catalog's, an action that is not "unknown" (no action has a
// registered proof: query-api enforces none), an "unknown" without its named
// gap, a resource_resolver or disclosure_behavior that is not the statement
// registered for the field's class or whose proof is missing from the
// declaration it names, and a resolver pointer the field name does not derive.
func checkFieldActionMap(served []string, orgArg map[string]bool, doc fieldActionDoc, cat *policycatalog.Catalog, claims map[string]mapClaim, read func(file string) string) []string {
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
		case row.Action != unknownValue:
			defects = append(defects, fmt.Sprintf("%s: action %q has no registered proof, state it as %q with a gap", key, row.Action, unknownValue))
		case !hasGap(row, "action"):
			defects = append(defects, fmt.Sprintf("%s: action unknown without an \"action:\" gap naming %s", key, declaredGapTicket))
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
	for _, defect := range checkFieldActionMap(served, orgArg, doc, policycatalog.Default(), mapClaims, readSource(t)) {
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
	gap := "action: none; " + declaredGapTicket
	good := func() fieldActionDoc {
		return fieldActionDoc{SchemaVersion: "graphql-field-actions.v1", CatalogVersion: cat.Version(), Rows: []fieldActionRow{
			mk("Mutation", "write", unknownValue, "by argument", "disclosure mutation", gap),
			mk("Query", "byArg", unknownValue, "by argument", "disclosure by argument", gap),
			mk("Query", "noArg", unknownValue, "from credential", "disclosure credential", gap),
		}}
	}
	if defects := checkFieldActionMap(served, orgArg, good(), cat, claims, read); len(defects) != 0 {
		t.Fatalf("the clean fixture is refused: %v", defects)
	}
	swap := func(to string) func(string) string { return func(string) string { return to } }
	cases := []struct {
		name   string
		mutate func(*fieldActionDoc)
		read   func(string) string
		want   string
	}{
		{"orphan row", func(d *fieldActionDoc) {
			d.Rows = append(d.Rows, mk("Query", "gone", unknownValue, "from credential", "disclosure credential", gap))
		}, nil, "nothing serves: Query.gone"},
		{"missing row", func(d *fieldActionDoc) { d.Rows = d.Rows[1:] }, nil, "no action row: Mutation.write"},
		{"duplicate row", func(d *fieldActionDoc) { d.Rows = append(d.Rows, d.Rows[1]) }, nil, "2 action rows: Query.byArg"},
		{"null action", func(d *fieldActionDoc) { d.Rows[2].Action = "" }, nil, "action is empty or null"},
		{"catalog action with no registered proof", func(d *fieldActionDoc) { d.Rows[2].Action = "analytics.read" }, nil, "has no registered proof"},
		{"unknown action without a gap", func(d *fieldActionDoc) { d.Rows[0].Gaps = nil }, nil, "unknown without an"},
		{"unknown action whose gap omits the ticket", func(d *fieldActionDoc) { d.Rows[0].Gaps = []string{"action: none"} }, nil, "unknown without an"},
		{"false disclosure", func(d *fieldActionDoc) { d.Rows[1].Disclosure = "any caller may read" }, nil, "not the statement proven for class darg"},
		{"disclosure of another registered class", func(d *fieldActionDoc) { d.Rows[2].Disclosure = "disclosure by argument" }, nil, "not the statement proven for class dcred"},
		{"resource of another registered class", func(d *fieldActionDoc) { d.Rows[2].ResourceResolver = "by argument" }, nil, "not the statement proven for class cred"},
		{"empty disclosure", func(d *fieldActionDoc) { d.Rows[2].Disclosure = "" }, nil, "disclosure_behavior is empty or null"},
		{"needle gone from the declaration", func(d *fieldActionDoc) {}, swap("package p\nfunc f() {}\nfunc g() { _ = \"other\" }\n"), "does not contain"},
		{"needle only in a comment", func(d *fieldActionDoc) {}, swap("package p\nfunc f() { /* needle-a */ }\nfunc g() { _ = \"other\" }\n"), "does not contain"},
		{"declaration absent", func(d *fieldActionDoc) {}, swap("package p\nfunc f() { _ = \"needle-a\" }\n"), "holds no declaration"},
		{"wrong resolver pointer", func(d *fieldActionDoc) { d.Rows[2].Resolver = "queryResolver.Missing in x" }, nil, "is not"},
		{"stale catalog version", func(d *fieldActionDoc) { d.CatalogVersion++ }, nil, "catalog_version"},
	}
	for _, c := range cases {
		doc := good()
		c.mutate(&doc)
		reader := read
		if c.read != nil {
			reader = c.read
		}
		joined := strings.Join(checkFieldActionMap(served, orgArg, doc, cat, claims, reader), "\n")
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
	if defects := checkFieldActionMap(served, orgArg, doc, cat, mapClaims, real); len(defects) != 0 {
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
	branch := "claims.IsSuperuser && !claims.ImpersonationActive"
	deleted := swapped("operation_org_guard.go", "if "+branch+" {", "if false {")
	if joined := strings.Join(checkFieldActionMap(served, orgArg, doc, cat, mapClaims, deleted), "\n"); !strings.Contains(joined, "does not contain") {
		t.Errorf("the superuser branch deleted from operationOrgDecision stayed green")
	}
	commented := swapped("operation_org_guard.go", "if "+branch+" {", "if false { /* "+branch+" */")
	if joined := strings.Join(checkFieldActionMap(served, orgArg, doc, cat, mapClaims, commented), "\n"); !strings.Contains(joined, "does not contain") {
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
	if joined := strings.Join(checkFieldActionMap(served, orgArg, planted, cat, mapClaims, real), "\n"); !strings.Contains(joined, "Query.securityAlerts: disclosure_behavior is not the statement proven") {
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
	if joined := strings.Join(checkFieldActionMap(served, orgArg, otherClass, cat, mapClaims, real), "\n"); !strings.Contains(joined, "not the statement proven for class darg") {
		t.Errorf("another class's registered disclosure on Query.securityAlerts stayed green")
	}
}
