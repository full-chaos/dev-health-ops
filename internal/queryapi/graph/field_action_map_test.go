package graph

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const fieldActionMapPath = "../../../contracts/auth/v1/graphql-field-actions.json"

type fieldActionRow struct {
	Type             string   `json:"type"`
	Field            string   `json:"field"`
	Action           *string  `json:"action"`
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

func servedRootFields(t *testing.T) []string {
	t.Helper()
	served, _ := servedRootFieldsWithOrgArg(t)
	return served
}

// servedRootFieldsWithOrgArg also reports which served fields declare an
// orgId argument, read from the executable schema.
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

func resolverPointer(row fieldActionRow) string {
	return strings.ToLower(row.Type[:1]) + row.Type[1:] + "Resolver." + strings.ToUpper(row.Field[:1]) + row.Field[1:] + " in internal/queryapi/graph/schema.resolvers.go"
}

// checkFieldActionTruth holds the rows' statements to what the schema and the
// resolver set say: a field with an orgId argument resolves its organization
// from that argument, a field without one does not claim to; the resolver
// pointer is the one the field's name derives.
func checkFieldActionTruth(orgArg map[string]bool, doc fieldActionDoc) []string {
	var defects []string
	for _, row := range doc.Rows {
		key := row.Type + "." + row.Field
		if row.Resolver != resolverPointer(row) {
			defects = append(defects, fmt.Sprintf("%s: resolver %q is not %q", key, row.Resolver, resolverPointer(row)))
		}
		byArgument := strings.HasPrefix(row.ResourceResolver, "organization_from_argument:")
		if orgArg[key] != byArgument {
			defects = append(defects, fmt.Sprintf("%s: the schema field has an orgId argument = %t, the row's resource_resolver says by-argument = %t", key, orgArg[key], byArgument))
		}
	}
	return defects
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

// checkFieldActionMap returns every defect of doc against the served root
// fields and the closed action catalog: a served field with no row or with
// more than one, a row for a field nothing serves, an action the catalog does
// not hold, a null action without an "action:" gap, and a row missing the
// fields every row carries.
func checkFieldActionMap(served []string, doc fieldActionDoc, cat *policycatalog.Catalog) []string {
	var defects []string
	count := map[string]int{}
	for _, row := range doc.Rows {
		count[row.Type+"."+row.Field]++
	}
	for _, key := range served {
		switch n := count[key]; {
		case n == 0:
			defects = append(defects, "served field with no action row: "+key)
		case n > 1:
			defects = append(defects, fmt.Sprintf("served field with %d action rows: %s", n, key))
		}
	}
	servedSet := map[string]bool{}
	for _, key := range served {
		servedSet[key] = true
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
		if row.Action != nil {
			if _, err := cat.Action(*row.Action); err != nil {
				defects = append(defects, fmt.Sprintf("%s: action %q is not in the catalog", key, *row.Action))
			}
		} else {
			named := false
			for _, gap := range row.Gaps {
				if strings.HasPrefix(gap, "action:") {
					named = true
				}
			}
			if !named {
				defects = append(defects, key+`: null action without an "action:" gap`)
			}
		}
		if row.ResourceResolver == "" || row.Disclosure == "" || row.Resolver == "" {
			defects = append(defects, key+": a row without resource_resolver, disclosure_behavior or resolver")
		}
	}
	return defects
}

func TestEveryServedGraphQLFieldHasExactlyOneActionRowAndNoRowIsOrphaned(t *testing.T) {
	served := servedRootFields(t)
	if len(served) < 40 {
		t.Fatalf("only %d served root fields: the schema walk stopped seeing them", len(served))
	}
	doc := loadFieldActionDoc(t)
	if doc.SchemaVersion != "graphql-field-actions.v1" {
		t.Fatalf("schema_version %q", doc.SchemaVersion)
	}
	for _, defect := range checkFieldActionMap(served, doc, policycatalog.Default()) {
		t.Error(defect)
	}
	_, orgArg := servedRootFieldsWithOrgArg(t)
	for _, defect := range checkFieldActionTruth(orgArg, doc) {
		t.Error(defect)
	}
}

func TestFieldActionRowsNameTheirResolverMethod(t *testing.T) {
	raw, err := os.ReadFile("schema.resolvers.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, row := range loadFieldActionDoc(t).Rows {
		want := "func (r *" + strings.ToLower(row.Type[:1]) + row.Type[1:] + "Resolver) " + strings.ToUpper(row.Field[:1]) + row.Field[1:] + "("
		if row.Resolver != resolverPointer(row) {
			t.Errorf("%s.%s: row resolver %q is not %q", row.Type, row.Field, row.Resolver, resolverPointer(row))
		}
		if !strings.Contains(text, want) {
			t.Errorf("%s.%s: schema.resolvers.go holds no %q", row.Type, row.Field, want)
		}
	}
}

func TestFieldActionMapChecksCatchPlantedDefects(t *testing.T) {
	served := []string{"Mutation.write", "Query.read", "Query.readNull"}
	action := "analytics.read"
	good := func() fieldActionDoc {
		return fieldActionDoc{
			SchemaVersion:  "graphql-field-actions.v1",
			CatalogVersion: policycatalog.Default().Version(),
			Rows: []fieldActionRow{
				{Type: "Mutation", Field: "write", Action: nil, ResourceResolver: "r", Disclosure: "d", Resolver: "m", Gaps: []string{"action: no catalog action"}},
				{Type: "Query", Field: "read", Action: &action, ResourceResolver: "r", Disclosure: "d", Resolver: "q"},
				{Type: "Query", Field: "readNull", Action: nil, ResourceResolver: "r", Disclosure: "d", Resolver: "q", Gaps: []string{"action: no catalog action"}},
			},
		}
	}
	cat := policycatalog.Default()
	if defects := checkFieldActionMap(served, good(), cat); len(defects) != 0 {
		t.Fatalf("the clean fixture is refused: %v", defects)
	}
	unknown := "analytics.everything"
	cases := []struct {
		name   string
		mutate func(*fieldActionDoc)
		want   string
	}{
		{"orphan row", func(d *fieldActionDoc) {
			d.Rows = append(d.Rows, fieldActionRow{Type: "Query", Field: "gone", Action: &action, ResourceResolver: "r", Disclosure: "d", Resolver: "q"})
		}, "nothing serves: Query.gone"},
		{"missing row", func(d *fieldActionDoc) { d.Rows = d.Rows[1:] }, "no action row: Mutation.write"},
		{"duplicate row", func(d *fieldActionDoc) { d.Rows = append(d.Rows, d.Rows[1]) }, "2 action rows: Query.read"},
		{"unknown action", func(d *fieldActionDoc) { d.Rows[1].Action = &unknown }, "not in the catalog"},
		{"null action without gap", func(d *fieldActionDoc) { d.Rows[2].Gaps = nil }, `null action without an "action:" gap`},
		{"stale catalog version", func(d *fieldActionDoc) { d.CatalogVersion++ }, "catalog_version"},
		{"empty resolver text", func(d *fieldActionDoc) { d.Rows[1].ResourceResolver = "" }, "a row without resource_resolver"},
	}
	for _, c := range cases {
		doc := good()
		c.mutate(&doc)
		joined := strings.Join(checkFieldActionMap(served, doc, cat), "\n")
		if !strings.Contains(joined, c.want) {
			t.Errorf("%s: the check did not report %q, it reported %q", c.name, c.want, joined)
		}
	}
}

func TestFieldActionTruthChecksCatchPlantedDefects(t *testing.T) {
	orgArg := map[string]bool{"Query.byArg": true}
	row := func(field, resolver string) fieldActionRow {
		r := fieldActionRow{Type: "Query", Field: field, ResourceResolver: resolver, Disclosure: "d"}
		r.Resolver = resolverPointer(r)
		return r
	}
	good := fieldActionDoc{Rows: []fieldActionRow{
		row("byArg", "organization_from_argument: x"),
		row("noArg", "organization_from_credential: x"),
	}}
	if defects := checkFieldActionTruth(orgArg, good); len(defects) != 0 {
		t.Fatalf("the clean fixture is refused: %v", defects)
	}
	wrongPointer := fieldActionDoc{Rows: []fieldActionRow{row("noArg", "organization_from_credential: x")}}
	wrongPointer.Rows[0].Resolver = "queryResolver.Missing in internal/queryapi/graph/schema.resolvers.go"
	cases := []struct {
		name string
		doc  fieldActionDoc
		want string
	}{
		{"argument field claimed as credential", fieldActionDoc{Rows: []fieldActionRow{row("byArg", "organization_from_credential: x")}}, "has an orgId argument = true"},
		{"credential field claimed as argument", fieldActionDoc{Rows: []fieldActionRow{row("noArg", "organization_from_argument: x")}}, "has an orgId argument = false"},
		{"wrong resolver pointer", wrongPointer, "is not"},
	}
	for _, c := range cases {
		joined := strings.Join(checkFieldActionTruth(orgArg, c.doc), "\n")
		if !strings.Contains(joined, c.want) {
			t.Errorf("%s: the check did not report %q, it reported %q", c.name, c.want, joined)
		}
	}
}
