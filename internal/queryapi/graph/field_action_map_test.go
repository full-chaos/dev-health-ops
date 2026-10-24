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

func hasGap(row fieldActionRow, field string) bool {
	for _, gap := range row.Gaps {
		if strings.HasPrefix(gap, field+":") && strings.Contains(gap, declaredGapTicket) {
			return true
		}
	}
	return false
}

// checkFieldActionMap returns every defect of doc: a served field with no row
// or with more than one, a row for a field nothing serves, a catalog version
// that is not the catalog's, a row field that is empty or null, an action that
// is neither a catalog action nor "unknown", an "unknown" without its named
// gap, a statement no registered claim states, a claim whose source needle is
// missing, an orgId-argument field that does not say it resolves by argument
// with the superuser exception, and a resolver pointer the field name does not
// derive.
func checkFieldActionMap(served []string, orgArg map[string]bool, doc fieldActionDoc, cat *policycatalog.Catalog, claims []mapClaim, read func(file string) string) []string {
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
	statement := func(key, field, value string) {
		if value == "" {
			defects = append(defects, fmt.Sprintf("%s: %s is empty or null", key, field))
			return
		}
		for _, claim := range claims {
			if claim.value != value {
				continue
			}
			for file, needles := range claim.proofs {
				text := read(file)
				for _, needle := range needles {
					if !strings.Contains(text, needle) {
						defects = append(defects, fmt.Sprintf("%s: the proof of %s names %q, %s does not contain it", key, field, needle, file))
					}
				}
			}
			return
		}
		defects = append(defects, fmt.Sprintf("%s: %s is a statement no registered claim states: %.80q", key, field, value))
	}
	for _, row := range doc.Rows {
		key := row.Type + "." + row.Field
		switch {
		case row.Action == "":
			defects = append(defects, key+": action is empty or null")
		case row.Action == unknownValue:
			if !hasGap(row, "action") {
				defects = append(defects, fmt.Sprintf("%s: action unknown without an \"action:\" gap naming %s", key, declaredGapTicket))
			}
		default:
			if _, err := cat.Action(row.Action); err != nil {
				defects = append(defects, fmt.Sprintf("%s: action %q is not in the catalog", key, row.Action))
			}
		}
		statement(key, "resource_resolver", row.ResourceResolver)
		statement(key, "disclosure_behavior", row.Disclosure)
		if row.Resolver != resolverPointer(row) {
			defects = append(defects, fmt.Sprintf("%s: resolver %q is not %q", key, row.Resolver, resolverPointer(row)))
		}
		byArgument := strings.HasPrefix(row.ResourceResolver, "organization_from_argument:")
		if orgArg[key] != byArgument {
			defects = append(defects, fmt.Sprintf("%s: the schema field has an orgId argument = %t, the row's resource_resolver says by-argument = %t", key, orgArg[key], byArgument))
		}
		if byArgument && !strings.Contains(row.Disclosure, "superuser") {
			defects = append(defects, key+": a by-argument field's disclosure_behavior omits the verified non-impersonating superuser exception of OperationOrgGuard")
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
	orgArg := map[string]bool{"Query.byArg": true}
	claims := []mapClaim{
		{value: "organization_from_argument: x", proofs: map[string][]string{"a.go": {"needle-a"}}},
		{value: "organization_from_credential: x", proofs: map[string][]string{"a.go": {"needle-a"}}},
		{value: "disclosure superuser", proofs: map[string][]string{"a.go": {"needle-a"}}},
		{value: "disclosure plain", proofs: map[string][]string{"a.go": {"needle-a"}}},
	}
	read := func(file string) string { return "needle-a" }
	cat := policycatalog.Default()
	action := "analytics.read"
	mk := func(typ, field, act, res, disc string, gaps ...string) fieldActionRow {
		r := fieldActionRow{Type: typ, Field: field, Action: act, ResourceResolver: res, Disclosure: disc, Gaps: gaps}
		r.Resolver = resolverPointer(r)
		return r
	}
	good := func() fieldActionDoc {
		return fieldActionDoc{SchemaVersion: "graphql-field-actions.v1", CatalogVersion: cat.Version(), Rows: []fieldActionRow{
			mk("Mutation", "write", unknownValue, "organization_from_credential: x", "disclosure plain", "action: none; "+declaredGapTicket),
			mk("Query", "byArg", action, "organization_from_argument: x", "disclosure superuser"),
			mk("Query", "noArg", action, "organization_from_credential: x", "disclosure plain"),
		}}
	}
	if defects := checkFieldActionMap(served, orgArg, good(), cat, claims, read); len(defects) != 0 {
		t.Fatalf("the clean fixture is refused: %v", defects)
	}
	cases := []struct {
		name   string
		mutate func(*fieldActionDoc)
		read   func(string) string
		want   string
	}{
		{"orphan row", func(d *fieldActionDoc) {
			d.Rows = append(d.Rows, mk("Query", "gone", action, "organization_from_credential: x", "disclosure plain"))
		}, nil, "nothing serves: Query.gone"},
		{"missing row", func(d *fieldActionDoc) { d.Rows = d.Rows[1:] }, nil, "no action row: Mutation.write"},
		{"duplicate row", func(d *fieldActionDoc) { d.Rows = append(d.Rows, d.Rows[1]) }, nil, "2 action rows: Query.byArg"},
		{"null action", func(d *fieldActionDoc) { d.Rows[2].Action = "" }, nil, "action is empty or null"},
		{"action outside the catalog", func(d *fieldActionDoc) { d.Rows[2].Action = "analytics.everything" }, nil, "not in the catalog"},
		{"unknown action without a gap", func(d *fieldActionDoc) { d.Rows[0].Gaps = nil }, nil, "unknown without an"},
		{"unknown action whose gap omits the ticket", func(d *fieldActionDoc) { d.Rows[0].Gaps = []string{"action: none"} }, nil, "unknown without an"},
		{"unproven resource statement", func(d *fieldActionDoc) { d.Rows[2].ResourceResolver = "organization_from_credential: something else" }, nil, "no registered claim states"},
		{"unproven disclosure statement", func(d *fieldActionDoc) { d.Rows[2].Disclosure = "always allowed" }, nil, "no registered claim states"},
		{"empty disclosure", func(d *fieldActionDoc) { d.Rows[2].Disclosure = "" }, nil, "disclosure_behavior is empty or null"},
		{"proof needle missing from source", func(d *fieldActionDoc) {}, func(string) string { return "nothing" }, "does not contain it"},
		{"argument field claimed as credential", func(d *fieldActionDoc) { d.Rows[1].ResourceResolver = "organization_from_credential: x" }, nil, "has an orgId argument = true"},
		{"credential field claimed as argument", func(d *fieldActionDoc) { d.Rows[2].ResourceResolver = "organization_from_argument: x" }, nil, "has an orgId argument = false"},
		{"argument disclosure omits the superuser exception", func(d *fieldActionDoc) { d.Rows[1].Disclosure = "disclosure plain" }, nil, "omits the verified non-impersonating superuser"},
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
