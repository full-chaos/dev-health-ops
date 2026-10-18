package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode"

	"github.com/full-chaos/dev-health-go/authverify"
)

// The property sweep: contracts/auth/v1/sweeps/jwks.property-sweep.json is the
// closed enumeration of where jwks.v1 differs from its consumer, by PROPERTY.
// Every verdict in it is a literal a human owns. Nothing here computes an
// expectation from the code under test; the real consumer and the schema are
// each asked and each must equal the literal.

const jwksSweepPath = "contracts/auth/v1/sweeps/jwks.property-sweep.json"

type sweepOp struct {
	Path   []any  `json:"path"`
	Set    any    `json:"set"`
	Delete bool   `json:"delete"`
	Rename string `json:"rename"`
}

type sweepRow struct {
	ID         string    `json:"id"`
	DeclaredAs string    `json:"declared_as"`
	Property   string    `json:"property"`
	Class      string    `json:"class"`
	Consumer   string    `json:"consumer"`
	Schema     string    `json:"schema"`
	Ops        []sweepOp `json:"ops"`
}

type sweepCategory struct {
	Category string `json:"category"`
	Rune     string `json:"rune"`
	Consumer string `json:"consumer"`
	Schema   string `json:"schema"`
}

type sweepFile struct {
	Base       map[string]any    `json:"base_document"`
	Excluded   map[string]string `json:"unicode_categories_excluded"`
	Categories []sweepCategory   `json:"unicode_categories"`
	Rows       []sweepRow        `json:"rows"`
	Classes    []struct {
		ID string `json:"id"`
	} `json:"declared_classes"`
}

func loadSweep(t *testing.T) sweepFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), jwksSweepPath))
	if err != nil {
		t.Fatalf("reading the property sweep: %v", err)
	}
	var f sweepFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parsing the property sweep: %v", err)
	}
	if len(f.Rows) == 0 || len(f.Categories) == 0 {
		t.Fatal("the property sweep is empty; every assertion below would be vacuous")
	}
	return f
}

func deepCopy(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func expandValue(v any) any {
	if m, ok := v.(map[string]any); ok {
		if rep, ok := m["$repeat"].([]any); ok {
			n := int(rep[1].(float64))
			prefix, _ := m["prefix"].(string)
			suffix, _ := m["suffix"].(string)
			return prefix + strings.Repeat(rep[0].(string), n) + suffix
		}
	}
	return v
}

func applyOps(t *testing.T, base map[string]any, ops []sweepOp) any {
	t.Helper()
	var doc any = deepCopy(t, base)
	for _, op := range ops {
		if len(op.Path) == 0 {
			doc = expandValue(op.Set)
			continue
		}
		parent := doc
		for _, step := range op.Path[:len(op.Path)-1] {
			parent = descend(t, parent, step)
		}
		last := op.Path[len(op.Path)-1]
		switch {
		case op.Rename != "":
			obj := parent.(map[string]any)
			name := last.(string)
			obj[op.Rename] = obj[name]
			delete(obj, name)
		case op.Delete:
			delete(parent.(map[string]any), last.(string))
		default:
			setAt(t, &doc, op.Path, expandValue(op.Set))
		}
	}
	return doc
}

func descend(t *testing.T, node any, step any) any {
	t.Helper()
	switch s := step.(type) {
	case string:
		return node.(map[string]any)[s]
	case float64:
		return node.([]any)[int(s)]
	}
	t.Fatalf("bad path step %v", step)
	return nil
}

func setAt(t *testing.T, root *any, path []any, value any) {
	t.Helper()
	if len(path) == 0 {
		*root = value
		return
	}
	var walk func(node any, rest []any) any
	walk = func(node any, rest []any) any {
		step := rest[0]
		if len(rest) == 1 {
			switch s := step.(type) {
			case string:
				node.(map[string]any)[s] = value
			case float64:
				list := node.([]any)
				if int(s) == len(list) {
					return append(list, value)
				}
				list[int(s)] = value
			}
			return node
		}
		switch s := step.(type) {
		case string:
			obj := node.(map[string]any)
			obj[s] = walk(obj[s], rest[1:])
		case float64:
			list := node.([]any)
			list[int(s)] = walk(list[int(s)], rest[1:])
		}
		return node
	}
	*root = walk(*root, path)
}

func sweepConsumerVerdict(t *testing.T, doc any) string {
	t.Helper()
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := authverify.NewEd25519JWKSVerifier(path).Keys(); err != nil {
		return "refuse"
	}
	return "accept"
}

func schemaVerdict(t *testing.T, doc any) string {
	t.Helper()
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if Validate(testRoot(t), JWKSSurface, decoded) != nil {
		return "refuse"
	}
	return "accept"
}

func classOf(consumer, schema string) string {
	switch {
	case consumer == schema:
		return "agreement"
	case consumer == "accept":
		return "narrowing"
	}
	return "client_enforced"
}

func TestEverySweepRowMatchesItsLiteralVerdictsOnTheRealConsumerAndTheSchema(t *testing.T) {
	sweep := loadSweep(t)
	seen := map[string]bool{}
	byProperty := map[string]int{}
	for _, row := range sweep.Rows {
		if seen[row.ID] {
			t.Fatalf("row id %s appears twice", row.ID)
		}
		seen[row.ID] = true
		byProperty[row.Property]++
		if row.Class != classOf(row.Consumer, row.Schema) {
			t.Errorf("%s: class %q does not follow from consumer=%s schema=%s", row.ID, row.Class, row.Consumer, row.Schema)
		}
		doc := applyOps(t, sweep.Base, row.Ops)
		if got := sweepConsumerVerdict(t, doc); got != row.Consumer {
			t.Errorf("%s: the real consumer %ss, the row says %s", row.ID, got, row.Consumer)
		}
		if got := schemaVerdict(t, doc); got != row.Schema {
			t.Errorf("%s: the schema %ss, the row says %s", row.ID, got, row.Schema)
		}
	}
	for _, property := range []string{"P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10"} {
		if byProperty[property] == 0 {
			t.Errorf("property %s has no row", property)
		}
	}
	var classes = map[string]int{}
	for _, row := range sweep.Rows {
		classes[row.Class]++
	}
	for _, class := range []string{"agreement", "narrowing", "client_enforced"} {
		if classes[class] == 0 {
			t.Errorf("no row of class %s: the sweep would not notice that class vanishing", class)
		}
	}
}

func kidDoc(t *testing.T, base map[string]any, kid string) any {
	t.Helper()
	doc := deepCopy(t, base).(map[string]any)
	doc["keys"].([]any)[0].(map[string]any)["kid"] = kid
	return doc
}

func parseRune(t *testing.T, s string) rune {
	t.Helper()
	n, err := strconv.ParseInt(strings.TrimPrefix(s, "U+"), 16, 32)
	if err != nil {
		t.Fatalf("bad rune literal %q: %v", s, err)
	}
	return rune(n)
}

func TestEveryUnicodeCategoryHasAnEntryAndItsClaimHoldsAcrossTheCategory(t *testing.T) {
	sweep := loadSweep(t)
	listed := map[string]sweepCategory{}
	for _, c := range sweep.Categories {
		listed[c.Category] = c
	}
	var missing []string
	for name := range unicode.Categories {
		if len(name) != 2 {
			continue
		}
		if _, ok := listed[name]; ok {
			continue
		}
		if _, ok := sweep.Excluded[name]; ok {
			continue
		}
		missing = append(missing, name)
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("Unicode categories with no entry in the property sweep: %v", missing)
	}
	for name := range sweep.Excluded {
		if _, ok := unicode.Categories[name]; !ok {
			t.Errorf("excluded category %s is not a Go category: stale exclusion", name)
		}
	}

	for _, c := range sweep.Categories {
		table, ok := unicode.Categories[c.Category]
		if !ok && c.Category != "Cn" {
			t.Errorf("entry %s is not a Go category", c.Category)
			continue
		}
		representative := parseRune(t, c.Rune)
		if table == nil && !isUnassigned(representative) {
			t.Errorf("%s: representative %s is assigned", c.Category, c.Rune)
		}
		if table != nil && !unicode.Is(table, representative) {
			t.Errorf("%s: representative %s is not in the category", c.Category, c.Rune)
		}
		samples := []rune{representative}
		if table != nil {
			samples = append(samples, sampleRunes(table)...)
		} else {
			samples = append(samples, 0x0378, 0x0379, 0xfdd0, 0xfffe, 0xffff, 0xe0080, 0x10fffe)
		}
		for _, r := range samples {
			if r >= 0x21 && r <= 0x7e {
				continue
			}
			if r >= 0xd800 && r <= 0xdfff {
				continue
			}
			if table == nil && !isUnassigned(r) {
				continue
			}
			doc := kidDoc(t, sweep.Base, "a"+string(r)+"b")
			if got := sweepConsumerVerdict(t, doc); got != c.Consumer {
				t.Errorf("%s U+%04X: the real consumer %ss, the entry says %s", c.Category, r, got, c.Consumer)
			}
			if got := schemaVerdict(t, doc); got != c.Schema {
				t.Errorf("%s U+%04X: the schema %ss, the entry says %s", c.Category, r, got, c.Schema)
			}
		}
	}
}

func sampleRunes(table *unicode.RangeTable) []rune {
	var out []rune
	for _, r := range table.R16 {
		out = append(out, rune(r.Lo), rune(r.Hi), rune(r.Lo)+rune((r.Hi-r.Lo)/2))
	}
	for _, r := range table.R32 {
		out = append(out, rune(r.Lo), rune(r.Hi), rune(r.Lo)+rune((r.Hi-r.Lo)/2))
	}
	step := 1
	if len(out) > 120 {
		step = len(out) / 120
	}
	var picked []rune
	for i := 0; i < len(out); i += step {
		picked = append(picked, out[i])
	}
	return picked
}

func TestEveryPrintableAsciiCharacterAsAKidIsAcceptedByBoth(t *testing.T) {
	sweep := loadSweep(t)
	for r := rune(0x21); r <= 0x7e; r++ {
		doc := kidDoc(t, sweep.Base, string(r))
		if got := sweepConsumerVerdict(t, doc); got != "accept" {
			t.Errorf("%q: consumer %ss", r, got)
		}
		if got := schemaVerdict(t, doc); got != "accept" {
			t.Errorf("%q: schema %ss", r, got)
		}
	}
}

func isUnassigned(r rune) bool {
	for name, table := range unicode.Categories {
		if len(name) == 2 && unicode.Is(table, r) {
			return false
		}
	}
	return true
}

// Every narrowing (and every client-enforced row) belongs to a declared class,
// and every class is NAMED in the schema's own description by its bracketed id.
// A narrowing nobody declared is the round-1 defect; a class the schema never
// names is the round-2 defect (predicate and fixture, no prose).
func TestEverySweepNarrowingIsInADeclaredClassTheSchemaNames(t *testing.T) {
	sweep := loadSweep(t)
	classes := map[string]int{}
	for _, c := range sweep.Classes {
		classes[c.ID] = 0
	}
	if len(classes) == 0 {
		t.Fatal("the sweep declares no narrowing classes")
	}
	for _, row := range sweep.Rows {
		if row.Class == "agreement" {
			if row.DeclaredAs != "" {
				t.Errorf("%s: an agreement row names class %s", row.ID, row.DeclaredAs)
			}
			continue
		}
		if _, ok := classes[row.DeclaredAs]; !ok {
			t.Errorf("%s: %s row declared_as %q is not a declared class", row.ID, row.Class, row.DeclaredAs)
			continue
		}
		classes[row.DeclaredAs]++
	}
	raw, err := os.ReadFile(filepath.Join(testRoot(t), "contracts/auth/v1/jsonschema/jwks.v1.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Description string `json:"description"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	for id, rows := range classes {
		if rows == 0 {
			t.Errorf("class %s has no row", id)
		}
		if !strings.Contains(schema.Description, "["+id+"]") {
			t.Errorf("class %s is not named as [%s] in the schema description", id, id)
		}
	}
}
