package server

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

const (
	declaredGapTicket = "CHAOS-11042"
	unknownValue      = "unknown"
)

var actionFieldKeys = []string{"action", "resource_resolver", "current_state_cache_behavior", "impersonation_policy", "disclosure_behavior"}

var impersonationPolicies = map[string]bool{
	"subject_to_impersonation_override": true,
	"exempt_no_org_context":             true,
	"not_applicable_non_main_app":       true,
	unknownValue:                        true,
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

func rowGaps(row map[string]any) []string {
	var gaps []string
	list, _ := row["gaps"].([]any)
	for _, gap := range list {
		if text, ok := gap.(string); ok {
			gaps = append(gaps, text)
		}
	}
	return gaps
}

func rowFamily(id, route string) string {
	switch {
	case route == "/graphql":
		return "gql"
	case strings.Contains(id, "@mcp"):
		return "mcp"
	case route == "/buildinfo":
		return "build"
	case route == "/query/proof-write":
		return "proofw"
	case strings.HasPrefix(route, "/query"):
		return "query"
	}
	return "rest"
}

// checkRowFields returns every defect of the query-api rows against the rule
// that a row asserts only what a source proof in this change states: a public
// row carries none of the five action fields; a protected row carries all
// five, never null; a value is the catalog action (action only), "unknown"
// with a named gap that names CHAOS-11042, or the exact statement registered
// for the row's family and field, whose every proof needle is present in the
// named source or test file; an impersonation policy is a schema value; the
// REST validator description is the registered statement.
func checkRowFields(rows []map[string]any, cat *policycatalog.Catalog, claims map[string]map[string]rowClaim, read func(file string) string) []string {
	var defects []string
	prove := func(id, what string, claim rowClaim) {
		for _, proof := range claim.proofs {
			defects = append(defects, proofDefects(id+" "+what, proof, read, proof.decl)...)
		}
	}
	for _, row := range rows {
		id, _ := row["id"].(string)
		route, _ := row["route"].(string)
		if row["classification"] == "public" {
			for _, key := range actionFieldKeys {
				if _, present := row[key]; present {
					defects = append(defects, fmt.Sprintf("%s: public row carries %s", id, key))
				}
			}
			continue
		}
		fam := rowFamily(id, route)
		gaps := rowGaps(row)
		for _, key := range actionFieldKeys {
			value, present := row[key]
			if !present {
				defects = append(defects, fmt.Sprintf("%s: protected row has no %s", id, key))
				continue
			}
			if value == nil {
				defects = append(defects, fmt.Sprintf("%s: %s is null, a gap is the value %q with a named gap", id, key, unknownValue))
				continue
			}
			text, isString := value.(string)
			if !isString || text == "" {
				defects = append(defects, fmt.Sprintf("%s: %s is not a non-empty string", id, key))
				continue
			}
			switch {
			case text == unknownValue:
				named := false
				for _, gap := range gaps {
					if strings.HasPrefix(gap, key+":") && strings.Contains(gap, declaredGapTicket) {
						named = true
					}
				}
				if !named {
					defects = append(defects, fmt.Sprintf("%s: %s is unknown without a %q gap naming %s", id, key, key+":", declaredGapTicket))
				}
			default:
				claim, registered := claims[fam][key]
				switch {
				case !registered:
					defects = append(defects, fmt.Sprintf("%s: no proof is registered for %s %q of family %s, state it as %q with a gap", id, key, text, fam, unknownValue))
				case key == "action" && func() bool { _, err := cat.Action(text); return err != nil }():
					defects = append(defects, fmt.Sprintf("%s: action %q is not in the catalog", id, text))
				case claim.value != text:
					defects = append(defects, fmt.Sprintf("%s: %s is not the statement proven for family %s: %.80q", id, key, fam, text))
				default:
					prove(id, key, claim)
				}
			}
			if key == "impersonation_policy" && !impersonationPolicies[text] {
				defects = append(defects, fmt.Sprintf("%s: impersonation_policy %q is not a schema value", id, text))
			}
		}
		if fam == "rest" {
			if pv, ok := row["primary_validator"].(map[string]any); ok {
				description, _ := pv["description"].(string)
				if claim, registered := claims["rest"]["primary_validator"]; registered {
					if description != claim.value {
						defects = append(defects, fmt.Sprintf("%s: primary_validator description is not the proven statement: %.80q", id, description))
					} else {
						prove(id, "primary_validator", claim)
					}
				}
			}
		}
	}
	return defects
}

func readServerSource(t *testing.T) func(file string) string {
	return func(file string) string {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
}

func TestQueryAPIRowsAssertOnlyProvenStatementsOrDeclareAGap(t *testing.T) {
	rows := loadQueryAPIRows(t)
	if len(rows) < 40 {
		t.Fatalf("only %d rows: the row load stopped seeing them", len(rows))
	}
	for _, defect := range checkRowFields(rows, policycatalog.Default(), rowClaims, readServerSource(t)) {
		t.Error(defect)
	}
}

func TestRowFieldGuardCatchesPlantedDefects(t *testing.T) {
	cat := policycatalog.Default()
	src := "package p\nfunc f() { _ = \"needle-a\" /* needle-comment */ }\nfunc g() { _ = \"other\" }\n"
	claims := map[string]map[string]rowClaim{
		"rest": {
			"resource_resolver":   {value: "proven resolver", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"needle-a"}}}},
			"disclosure_behavior": {value: "proven disclosure", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"needle-a"}}, {file: "a.go", decl: "g", anyOf: []string{"nothing", "other"}}}},
			"primary_validator":   {value: "proven validator", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"needle-a"}}}},
			"action":              {value: "analytics.read", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"needle-a"}}}},
		},
	}
	read := func(string) string { return src }
	gap := func(field string) []any { return []any{field + ": declared gap tracked in " + declaredGapTicket} }
	protected := func() map[string]any {
		gaps := append(gap("action"), gap("current_state_cache_behavior")[0], gap("impersonation_policy")[0])
		return map[string]any{
			"id": "GET /x", "route": "/x", "classification": "protected", "gaps": gaps,
			"action": unknownValue, "resource_resolver": "proven resolver", "current_state_cache_behavior": unknownValue,
			"impersonation_policy": unknownValue, "disclosure_behavior": "proven disclosure",
			"primary_validator": map[string]any{"description": "proven validator"},
		}
	}
	public := func() map[string]any {
		return map[string]any{"id": "GET /p", "classification": "public", "gaps": []any{}}
	}
	if defects := checkRowFields([]map[string]any{protected(), public()}, cat, claims, read); len(defects) != 0 {
		t.Fatalf("the clean fixture is refused: %v", defects)
	}
	with := func(key string, value any) map[string]any { r := protected(); r[key] = value; return r }
	noAction := map[string]map[string]rowClaim{"rest": {"resource_resolver": claims["rest"]["resource_resolver"], "disclosure_behavior": claims["rest"]["disclosure_behavior"], "primary_validator": claims["rest"]["primary_validator"]}}
	cases := []struct {
		name   string
		row    map[string]any
		claims map[string]map[string]rowClaim
		read   func(string) string
		want   string
	}{
		{"public row with a field", func() map[string]any { r := public(); r["action"] = "analytics.read"; return r }(), nil, nil, "public row carries action"},
		{"protected row missing a field", func() map[string]any { r := protected(); delete(r, "resource_resolver"); return r }(), nil, nil, "has no resource_resolver"},
		{"null field", with("disclosure_behavior", nil), nil, nil, "is null"},
		{"empty string", with("disclosure_behavior", ""), nil, nil, "not a non-empty string"},
		{"unknown without a gap", with("gaps", gap("action")), nil, nil, "is unknown without"},
		{"unknown gap without the ticket", with("gaps", []any{"action: x", "current_state_cache_behavior: x", "impersonation_policy: x"}), nil, nil, "naming " + declaredGapTicket},
		{"catalog action with no registered proof", with("action", "analytics.read"), noAction, nil, "no proof is registered for action"},
		{"unproven statement", with("resource_resolver", "a guess"), nil, nil, "not the statement proven"},
		{"statement for a field with no registered proof", with("current_state_cache_behavior", "none"), nil, nil, "no proof is registered"},
		{"needle present only in a comment", protected(), map[string]map[string]rowClaim{"rest": {"resource_resolver": {value: "proven resolver", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"needle-comment"}}}}, "disclosure_behavior": claims["rest"]["disclosure_behavior"], "primary_validator": claims["rest"]["primary_validator"]}}, nil, "does not contain"},
		{"needle in another declaration of the file", protected(), map[string]map[string]rowClaim{"rest": {"resource_resolver": {value: "proven resolver", proofs: []claimProof{{file: "a.go", decl: "f", needles: []string{"other"}}}}, "disclosure_behavior": claims["rest"]["disclosure_behavior"], "primary_validator": claims["rest"]["primary_validator"]}}, nil, "does not contain"},
		{"declaration absent", protected(), map[string]map[string]rowClaim{"rest": {"resource_resolver": {value: "proven resolver", proofs: []claimProof{{file: "a.go", decl: "missing", needles: []string{"x"}}}}, "disclosure_behavior": claims["rest"]["disclosure_behavior"], "primary_validator": claims["rest"]["primary_validator"]}}, nil, "holds no declaration"},
		{"needle gone from the source", protected(), nil, func(string) string { return "package p\nfunc f() {}\nfunc g() { _ = \"other\" }\n" }, "does not contain"},
		{"no alternative present", protected(), nil, func(string) string { return "package p\nfunc f() { _ = \"needle-a\" }\nfunc g() {}\n" }, "contains none of"},
		{"bad impersonation value", with("impersonation_policy", "maybe"), nil, nil, "not a schema value"},
		{"validator is not the proven statement", with("primary_validator", map[string]any{"description": "every failure answers the same 401"}), nil, nil, "primary_validator description is not the proven statement"},
	}
	for _, c := range cases {
		reader, table := read, claims
		if c.read != nil {
			reader = c.read
		}
		if c.claims != nil {
			table = c.claims
		}
		joined := strings.Join(checkRowFields([]map[string]any{c.row}, cat, table, reader), "\n")
		if !strings.Contains(joined, c.want) {
			t.Errorf("%s: the guard did not report %q, it reported %q", c.name, c.want, joined)
		}
	}
}

// TestRowGuardGoesRedOnTheVerifierPlants plants, in the real source text, the
// defects a verifier named: a status constant changed, and a REST row that
// asserts a catalog action nothing proves.
func TestRowGuardGoesRedOnTheVerifierPlants(t *testing.T) {
	cat := policycatalog.Default()
	real := readServerSource(t)
	rows := loadQueryAPIRows(t)
	if defects := checkRowFields(rows, cat, rowClaims, real); len(defects) != 0 {
		t.Fatalf("the real rows are refused before any plant: %v", defects[:1])
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
	forbidden := swapped("rest_error_response.go", `http.StatusForbidden, "Organization context required"`, `http.StatusUnauthorized, "Organization context required"`)
	if joined := strings.Join(checkRowFields(rows, cat, rowClaims, forbidden), "\n"); !strings.Contains(joined, "Organization context required") {
		t.Errorf("a 403 changed to 401 in authenticateRESTRequest stayed green")
	}
	commented := swapped("rest_error_response.go", `policy.WriteAuthFailure(`, `/* policy.WriteAuthFailure( */ nothing(`)
	if joined := strings.Join(checkRowFields(rows, cat, rowClaims, commented), "\n"); !strings.Contains(joined, "WriteAuthFailure") {
		t.Errorf("a needle moved into a comment stayed green")
	}
	var planted []map[string]any
	for _, row := range rows {
		if row["classification"] == "protected" && rowFamily(row["id"].(string), row["route"].(string)) == "rest" {
			copyRow := map[string]any{}
			for k, v := range row {
				copyRow[k] = v
			}
			copyRow["action"] = "analytics.read"
			planted = append(planted, copyRow)
			break
		}
	}
	if joined := strings.Join(checkRowFields(planted, cat, rowClaims, real), "\n"); !strings.Contains(joined, "no proof is registered for action") {
		t.Errorf("a REST row with a catalog action and no proof stayed green: %q", joined)
	}
}
