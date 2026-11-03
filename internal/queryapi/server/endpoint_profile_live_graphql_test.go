package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
)

// The GraphQL per-field action map states, for every served root field, how
// the organization is resolved and what a refusal answers. Each field is
// driven through the production gqlgen server (newGraphQLServer: the org
// guard, the argument check and the resolvers' own checks) with a caller the
// cell shapes, and what it answers is compared with the row.

var (
	liveQuotedRe   = regexp.MustCompile(`'([A-Z][^']*)'`)
	liveGuardTexts = []string{
		"A valid organization ID is required",
		"Only one organization may be queried per operation",
		"Authorization required",
		"Access denied: cannot query org",
	}
)

func loadLiveFieldRows(t *testing.T) []liveFieldRow {
	t.Helper()
	raw, err := os.ReadFile("../../../contracts/auth/v1/graphql-field-actions.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Rows []map[string]any `json:"rows"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	var rows []liveFieldRow
	for _, row := range doc.Rows {
		rows = append(rows, liveFieldRow{raw: row})
	}
	return rows
}

type liveFieldRow struct{ raw map[string]any }

func (r liveFieldRow) str(field string) string {
	value, _ := r.raw[field].(string)
	return value
}

func (r liveFieldRow) clone() liveFieldRow {
	raw := map[string]any{}
	for key, value := range r.raw {
		raw[key] = value
	}
	return liveFieldRow{raw: raw}
}

func (r liveFieldRow) hasGap(field string) bool {
	gaps, _ := r.raw["gaps"].([]any)
	for _, gap := range gaps {
		if text, _ := gap.(string); strings.HasPrefix(text, field+": ") && strings.Contains(text, "declared gap") {
			return true
		}
	}
	return false
}

func (r liveFieldRow) name() string { return r.str("type") + "." + r.str("field") }

type liveFieldAnswer struct {
	messages []string
	codes    []string
	reached  int
}

type (
	liveProbeKey struct{}
	liveProbe    struct {
		reached     int
		runResolver bool
	}
)

type liveGraphQLHarness struct {
	server http.Handler
	schema *ast.Schema
}

func newLiveGraphQLHarness() *liveGraphQLHarness {
	server := newGraphQLServer(&graph.Resolver{})
	// Resolvers run with no stores here; a nil store is a recovered panic,
	// which only needs to answer without printing a stack.
	server.SetRecoverFunc(func(context.Context, any) error { return errors.New("internal system error") })
	// Innermost field interceptor, after the guard and the argument check: it
	// records that a root field was reached and, unless the cell asks for the
	// resolver, answers null instead of running it against a store this test
	// does not have.
	server.AroundFields(func(ctx context.Context, next graphql.Resolver) (any, error) {
		field := graphql.GetFieldContext(ctx)
		if field != nil && field.Parent != nil && field.Parent.Parent == nil {
			probe, _ := ctx.Value(liveProbeKey{}).(*liveProbe)
			if probe != nil {
				probe.reached++
				if !probe.runResolver {
					return nil, nil
				}
			}
		}
		return next(ctx)
	})
	return &liveGraphQLHarness{
		server: server,
		schema: graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}}).Schema(),
	}
}

func (h *liveGraphQLHarness) field(row liveFieldRow) *ast.FieldDefinition {
	root := h.schema.Query
	if row.str("type") == "Mutation" {
		root = h.schema.Mutation
	}
	if root == nil {
		return nil
	}
	return root.Fields.ForName(row.str("field"))
}

func orgArgName(def *ast.FieldDefinition) string {
	for _, argument := range def.Arguments {
		if argument.Name == "orgId" || argument.Name == "org_id" {
			return argument.Name
		}
	}
	return ""
}

func (h *liveGraphQLHarness) literal(t *ast.Type) string {
	if t.Elem != nil {
		return "[" + h.literal(t.Elem) + "]"
	}
	def := h.schema.Types[t.NamedType]
	switch {
	case def == nil:
		return `"x"`
	case def.Kind == ast.Enum:
		return def.EnumValues[0].Name
	case def.Kind == ast.InputObject:
		var parts []string
		for _, field := range def.Fields {
			if field.Type.NonNull && field.DefaultValue == nil {
				value := h.literal(field.Type)
				if strings.HasSuffix(field.Name, "Utc") {
					value = `"2026-09-01T00:00:00Z"`
				}
				parts = append(parts, field.Name+": "+value)
			}
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	switch t.NamedType {
	case "Date":
		return `"2026-09-01"`
	case "DateTime":
		return `"2026-09-01T00:00:00Z"`
	case "JSON":
		return `{}`
	case "Int":
		return "1"
	case "Float":
		return "1.5"
	case "Boolean":
		return "true"
	}
	return `"x"`
}

// document is one valid operation selecting the field once per org literal
// (several literals: aliased copies), every other required argument given.
func (h *liveGraphQLHarness) document(row liveFieldRow, def *ast.FieldDefinition, orgLiterals ...string) string {
	keyword := "query"
	if row.str("type") == "Mutation" {
		keyword = "mutation"
	}
	selection := ""
	if returned := h.schema.Types[def.Type.Name()]; returned != nil && returned.Kind != ast.Scalar && returned.Kind != ast.Enum {
		selection = " { __typename }"
	}
	call := func(org string, alias string) string {
		var args []string
		for _, argument := range def.Arguments {
			switch {
			case argument.Name == orgArgName(def) && org != "":
				args = append(args, argument.Name+": "+org)
			case argument.Type.NonNull && argument.DefaultValue == nil:
				args = append(args, argument.Name+": "+h.literal(argument.Type))
			}
		}
		text := def.Name
		if len(args) > 0 {
			text += "(" + strings.Join(args, ", ") + ")"
		}
		return alias + text + selection
	}
	if len(orgLiterals) == 0 {
		orgLiterals = []string{""}
	}
	var calls []string
	for index, org := range orgLiterals {
		alias := ""
		if len(orgLiterals) > 1 {
			alias = fmt.Sprintf("a%d: ", index)
		}
		calls = append(calls, call(org, alias))
	}
	return keyword + " LiveField { " + strings.Join(calls, " ") + " }"
}

func (h *liveGraphQLHarness) send(document string, claims authctx.Claims) liveFieldAnswer {
	return h.sendWith(document, claims, false)
}

func (h *liveGraphQLHarness) sendWith(document string, claims authctx.Claims, runResolver bool) liveFieldAnswer {
	probe := &liveProbe{runResolver: runResolver}
	body, _ := json.Marshal(map[string]string{"query": document})
	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(context.WithValue(authctx.WithClaims(context.Background(), claims), liveProbeKey{}, probe))
	recorder := httptest.NewRecorder()
	h.server.ServeHTTP(recorder, request)
	var out struct {
		Errors []struct {
			Message    string         `json:"message"`
			Extensions map[string]any `json:"extensions"`
		} `json:"errors"`
	}
	_ = json.Unmarshal(recorder.Body.Bytes(), &out)
	answer := liveFieldAnswer{reached: probe.reached}
	for _, e := range out.Errors {
		answer.messages = append(answer.messages, e.Message)
		code, _ := e.Extensions["code"].(string)
		answer.codes = append(answer.codes, code)
	}
	return answer
}

func guardMessage(answer liveFieldAnswer) string {
	for _, message := range answer.messages {
		for _, text := range liveGuardTexts {
			if strings.HasPrefix(message, text) {
				return message
			}
		}
	}
	return ""
}

func hasCode(answer liveFieldAnswer, code string) bool {
	for _, c := range answer.codes {
		if c == code {
			return true
		}
	}
	return false
}

// driveField is the defects of one field row.
func driveField(h *liveGraphQLHarness, row liveFieldRow) []string {
	var defects []string
	fail := func(format string, args ...any) {
		defects = append(defects, row.name()+": "+fmt.Sprintf(format, args...))
	}
	def := h.field(row)
	if def == nil {
		return []string{row.name() + ": the schema serves no such root field"}
	}
	for _, field := range []string{"action"} {
		if row.str(field) != "unknown" {
			fail("%s is %q: no live cell observes a field action", field, row.str(field))
		} else if !row.hasGap(field) {
			fail("%s is unknown with no declared gap", field)
		}
	}
	own := authctx.Claims{OrgID: ecOrg.String(), Role: "member"}
	text := row.str("disclosure_behavior")
	resolver := row.str("resource_resolver")

	if arg := orgArgName(def); arg != "" {
		if !strings.HasPrefix(resolver, "organization_from_argument") {
			fail("resource_resolver %q: the schema field takes an %s argument, so the organization comes from the argument", resolver, arg)
		}
		quote := func(org string) string { return `"` + org + `"` }
		observed := map[string]bool{}
		cell := func(name string, claims authctx.Claims, want string, orgs ...string) {
			answer := h.send(h.document(row, def, orgs...), claims)
			got := guardMessage(answer)
			if want == "" && got != "" {
				fail("%s: refused by the guard with %q, want the operation to run", name, got)
			}
			if want == "" && got == "" && answer.reached == 0 {
				fail("%s: not refused, but the field was never reached (%v)", name, answer.messages)
			}
			if want != "" && answer.reached != 0 {
				fail("%s: the field was reached for a refused operation", name)
			}
			if want != "" && !strings.HasPrefix(got, want) {
				fail("%s: answered %q, want %q", name, got, want)
			}
			if got != "" {
				observed[strings.SplitN(got, " '", 2)[0]] = true
			}
		}
		cell("own org", own, "", quote(ecOrg.String()))
		cell("another org", own, "Access denied: cannot query org", quote(liveOtherOrg.String()))
		cell("empty org", own, "A valid organization ID is required", quote(""))
		cell("padded org", own, "A valid organization ID is required", quote(" "+ecOrg.String()))
		cell("two organizations", own, "Only one organization may be queried per operation", quote(ecOrg.String()), quote(liveOtherOrg.String()))
		cell("caller with no organization", authctx.Claims{Role: "member"}, "Authorization required", quote(ecOrg.String()))
		cell("superuser naming another org", authctx.Claims{OrgID: ecOrg.String(), Role: "member", IsSuperuser: true}, "", quote(liveOtherOrg.String()))
		cell("impersonating superuser naming another org", authctx.Claims{OrgID: ecOrg.String(), Role: "member", IsSuperuser: true, ImpersonationActive: true}, "Access denied: cannot query org", quote(liveOtherOrg.String()))
		for message := range observed {
			if !strings.Contains(text, "'"+message+"'") {
				fail("disclosure_behavior does not state %q, which the live stack answers", message)
			}
		}
		for _, match := range liveQuotedRe.FindAllStringSubmatch(text, -1) {
			stated := false
			for message := range observed {
				stated = stated || strings.HasPrefix(message, match[1])
			}
			if !stated {
				fail("disclosure_behavior states %q, no live cell answered it", match[1])
			}
		}
		return defects
	}

	if strings.HasPrefix(resolver, "organization_from_argument") {
		fail("resource_resolver %q: the schema field takes no orgId argument", resolver)
	}
	document := h.document(row, def)
	switch {
	case strings.Contains(text, "a request with no organization answers AUTHORIZATION_ERROR"):
		if !hasCode(h.sendWith(document, authctx.Claims{Role: "member"}, true), "AUTHORIZATION_ERROR") {
			fail("a caller with no organization is not answered AUTHORIZATION_ERROR")
		}
		if !strings.HasPrefix(resolver, "organization_from_credential") {
			fail("resource_resolver %q: the organization is the credential's", resolver)
		}
	case strings.Contains(text, "refused with AUTHORIZATION_ERROR unless"):
		allowed, refused := authctx.Claims{OrgID: ecOrg.String(), Role: "admin"}, authctx.Claims{OrgID: ecOrg.String(), Role: "viewer"}
		if def.Name == "productTelemetryPlatformDashboard" {
			allowed = authctx.Claims{OrgID: ecOrg.String(), Role: "member", IsSuperuser: true}
			if !hasCode(h.sendWith(document, authctx.Claims{OrgID: ecOrg.String(), Role: "member", IsSuperuser: true, ImpersonationActive: true}, true), "AUTHORIZATION_ERROR") {
				fail("an impersonating superuser is not refused")
			}
		}
		if !hasCode(h.sendWith(document, refused, true), "AUTHORIZATION_ERROR") {
			fail("a plain caller is not refused with AUTHORIZATION_ERROR")
		}
		_ = allowed
	default:
		fail("disclosure_behavior matches no live driver: %q", text)
	}
	return defects
}

func TestGraphQLFieldActionsMatchLiveBehaviour(t *testing.T) {
	quietStdLog(t)
	h := newLiveGraphQLHarness()
	rows := loadLiveFieldRows(t)
	for _, row := range rows {
		t.Run(row.name(), func(t *testing.T) {
			for _, defect := range driveField(h, row) {
				t.Error(defect)
			}
		})
	}
	if missing := liveUnrowedFields(h, rows); len(missing) != 0 {
		t.Errorf("served root fields with no row to drive: %v", missing)
	}
	if len(rows) < 40 {
		t.Fatalf("only %d field rows: the contract was not read", len(rows))
	}
}

// TestLiveFieldGuardFiresOnPlantedRows: a field row that differs from the live
// answer, and a row for a field nothing serves or no driver class covers,
// fail by name.
func TestLiveFieldGuardFiresOnPlantedRows(t *testing.T) {
	quietStdLog(t)
	h := newLiveGraphQLHarness()
	byName := map[string]liveFieldRow{}
	for _, row := range loadLiveFieldRows(t) {
		byName[row.name()] = row
	}
	has := func(defects []string, name, contains string) {
		t.Helper()
		for _, defect := range defects {
			if strings.HasPrefix(defect, name) && strings.Contains(defect, contains) {
				return
			}
		}
		t.Errorf("no defect naming %q containing %q; got %v", name, contains, defects)
	}
	t.Run("R2 a by-argument field recorded as credential-resolved", func(t *testing.T) {
		row := byName["Query.catalog"].clone()
		row.raw["resource_resolver"] = "organization_from_credential: the credential's organization"
		has(driveField(h, row), "Query.catalog", "takes an orgId argument")
	})
	t.Run("R2b a credential field recorded as argument-resolved", func(t *testing.T) {
		row := byName["Query.hotspots"].clone()
		row.raw["resource_resolver"] = "organization_from_argument: the orgId argument"
		has(driveField(h, row), "Query.hotspots", "takes no orgId argument")
	})
	t.Run("R3 a field nothing serves", func(t *testing.T) {
		row := liveFieldRow{raw: map[string]any{"type": "Query", "field": "zzNoSuchField"}}
		has(driveField(h, row), "Query.zzNoSuchField", "serves no such root field")
	})
	t.Run("R3b a served field whose disclosure no driver class covers", func(t *testing.T) {
		row := byName["Query.hotspots"].clone()
		row.raw["disclosure_behavior"] = "answers whatever the resolver answers"
		has(driveField(h, row), "Query.hotspots", "matches no live driver")
	})
	t.Run("R3c a guard message the row does not state", func(t *testing.T) {
		row := byName["Query.catalog"].clone()
		row.raw["disclosure_behavior"] = strings.Replace(row.str("disclosure_behavior"), "'Only one organization may be queried per operation'", "'one org'", 1)
		has(driveField(h, row), "Query.catalog", "does not state")
	})
	t.Run("a served field with no row", func(t *testing.T) {
		rows := loadLiveFieldRows(t)
		kept := rows[:0]
		for _, row := range rows {
			if row.name() != "Query.catalog" {
				kept = append(kept, row)
			}
		}
		if len(kept) != len(rows)-1 {
			t.Fatal("Query.catalog has no row to drop")
		}
		if missing := liveUnrowedFields(h, kept); len(missing) != 1 || missing[0] != "Query.catalog" {
			t.Errorf("unrowed fields %v, want [Query.catalog]", missing)
		}
	})
}

func liveUnrowedFields(h *liveGraphQLHarness, rows []liveFieldRow) []string {
	served := map[string]bool{}
	for _, root := range []*ast.Definition{h.schema.Query, h.schema.Mutation} {
		for _, field := range root.Fields {
			if !strings.HasPrefix(field.Name, "__") {
				served[root.Name+"."+field.Name] = true
			}
		}
	}
	for _, row := range rows {
		delete(served, row.name())
	}
	var missing []string
	for name := range served {
		missing = append(missing, name)
	}
	sort.Strings(missing)
	return missing
}
