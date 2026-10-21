package graph

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"os"
	"slices"
	"testing"

	"github.com/99designs/gqlgen/graphql"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func TestMain(m *testing.M) { os.Exit(venueoracle.RunTests(m)) }

// orgGuardGoldens is the set of this package's frozen Python answers. The
// producer is the OrgIdAuthExtension of the pinned build on a Strawberry
// schema, so Identity names that distribution. A golden recorded by another
// producer is refused.
var orgGuardGoldens = programoracle.Set{
	Build:    "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity: "python 3.14.7\nunicodedata 16.0.0\nstrawberry-graphql 0.327.7",
	Pins: map[string]string{
		"org-guard.golden.json": "3689953d5f5ebc64d04becf73493af684d7cff1d3bb8d57179b8fdb1cc28d485",
	},
}

// Go's guard answers every case exactly as the frozen Python extension did:
// the same refusal message, and no resolver run when it refuses.
func TestOperationOrgViolationMatchesFrozenPythonExtension(t *testing.T) {
	const own = "org-own"
	docs := map[string]string{
		"query":           `query Q($orgId: String) { doIt(orgId: $orgId) { __typename } }`,
		"query literal":   `query Q { doIt(orgId: "org-own") { __typename } }`,
		"query nested":    `query Q($orgId: String) { doIt(orgId: $orgId) { inner(orgId: "org-other") } }`,
		"query two":       `query Q($orgId: String, $x: String) { a: doIt(orgId: $orgId) { __typename } b: doIt(orgId: $x) { __typename } }`,
		"query no org":    `query Q { doIt { __typename } }`,
		"query fragment":  `query Q($orgId: String) { ...F } fragment F on Query { doIt(orgId: $orgId) { __typename } }`,
		"mutation":        `mutation M($orgId: String) { write(orgId: $orgId) }`,
		"mutation nested": `mutation M { write(orgId: "org-other") }`,
		// The org argument inside an inline fragment, as a block string, and
		// under its snake-case name.
		"query inline fragment":    `query Q($orgId: String) { ... on Query { doIt(orgId: $orgId) { __typename } } }`,
		"query block string other": `query Q { doIt(orgId: """org-other""") { __typename } }`,
		"query block string own":   `query Q { doIt(orgId: """org-own""") { __typename } }`,
		"query snake argument":     `query Q($orgId: String) { raw(org_id: $orgId) }`,
	}
	values := map[string]any{
		"own": own, "other": "org-other", "empty": "", "left pad": " " + own, "right pad": own + "\n",
		"tab": "\t" + own, "number": 7, "null": nil,
	}
	type caller struct {
		Superuser     bool `json:"superuser"`
		Verified      bool `json:"verified"`
		Impersonating bool `json:"impersonating"`
	}
	type oracleCase struct {
		Label     string         `json:"label"`
		Document  string         `json:"document"`
		Variables map[string]any `json:"variables"`
		Caller    caller         `json:"caller"`
	}
	callers := map[string]caller{
		"ordinary":                 {},
		"verified superuser":       {Superuser: true, Verified: true},
		"unverified superuser":     {Superuser: true},
		"impersonating superuser":  {Superuser: true, Verified: true, Impersonating: true},
		"impersonating unverified": {Superuser: true, Impersonating: true},
	}
	// The cases are the program's input: they are built in the sorted order of
	// the three maps, so the input is the same in every run.
	var cases []oracleCase
	for _, callerName := range slices.Sorted(maps.Keys(callers)) {
		who := callers[callerName]
		for _, docName := range slices.Sorted(maps.Keys(docs)) {
			document := docs[docName]
			if callerName != "ordinary" && docName != "query" && docName != "mutation" && docName != "query two" && docName != "query nested" {
				continue
			}
			if docName == "query literal" || docName == "query no org" || docName == "mutation nested" || docName == "query block string other" || docName == "query block string own" {
				cases = append(cases, oracleCase{callerName + "/" + docName, document, map[string]any{}, who})
				continue
			}
			for _, valueName := range slices.Sorted(maps.Keys(values)) {
				value := values[valueName]
				if callerName != "ordinary" && valueName != "own" && valueName != "other" && valueName != "empty" {
					continue
				}
				variables := map[string]any{"orgId": value}
				if docName == "query two" {
					variables["x"] = "org-other"
				}
				cases = append(cases, oracleCase{callerName + "/" + docName + "/" + valueName, document, variables, who})
			}
			if callerName == "ordinary" {
				cases = append(cases, oracleCase{callerName + "/" + docName + "/absent", document, map[string]any{}, who})
			}
		}
	}

	payload, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	output := []byte(orgGuardGoldens.Outputs(t, "org-guard.golden.json", programoracle.Program{Name: "org guard", Stdin: payload})[0])
	var results []struct {
		Errors      []string         `json:"errors"`
		Formatted   []map[string]any `json:"formatted"`
		ResolverRan bool             `json:"resolverRan"`
		SeenOrg     string           `json:"seenOrg"`
	}
	for _, line := range bytes.Split(output, []byte("\n")) {
		if rest, ok := bytes.CutPrefix(line, []byte("RESULT ")); ok {
			if err := json.Unmarshal(rest, &results); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(results) != len(cases) {
		t.Fatalf("python answered %d of %d cases", len(results), len(cases))
	}

	refused, allowed, rebound := 0, 0, 0
	for i, c := range cases {
		claims := authctx.WithClaims(t.Context(), authctx.Claims{
			OrgID: own, IsSuperuser: c.Caller.Superuser && c.Caller.Verified, ImpersonationActive: c.Caller.Impersonating,
		})
		// Drive the real interceptor: the response it answers with, and the org
		// its next handler (the resolvers) would run as.
		operation := guardOperation(t, c.Document, c.Variables)
		ctx := graphql.WithOperationContext(claims, operation)
		ran, effective := false, ""
		handler := OperationOrgGuard{}.InterceptOperation(ctx, func(inner context.Context) graphql.ResponseHandler {
			ran = true
			seen, _ := authctx.FromContext(inner)
			effective = seen.OrgID
			return func(context.Context) *graphql.Response { return &graphql.Response{} }
		})
		response := handler(ctx)
		got := ""
		if !ran && len(response.Errors) == 1 {
			got = response.Errors[0].Message
			if response.Errors[0].Extensions != nil || response.Errors[0].Path != nil || response.Data != nil {
				t.Errorf("%s: Go refusal is not message-only: %+v", c.Label, response)
			}
		}
		rebindTo := ""
		if ran && effective != own {
			rebindTo = effective
		}
		py := results[i]
		switch {
		case len(py.Errors) == 0:
			allowed++
			effective := own
			if rebindTo != "" {
				effective = rebindTo
				rebound++
			}
			if got != "" || !py.ResolverRan || py.SeenOrg != effective {
				t.Errorf("%s: python ran the operation as org %q (resolver ran %v); Go guard answered %q, runs as %q", c.Label, py.SeenOrg, py.ResolverRan, got, effective)
			}
		case len(py.Errors) == 1:
			refused++
			// The wire error is the message alone: Python's extension refusal
			// carries no extensions.code, and neither does Go's.
			if got != py.Errors[0] || py.ResolverRan || len(py.Formatted) != 1 || len(py.Formatted[0]) != 1 || py.Formatted[0]["message"] != got {
				t.Errorf("%s: python refused with %v (resolver ran %v); Go guard answered %q", c.Label, py.Formatted, py.ResolverRan, got)
			}
		default:
			t.Errorf("%s: python answered %d errors %v", c.Label, len(py.Errors), py.Errors)
		}
	}
	if refused == 0 || allowed == 0 || rebound == 0 {
		t.Fatalf("one-sided comparison: %d refused, %d allowed, %d rebound", refused, allowed, rebound)
	}
	t.Logf("%d cases match the frozen OrgIdAuthExtension: %d refused, %d allowed (%d of them as the named org)", len(cases), refused, allowed, rebound)
}
