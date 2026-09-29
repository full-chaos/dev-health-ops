package graph

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/99designs/gqlgen/graphql"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonOrgGuardProgram executes each case through a real Strawberry schema
// carrying the production OrgIdAuthExtension, with a non-superuser caller in
// org "org-own", and prints the operation's errors and whether a resolver ran.
const pythonOrgGuardProgram = `
import asyncio, json, sys, types
import strawberry
from dev_health_ops.api.graphql.extensions import OrgIdAuthExtension

ran = []

@strawberry.type
class Inner:
    @strawberry.field
    def inner(self, org_id: str = "x") -> int:
        ran.append("inner")
        return 1

@strawberry.type
class Query:
    @strawberry.field
    def do_it(self, org_id: str | None = None) -> Inner:
        ran.append("doIt")
        return Inner()

@strawberry.type
class Mutation:
    @strawberry.mutation
    def write(self, org_id: str | None = None) -> int:
        ran.append("write")
        return 1

schema = strawberry.Schema(query=Query, mutation=Mutation, extensions=[OrgIdAuthExtension])

def make_context():
    return types.SimpleNamespace(
        org_id="org-own",
        user=types.SimpleNamespace(is_superuser=False, is_superuser_verified=False),
    )

async def run(case):
    del ran[:]
    result = await schema.execute(case["document"], variable_values=case["variables"], context_value=make_context())
    return {"errors": [e.message for e in (result.errors or [])], "resolverRan": bool(ran)}

async def main():
    cases = json.loads(sys.stdin.read())
    print("RESULT " + json.dumps([await run(c) for c in cases]))

asyncio.run(main())
`

// Go's guard answers every case exactly as the live Python extension does:
// the same refusal message, and no resolver run when it refuses.
func TestOperationOrgViolationMatchesLivePythonExtension(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)

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
	}
	values := map[string]any{
		"own": own, "other": "org-other", "empty": "", "left pad": " " + own, "right pad": own + "\n",
		"tab": "\t" + own, "number": 7, "null": nil,
	}
	type oracleCase struct {
		Label     string         `json:"label"`
		Document  string         `json:"document"`
		Variables map[string]any `json:"variables"`
	}
	var cases []oracleCase
	for docName, document := range docs {
		if docName == "query literal" || docName == "query no org" || docName == "mutation nested" {
			cases = append(cases, oracleCase{docName, document, map[string]any{}})
			continue
		}
		for valueName, value := range values {
			variables := map[string]any{"orgId": value}
			if docName == "query two" {
				variables["x"] = "org-other"
			}
			cases = append(cases, oracleCase{docName + "/" + valueName, document, variables})
		}
		cases = append(cases, oracleCase{docName + "/absent", document, map[string]any{}})
	}

	payload, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "-c", pythonOrgGuardProgram)
	command.Stdin = bytes.NewReader(payload)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	var results []struct {
		Errors      []string `json:"errors"`
		ResolverRan bool     `json:"resolverRan"`
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

	claims := authctx.WithClaims(t.Context(), authctx.Claims{OrgID: own})
	refused, allowed := 0, 0
	for i, c := range cases {
		var operation *graphql.OperationContext = guardOperation(t, c.Document, c.Variables)
		got := operationOrgViolation(claims, operation)
		py := results[i]
		switch {
		case len(py.Errors) == 0:
			allowed++
			if got != "" || !py.ResolverRan {
				t.Errorf("%s: python ran the operation (errors none, resolver ran %v); Go guard answered %q", c.Label, py.ResolverRan, got)
			}
		case len(py.Errors) == 1:
			refused++
			if got != py.Errors[0] || py.ResolverRan {
				t.Errorf("%s: python refused with %q (resolver ran %v); Go guard answered %q", c.Label, py.Errors[0], py.ResolverRan, got)
			}
		default:
			t.Errorf("%s: python answered %d errors %v", c.Label, len(py.Errors), py.Errors)
		}
	}
	if refused == 0 || allowed == 0 {
		t.Fatalf("one-sided comparison: %d refused, %d allowed", refused, allowed)
	}
	t.Logf("%d cases match the live OrgIdAuthExtension: %d refused, %d allowed", len(cases), refused, allowed)
	if proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"); proof != "" && !t.Failed() {
		if err := os.WriteFile(filepath.Join(proof, "query-api-org-guard"), []byte("executed"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
