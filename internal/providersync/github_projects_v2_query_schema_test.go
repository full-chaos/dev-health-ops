package providersync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"github.com/vektah/gqlparser/v2/parser"
)

// The Projects v2 queries are validated OFFLINE against GitHub's published
// public schema (vendored, its header names the source commit), because the
// fetcher tests answer them with hand-written JSON that GitHub never checks:
// a query GitHub refuses still passed them (CHAOS-14195).
const gitHubPublicSchemaFile = "github_graphql_schema/schema.docs.graphql"

func loadGitHubPublicSchema(t *testing.T) *ast.Schema {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", gitHubPublicSchemaFile))
	if err != nil {
		t.Fatal(err)
	}
	header, body, found := bytes.Cut(raw, []byte("\n\"\"\"\n"))
	if !found {
		t.Fatalf("%s: no header before the upstream SDL", gitHubPublicSchemaFile)
	}
	stated := regexp.MustCompile(`upstream file, sha256 ([0-9a-f]{64})\.`).FindSubmatch(header)
	if stated == nil {
		t.Fatalf("%s: header states no upstream sha256", gitHubPublicSchemaFile)
	}
	digest := sha256.Sum256(append([]byte("\"\"\"\n"), body...))
	if got := hex.EncodeToString(digest[:]); got != string(stated[1]) {
		t.Fatalf("%s: bytes below the header have sha256 %s, the header states %s", gitHubPublicSchemaFile, got, stated[1])
	}
	schema, err := gqlparser.LoadSchema(&ast.Source{Name: gitHubPublicSchemaFile, Input: string(raw)})
	if err != nil {
		t.Fatalf("%s does not load: %v", gitHubPublicSchemaFile, err)
	}
	return schema
}

// gitHubAcceptedTypeConflict is the one validation error the offline validator
// raises that GitHub does not: gqlparser (like graphql-js) wants the same
// response shape for `state` on the Issue and the PullRequest fragments, while
// GitHub's own validator accepts the query (live read-only validation of the
// fixed items query: only token-scope errors, no validation error). It counts
// only as this rule, this exact text and this field (the field path that the
// error's location resolves to in the query). Each query states how often it
// must appear, so it is strict both ways: any other error fails, and so does
// the allowance when the error stops appearing (a new SDL or gqlparser). The
// vendored SDL header names it too.
var gitHubAcceptedTypeConflict = gitHubValidationRead{
	Rule:    "OverlappingFieldsCanBeMerged",
	Message: `Fields "state" conflict because they return conflicting types "IssueState!" and "PullRequestState!". Use different aliases on the fields to fetch both if this was intentional.`,
	Path:    "organization/projectV2/items/nodes/content/on PullRequest/state",
}

type gitHubValidationRead struct{ Rule, Message, Path string }

// gitHubFieldPathAt is the response path of the field that starts at the
// location, with inline fragments as "on <Type>"; "" when no field starts there.
func gitHubFieldPathAt(query string, locations []gqlerror.Location) string {
	doc, err := parser.ParseQuery(&ast.Source{Input: query})
	if err != nil || len(locations) != 1 {
		return ""
	}
	at := locations[0]
	var walk func(selections ast.SelectionSet, path []string) string
	walk = func(selections ast.SelectionSet, path []string) string {
		for _, selection := range selections {
			switch node := selection.(type) {
			case *ast.Field:
				fieldPath := append(slices.Clip(path), node.Alias)
				if node.Position != nil && node.Position.Line == at.Line && node.Position.Column == at.Column {
					return strings.Join(fieldPath, "/")
				}
				if found := walk(node.SelectionSet, fieldPath); found != "" {
					return found
				}
			case *ast.InlineFragment:
				if found := walk(node.SelectionSet, append(slices.Clip(path), "on "+node.TypeCondition)); found != "" {
					return found
				}
			}
		}
		return ""
	}
	for _, operation := range doc.Operations {
		if found := walk(operation.SelectionSet, nil); found != "" {
			return found
		}
	}
	return ""
}

// classifyGitHubValidation turns validation errors into sorted "Type.field"
// names; the accepted state conflict is counted apart, and any other error
// (the conflict text at another field or from another rule included) is
// returned verbatim with its field path so that it cannot hide.
func classifyGitHubValidation(query string, errs gqlerror.List) (fields []string, accepted int, other []string) {
	undefined := regexp.MustCompile(`^Cannot query field "(\w+)" on type "(\w+)"\.`)
	for _, validationErr := range errs {
		if match := undefined.FindStringSubmatch(validationErr.Message); match != nil {
			fields = append(fields, match[2]+"."+match[1])
			continue
		}
		read := gitHubValidationRead{validationErr.Rule, validationErr.Message, gitHubFieldPathAt(query, validationErr.Locations)}
		if read == gitHubAcceptedTypeConflict {
			accepted++
			continue
		}
		other = append(other, validationErr.Error()+" [rule "+read.Rule+", field "+read.Path+"]")
	}
	slices.Sort(fields)
	return fields, accepted, other
}

// gitHubValidationProblem is the cell's verdict on one query: "" when the
// undefined fields, the accepted conflicts and nothing else are exactly what
// the query wants.
func gitHubValidationProblem(query string, errs gqlerror.List, wantFields []string, wantAccepted int) string {
	fields, accepted, other := classifyGitHubValidation(query, errs)
	if slices.Equal(fields, wantFields) && accepted == wantAccepted && len(other) == 0 {
		return ""
	}
	return "undefined fields " + strings.Join(fields, ",") + " (want " + strings.Join(wantFields, ",") +
		"), accepted state conflicts " + strconv.Itoa(accepted) + " (want " + strconv.Itoa(wantAccepted) +
		"), other errors [" + strings.Join(other, "; ") + "]"
}

func TestChaos14195ProjectsV2QueriesValidateAgainstGitHubSchema(t *testing.T) {
	schema := loadGitHubPublicSchema(t)
	for _, query := range []struct {
		name, text string
		accepted   int
	}{
		{"items", gitHubProjectsV2ItemsQuery, 1},
		{"fieldValues", gitHubProjectsV2FieldValuesQuery, 0},
	} {
		t.Run(query.name, func(t *testing.T) {
			_, errs := gqlparser.LoadQuery(schema, query.text)
			if problem := gitHubValidationProblem(query.text, errs, nil, query.accepted); problem != "" {
				t.Fatalf("GitHub's schema refuses the %s query: %s", query.name, problem)
			}
		})
	}
}

// The allowance holds in both directions. The real conflict passes; the same
// text at another field, another text at the field, another rule, a second
// copy, and no conflict at all (the allowance went stale) each fail the cell.
func TestChaos14195AcceptedStateConflictIsPinnedBothWays(t *testing.T) {
	schema := loadGitHubPublicSchema(t)
	query := gitHubProjectsV2ItemsQuery
	_, errs := gqlparser.LoadQuery(schema, query)
	if len(errs) != 1 {
		t.Fatalf("the items query has %d validation errors, want the one accepted conflict: %v", len(errs), errs)
	}
	conflict := *errs[0]
	if path := gitHubFieldPathAt(query, conflict.Locations); path != gitHubAcceptedTypeConflict.Path {
		t.Fatalf("the conflict resolves to field %q, want %q", path, gitHubAcceptedTypeConflict.Path)
	}
	issueState := conflict
	issueState.Locations = []gqlerror.Location{gitHubQueryLocation(t, query, "... on Issue {", " state ")}
	if path := gitHubFieldPathAt(query, issueState.Locations); path != "organization/projectV2/items/nodes/content/on Issue/state" {
		t.Fatalf("the Issue state location resolves to field %q", path)
	}
	otherText := conflict
	otherText.Message = strings.Replace(conflict.Message, `"IssueState!"`, `"IssueState"`, 1)
	otherRule := conflict
	otherRule.Rule = "FieldsOnCorrectType"
	for name, row := range map[string]struct {
		errs gqlerror.List
		ok   bool
	}{
		"the real conflict":            {gqlerror.List{&conflict}, true},
		"no conflict (stale)":          {nil, false},
		"the text at the Issue field":  {gqlerror.List{&issueState}, false},
		"another text at the field":    {gqlerror.List{&otherText}, false},
		"another rule at the field":    {gqlerror.List{&otherRule}, false},
		"the real conflict twice":      {gqlerror.List{&conflict, &conflict}, false},
		"the real conflict plus other": {gqlerror.List{&conflict, &otherRule}, false},
	} {
		if problem := gitHubValidationProblem(query, row.errs, nil, 1); (problem == "") != row.ok {
			t.Errorf("%s: problem %q, want pass=%t", name, problem, row.ok)
		}
	}
}

// gitHubQueryLocation is the 1-based location of the first `token` (its first
// non-space byte) on the first query line that holds `line`.
func gitHubQueryLocation(t *testing.T, query, line, token string) gqlerror.Location {
	t.Helper()
	for number, text := range strings.Split(query, "\n") {
		if strings.Contains(text, line) {
			if column := strings.Index(text, token); column >= 0 {
				return gqlerror.Location{Line: number + 1, Column: column + len(token) - len(strings.TrimLeft(token, " ")) + 1}
			}
		}
	}
	t.Fatalf("no query line holds %q with %q", line, token)
	return gqlerror.Location{}
}

// The items query as #1738 (0dcecf34a) shipped it. GitHub answered it with 5
// undefinedField errors and no data (live read, lane records
// GH-QUERY-14184-1.log); the offline cell must name the same 5.
const gitHubProjectsV2ItemsQueryAt0dcecf34a = `
query($login: String!, $number: Int!, $after: String, $first: Int!) {
  organization(login: $login) {
    projectV2(number: $number) {
      items(first: $first, after: $after) {
        nodes {
          id createdAt updatedAt
          content {
            __typename
            ... on Issue { id number title url state createdAt updatedAt closedAt repository { nameWithOwner } labels(first: 50) { nodes { name } } assignees(first: 10) { nodes { login email name } } author { login email name } }
            ... on PullRequest { id number title url state createdAt updatedAt closedAt mergedAt repository { nameWithOwner } labels(first: 50) { nodes { name } } assignees(first: 10) { nodes { login email name } } author { login email name } }
            ... on DraftIssue { id title createdAt updatedAt }
          }
          fieldValues(first: 20) { nodes {
            __typename
            ... on ProjectV2ItemFieldSingleSelectValue { name field { ... on ProjectV2SingleSelectField { name } } }
            ... on ProjectV2ItemFieldTextValue { text field { ... on ProjectV2FieldCommon { name } } }
            ... on ProjectV2ItemFieldIterationValue { title id field { ... on ProjectV2FieldCommon { name } } }
            ... on ProjectV2ItemFieldNumberValue { number field { ... on ProjectV2FieldCommon { name } } }
          } }
          changes(first: 100, orderBy: {field: CREATED_AT, direction: ASC}) { nodes { field { ... on ProjectV2FieldCommon { name } } previousValue { ... on ProjectV2ItemFieldSingleSelectValue { name } } newValue { ... on ProjectV2ItemFieldSingleSelectValue { name } } createdAt actor { login } } pageInfo { hasNextPage endCursor } }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`

func TestChaos14195ItemsQueryAt0dcecf34aFailsGitHubSchema(t *testing.T) {
	schema := loadGitHubPublicSchema(t)
	_, errs := gqlparser.LoadQuery(schema, gitHubProjectsV2ItemsQueryAt0dcecf34a)
	want := []string{"Actor.email", "Actor.email", "Actor.name", "Actor.name", "ProjectV2Item.changes"}
	if problem := gitHubValidationProblem(gitHubProjectsV2ItemsQueryAt0dcecf34a, errs, want, 1); problem != "" {
		t.Fatalf("0dcecf34a items query: %s", problem)
	}
}

// The author is an Actor: only a User has email and name, so the query asks
// them on the User fragment. A Bot or a Mannequin author answers its login
// alone and still names the reporter.
func TestChaos14195BoardAuthorKeepsLoginForUserBotAndMannequin(t *testing.T) {
	item := func(id string, number int, author string) string {
		return `{"id":"` + id + `","content":{"__typename":"Issue","number":` + strconv.Itoa(number) +
			`,"title":"t","state":"OPEN","createdAt":"2026-08-01T08:00:00Z","updatedAt":"2026-08-02T08:00:00Z",` +
			`"repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[],"pageInfo":{"hasNextPage":false}},` +
			`"assignees":{"nodes":[]},"author":` + author + `},` +
			`"fieldValues":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}`
	}
	doer := &gitHubProjectV2Doer{t: t, replies: []string{
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[` +
			item("PVTI_USER", 1, `{"login":"octocat","email":"Octo@Example.COM","name":"Mona"}`) + `,` +
			item("PVTI_BOT", 2, `{"login":"dependabot"}`) + `,` +
			item("PVTI_MANNEQUIN", 3, `{"login":"mona-imported"}`) + `,` +
			item("PVTI_NONE", 4, `null`) +
			`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
	}}
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 3}}}
	result, err := (GitHubProjectV2Fetcher{}).Fetch(context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		githubProjectV2TestClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"gh:acme/api#1": "octo@example.com", "gh:acme/api#2": "github:dependabot",
		"gh:acme/api#3": "github:mona-imported", "gh:acme/api#4": "<nil>",
	}
	got := map[string]string{}
	for _, row := range result.Rows.WorkItems {
		got[row.WorkItemID] = "<nil>"
		if row.Reporter != nil {
			got[row.WorkItemID] = *row.Reporter
		}
	}
	if !maps.Equal(got, want) {
		t.Fatalf("reporters=%v want %v", got, want)
	}
	query, _ := doer.bodies[0]["query"].(string)
	if strings.Count(query, "author { login ... on User { email name } }") != 2 {
		t.Fatalf("the Issue and PullRequest author must ask login on Actor and email/name on User: %s", query)
	}
}
