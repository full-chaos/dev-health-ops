package providersync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
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

// undefinedFields turns validation errors into sorted "Type.field" names; any
// other kind of error is returned verbatim so that it cannot hide.
func undefinedFields(errs gqlerror.List) (fields, other []string) {
	undefined := regexp.MustCompile(`^Cannot query field "(\w+)" on type "(\w+)"\.`)
	for _, validationErr := range errs {
		if match := undefined.FindStringSubmatch(validationErr.Message); match != nil {
			fields = append(fields, match[2]+"."+match[1])
			continue
		}
		other = append(other, validationErr.Error())
	}
	slices.Sort(fields)
	return fields, other
}

func TestChaos14195ProjectsV2QueriesValidateAgainstGitHubSchema(t *testing.T) {
	schema := loadGitHubPublicSchema(t)
	for _, query := range []struct{ name, text string }{
		{"items", gitHubProjectsV2ItemsQuery},
		{"fieldValues", gitHubProjectsV2FieldValuesQuery},
		{"changes", gitHubProjectsV2ChangesQuery},
	} {
		t.Run(query.name, func(t *testing.T) {
			if _, errs := gqlparser.LoadQuery(schema, query.text); len(errs) > 0 {
				fields, other := undefinedFields(errs)
				t.Fatalf("GitHub's schema refuses the %s query: %d errors, undefined fields %v, other %v", query.name, len(errs), fields, other)
			}
		})
	}
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
	fields, other := undefinedFields(errs)
	want := []string{"Actor.email", "Actor.email", "Actor.name", "Actor.name", "ProjectV2Item.changes"}
	if !slices.Equal(fields, want) || len(other) > 0 {
		t.Fatalf("0dcecf34a items query: undefined fields %v (want %v), other errors %v", fields, want, other)
	}
}
