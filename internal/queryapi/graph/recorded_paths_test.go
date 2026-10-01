package graph

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedpaths"
)

// CHAOS-7473: every scalar path of the recorded pull request golden is declared. A CLAIM path is
// one whose change makes TestPrMatchesTheFrozenGolden fail (the mutation run in the pull request
// that added this file backs that); the other three are named with their reason.

var prGoldenClaims = []string{
	"[].commits[].author_email",
	"[].commits[].author_name",
	"[].commits[].author_when",
	"[].commits[].confidence",
	"[].commits[].evidence",
	"[].commits[].hash",
	"[].commits[].message",
	"[].commits[].provenance",
	"[].core[].additions",
	"[].core[].author_email",
	"[].core[].author_name",
	"[].core[].base_branch",
	"[].core[].body",
	"[].core[].changed_files",
	"[].core[].changes_requested_count",
	"[].core[].closed_at",
	"[].core[].comments_count",
	"[].core[].created_at",
	"[].core[].deletions",
	"[].core[].first_comment_at",
	"[].core[].first_review_at",
	"[].core[].head_branch",
	"[].core[].merged_at",
	"[].core[].repo_name",
	"[].core[].reviews_count",
	"[].core[].state",
	"[].core[].title",
	"[].expected",
	"[].expected.additions",
	"[].expected.author_email",
	"[].expected.author_name",
	"[].expected.base_branch",
	"[].expected.body",
	"[].expected.changed_files",
	"[].expected.changes_requested_count",
	"[].expected.closed_at",
	"[].expected.comments_count",
	"[].expected.commits[].author_email",
	"[].expected.commits[].author_name",
	"[].expected.commits[].author_when",
	"[].expected.commits[].confidence",
	"[].expected.commits[].evidence",
	"[].expected.commits[].hash",
	"[].expected.commits[].message",
	"[].expected.commits[].provenance",
	"[].expected.created_at",
	"[].expected.deletions",
	"[].expected.first_comment_at",
	"[].expected.first_review_at",
	"[].expected.head_branch",
	"[].expected.id",
	"[].expected.linked_issues[].confidence",
	"[].expected.linked_issues[].evidence",
	"[].expected.linked_issues[].provenance",
	"[].expected.linked_issues[].work_item_id",
	"[].expected.merged_at",
	"[].expected.number",
	"[].expected.org_id",
	"[].expected.repo_id",
	"[].expected.repo_name",
	"[].expected.reviews[].review_id",
	"[].expected.reviews[].reviewer",
	"[].expected.reviews[].state",
	"[].expected.reviews[].submitted_at",
	"[].expected.reviews_count",
	"[].expected.state",
	"[].expected.title",
	"[].id",
	"[].issues[].confidence",
	"[].issues[].evidence",
	"[].issues[].provenance",
	"[].issues[].work_item_id",
	"[].kind",
	"[].python_params[].number",
	"[].python_params[].org_id",
	"[].python_params[].repo_id",
	"[].python_queries[]",
	"[].reviews[].review_id",
	"[].reviews[].reviewer",
	"[].reviews[].state",
	"[].reviews[].submitted_at",
}

var prGoldenNotClaims = map[string]string{
	"[].name":           "a case label (subtest name only)",
	"[].core[].number":  "a recorded Python key column the Go core query does not select; TestTheCoreQueryDoesNotSelectTheKeyColumns checks the Go select list",
	"[].core[].repo_id": "a recorded Python key column the Go core query does not select; TestTheCoreQueryDoesNotSelectTheKeyColumns checks the Go select list",
}

func TestEveryRecordedPathIsDeclared(t *testing.T) {
	raw, err := os.ReadFile("testdata/pr_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	recordedpaths.Check(t, raw, prGoldenClaims, prGoldenNotClaims)
}

// TestTheCoreQueryDoesNotSelectTheKeyColumns backs the two not-a-claim paths: the recorded core
// rows carry repo_id and number because the reference SELECT started with them, but the Go core
// query already holds both from the parsed id and leaves them out. If it starts selecting either,
// the scripted rows would have to supply it and the two paths become claims.
func TestTheCoreQueryDoesNotSelectTheKeyColumns(t *testing.T) {
	raw, err := os.ReadFile("testdata/pr_golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []prGoldenCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		if tc.Kind == "parse" || len(tc.Core) == 0 {
			continue
		}
		wantList := tc.PythonQueries[0][:strings.Index(tc.PythonQueries[0], " FROM ")]
		for _, column := range []string{" AS repo_id", " AS number"} {
			if !strings.Contains(wantList, column) {
				t.Fatalf("case %q: the recorded reference query no longer selects%s; the not-a-claim reason is stale", tc.Name, column)
			}
		}
		client := &prGoldenClient{c: tc}
		ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1"})
		if _, err := (&Resolver{ClickHouse: client}).Query().Pr(ctx, "org-argument-ignored", tc.ID); err != nil {
			t.Fatalf("Pr: %v", err)
		}
		for i, kind := range client.kinds {
			if kind != "core" {
				continue
			}
			text := strings.Join(strings.Fields(client.statements[i]), " ")
			gotList := text[:strings.Index(text, " FROM ")]
			for _, column := range []string{" AS repo_id", " AS number", "pr.number", "pr.repo_id"} {
				if strings.Contains(gotList, column) {
					t.Errorf("case %q: the Go core query selects %q; the recorded core[].repo_id and core[].number are now read and must be claims", tc.Name, column)
				}
			}
			return
		}
		t.Fatalf("case %q: no core query was issued", tc.Name)
	}
}
