package investment

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/promptpii"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// piiValues are the values that must never reach the prompt.
var piiValues = []string{
	"jane.doe@corp.example", "jdoe-handle", "accountid:5b10ac8d82e05b22cc7d4ef5",
	"(415) 555-0132", "hunter2pass", "ghp_abcdefghijklmnopqrstuvwxyz0123456789",
	"boss@corp.example", "ceo-handle", "+44 20 7946 0958", "pr.author@corp.example", "dev@corp.example",
}

func piiComponentInput() MaterializeComponentInput {
	issueID := "issue-pii"
	prID := testRepoID + "#pr9"
	commitID := testRepoID + "@def456"
	created := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)

	input := windowedInput()
	input.Component = units.Component{
		Nodes: []units.NodeKey{{Type: "issue", ID: issueID}, {Type: "pr", ID: prID}, {Type: "commit", ID: commitID}},
		Edges: []units.Edge{
			{EdgeID: "e1", SourceType: "issue", SourceID: issueID, TargetType: "pr", TargetID: prID, Confidence: 1.0},
			{EdgeID: "e2", SourceType: "issue", SourceID: issueID, TargetType: "commit", TargetID: commitID, Confidence: 1.0},
		},
	}
	input.WorkItems = map[string]chquery.WorkItem{issueID: {
		WorkItemID: issueID, Provider: "jira", Type: "bug", CreatedAt: created, UpdatedAt: created,
		Title:       "Login fails for jane.doe@corp.example",
		Description: "Reported by [~accountid:5b10ac8d82e05b22cc7d4ef5], cc @jdoe-handle. Call (415) 555-0132. Deploy via https://deploy:hunter2pass@git.corp.example/x with ghp_abcdefghijklmnopqrstuvwxyz0123456789",
		Labels:      []string{"customer-boss@corp.example"},
		ParentID:    "parent-1", EpicID: "epic-1",
	}}
	input.ParentTitles = map[string]string{"parent-1": "Escalation from boss@corp.example"}
	input.EpicTitles = map[string]string{"epic-1": "Owned by @ceo-handle, call +44 20 7946 0958"}
	input.PRs = map[string]chquery.PullRequest{prID: {
		RepoID: testRepoID, Number: 9, CreatedAt: created,
		Title: "Fix login", Body: "Authored by pr.author@corp.example",
	}}
	input.Commits = map[string]chquery.Commit{commitID: {
		RepoID: testRepoID, Hash: "def456", AuthorWhen: created, Message: "Fix login (dev@corp.example)",
	}}
	input.EdgeRepoIDs = map[string]string{"e1": testRepoID, "e2": testRepoID}
	return input
}

func TestMaterializeComponentFiltersPIIBeforeTheBundleAndPrompt(t *testing.T) {
	result, err := MaterializeComponent(piiComponentInput())
	if err != nil {
		t.Fatalf("MaterializeComponent: %v", err)
	}
	prompt := categorize.BuildCategorizationPrompt(result.Bundle)
	hashed, _ := json.Marshal(result.Bundle.SourceTexts)
	for _, value := range piiValues {
		for name, text := range map[string]string{"prompt": prompt, "source texts": string(hashed), "source block": result.Bundle.SourceBlock} {
			if strings.Contains(text, value) {
				t.Errorf("%s still holds a value that must be filtered: %q", name, value)
			}
		}
	}
	// Titles, descriptions, labels, parent and epic titles, PR body and commit
	// message all pass the same filter.
	got := result.PromptRedactions
	if got.Emails != 5 || got.Mentions != 3 || got.Phones != 2 || got.Credentials < 2 {
		t.Errorf("redaction counts = %+v, want 5 emails, 3 mentions, 2 phones, >=2 credentials", got)
	}
	if !strings.Contains(prompt, "Login fails for [email-1]") {
		t.Errorf("the title is not filtered in place: %q", prompt)
	}
}

func TestMaterializeComponentPIIFilterIsDeterministic(t *testing.T) {
	first, err := MaterializeComponent(piiComponentInput())
	if err != nil {
		t.Fatal(err)
	}
	second, err := MaterializeComponent(piiComponentInput())
	if err != nil {
		t.Fatal(err)
	}
	if first.Bundle.InputHash != second.Bundle.InputHash || first.Bundle.SourceBlock != second.Bundle.SourceBlock {
		t.Fatal("two runs over the same input build different bundles")
	}
}

func TestMaterializeComponentTextWithoutPIIIsUnchanged(t *testing.T) {
	input := piiComponentInput()
	clean := windowedInput()
	clean.Component = input.Component
	clean.EdgeRepoIDs = input.EdgeRepoIDs
	clean.WorkItems = map[string]chquery.WorkItem{"issue-pii": {
		WorkItemID: "issue-pii", Provider: "jira", Type: "bug", Title: "Login fails", Description: "Timeout in the refresh path",
		CreatedAt: time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC), UpdatedAt: time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC),
	}}
	clean.PRs = map[string]chquery.PullRequest{testRepoID + "#pr9": {RepoID: testRepoID, Number: 9, Title: "Fix login", Body: "Raises the timeout"}}
	clean.Commits = map[string]chquery.Commit{testRepoID + "@def456": {RepoID: testRepoID, Hash: "def456", Message: "Fix login"}}
	result, err := MaterializeComponent(clean)
	if err != nil {
		t.Fatal(err)
	}
	if result.PromptRedactions.Total() != 0 {
		t.Fatalf("redactions on clean text: %+v", result.PromptRedactions)
	}
	raw, err := units.BuildTextBundle(units.BuildTextBundleInput{
		IssueIDs: []string{"issue-pii"}, PRIDs: []string{testRepoID + "#pr9"}, CommitIDs: []string{testRepoID + "@def456"},
		WorkItemMap: workItemMapForBundle([]string{"issue-pii"}, clean.WorkItems),
		PRMap:       prMapForBundle([]string{testRepoID + "#pr9"}, clean.PRs),
		CommitMap:   commitMapForBundle([]string{testRepoID + "@def456"}, clean.Commits),
		WorkUnitID:  units.WorkUnitID(clean.Component.Nodes),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Bundle.InputHash != raw.InputHash || result.Bundle.SourceBlock != raw.SourceBlock {
		t.Fatalf("text without PII changed:\n got %q\nwant %q", result.Bundle.SourceBlock, raw.SourceBlock)
	}
}

type quoteCompleter struct{ quote string }

func (c quoteCompleter) CompleteBundle(context.Context, units.TextBundle) (categorize.CompletionResult, error) {
	text := `{"subcategories":{"quality.bugfix":3,"quality.reliability":1},"evidence_quotes":[{"quote":"` + c.quote + `","source":"issue","id":"E1"}],"uncertainty":"The issue reads as a login bug."}`
	in, out := 10, 5
	return categorize.CompletionResult{Text: text, InputTokens: &in, OutputTokens: &out, Model: "m"}, nil
}

// The model sees only filtered text, so the quote it copies is filtered text.
// It must still validate against the bundle, and the quote that is persisted
// must hold no PII.
func TestFilteredBundleStillCategorizesAndPersistedQuotesHoldNoPII(t *testing.T) {
	result, err := MaterializeComponent(piiComponentInput())
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := categorize.CategorizeBundleOnce(context.Background(), result.Bundle, quoteCompleter{quote: "Login fails for [email-1]"})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != "ok" {
		t.Fatalf("status = %q errors = %v, want ok", outcome.Status, outcome.Errors)
	}
	if len(outcome.EvidenceQuotes) != 1 || strings.Contains(outcome.EvidenceQuotes[0].Quote, "@") {
		t.Fatalf("quotes = %+v", outcome.EvidenceQuotes)
	}
	// A quote with the raw address is no longer a substring of any source.
	raw, err := categorize.CategorizeBundleOnce(context.Background(), result.Bundle, quoteCompleter{quote: "Login fails for jane.doe@corp.example"})
	if err != nil {
		t.Fatal(err)
	}
	if raw.Status == "ok" {
		t.Fatal("a quote holding the raw address validated against the filtered bundle")
	}
}

func TestPromptRedactionsLogHoldsCountsAndNoValues(t *testing.T) {
	result, err := MaterializeComponent(piiComponentInput())
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	logPromptRedactions(context.Background(), slog.New(slog.NewTextHandler(&logs, nil)), "org-1", "run-1", result.PromptRedactions)
	line := logs.String()
	for _, want := range []string{promptRedactionsEvent, "emails=5", "mentions=3", "phones=2", "total="} {
		if !strings.Contains(line, want) {
			t.Errorf("log line lacks %q: %s", want, line)
		}
	}
	for _, value := range piiValues {
		if strings.Contains(line, value) {
			t.Errorf("log line holds a filtered value %q", value)
		}
	}
}

var _ = promptpii.Counts{}
