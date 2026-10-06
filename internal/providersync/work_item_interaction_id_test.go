package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

// CHAOS-8790: the comment id is the last part of the sink key. Two comments of
// one work item with the same timestamp are two rows, for every provider; a
// comment without an id is skipped and counted, never written with ''.

var interactionIDTestNow = time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

type interactionIDProvider struct {
	provider string
	// build normalizes one batch of comments: for each entry the id as the
	// provider sends it (nil = absent) and the SAME created timestamp.
	build func(t *testing.T, ids []any) []githubWorkItemInteractionRow
}

func interactionIDProviders() []interactionIDProvider {
	const created = "2026-08-03T10:00:00Z"
	return []interactionIDProvider{
		{"github", func(t *testing.T, ids []any) []githubWorkItemInteractionRow {
			raws := make([]json.RawMessage, 0, len(ids))
			for _, id := range ids {
				comment := map[string]any{"body": "same millisecond", "created_at": created}
				if id != nil {
					comment["id"] = id
				}
				raw, err := json.Marshal(comment)
				if err != nil {
					t.Fatal(err)
				}
				raws = append(raws, raw)
			}
			rows, err := normalizeGitHubWorkItemComments(
				nativeTestClaim("github", "work-items"), "gh:acme/api#42", raws, nil, interactionIDTestNow)
			if err != nil {
				t.Fatal(err)
			}
			return rows
		}},
		{"gitlab", func(t *testing.T, ids []any) []githubWorkItemInteractionRow {
			notes := make([]gitlabNotePayload, 0, len(ids))
			at := created
			for _, id := range ids {
				notes = append(notes, gitlabNotePayload{ID: id, Body: "same millisecond", CreatedAt: &at})
			}
			return normalizeGitLabNotes(
				nativeTestClaim("gitlab", "work-items"), "gitlab:acme/api#42", notes, nil, interactionIDTestNow)
		}},
		{"jira", func(t *testing.T, ids []any) []githubWorkItemInteractionRow {
			comments := make([]map[string]any, 0, len(ids))
			for _, id := range ids {
				comment := map[string]any{"created": created, "body": "same millisecond"}
				if id != nil {
					comment["id"] = id
				}
				comments = append(comments, comment)
			}
			return normalizeJiraInteractions(
				nativeTestClaim("jira", "work-items"), "jira:OPS-42", comments, nil, interactionIDTestNow)
		}},
		{"linear", func(t *testing.T, ids []any) []githubWorkItemInteractionRow {
			comments := make([]linearCommentPayload, 0, len(ids))
			for _, id := range ids {
				comment := linearCommentPayload{Body: "same millisecond", CreatedAt: created}
				if text, ok := id.(string); ok {
					comment.ID = text
				}
				comments = append(comments, comment)
			}
			return normalizeLinearInteractions(
				nativeTestClaim("linear", "work-items"), "linear:ENG-42", comments, interactionIDTestNow)
		}},
	}
}

func TestEveryProviderKeepsTwoSameTimestampCommentsAsTwoRows(t *testing.T) {
	for _, provider := range interactionIDProviders() {
		t.Run(provider.provider, func(t *testing.T) {
			rows := provider.build(t, []any{"c-1", "c-2"})
			if len(rows) != 2 {
				t.Fatalf("%s: two comments gave %d rows", provider.provider, len(rows))
			}
			if rows[0].InteractionID != "c-1" || rows[1].InteractionID != "c-2" {
				t.Fatalf("%s: ids = %q, %q", provider.provider, rows[0].InteractionID, rows[1].InteractionID)
			}
			if !rows[0].OccurredAt.Equal(rows[1].OccurredAt) {
				t.Fatalf("%s: the two comments must share one timestamp", provider.provider)
			}
			// the writer-side dedupe is the first place the pair could collapse
			kept := dedupeBySortingKey(rows, workItemInteractionSortingKey)
			if len(kept) != 2 {
				t.Fatalf("%s: the writer's sorting-key dedupe kept %d of 2", provider.provider, len(kept))
			}
			// the same comment sent twice (a re-sync) is still one row
			again := dedupeBySortingKey(append(append([]githubWorkItemInteractionRow{}, rows...), rows...), workItemInteractionSortingKey)
			if len(again) != 2 {
				t.Fatalf("%s: a re-sync of the same two comments kept %d rows", provider.provider, len(again))
			}
		})
	}
}

func TestACommentWithoutAnIDIsSkippedAndCountedNeverWrittenEmpty(t *testing.T) {
	for _, provider := range interactionIDProviders() {
		t.Run(provider.provider, func(t *testing.T) {
			before := InteractionMissingIDCount(provider.provider)
			// absent, empty, blank, zero, and a type that is no id
			rows := provider.build(t, []any{nil, "", "  ", "0", true, "good"})
			if len(rows) != 1 || rows[0].InteractionID != "good" {
				t.Fatalf("%s: rows=%+v, want only the comment with an id", provider.provider, rows)
			}
			for _, row := range rows {
				if row.InteractionID == "" {
					t.Fatalf("%s: a row was built with an empty interaction_id", provider.provider)
				}
			}
			skipped := InteractionMissingIDCount(provider.provider) - before
			// absent, empty, blank, zero and a non-id type: five skipped
			const wantSkipped = int64(5)
			if skipped != wantSkipped {
				t.Fatalf("%s: counted %d skipped comments, want %d", provider.provider, skipped, wantSkipped)
			}
		})
	}
}

func TestInteractionRowWithoutAnIDIsInvalidForEveryProvider(t *testing.T) {
	base := githubWorkItemInteractionRow{
		WorkItemID: "wi", InteractionType: "comment", OccurredAt: interactionIDTestNow,
		LastSynced: interactionIDTestNow, OrgID: "org-acme", InteractionID: "c-1",
	}
	checks := map[string]func(row githubWorkItemInteractionRow) error{
		"github": func(row githubWorkItemInteractionRow) error {
			row.Provider = "github"
			return row.validate(nativeTestClaim("github", "work-items"))
		},
		"gitlab": func(row githubWorkItemInteractionRow) error {
			row.Provider = "gitlab"
			return validateGitLabInteractionRow(row, nativeTestClaim("gitlab", "work-items"))
		},
		"jira": func(row githubWorkItemInteractionRow) error {
			row.Provider = "jira"
			return validateJiraInteraction(row, nativeTestClaim("jira", "work-items"))
		},
	}
	for provider, check := range checks {
		if err := check(base); err != nil {
			t.Fatalf("%s: a row with an id must be valid: %v", provider, err)
		}
		empty := base
		empty.InteractionID = ""
		if err := check(empty); err == nil {
			t.Fatalf("%s: a row with an empty interaction_id must be invalid", provider)
		}
	}
}

func TestInteractionSortingKeyAndValuesCarryTheID(t *testing.T) {
	first := githubWorkItemInteractionRow{
		WorkItemID: "wi", Provider: "github", InteractionType: "comment",
		OccurredAt: interactionIDTestNow, LastSynced: interactionIDTestNow,
		OrgID: "org-acme", InteractionID: "c-1",
	}
	second := first
	second.InteractionID = "c-2"
	if workItemInteractionSortingKey(first) == workItemInteractionSortingKey(second) {
		t.Fatal("two ids must give two sorting keys")
	}
	same := first
	if workItemInteractionSortingKey(first) != workItemInteractionSortingKey(same) {
		t.Fatal("one id must give one sorting key")
	}
	values := workItemInteractionValues(first)
	if got := values[len(values)-1]; got != "c-1" {
		t.Fatalf("the last inserted value is %v, want the interaction id", got)
	}
}
