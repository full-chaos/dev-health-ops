package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// CHAOS-8790 (review P1): a provider comment id keeps its exact text. A JSON
// number must never pass through float64: 9007199254740992 and
// 9007199254740993 are two comments, not one. These tests drive the
// provider's own decode step (the GitLab route, the GitHub and Jira decoders,
// the Linear payload) with raw JSON text, never a pre-built Go value.

const (
	idTwoPow53      = "9007199254740992"
	idTwoPow53Plus1 = "9007199254740993"
	idHuge          = "9223372036854775807"
)

// gitLabNotesInteractionRows runs the real GitLab work-items route over a
// fake API whose note list is the given raw JSON objects, and returns the
// work_item_interactions rows the route produced.
func gitLabNotesInteractionRows(t *testing.T, noteJSON ...string) []githubWorkItemInteractionRow {
	t.Helper()
	root := "/api/v4/projects/123"
	responses := gitLabWorkItemResponses()
	responses[root+"/issues/42/notes?page=1"] = []string{"[" + strings.Join(noteJSON, ",") + "]", `[]`}
	handler := GitLabWorkItemsRouteHandler{PerPage: 100, MaxPages: 10, NestedMaxPages: 5}
	handler.StatusMapping = loadRealStatusMapping(t)
	handler.IncludeMRs = boolPointer(false)
	claim := nativeTestClaim("gitlab", "work-items")
	batch, err := handler.Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, fakehttp.Client(&gitLabWorkItemsDoer{responses: responses})),
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	var rows []githubWorkItemInteractionRow
	for _, effect := range batch.Effects {
		if effect.Destination != "work_item_interactions" {
			continue
		}
		for _, raw := range effect.Rows {
			var row githubWorkItemInteractionRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
		}
	}
	return rows
}

func noteJSON(id string) string {
	return fmt.Sprintf(`{"id":%s,"system":false,"body":"same instant","created_at":"2026-07-02T12:00:00Z","author":{"username":"alice"}}`, id)
}

func TestGitLabRouteKeepsNoteIDsAbove2Pow53Apart(t *testing.T) {
	rows := gitLabNotesInteractionRows(t, noteJSON(idTwoPow53), noteJSON(idTwoPow53Plus1))
	if len(rows) != 2 || rows[0].InteractionID != idTwoPow53 || rows[1].InteractionID != idTwoPow53Plus1 {
		t.Fatalf("rows=%+v, want ids %s and %s", rows, idTwoPow53, idTwoPow53Plus1)
	}
	if !rows[0].OccurredAt.Equal(rows[1].OccurredAt) {
		t.Fatal("the two notes must share one instant")
	}
	if kept := dedupeBySortingKey(rows, workItemInteractionSortingKey); len(kept) != 2 {
		t.Fatalf("the writer's sorting-key dedupe kept %d of 2", len(kept))
	}
}

func TestGitLabRouteNoteIDForms(t *testing.T) {
	rows := gitLabNotesInteractionRows(t,
		noteJSON(`"abc-1"`), // a string id is still an id
		noteJSON(idHuge),
		noteJSON(`501`), // a normal id: unchanged text
	)
	got := []string{}
	for _, row := range rows {
		got = append(got, row.InteractionID)
	}
	sort.Strings(got)
	want := []string{"501", idHuge, "abc-1"}
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("ids=%v", got)
	}
}

func TestEveryProviderDecodesLargeCommentIDsExactly(t *testing.T) {
	const created = "2026-08-03T10:00:00Z"
	decoders := map[string]func(t *testing.T, ids []string) []githubWorkItemInteractionRow{
		"github": func(t *testing.T, ids []string) []githubWorkItemInteractionRow {
			raws := []json.RawMessage{}
			for _, id := range ids {
				raws = append(raws, json.RawMessage(fmt.Sprintf(`{"id":%s,"body":"x","created_at":%q}`, id, created)))
			}
			rows, err := normalizeGitHubWorkItemComments(nativeTestClaim("github", "work-items"), "gh:acme/api#42", raws, nil, interactionIDTestNow)
			if err != nil {
				t.Fatal(err)
			}
			return rows
		},
		"jira": func(t *testing.T, ids []string) []githubWorkItemInteractionRow {
			comments := []map[string]any{}
			for _, id := range ids {
				var comment map[string]any
				if err := decodeJiraJSON([]byte(fmt.Sprintf(`{"id":%s,"created":%q,"body":"x"}`, id, created)), &comment); err != nil {
					t.Fatal(err)
				}
				comments = append(comments, comment)
			}
			return normalizeJiraInteractions(nativeTestClaim("jira", "work-items"), "jira:OPS-42", comments, nil, interactionIDTestNow)
		},
	}
	for provider, decode := range decoders {
		t.Run(provider, func(t *testing.T) {
			rows := decode(t, []string{idTwoPow53, idTwoPow53Plus1})
			if len(rows) != 2 || rows[0].InteractionID != idTwoPow53 || rows[1].InteractionID != idTwoPow53Plus1 {
				t.Fatalf("rows=%+v", rows)
			}
			if kept := dedupeBySortingKey(rows, workItemInteractionSortingKey); len(kept) != 2 {
				t.Fatalf("kept %d of 2", len(kept))
			}
			// exact boundary and a normal id
			rows = decode(t, []string{"9007199254740991", "501"})
			if len(rows) != 2 || rows[0].InteractionID != "9007199254740991" || rows[1].InteractionID != "501" {
				t.Fatalf("rows=%+v", rows)
			}
			// string id still accepted
			rows = decode(t, []string{`"c-7"`})
			if len(rows) != 1 || rows[0].InteractionID != "c-7" {
				t.Fatalf("rows=%+v", rows)
			}
			// negative, fractional, exponent: missing-id path (skipped, counted)
			before := InteractionMissingIDCount(provider)
			rows = decode(t, []string{"-5", "1.5", "1e3", "0"})
			if len(rows) != 0 {
				t.Fatalf("rows=%+v, want none", rows)
			}
			if got := InteractionMissingIDCount(provider) - before; got != 4 {
				t.Fatalf("counted %d skipped, want 4", got)
			}
		})
	}
}

// Linear's comment id is a GraphQL ID string; a JSON number there is a
// malformed payload (decode error), never a rounded id.
func TestLinearCommentIDIsAStringAndANumberIsRefused(t *testing.T) {
	var comment linearCommentPayload
	if err := json.Unmarshal([]byte(`{"id":"`+idTwoPow53Plus1+`","body":"x"}`), &comment); err != nil || comment.ID != idTwoPow53Plus1 {
		t.Fatalf("id=%q err=%v", comment.ID, err)
	}
	if err := json.Unmarshal([]byte(`{"id":`+idTwoPow53Plus1+`,"body":"x"}`), &comment); err == nil {
		t.Fatal("a numeric Linear id must not decode")
	}
}

func TestInteractionIDFromValues(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{json.Number(idTwoPow53Plus1), idTwoPow53Plus1},
		{json.Number("501"), "501"},
		{json.Number("-5"), ""},
		{json.Number("1.5"), ""},
		{json.Number("1e3"), ""},
		{json.Number("0"), ""},
		{float64(501), "501"},
		{float64(-5), ""},
		{float64(1.5), ""},
		{float64(1 << 60), ""}, // float64 cannot carry it exactly
		{" c-1 ", "c-1"},
		{true, ""},
		{nil, ""},
	}
	for _, tc := range cases {
		if got := interactionIDFrom(tc.in); got != tc.want {
			t.Errorf("interactionIDFrom(%#v)=%q want %q", tc.in, got, tc.want)
		}
	}
}
