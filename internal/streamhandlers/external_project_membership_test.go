package streamhandlers

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// externalProjectMembershipPayload is the minimum shape CHAOS-4193's producers
// are gated on. It is spelled out here rather than derived from the schema
// table so a silent relaxation of a required field fails a test instead of
// widening what the sink accepts.
func externalProjectMembershipPayload(provider string, overrides map[string]any) map[string]any {
	payload := map[string]any{
		"externalKey": "7", "provider": provider,
		"eventId":      "evt-1",
		"subjectKind":  "work_item",
		"workItemType": "issue",
		"occurredAt":   "2026-07-22T11:00:00Z",
		"toProjectId":  "ghprojv2:full-chaos#4",
		"toProjectKey": "PLATFORM",
		"actor":        "ada",
	}
	// github carries its repository; jira and linear are repo-less and must
	// not, or the fixture would assert a field their producers never send.
	// Deliberately the DEFAULT rather than an opt-in override: a fixture that
	// omitted it would make every other test in this file exercise the
	// org-scoped fallback path instead of the shape production sends, and the
	// refusal that closes that path would then be the only thing they proved.
	if provider == "github" {
		payload["repositoryExternalId"] = "full-chaos/dev-health"
	}
	for key, value := range overrides {
		payload[key] = value
	}
	return payload
}

// externalPullRequestMembershipPayload is the pull_request subject arm: a repo
// plus a decimal PR number, and no workItemType at all.
func externalPullRequestMembershipPayload(overrides map[string]any) map[string]any {
	payload := externalProjectMembershipPayload("github", map[string]any{
		"subjectKind":          "pull_request",
		"externalKey":          "42",
		"repositoryExternalId": "full-chaos/dev-health",
	})
	delete(payload, "workItemType")
	for key, value := range overrides {
		payload[key] = value
	}
	return payload
}

// TestExternalIngestRefusesUnregisteredKindRatherThanDroppingIt is the property
// CHAOS-4193 said it would verify before gating its producers on kind
// registration: an unregistered kind must be REFUSED and the refusal must be
// durably recorded, not swallowed.
//
// The distinction is the whole point. A silent drop and a refusal look
// identical from the sink's side -- zero rows either way -- so a producer
// shipping against an unregistered kind would see a clean, successful sync and
// no data. Only the recorded rejection tells the two apart. The kind used here
// is deliberately one that will never be registered, so this test stays green
// after `project_membership_transition.v1` is added and keeps guarding the
// mechanism rather than a moment in time.
func TestExternalIngestRefusesUnregisteredKindRatherThanDroppingIt(t *testing.T) {
	pointer := externalTestPointer()
	payload := externalTestPayload(t, pointer, "legacy", []map[string]any{
		{
			"kind": "repository.v1", "externalId": "repo-1",
			"payload": map[string]any{
				"externalId": pointer.SourceInstance, "sourceSystem": "github",
			},
		},
		{
			"kind": "project_membership_teleport.v1", "externalId": "never-registered-1",
			"payload": map[string]any{"externalKey": "7", "provider": "github"},
		},
	})
	repository := &externalRepositoryFake{
		allowed: true,
		batch: externalBatch{
			Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
			ItemsReceived: 2, Payload: payload,
		},
	}
	sink := &externalSinkFake{}
	handler, err := NewExternalIngestHandler(repository, sink, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.backoff = nil
	if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err != nil {
		t.Fatal(err)
	}
	if len(repository.completions) != 1 {
		t.Fatalf("batch outcome not persisted: %#v", repository.completions)
	}
	completion := repository.completions[0]
	if completion.Accepted != 1 || completion.Rejected != 1 || len(completion.Rejections) != 1 {
		t.Fatalf("unregistered kind was not refused: %#v", completion)
	}
	rejection := completion.Rejections[0]
	if rejection.Code != "unsupported_kind_for_system" || rejection.Kind != "project_membership_teleport.v1" ||
		rejection.ExternalID != "never-registered-1" {
		t.Fatalf("refusal is not attributable to the record: %#v", rejection)
	}
	if len(sink.calls) != 1 || len(sink.calls[0].Records) != 1 ||
		sink.calls[0].Records[0].Kind != "repository.v1" {
		t.Fatalf("the refused record reached the sink: %#v", sink.calls)
	}
	if completion.RecordCounts["project_membership_teleport.v1"] != 0 {
		t.Fatalf("a refused kind was counted as received: %#v", completion.RecordCounts)
	}
}

// TestExternalIngestAcceptsProjectMembershipForItsRegisteredProviders pins the
// registration ruling itself: github, jira and linear carry the kind, and
// GITLAB DOES NOT.
//
// The gitlab half is the interesting one and it is not an omission. GitLab's
// own "project" concept IS this schema's repo_id, so a gitlab producer could
// only ever write a repo-derived id into to_project_id -- a value that resolves
// to no `projects` row. There is no correct gitlab row to admit, so the
// registry refusal is the fail-closed guard, and it must be a RECORDED refusal
// rather than a quietly missing producer.
func TestExternalIngestAcceptsProjectMembershipForItsRegisteredProviders(t *testing.T) {
	for _, testCase := range []struct {
		provider  string
		wantSunk  bool
		wantRefus string
	}{
		{provider: "github", wantSunk: true},
		{provider: "jira", wantSunk: true},
		{provider: "linear", wantSunk: true},
		{provider: "gitlab", wantRefus: "unsupported_kind_for_system"},
	} {
		t.Run(testCase.provider, func(t *testing.T) {
			pointer := externalTestPointer()
			pointer.SourceSystem = testCase.provider
			if testCase.provider == "jira" || testCase.provider == "linear" {
				pointer.SourceInstance = testCase.provider + "-instance"
			}
			overrides := map[string]any{}
			if testCase.provider != "github" {
				overrides["toProjectId"] = "PLATFORM"
			}
			payload := externalTestPayload(t, pointer, "legacy", []map[string]any{{
				"kind": "project_membership_transition.v1", "externalId": "transition-1",
				"payload": externalProjectMembershipPayload(testCase.provider, overrides),
			}})
			repository := &externalRepositoryFake{
				allowed: true,
				batch: externalBatch{
					Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
					ItemsReceived: 1, Payload: payload,
				},
			}
			sink := &externalSinkFake{}
			handler, err := NewExternalIngestHandler(repository, sink, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			handler.backoff = nil
			if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err != nil {
				t.Fatal(err)
			}
			completion := repository.completions[0]
			if testCase.wantSunk {
				if completion.Accepted != 1 || completion.Rejected != 0 {
					t.Fatalf("%s: %#v", testCase.provider, completion)
				}
				if len(sink.calls) != 1 || len(sink.calls[0].Records) != 1 ||
					sink.calls[0].Records[0].Kind != "project_membership_transition.v1" {
					t.Fatalf("%s record never reached the sink: %#v", testCase.provider, sink.calls)
				}
				return
			}
			if completion.Accepted != 0 || completion.Rejected != 1 {
				t.Fatalf("gitlab was admitted: %#v", completion)
			}
			if completion.Rejections[0].Code != testCase.wantRefus {
				t.Fatalf("gitlab refusal code = %q, want %q -- a silently missing producer is not the guard",
					completion.Rejections[0].Code, testCase.wantRefus)
			}
			if len(sink.calls) != 0 {
				t.Fatalf("a gitlab membership row reached the sink: %#v", sink.calls)
			}
		})
	}
}

// TestExternalProjectMembershipRequiresAPositiveSubjectDeclaration is the
// anti-fall-through property, carried over from the work-item-only build and
// re-aimed at the field that now decides the derivation.
//
// The earlier build refused pull requests by rejecting the VALUE "pr" on
// workItemType, and a PR payload need only OMIT the field to fall through to
// the issue-shaped id derivation and be accepted (codex adversarial review,
// round 1). subjectKind is required with a closed enum for exactly that
// reason: the sink branches its entire identity derivation on it, so an
// undeclared subject cannot be allowed to mean "whatever the default branch
// does".
func TestExternalProjectMembershipRequiresAPositiveSubjectDeclaration(t *testing.T) {
	t.Run("omitted", func(t *testing.T) {
		payload := externalProjectMembershipPayload("github", nil)
		delete(payload, "subjectKind")
		err := validateExternalRecord("project_membership_transition.v1", payload)
		if err == nil {
			t.Fatal("an undeclared subject kind was accepted; the derivation would silently pick a branch")
		}
		if !strings.Contains(err.Error(), "subjectKind") {
			t.Fatalf("refusal does not name the offending field: %v", err)
		}
	})
	for _, value := range []string{"pr", "merge_request", "issue", ""} {
		t.Run("rejects_"+value, func(t *testing.T) {
			err := validateExternalRecord("project_membership_transition.v1",
				externalProjectMembershipPayload("github", map[string]any{"subjectKind": value}))
			if err == nil {
				t.Fatalf("subjectKind %q was accepted", value)
			}
			if !strings.Contains(err.Error(), "subjectKind") {
				t.Fatalf("refusal does not name the offending field: %v", err)
			}
		})
	}
	// A work_item subject still has to declare workItemType positively: the id
	// derivation branches on it too (externalWorkItemID's "pr" case), so
	// omitting it would reintroduce the same fall-through one level down.
	t.Run("work_item_without_type", func(t *testing.T) {
		payload := externalProjectMembershipPayload("github", nil)
		delete(payload, "workItemType")
		code, message := refuseProjectMembershipContradiction(externalTestPointer(), payload, map[string]string{})
		if code != externalRefusalInvalidField || !strings.Contains(message, "workItemType") {
			t.Fatalf("code=%q message=%q, want a workItemType refusal", code, message)
		}
	})
}

// TestExternalProjectMembershipRequiresAProviderEventTime pins the deviation
// from CHAOS-4194's provisional "occurred_at falls back to last_synced"
// default, which Context Fabric then ratified as ruling A on 2026-08-24.
// occurred_at is a member of the sorting key, so a sink-supplied timestamp
// differs on every re-sync of the same provider event: the keys differ, FINAL
// keeps both, and the table accumulates one row per sync of a single
// reassignment -- exactly the duplication event_id is in the key to prevent.
// The sink cannot invent a stable value; only the producer can.
func TestExternalProjectMembershipRequiresAProviderEventTime(t *testing.T) {
	payload := externalProjectMembershipPayload("github", nil)
	delete(payload, "occurredAt")
	err := validateExternalRecord("project_membership_transition.v1", payload)
	if err == nil {
		t.Fatal("a transition with no provider event time was accepted; it cannot dedupe on re-sync")
	}
	if !strings.Contains(err.Error(), "occurredAt") {
		t.Fatalf("refusal does not name the offending field: %v", err)
	}
}

// TestExternalProjectMembershipSubjectIDMatchesItsWorkItemID is the join
// guarantee for the work_item arm. The presence projection resolves these rows
// against `work_items`, so the two derivations must agree. They only do if this
// kind consults the record's own repositoryExternalId the way work_item.v1
// does: with a batch pointed at an org while the records name org/repo,
// deriving from the pointer alone yields `gh:full-chaos#7` against the work
// item's `gh:full-chaos/dev-health#7` -- a well-formed row that joins to
// nothing.
func TestExternalProjectMembershipSubjectIDMatchesItsWorkItemID(t *testing.T) {
	pointer := externalTestPointer()
	pointer.SourceInstance = "full-chaos"
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	build := func(kind string, payload map[string]any) []any {
		t.Helper()
		batch := &productBatch{}
		connection := &productSink{batches: []*productBatch{batch}}
		sink, err := NewClickHouseExternalBatchSink(connection)
		if err != nil {
			t.Fatal(err)
		}
		sink.now = func() time.Time { return now }
		if _, err := sink.Write(context.Background(), externalSinkBatch{
			Pointer: pointer, SourceID: uuid.New(),
			Records: []externalSinkRecord{externalSinkFixture(kind, payload)},
		}); err != nil {
			t.Fatal(err)
		}
		return batch.rows[0]
	}
	workItemRow := build("work_item.v1", map[string]any{
		"externalKey": "7", "provider": "github", "title": "Issue", "type": "issue",
		"status": "todo", "createdAt": "2026-07-20T10:00:00Z",
		"repositoryExternalId": "full-chaos/dev-health",
	})
	membershipRow := build("project_membership_transition.v1",
		externalProjectMembershipPayload("github", map[string]any{
			"repositoryExternalId": "full-chaos/dev-health",
		}))
	// work_item.v1 column 1 is work_item_id; the membership row's subject_id is
	// column 4 (org_id, source_id, repo_id, subject_kind, subject_id).
	if workItemRow[1] != membershipRow[4] {
		t.Fatalf("subject id mismatch: work_item.v1=%v membership=%v", workItemRow[1], membershipRow[4])
	}
	if membershipRow[4] != "gh:full-chaos/dev-health#7" {
		t.Fatalf("derived subject_id = %v", membershipRow[4])
	}
	// repo_id must agree too, or the row is scoped to a repository the work
	// item does not belong to -- and repo_id is now a sorting-key member, so a
	// disagreement is not merely a wrong attribute but a different row.
	if workItemRow[0] != membershipRow[2] {
		t.Fatalf("repo_id mismatch: work_item.v1=%v membership=%v", workItemRow[0], membershipRow[2])
	}
}

// TestExternalProjectMembershipDerivesPullRequestSubjects is the other subject
// arm, and the half CHAOS-4194 was filed for: PRs had no project mapping
// anywhere in the graph.
//
// A PR is keyed (repo_id, number) -- there is no work_item_id for one, and
// minting an issue-shaped id would point the row at a `work_items` row that
// does not exist. So the assertions that matter are that repo_id equals the one
// pull_request.v1 derives for the same repository, and that subject_id is the
// number verbatim rather than anything prefixed or hashed.
func TestExternalProjectMembershipDerivesPullRequestSubjects(t *testing.T) {
	pointer := externalTestPointer()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	build := func(kind string, payload map[string]any) []any {
		t.Helper()
		batch := &productBatch{}
		connection := &productSink{batches: []*productBatch{batch}}
		sink, err := NewClickHouseExternalBatchSink(connection)
		if err != nil {
			t.Fatal(err)
		}
		sink.now = func() time.Time { return now }
		if _, err := sink.Write(context.Background(), externalSinkBatch{
			Pointer: pointer, SourceID: uuid.New(),
			Records: []externalSinkRecord{externalSinkFixture(kind, payload)},
		}); err != nil {
			t.Fatal(err)
		}
		return batch.rows[0]
	}
	pullRequestRow := build("pull_request.v1", map[string]any{
		"repositoryExternalId": pointer.SourceInstance, "number": 42,
		"state": "open", "createdAt": "2026-07-20T10:00:00Z",
	})
	membershipRow := build("project_membership_transition.v1",
		externalPullRequestMembershipPayload(map[string]any{
			"repositoryExternalId": pointer.SourceInstance,
		}))
	if membershipRow[3] != "pull_request" {
		t.Fatalf("subject_kind = %v, want pull_request", membershipRow[3])
	}
	if membershipRow[4] != "42" {
		t.Fatalf("subject_id = %v, want the PR number as a decimal string", membershipRow[4])
	}
	// pull_request.v1 column 0 is repo_id; the membership row's is column 2.
	if pullRequestRow[0] != membershipRow[2] {
		t.Fatalf("repo_id mismatch: pull_request.v1=%v membership=%v", pullRequestRow[0], membershipRow[2])
	}
	if membershipRow[2] == uuid.Nil {
		t.Fatal("a pull_request subject wrote uuid.Nil; the number alone identifies nothing")
	}
}

// TestExternalProjectMembershipRefusesUnidentifiablePullRequestSubjects covers
// the two ways a pull_request row can look well-formed and key to nothing: no
// repository (the number alone identifies no PR anywhere) and a non-decimal
// externalKey (a value git_pull_requests.number can never equal, so the row
// would join to no PR while looking like it named one). Both are checked
// against the batch as a whole, which is why the schema table cannot see them.
func TestExternalProjectMembershipRefusesUnidentifiablePullRequestSubjects(t *testing.T) {
	pointer := externalTestPointer()
	for name, payload := range map[string]map[string]any{
		"no_repository": func() map[string]any {
			payload := externalPullRequestMembershipPayload(nil)
			delete(payload, "repositoryExternalId")
			return payload
		}(),
		"node_id_instead_of_number": externalPullRequestMembershipPayload(
			map[string]any{"externalKey": "PR_kwDOABCD"}),
		"declares_work_item_type": externalPullRequestMembershipPayload(
			map[string]any{"workItemType": "issue"}),
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateExternalRecord("project_membership_transition.v1", payload); err != nil {
				t.Fatalf("payload should pass the field table and fail the whole-record check: %v", err)
			}
			code, message := refuseProjectMembershipContradiction(pointer, payload, map[string]string{})
			if code != externalRefusalInvalidField {
				t.Fatalf("code = %q, want %q (message %q)", code, externalRefusalInvalidField, message)
			}
		})
	}
}

// TestExternalProjectMembershipRefusesAProviderTheBatchDidNotComeFrom covers
// the contradiction the schema enum cannot see, because it validates the field
// in isolation while the conflict only exists relative to the batch pointer.
// Project ids are provider-scoped, so filing a jira project id under a github
// batch produces a row that resolves against the wrong catalogue.
func TestExternalProjectMembershipRefusesAProviderTheBatchDidNotComeFrom(t *testing.T) {
	pointer := externalTestPointer()
	payload := externalTestPayload(t, pointer, "legacy", []map[string]any{{
		"kind": "project_membership_transition.v1", "externalId": "evt-1",
		"payload": externalProjectMembershipPayload("jira", nil),
	}})
	repository := &externalRepositoryFake{
		allowed: true,
		batch: externalBatch{
			Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
			ItemsReceived: 1, Payload: payload,
		},
	}
	sink := &externalSinkFake{}
	handler, err := NewExternalIngestHandler(repository, sink, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.backoff = nil
	if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err != nil {
		t.Fatal(err)
	}
	completion := repository.completions[0]
	if completion.Accepted != 0 || completion.Rejected != 1 {
		t.Fatalf("a cross-provider transition was accepted: %#v", completion)
	}
	if completion.Rejections[0].Code != "invalid_field" ||
		!strings.Contains(completion.Rejections[0].Message, "source system") {
		t.Fatalf("refusal is not attributable: %#v", completion.Rejections[0])
	}
	if len(sink.calls) != 0 {
		t.Fatalf("the refused record reached the sink: %#v", sink.calls)
	}
}

// TestExternalProjectMembershipRefusesRepoAsProjectValues is the vocabulary
// constraint at the write boundary.
//
// There are THREE things called "project" in this schema, and only one of them
// belongs here. The external-ingest path writes the REPOSITORY full name into
// work_items.project_id for github and gitlab (externalProjectScope), while the
// Projects V2 route mints the real entity `ghprojv2:<org>#<n>`. A membership
// row carrying the repo-as-project value is not a near-miss: it names a
// `projects` row that does not and will never exist, so the edge resolves to
// nothing while looking entirely well-formed. It is refused with its own
// bounded reason rather than folded into invalid_field, because "a producer is
// emitting the wrong vocabulary" and "a producer sent a malformed field" call
// for different responses.
func TestExternalProjectMembershipRefusesRepoAsProjectValues(t *testing.T) {
	pointer := externalTestPointer()
	for _, field := range []string{"toProjectId", "fromProjectId"} {
		t.Run(field, func(t *testing.T) {
			payload := externalProjectMembershipPayload("github",
				map[string]any{field: "full-chaos/dev-health"})
			if err := validateExternalRecord("project_membership_transition.v1", payload); err != nil {
				t.Fatalf("the field table should accept the string; the vocabulary check owns this: %v", err)
			}
			code, message := refuseProjectMembershipContradiction(pointer, payload, map[string]string{})
			if code != externalRefusalUnresolvableProject {
				t.Fatalf("code = %q, want %q", code, externalRefusalUnresolvableProject)
			}
			if !strings.Contains(message, field) {
				t.Fatalf("refusal does not name the offending field: %q", message)
			}
		})
	}
	// jira and linear carry the provider's own project key in that column and
	// have no second meaning to exclude, so no prefix applies to them. A fence
	// that refused their values would be a fence around nothing.
	for _, provider := range []string{"jira", "linear"} {
		t.Run(provider+"_has_no_prefix_rule", func(t *testing.T) {
			providerPointer := externalTestPointer()
			providerPointer.SourceSystem = provider
			payload := externalProjectMembershipPayload(provider,
				map[string]any{"toProjectId": "PLATFORM"})
			if code, message := refuseProjectMembershipContradiction(
				providerPointer, payload, map[string]string{}); code != "" {
				t.Fatalf("%s project id refused: %s / %s", provider, code, message)
			}
		})
	}
}

// TestExternalProjectMembershipRequiresAnEventToNameAProject pins the shape
// rules Context Fabric ruled on 2026-08-24, once presence became keyed per
// (subject, project).
//
// A row names the project JOINED in to_project_id and the project LEFT in
// from_project_id: an add is ("", P), a removal is (P, ""), a move is (P, Q).
//
// ("", "") is now REFUSED, and this reverses an earlier build. Under the
// previous one-project-per-subject keying it was the sentinel for "removed from
// every project"; per-project, it names nothing, so it could not retire or
// create any membership and would sit in the history looking like a removal
// that silently did nothing. A removal has to say WHICH membership it ends.
func TestExternalProjectMembershipRequiresAnEventToNameAProject(t *testing.T) {
	pointer := externalTestPointer()
	t.Run("names_neither_side", func(t *testing.T) {
		payload := externalProjectMembershipPayload("github", map[string]any{
			"fromProjectId": "", "fromProjectKey": "", "toProjectId": "", "toProjectKey": "",
		})
		if err := validateExternalRecord("project_membership_transition.v1", payload); err != nil {
			t.Fatalf("the field table should accept it; the whole-record check owns this: %v", err)
		}
		code, message := refuseProjectMembershipContradiction(pointer, payload, map[string]string{})
		if code != externalRefusalNamelessMembership {
			t.Fatalf("code = %q, want %q", code, externalRefusalNamelessMembership)
		}
		if !strings.Contains(message, "joined") || !strings.Contains(message, "left") {
			t.Fatalf("refusal does not say what is missing: %q", message)
		}
	})
	// A removal that NAMES the board left is the correct shape and must be
	// accepted -- it is how every membership is ever retired.
	t.Run("removal_naming_the_board_left", func(t *testing.T) {
		payload := externalProjectMembershipPayload("github", map[string]any{
			"fromProjectId": "ghprojv2:full-chaos#4", "fromProjectKey": "PLATFORM",
			"toProjectId": "", "toProjectKey": "",
		})
		if code, message := refuseProjectMembershipContradiction(
			pointer, payload, map[string]string{}); code != "" {
			t.Fatalf("a removal naming its board was refused: %s / %s", code, message)
		}
	})
	// A move carries both sides in ONE row, which is what lets the view retire
	// one membership and create another from a single observed event.
	t.Run("move_carries_both_sides", func(t *testing.T) {
		payload := externalProjectMembershipPayload("github", map[string]any{
			"fromProjectId": "ghprojv2:full-chaos#4", "fromProjectKey": "PLATFORM",
			"toProjectId": "ghprojv2:full-chaos#9", "toProjectKey": "RUNTIME",
		})
		if code, message := refuseProjectMembershipContradiction(
			pointer, payload, map[string]string{}); code != "" {
			t.Fatalf("a move was refused: %s / %s", code, message)
		}
	})
	t.Run("key_without_id_is_refused", func(t *testing.T) {
		payload := externalProjectMembershipPayload("github",
			map[string]any{"toProjectId": "", "toProjectKey": "PLATFORM"})
		code, message := refuseProjectMembershipContradiction(pointer, payload, map[string]string{})
		if code != externalRefusalInvalidField {
			t.Fatalf("a key-only destination was accepted (code %q)", code)
		}
		if !strings.Contains(message, "project id") {
			t.Fatalf("refusal does not say what is wrong: %q", message)
		}
	})
	// The mirror case is NORMAL and must stay accepted. GitHub Projects V2
	// boards have a number and a title and no key concept at all, so refusing
	// an id without a key would refuse every github membership row.
	t.Run("id_without_key_is_normal", func(t *testing.T) {
		payload := externalProjectMembershipPayload("github",
			map[string]any{"toProjectId": "ghprojv2:full-chaos#4", "toProjectKey": ""})
		if code, message := refuseProjectMembershipContradiction(
			pointer, payload, map[string]string{}); code != "" {
			t.Fatalf("a keyless github destination was refused: %s / %s", code, message)
		}
	})
}

// TestExternalProjectMembershipRefusesContradictoryEventIDs takes the option
// Context Fabric left open ("the sink MAY refuse a same-event_id/different-
// content row with a counter") because within one batch it costs a map lookup.
//
// event_id is the idempotency member of the sorting key, so two records
// asserting the same event with different destinations leave ReplacingMergeTree
// to pick arbitrarily -- there is no right answer, and inventing a tiebreak
// would hide a producer defect. An identical repeat is NOT refused: that is an
// idempotent replay, and the engine collapses it correctly.
func TestExternalProjectMembershipRefusesContradictoryEventIDs(t *testing.T) {
	pointer := externalTestPointer()
	seen := map[string]string{}
	first := externalProjectMembershipPayload("github", nil)
	if code, _ := refuseProjectMembershipContradiction(pointer, first, seen); code != "" {
		t.Fatalf("the first record was refused: %s", code)
	}
	t.Run("identical_replay_is_fine", func(t *testing.T) {
		replay := externalProjectMembershipPayload("github", nil)
		if code, message := refuseProjectMembershipContradiction(pointer, replay, seen); code != "" {
			t.Fatalf("an idempotent replay was refused: %s / %s", code, message)
		}
	})
	t.Run("different_destination_is_refused", func(t *testing.T) {
		contradiction := externalProjectMembershipPayload("github",
			map[string]any{"toProjectId": "ghprojv2:full-chaos#9", "toProjectKey": "RUNTIME"})
		code, message := refuseProjectMembershipContradiction(pointer, contradiction, seen)
		if code != externalRefusalContradictoryEvent {
			t.Fatalf("code = %q, want %q", code, externalRefusalContradictoryEvent)
		}
		if !strings.Contains(message, "event_id") {
			t.Fatalf("refusal does not name event_id: %q", message)
		}
	})
}

// TestExternalProjectMembershipRefusesBlankIdempotencyMembers proves the
// required-string rule rejects whitespace, not merely absence. An empty
// event_id is worse than a missing one: it validates as "present", lands in the
// sorting key, and silently merges every timeless reassignment of a subject
// into one row.
//
// toProjectId is deliberately NOT in this list any more. It became the
// unassignment sentinel, so blank is now meaningful there -- see
// TestExternalProjectMembershipAcceptsTheUnassignmentSentinel, which owns the
// blank case for that field.
func TestExternalProjectMembershipRefusesBlankIdempotencyMembers(t *testing.T) {
	for _, field := range []string{"eventId", "externalKey"} {
		t.Run(field, func(t *testing.T) {
			for _, blank := range []string{"", "   "} {
				payload := externalProjectMembershipPayload("github", map[string]any{field: blank})
				if err := validateExternalRecord("project_membership_transition.v1", payload); err == nil {
					t.Fatalf("%s = %q was accepted", field, blank)
				}
			}
		})
	}
}

// TestExternalProjectMembershipRequiresIdempotencyMembers pins the payload
// fields that are load-bearing for dedupe and identity. `eventId` is a member
// of the sorting key, so an empty one collapses genuinely distinct
// reassignments that share a timestamp into a single ReplacingMergeTree row.
// `externalKey` and `provider` decide which subject and which catalogue the row
// names.
func TestExternalProjectMembershipRequiresIdempotencyMembers(t *testing.T) {
	for _, field := range []string{"eventId", "provider", "externalKey", "subjectKind"} {
		t.Run(field, func(t *testing.T) {
			payload := externalProjectMembershipPayload("github", nil)
			delete(payload, field)
			err := validateExternalRecord("project_membership_transition.v1", payload)
			if err == nil {
				t.Fatalf("%s is not required", field)
			}
			// Without this the whole subtest passes vacuously before the kind
			// is registered: every payload is refused as an unknown kind, so
			// "required" would be indistinguishable from "unrecognised".
			if !strings.Contains(err.Error(), field) {
				t.Fatalf("refusal does not name the missing field %s: %v", field, err)
			}
		})
	}
}

// TestClickHouseExternalSinkWritesProjectMembershipColumns pins the row the
// sink actually appends against the final locked column order. The insert
// statement and the value slice are written in two different files; nothing but
// a test compares them, and a silent reorder would land every project id in the
// project key column.
func TestClickHouseExternalSinkWritesProjectMembershipColumns(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	sourceID := uuid.MustParse("9749bda0-fc9f-4076-b19d-7b26c4f306ff")
	pointer := externalTestPointer()
	batch := &productBatch{}
	connection := &productSink{batches: []*productBatch{batch}}
	sink, err := NewClickHouseExternalBatchSink(connection)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return now }
	if _, err := sink.Write(context.Background(), externalSinkBatch{
		Pointer: pointer, SourceID: sourceID,
		Records: []externalSinkRecord{externalSinkFixture(
			"project_membership_transition.v1",
			externalProjectMembershipPayload("github", map[string]any{"fromProjectId": "ghprojv2:full-chaos#1"}),
		)},
	}); err != nil {
		t.Fatal(err)
	}
	if len(connection.queries) != 1 {
		t.Fatalf("prepared queries = %#v", connection.queries)
	}
	query := connection.queries[0]
	const wantColumns = "(org_id,source_id,repo_id,subject_kind,subject_id,provider,from_project_id,to_project_id," +
		"from_project_key,to_project_key,actor,occurred_at,last_synced,event_id)"
	if !strings.Contains(query, "INSERT INTO project_membership_transitions "+wantColumns) {
		t.Fatalf("insert does not match the locked column order: %s", query)
	}
	if !batch.sent || len(batch.rows) != 1 {
		t.Fatalf("project membership row not durable: %#v", batch)
	}
	row := batch.rows[0]
	columns := strings.Split(strings.TrimSuffix(strings.TrimPrefix(wantColumns, "("), ")"), ",")
	if len(row) != len(columns) {
		t.Fatalf("row width %d does not match %d locked columns", len(row), len(columns))
	}
	want := map[string]any{
		"org_id": pointer.OrgID,
		// A POINTER, not a value: source_id is the schema's only Nullable
		// column, so the shared row types it as one. Asserting the pointed-to
		// value below rather than the pointer itself, since the address is not
		// stable.
		"source_id":        "checked separately",
		"repo_id":          uuid.MustParse("00b02aea-81bc-1244-b364-f93a0276ede5"),
		"subject_kind":     "work_item",
		"subject_id":       "gh:full-chaos/dev-health#7",
		"provider":         "github",
		"from_project_id":  "ghprojv2:full-chaos#1",
		"to_project_id":    "ghprojv2:full-chaos#4",
		"from_project_key": "",
		"to_project_key":   "PLATFORM",
		// actor is a bare String, not Nullable: the Context Fabric correction
		// of 2026-08-24 mirrors work_item_transitions column-for-column, and an
		// `any` holding a *string here would mean the column was declared
		// Nullable against the ruling.
		"actor":       "github:ada",
		"occurred_at": time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC),
		"last_synced": now,
		"event_id":    "evt-1",
	}
	for index, column := range columns {
		if column == "source_id" {
			stored, ok := row[index].(*uuid.UUID)
			if !ok || stored == nil || *stored != sourceID {
				t.Errorf("source_id = %#v, want a pointer to %s", row[index], sourceID)
			}
			continue
		}
		if row[index] != want[column] {
			t.Errorf("%s = %#v, want %#v", column, row[index], want[column])
		}
	}
}

// TestExternalProjectMembershipTracksItsKindAndTimeInTheRecomputeScope pins the
// two things the sink must hand downstream: the kind, so the recompute
// controller knows what changed, and the event time, so the recomputed window
// covers the reassignment rather than only the sync.
//
// This replaces an earlier test asserting occurred_at fell back to last_synced
// when the provider carried no event time. That fallback was removed: it put a
// per-sync value into the sorting key and defeated dedupe on re-sync (codex
// adversarial review, round 1). See
// TestExternalProjectMembershipRequiresAProviderEventTime.
func TestExternalProjectMembershipTracksItsKindAndTimeInTheRecomputeScope(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	occurred := time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC)
	pointer := externalTestPointer()
	batch := &productBatch{}
	connection := &productSink{batches: []*productBatch{batch}}
	sink, err := NewClickHouseExternalBatchSink(connection)
	if err != nil {
		t.Fatal(err)
	}
	sink.now = func() time.Time { return now }
	scope, err := sink.Write(context.Background(), externalSinkBatch{
		Pointer: pointer, SourceID: uuid.New(),
		Records: []externalSinkRecord{externalSinkFixture(
			"project_membership_transition.v1", externalProjectMembershipPayload("github", nil))},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(scope.RecordKinds, "project_membership_transition.v1") {
		t.Fatalf("recompute scope omits the kind: %#v", scope.RecordKinds)
	}
	if batch.rows[0][11] != occurred {
		t.Fatalf("occurred_at = %#v, want the provider event time %#v", batch.rows[0][11], occurred)
	}
	if batch.rows[0][12] != now {
		t.Fatalf("last_synced = %#v, want the sink observation %#v", batch.rows[0][12], now)
	}
	if scope.WindowStart == nil || !scope.WindowStart.Equal(occurred) {
		t.Fatalf("recompute window does not cover the reassignment: %#v", scope.WindowStart)
	}
}

type externalObserverFake struct {
	sunk     map[string]int
	refusals []string
}

func (f *externalObserverFake) ObserveExternalProjectMembershipsSunk(provider string, rows int) error {
	if f.sunk == nil {
		f.sunk = map[string]int{}
	}
	f.sunk[provider] += rows
	return nil
}

func (f *externalObserverFake) ObserveExternalKindRefused(sourceSystem, reason string) error {
	f.refusals = append(f.refusals, sourceSystem+":"+reason)
	return nil
}

// TestExternalIngestReportsSunkMembershipsAndRefusalsTogether covers the
// standing telemetry order for this change. The batch deliberately mixes two
// durable rows -- one per subject kind -- with a refused record: reporting
// either side alone is what makes the counters unreadable, since a flat sunk
// counter cannot be told from a registry refusing everything.
func TestExternalIngestReportsSunkMembershipsAndRefusalsTogether(t *testing.T) {
	pointer := externalTestPointer()
	payload := externalTestPayload(t, pointer, "legacy", []map[string]any{
		{
			"kind": "project_membership_transition.v1", "externalId": "evt-1",
			"payload": externalProjectMembershipPayload("github", nil),
		},
		{
			"kind": "project_membership_transition.v1", "externalId": "evt-2",
			"payload": externalPullRequestMembershipPayload(map[string]any{
				"eventId": "evt-2", "repositoryExternalId": pointer.SourceInstance,
			}),
		},
		{
			"kind": "project_membership_transition.v1", "externalId": "evt-3",
			"payload": externalProjectMembershipPayload("github", map[string]any{
				"eventId": "evt-3", "toProjectId": "full-chaos/dev-health",
			}),
		},
	})
	repository := &externalRepositoryFake{
		allowed: true,
		batch: externalBatch{
			Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
			ItemsReceived: 3, Payload: payload,
		},
	}
	observer := &externalObserverFake{}
	handler, err := NewExternalIngestHandler(repository, &externalSinkFake{}, nil, observer)
	if err != nil {
		t.Fatal(err)
	}
	handler.backoff = nil
	if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err != nil {
		t.Fatal(err)
	}
	if observer.sunk["github"] != 2 {
		t.Errorf("sunk memberships = %#v, want 2 for github", observer.sunk)
	}
	if len(observer.refusals) != 1 || observer.refusals[0] != "github:unresolvable_project_entity" {
		t.Errorf("refusals = %#v, want one github:unresolvable_project_entity", observer.refusals)
	}
}

// TestExternalIngestReportsNothingWhenTheSinkNeverCommitted is the discipline
// the zero-unit finalization counter learned the hard way: a counter bumped
// before the write is durable overcounts every retry of a batch that
// eventually succeeds once. Here the sink never succeeds at all, so a handler
// that counted at classification time would report project memberships that do
// not exist in ClickHouse.
func TestExternalIngestReportsNothingWhenTheSinkNeverCommitted(t *testing.T) {
	pointer := externalTestPointer()
	payload := externalTestPayload(t, pointer, "legacy", []map[string]any{{
		"kind": "project_membership_transition.v1", "externalId": "evt-1",
		"payload": externalProjectMembershipPayload("github", nil),
	}})
	repository := &externalRepositoryFake{
		allowed: true,
		batch: externalBatch{
			Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
			ItemsReceived: 1, Payload: payload,
		},
	}
	sink := &externalSinkFake{errors: []error{errors.New("clickhouse unavailable")}}
	observer := &externalObserverFake{}
	handler, err := NewExternalIngestHandler(repository, sink, nil, observer)
	if err != nil {
		t.Fatal(err)
	}
	handler.backoff = nil
	if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err == nil {
		t.Fatal("a failed sink write was reported as a successful batch")
	}
	if len(observer.sunk) != 0 || len(observer.refusals) != 0 {
		t.Fatalf("telemetry recorded an uncommitted batch: sunk=%#v refusals=%#v", observer.sunk, observer.refusals)
	}
}

// TestExternalIngestHandlesABatchWithoutAnObserver kills the nil guard in
// observeOutcome. The stream-runner profiles that expose no Prometheus surface
// pass nil rather than a no-op double, so nil is a real production shape and
// not a test-only convenience.
func TestExternalIngestHandlesABatchWithoutAnObserver(t *testing.T) {
	pointer := externalTestPointer()
	payload := externalTestPayload(t, pointer, "legacy", []map[string]any{{
		"kind": "project_membership_transition.v1", "externalId": "evt-1",
		"payload": externalProjectMembershipPayload("github", nil),
	}})
	repository := &externalRepositoryFake{
		allowed: true,
		batch: externalBatch{
			Pointer: pointer, SourceID: uuid.New(), EntityFamily: "legacy",
			ItemsReceived: 1, Payload: payload,
		},
	}
	sink := &externalSinkFake{}
	handler, err := NewExternalIngestHandler(repository, sink, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.backoff = nil
	if err := handler.Handle(context.Background(), externalTestMessage(pointer)); err != nil {
		t.Fatalf("a nil observer broke ingest: %v", err)
	}
	if len(sink.calls) != 1 || len(sink.calls[0].Records) != 1 {
		t.Fatalf("record did not reach the sink without an observer: %#v", sink.calls)
	}
}

// TestExternalProjectMembershipRequiresARepositoryForGithubWorkItems closes the
// last way a work_item row could be well-formed and join to nothing (codex
// adversarial review, round 1, finding 1).
//
// `repositoryExternalId` was optional, and externalWorkItemInstance falls back
// to the BATCH POINTER when it is absent. For an org-scoped github batch that
// fallback is silently wrong: a record naming work item 7 with no repository
// derives `gh:acme#7`, while the work item it means -- written by work_item.v1
// from its own `repositoryExternalId` -- is `gh:acme/api#7`. The membership row
// then joins nothing, AND it suppresses the presence view's column arm for that
// subject, because the anti-join matches on the id nobody else uses. Two wrong
// answers from one omitted field.
//
// The earlier build fixed the DERIVATION (consult the record's own repository
// rather than the pointer) but left the field optional, so the fallback was
// still reachable by simply not sending it. Requiring it is the other half.
//
// Required for github only, because that is where the ambiguity exists: jira
// and linear are repo-less providers whose subject ids carry no instance at all
// (`jira:KEY`, `linear:KEY`), so no fallback can change what they derive.
func TestExternalProjectMembershipRequiresARepositoryForGithubWorkItems(t *testing.T) {
	pointer := externalTestPointer()
	pointer.SourceInstance = "acme"
	payload := externalProjectMembershipPayload("github", nil)
	delete(payload, "repositoryExternalId")
	if err := validateExternalRecord("project_membership_transition.v1", payload); err != nil {
		t.Fatalf("the field table should accept the payload; the whole-record check owns this: %v", err)
	}
	code, message := refuseProjectMembershipContradiction(pointer, payload, map[string]string{})
	if code != externalRefusalInvalidField {
		t.Fatalf("a github work_item with no repository was accepted (code %q); it derives gh:acme#7 and joins nothing", code)
	}
	if !strings.Contains(message, "repositoryExternalId") {
		t.Fatalf("refusal does not name the offending field: %q", message)
	}
	// jira and linear must NOT be caught by this: their subject ids carry no
	// instance, so there is no fallback to be wrong about, and requiring a
	// repository from a repo-less provider would refuse every valid row.
	for _, provider := range []string{"jira", "linear"} {
		t.Run(provider+"_needs_no_repository", func(t *testing.T) {
			providerPointer := externalTestPointer()
			providerPointer.SourceSystem = provider
			repoless := externalProjectMembershipPayload(provider, map[string]any{"toProjectId": "PLATFORM"})
			delete(repoless, "repositoryExternalId")
			if code, message := refuseProjectMembershipContradiction(
				providerPointer, repoless, map[string]string{}); code != "" {
				t.Fatalf("%s membership refused for want of a repository: %s / %s", provider, code, message)
			}
		})
	}
}
