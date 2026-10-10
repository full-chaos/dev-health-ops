package providersync

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

// The Linear provers against the answers Linear gave (testdata/provider_answers/linear).

func TestLinearTeamKeyAnswerIsClassifiedFromTheRecordedAnswers(t *testing.T) {
	notFound := loadRecordedAnswer(t, "linear", "teams_filter_key_not_found")
	found := loadRecordedAnswer(t, "linear", "teams_filter_key_found")
	cases := []struct {
		name   string
		status int
		body   string
		want   SnapshotAbsence
	}{
		{"the recorded answer for a key that does not exist", notFound.Status, notFound.verbatim(t), SnapshotAbsenceProven},
		{"a node with the key (built from the recorded shape)", found.Status, found.foundBody(t, map[string]any{"key": "OPS"}), SnapshotFactStillHeld},
		{"the key in another case still names the team", found.Status, found.foundBody(t, map[string]any{"key": "ops"}), SnapshotFactStillHeld},
		{"a node of another key proves nothing", found.Status, found.foundBody(t, map[string]any{"key": "ENG"}), SnapshotAbsenceNotProven},
		{"HTTP 500 proves nothing", http.StatusInternalServerError, notFound.verbatim(t), SnapshotAbsenceNotProven},
		{"GraphQL errors prove nothing, whatever data is next to them", notFound.Status,
			`{"errors":[{"message":"x","extensions":{"code":"RATELIMITED"}}],"data":{"teams":{"nodes":[]}}}`, SnapshotAbsenceNotProven},
		{"no node list proves nothing", notFound.Status, `{"data":{"teams":null}}`, SnapshotAbsenceNotProven},
		{"a body that is not JSON proves nothing", notFound.Status, `<html>`, SnapshotAbsenceNotProven},
	}
	for _, c := range cases {
		if got := classifyLinearTeamKeyAnswer(c.status, []byte(c.body), "OPS"); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestLinearProjectLinkAnswerIsClassifiedFromTheRecordedAnswers(t *testing.T) {
	notFound := loadRecordedAnswer(t, "linear", "project_by_id_not_found")
	found := loadRecordedAnswer(t, "linear", "project_by_id_found")
	nodes := func(keys ...string) []any {
		out := make([]any, 0, len(keys))
		for _, key := range keys {
			out = append(out, map[string]any{"key": key})
		}
		return out
	}
	many := make([]string, 0, linearDefaultPageSize)
	for index := 0; index < linearDefaultPageSize; index++ {
		many = append(many, "T"+strings.Repeat("X", index%7)+string(rune('A'+index%26)))
	}
	cases := []struct {
		name   string
		status int
		body   string
		want   SnapshotAbsence
	}{
		{"the recorded answer for a project that does not exist", notFound.Status, notFound.verbatim(t), SnapshotAbsenceProven},
		{"a project that names the team (built from the recorded shape)", found.Status,
			found.foundBody(t, map[string]any{"nodes": nodes("ENG", "OPS")}), SnapshotFactStillHeld},
		{"a project whose whole team list lacks the team: the link is gone", found.Status,
			found.foundBody(t, map[string]any{"nodes": nodes("ENG")}), SnapshotAbsenceProven},
		{"a project with no team at all: the link is gone", found.Status,
			found.foundBody(t, map[string]any{"nodes": nodes()}), SnapshotAbsenceProven},
		{"a full page that lacks the team may be the first of several", found.Status,
			found.foundBody(t, map[string]any{"nodes": nodes(many...)}), SnapshotAbsenceNotProven},
		{"another GraphQL error proves nothing", notFound.Status,
			`{"errors":[{"message":"Entity not found: Project","extensions":{"code":"FORBIDDEN"}}],"data":null}`, SnapshotAbsenceNotProven},
		{"the not-found error with the wrong entity proves nothing", notFound.Status,
			`{"errors":[{"message":"Entity not found: Team","extensions":{"code":"INPUT_ERROR"}}],"data":null}`, SnapshotAbsenceNotProven},
		{"the not-found error next to a project in the data proves nothing", notFound.Status,
			`{"errors":[{"message":"Entity not found: Project","extensions":{"code":"INPUT_ERROR"}}],"data":{"project":{"id":"p","teams":{"nodes":[]}}}}`, SnapshotAbsenceNotProven},
		{"two errors prove nothing", notFound.Status,
			`{"errors":[{"message":"Entity not found: Project","extensions":{"code":"INPUT_ERROR"}},{"message":"y","extensions":{"code":"INPUT_ERROR"}}],"data":null}`, SnapshotAbsenceNotProven},
		{"a project with no id proves nothing", found.Status, `{"data":{"project":{"id":"","teams":{"nodes":[]}}}}`, SnapshotAbsenceNotProven},
		{"HTTP 429 proves nothing", http.StatusTooManyRequests, notFound.verbatim(t), SnapshotAbsenceNotProven},
	}
	for _, c := range cases {
		if got := classifyLinearProjectLinkAnswer(c.status, []byte(c.body), "OPS"); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func linearProverClient(t *testing.T, responses ...string) (LinearAbsenceProver, *linearWorkItemsDoer) {
	t.Helper()
	doer := &linearWorkItemsDoer{responses: responses}
	return LinearAbsenceProver{Client: linearWorkItemsClient(t, doer)}, doer
}

func TestLinearAbsenceProverAsksTheQueriesThatWereRecorded(t *testing.T) {
	const org = "org-9124"
	teamKeyRow := OwnershipSnapshotRow{TeamID: "linear:OPS", Source: "native", ProjectID: ProjectID{org + ":linear:OPS"}}
	projectRow := OwnershipSnapshotRow{TeamID: "linear:OPS", Source: "native", ProjectID: ProjectID{"7e3f9a42-0000-4000-8000-000000000001"}}
	teamNotFound := loadRecordedAnswer(t, "linear", "teams_filter_key_not_found").verbatim(t)
	projectNotFound := loadRecordedAnswer(t, "linear", "project_by_id_not_found").verbatim(t)

	prover, doer := linearProverClient(t)
	doer.lookup = func(string) string { return teamNotFound }
	if got := prover.OwnershipAbsence(org)(context.Background(), teamKeyRow); got != SnapshotAbsenceProven {
		t.Fatalf("team-key row: %v, want proven", got)
	}
	body := requestBody(t, doer.requests[0])
	if !strings.Contains(body, `teams(filter: {key: {eq: \"OPS\"}})`) {
		t.Errorf("the team-key question is not the recorded one: %s", body)
	}

	prover, doer = linearProverClient(t)
	doer.lookup = func(string) string { return projectNotFound }
	if got := prover.OwnershipAbsence(org)(context.Background(), projectRow); got != SnapshotAbsenceProven {
		t.Fatalf("project row: %v, want proven", got)
	}
	body = requestBody(t, doer.requests[0])
	if !strings.Contains(body, `project(id: \"7e3f9a42-0000-4000-8000-000000000001\")`) || !strings.Contains(body, "teams { nodes { key } }") {
		t.Errorf("the project question is not the recorded one: %s", body)
	}

	// A failed read, a row that is not of the kind and a value that is not an
	// identifier send nothing or prove nothing.
	prover, doer = linearProverClient(t)
	doer.lookup = func(string) string { return "<html>bad gateway</html>" }
	if got := prover.OwnershipAbsence(org)(context.Background(), teamKeyRow); got != SnapshotAbsenceNotProven {
		t.Errorf("an answer that is not JSON: %v, want not proven", got)
	}
	prover, doer = linearProverClient(t)
	injected := OwnershipSnapshotRow{TeamID: "linear:OPS", Source: "native", ProjectID: ProjectID{`x") { id } } #`}}
	if got := prover.OwnershipAbsence(org)(context.Background(), injected); got != SnapshotAbsenceNotProven || len(doer.requests) != 0 {
		t.Errorf("a project id that is no identifier: %v with %d request(s), want not proven and none", got, len(doer.requests))
	}
	injectedKey := OwnershipSnapshotRow{TeamID: "linear:OPS", Source: "native", ProjectID: ProjectID{org + `:linear:OPS") { id } } #`}}
	if got := prover.OwnershipAbsence(org)(context.Background(), injectedKey); got != SnapshotAbsenceNotProven || len(doer.requests) != 0 {
		t.Errorf("a team key that is no identifier: %v with %d request(s), want not proven and none", got, len(doer.requests))
	}
	if got := prover.TeamAbsence(context.Background(), "gh:ops"); got != SnapshotAbsenceNotProven || len(doer.requests) != 0 {
		t.Errorf("a team id of another provider: %v with %d request(s), want not proven and none", got, len(doer.requests))
	}
	if got := (LinearAbsenceProver{}).TeamAbsence(context.Background(), "linear:OPS"); got != SnapshotAbsenceNotProven {
		t.Errorf("no client: %v, want not proven", got)
	}
}

func requestBody(t *testing.T, request *http.Request) string {
	t.Helper()
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
