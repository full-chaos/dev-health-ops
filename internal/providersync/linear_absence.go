package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// The per-kind provers of the two Linear cursor kinds (CHAOS-9124).
//
// A Linear listing is read by the provider's cursor, and Linear states no
// contract for a list that changes while it is read, so the end of the walk is
// not a proof that a fact is gone. An open fact the run does not hold is a
// CANDIDATE; the prover asks Linear itself about that one fact, inside the
// per-run budget (AbsenceLookups). A failed or refused read, an answer that is
// not one of the recorded ones, and a spent budget close nothing.
//
// The answers below are the ones Linear gave on 2026-10-10 (a read-only
// recording through the product's own client, see
// testdata/provider_answers/README.md):
//
//   - teams(filter: {key: {eq: KEY}}): HTTP 200 `{"data":{"teams":{"nodes":[]}}}`
//     for a key that does not exist; nodes with the key for one that does.
//   - project(id: ID): HTTP 200 with `errors[0].extensions.code = "INPUT_ERROR"`,
//     `errors[0].message = "Entity not found: Project"` and `data: null` for an
//     id that does not exist; `data.project` with its `teams.nodes[].key` for one
//     that does.

// maxLinearAbsenceBody bounds the body of one answer.
const maxLinearAbsenceBody = 1 << 20

// linearDefaultPageSize is the size of a connection page when the query names
// none (Linear's documented default). The recorded project answer carries no
// page info, so a project whose team list fills one page and does not name the
// team proves nothing.
const linearDefaultPageSize = 50

// linearIdentifier is what a team key or a project id looks like: the lookups
// put it in the query text, so anything else is refused (nothing is sent).
var linearIdentifier = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// LinearAbsenceProver asks Linear itself about one fact.
type LinearAbsenceProver struct {
	Client *providerfoundation.HTTPClient
}

// linearTeamKeyOfTeamID is the Linear team key of a team id ("linear:KEY").
func linearTeamKeyOfTeamID(teamID string) (string, bool) {
	key, ok := strings.CutPrefix(strings.TrimSpace(teamID), teamid.Prefix("linear"))
	key = strings.TrimSpace(key)
	return key, ok && key != ""
}

// TeamKeyAbsence asks whether the team of a key still exists in the workspace.
func (prover LinearAbsenceProver) TeamKeyAbsence(ctx context.Context, key string) SnapshotAbsence {
	key = strings.TrimSpace(key)
	if prover.Client == nil || key == "" {
		return SnapshotAbsenceNotProven
	}
	if !linearIdentifier.MatchString(key) {
		return SnapshotAbsenceNotProven
	}
	// The query text of the recorded answer (testdata/provider_answers/linear).
	query, err := json.Marshal(map[string]any{
		"query": fmt.Sprintf(`query { teams(filter: {key: {eq: %q}}) { nodes { id key } } }`, key),
	})
	if err != nil {
		return SnapshotAbsenceNotProven
	}
	status, body, ok := prover.post(ctx, query)
	if !ok {
		return SnapshotAbsenceNotProven
	}
	return classifyLinearTeamKeyAnswer(status, body, key)
}

// TeamAbsence is the answer for one team id ("linear:KEY"): a dropped team
// (TeamAbsenceProver) and the team-key ownership row of a team.
func (prover LinearAbsenceProver) TeamAbsence(ctx context.Context, teamID string) SnapshotAbsence {
	key, ok := linearTeamKeyOfTeamID(teamID)
	if !ok {
		return SnapshotAbsenceNotProven
	}
	return prover.TeamKeyAbsence(ctx, key)
}

// OwnershipAbsence is the answer for one open ownership row of the two Linear
// kinds: the {org}:linear:{team key} row of a team asks for the team; the row
// of a project asks for the project and its teams.
func (prover LinearAbsenceProver) OwnershipAbsence(orgID string) func(context.Context, OwnershipSnapshotRow) SnapshotAbsence {
	return func(ctx context.Context, row OwnershipSnapshotRow) SnapshotAbsence {
		if key, ok := row.ProjectID.LinearTeamKey(orgID); ok {
			return prover.TeamKeyAbsence(ctx, key)
		}
		return prover.projectLinkAbsence(ctx, row)
	}
}

func (prover LinearAbsenceProver) projectLinkAbsence(ctx context.Context, row OwnershipSnapshotRow) SnapshotAbsence {
	teamKey, ok := linearTeamKeyOfTeamID(row.TeamID)
	projectID := strings.TrimSpace(row.ProjectID.String())
	if prover.Client == nil || !ok || projectID == "" {
		return SnapshotAbsenceNotProven
	}
	if !linearIdentifier.MatchString(projectID) {
		return SnapshotAbsenceNotProven
	}
	query, err := json.Marshal(map[string]any{
		"query": fmt.Sprintf(`query { project(id: %q) { id name teams { nodes { key } } } }`, projectID),
	})
	if err != nil {
		return SnapshotAbsenceNotProven
	}
	status, body, ok := prover.post(ctx, query)
	if !ok {
		return SnapshotAbsenceNotProven
	}
	return classifyLinearProjectLinkAnswer(status, body, teamKey)
}

func (prover LinearAbsenceProver) post(ctx context.Context, query []byte) (int, []byte, bool) {
	response, err := prover.Client.DoRoute(ctx, http.MethodPost, graphQLRoute, "/graphql", bytes.NewReader(query))
	if err != nil {
		return 0, nil, false
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxLinearAbsenceBody+1))
	if err != nil || len(body) > maxLinearAbsenceBody {
		return 0, nil, false
	}
	return response.StatusCode, body, true
}

type linearAnswerError struct {
	Message    string `json:"message"`
	Extensions struct {
		Code string `json:"code"`
	} `json:"extensions"`
}

// classifyLinearTeamKeyAnswer reads the answer of teams(filter key eq KEY): an
// answer with an empty node list says the team is not there; a node of the key
// says it is. Anything else (a status other than 200, GraphQL errors, no node
// list) proves nothing.
func classifyLinearTeamKeyAnswer(status int, body []byte, key string) SnapshotAbsence {
	if status != http.StatusOK {
		return SnapshotAbsenceNotProven
	}
	var answer struct {
		Errors []linearAnswerError `json:"errors"`
		Data   *struct {
			Teams *struct {
				Nodes *[]struct {
					Key string `json:"key"`
				} `json:"nodes"`
			} `json:"teams"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &answer) != nil || len(answer.Errors) > 0 ||
		answer.Data == nil || answer.Data.Teams == nil || answer.Data.Teams.Nodes == nil {
		return SnapshotAbsenceNotProven
	}
	nodes := *answer.Data.Teams.Nodes
	if len(nodes) == 0 {
		return SnapshotAbsenceProven
	}
	for _, node := range nodes {
		if strings.EqualFold(strings.TrimSpace(node.Key), strings.TrimSpace(key)) {
			return SnapshotFactStillHeld
		}
	}
	return SnapshotAbsenceNotProven
}

// linearProjectNotFound is Linear's own answer for a project id that does not
// exist: GraphQL errors with the input error code and the entity message, and
// no data. A different error (a permission, a rate limit, a malformed id)
// proves nothing.
func linearProjectNotFound(errs []linearAnswerError) bool {
	if len(errs) != 1 {
		return false
	}
	return strings.TrimSpace(errs[0].Extensions.Code) == "INPUT_ERROR" &&
		strings.HasPrefix(strings.TrimSpace(errs[0].Message), "Entity not found: Project")
}

// classifyLinearProjectLinkAnswer reads the answer of project(id) for the link
// of a team to the project: the project that does not exist is gone with its
// links; a project whose complete team list lacks the team no longer has the
// link; a project that names the team still has it.
func classifyLinearProjectLinkAnswer(status int, body []byte, teamKey string) SnapshotAbsence {
	if status != http.StatusOK {
		return SnapshotAbsenceNotProven
	}
	var answer struct {
		Errors []linearAnswerError `json:"errors"`
		Data   *struct {
			Project *struct {
				ID    string `json:"id"`
				Teams *struct {
					Nodes *[]struct {
						Key string `json:"key"`
					} `json:"nodes"`
				} `json:"teams"`
			} `json:"project"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &answer) != nil {
		return SnapshotAbsenceNotProven
	}
	if len(answer.Errors) > 0 {
		if linearProjectNotFound(answer.Errors) && (answer.Data == nil || answer.Data.Project == nil) {
			return SnapshotAbsenceProven
		}
		return SnapshotAbsenceNotProven
	}
	if answer.Data == nil || answer.Data.Project == nil || strings.TrimSpace(answer.Data.Project.ID) == "" ||
		answer.Data.Project.Teams == nil || answer.Data.Project.Teams.Nodes == nil {
		return SnapshotAbsenceNotProven
	}
	for _, node := range *answer.Data.Project.Teams.Nodes {
		if strings.EqualFold(strings.TrimSpace(node.Key), strings.TrimSpace(teamKey)) {
			return SnapshotFactStillHeld
		}
	}
	// The team is not on the page: gone only when the page is the whole list.
	// The recorded answer has no page info, so a page that is not shorter than
	// the default page size may be the first of several.
	if len(*answer.Data.Project.Teams.Nodes) >= linearDefaultPageSize {
		return SnapshotAbsenceNotProven
	}
	return SnapshotAbsenceProven
}
