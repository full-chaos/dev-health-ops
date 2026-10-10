package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The direct lookups that prove a member is NOT in a team (MembershipAbsence).
// A member list read by offset or page number cannot prove it: when a member
// leaves between two page requests the later members move one place up and one
// of them is on no page. Only the provider's own answer for that member says so.

// linearInactiveAbsence is Linear's evidence of a departure: a member the team
// query RETURNS with active = false is a deactivated user, not a member. It is
// the provider's own statement (the node is in the answer), not an absence from
// a list. A member it does not name has no answer here.
type linearInactiveAbsence struct {
	inactive map[string]bool // team id + "\x00" + member id
}

func (prover linearInactiveAbsence) Absence(_ context.Context, teamID, memberID string) MembershipAbsence {
	if prover.inactive[teamID+"\x00"+memberID] {
		return AbsenceProven
	}
	return AbsenceNotAsked
}

// maxMembershipLookupBody bounds the body of one lookup answer.
const maxMembershipLookupBody = 1 << 20

// githubMembershipAbsence asks GET /orgs/{org}/teams/{slug}/memberships/{login}:
// 200 with a state is a member (an active member, or a pending invitation) and
// 404 is "not a member". Any other answer proves nothing.
type githubMembershipAbsence struct {
	client *providerfoundation.HTTPClient
	org    string
}

func (prover githubMembershipAbsence) Absence(ctx context.Context, teamID, memberID string) MembershipAbsence {
	slug, okTeam := strings.CutPrefix(teamID, "gh:")
	login, okMember := strings.CutPrefix(memberID, "gh:")
	if !okTeam || !okMember || strings.TrimSpace(slug) == "" || strings.TrimSpace(login) == "" || prover.client == nil {
		return AbsenceUnproven
	}
	response, err := prover.client.Do(ctx, http.MethodGet,
		"/orgs/"+url.PathEscape(prover.org)+"/teams/"+url.PathEscape(slug)+"/memberships/"+url.PathEscape(login), nil)
	if err != nil {
		var providerErr *providerfoundation.ProviderError
		if errors.As(err, &providerErr) && providerErr.Class == providerfoundation.ErrorNotFound && providerErr.StatusCode == http.StatusNotFound {
			return AbsenceProven
		}
		return AbsenceUnproven
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxMembershipLookupBody+1))
	if err != nil || len(body) > maxMembershipLookupBody {
		return AbsenceUnproven
	}
	var payload struct {
		State string `json:"state"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return AbsenceUnproven
	}
	switch payload.State {
	case "active", "pending":
		return AbsenceStillMember
	}
	return AbsenceUnproven
}

// gitlabMembershipAbsence asks GET /groups/:id/members?query=<username> (the
// same direct-members endpoint the list reads): a member whose username matches
// is a member; an answer with no such member that reached GitLab's own end
// signal is "not a member". Any other answer proves nothing.
type gitlabMembershipAbsence struct {
	client      *providerfoundation.HTTPClient
	groupByTeam map[string]string // team id -> the group's relative API path
}

func (prover gitlabMembershipAbsence) Absence(ctx context.Context, teamID, memberID string) MembershipAbsence {
	username, okMember := strings.CutPrefix(memberID, "gl:")
	groupPath, okTeam := prover.groupByTeam[teamID]
	if !okMember || !okTeam || strings.TrimSpace(username) == "" || prover.client == nil {
		return AbsenceUnproven
	}
	response, err := prover.client.Do(ctx, http.MethodGet,
		groupPath+"/members?per_page=100&query="+url.QueryEscape(username), nil)
	if err != nil {
		return AbsenceUnproven
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxMembershipLookupBody+1))
	if err != nil || len(body) > maxMembershipLookupBody {
		return AbsenceUnproven
	}
	var members []struct {
		Username string `json:"username"`
	}
	if json.Unmarshal(body, &members) != nil || members == nil {
		return AbsenceUnproven
	}
	for _, member := range members {
		if strings.EqualFold(strings.TrimSpace(member.Username), username) {
			return AbsenceStillMember
		}
	}
	// GitLab's end of an offset listing: X-Next-Page sent and empty.
	values, sent := response.Header["X-Next-Page"]
	if sent && len(values) == 1 && strings.TrimSpace(values[0]) == "" {
		return AbsenceProven
	}
	return AbsenceUnproven
}
