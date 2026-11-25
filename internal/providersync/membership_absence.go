package providersync

import (
	"context"
	"encoding/json"
	"errors"
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

// membershipRateLimit holds the first classified rate limit a run's departure lookups met. A rate limit says
// nothing about the member, so the run ends with it before the membership write (the non-strict caller snoozes
// and reads the catalog again). The lookups after it are not sent.
type membershipRateLimit struct{ err error }

// note keeps the first classified rate limit in err and reports whether err is one.
func (limit *membershipRateLimit) note(err error) bool {
	if limit == nil || !isClassifiedRateLimit(err) {
		return false
	}
	if limit.err == nil {
		limit.err = err
	}
	return true
}

func (limit *membershipRateLimit) hit() bool { return limit != nil && limit.err != nil }

// maxMembershipLookupBody bounds the body of one lookup answer.
const maxMembershipLookupBody = 1 << 20

// githubMembershipAbsence asks GET /orgs/{org}/teams/{slug}/memberships/{login}:
// 200 with a state is a member (an active member, or a pending invitation) and
// 404 is "not a member". Any other answer proves nothing.
type githubMembershipAbsence struct {
	client *providerfoundation.HTTPClient
	org    string
	limit  *membershipRateLimit
}

func (prover githubMembershipAbsence) Absence(ctx context.Context, teamID, memberID string) MembershipAbsence {
	slug, okTeam := strings.CutPrefix(teamID, "gh:")
	login, okMember := strings.CutPrefix(memberID, "gh:")
	if !okTeam || !okMember || strings.TrimSpace(slug) == "" || strings.TrimSpace(login) == "" || prover.client == nil || prover.limit.hit() {
		return AbsenceUnproven
	}
	response, err := prover.client.DoRoute(ctx, http.MethodGet, "/orgs/{org}/teams/{slug}/memberships/{login}",
		"/orgs/"+url.PathEscape(prover.org)+"/teams/"+url.PathEscape(slug)+"/memberships/"+url.PathEscape(login), nil)
	if err != nil {
		if prover.limit.note(err) {
			return AbsenceUnproven
		}
		var providerErr *providerfoundation.ProviderError
		if errors.As(err, &providerErr) && providerErr.Class == providerfoundation.ErrorNotFound && providerErr.StatusCode == http.StatusNotFound {
			return AbsenceProven
		}
		return AbsenceUnproven
	}
	defer response.Body.Close()
	body, err := providerfoundation.ReadResponseBody(response, maxMembershipLookupBody)
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
	limit       *membershipRateLimit
}

func (prover gitlabMembershipAbsence) Absence(ctx context.Context, teamID, memberID string) MembershipAbsence {
	username, okMember := strings.CutPrefix(memberID, "gl:")
	groupPath, okTeam := prover.groupByTeam[teamID]
	if !okMember || !okTeam || strings.TrimSpace(username) == "" || prover.client == nil || prover.limit.hit() {
		return AbsenceUnproven
	}
	response, err := prover.client.DoRoute(ctx, http.MethodGet, "/groups/{group}/members",
		groupPath+"/members?per_page=100&query="+url.QueryEscape(username), nil)
	if err != nil {
		prover.limit.note(err)
		return AbsenceUnproven
	}
	defer response.Body.Close()
	body, err := providerfoundation.ReadResponseBody(response, maxMembershipLookupBody)
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
