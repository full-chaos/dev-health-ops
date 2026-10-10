package providersync

import (
	"context"
	"log/slog"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The reasons an ownership close is skipped. Each one is a DegradedLeg reason
// on the run's result and a value of the ownership_close_skipped WARN line.
const (
	OwnershipCloseSkippedListingIncomplete = "listing_incomplete"
	OwnershipCloseSkippedScopeShared       = "scope_shared"
	OwnershipCloseSkippedCensusUnavailable = "scope_census_unavailable"
	OwnershipCloseSkippedCensusFailed      = "scope_census_failed"
	// OwnershipCloseSkippedNoTeamListed: no team listing returned at all.
	OwnershipCloseSkippedNoTeamListed = "no_team_listed"

	ownershipCloseLeg = "ownership_close"
)

// ownershipListingProvesEnd reports whether one team's listing may close that
// team's open ownership rows: the walker stopped on the provider's own
// end-of-list signal and no bound stopped the walk first.
func ownershipListingProvesEnd(pages providerfoundation.PageCollection) bool {
	return pages.EndProven && !pages.PageBudgetExhausted && !pages.ItemCapReached
}

// The walks that feed the held set of the two grant kinds: one listing per
// team (ListWalk.Name).
const (
	githubTeamRepositoriesWalk = "github team repositories (one listing per team)"
	gitlabGroupProjectsWalk    = "gitlab group projects (one listing per group)"
)

// ownershipListingResponses is the number of responses a listing took.
// Nothing can move inside one response, so a listing of one response that
// reached its end proves what it does not hold. A listing of more than one
// response, read by position, does not: when the list changes between two
// requests, a fact that still holds is on no page.
func ownershipListingResponses(pages providerfoundation.PageCollection) int {
	return pages.Pages
}

// OwnershipScopeCensus counts the active integrations of a provider in an org,
// other than the run's own. A failed read is an error, never zero.
type OwnershipScopeCensus interface {
	CountActiveSiblingIntegrations(ctx context.Context, orgID, provider, integrationID string) (int, error)
}

// ProveSoleScope is the ONE scope gate of every snapshot close, for every
// provider: the run may close open rows only when the organization has no
// other ACTIVE integration of the same provider (census:
// public.integrations.is_active, the run's own integration left out). Rows
// carry no integration key, and two integrations' configured scopes cannot be
// compared safely, so any other active integration may be the owner of a row
// this run does not hold. A run with no census or no integration id, and a
// census read that fails, are not proven either: a failed read is never zero.
func ProveSoleScope(ctx context.Context, census OwnershipScopeCensus, orgID, provider, integrationID string) ScopeProof {
	proof, _, _ := proveSoleScope(ctx, census, orgID, provider, integrationID)
	return proof
}

// proveSoleScope is ProveSoleScope with the sibling count and the census
// error text, for the gate's own log line.
func proveSoleScope(ctx context.Context, census OwnershipScopeCensus, orgID, provider, integrationID string) (ScopeProof, int, string) {
	if census == nil || strings.TrimSpace(integrationID) == "" {
		return ScopeProof{stated: true, missing: []string{OwnershipCloseSkippedCensusUnavailable}}, 0, ""
	}
	siblings, err := census.CountActiveSiblingIntegrations(ctx, orgID, provider, integrationID)
	if err != nil {
		return ScopeProof{stated: true, missing: []string{OwnershipCloseSkippedCensusFailed}}, 0, err.Error()
	}
	if siblings != 0 {
		return ScopeProof{stated: true, missing: []string{OwnershipCloseSkippedScopeShared}}, siblings, ""
	}
	return ScopeProof{stated: true}, 0, ""
}

type ownershipCloseRequest struct {
	ref      TeamCatalogReference
	provider string
	// listed is every team whose listing returned; unproven is the part of it
	// whose end the provider did not confirm.
	listed, unproven []string
	// responses is, for each listed team, the number of responses its
	// listing took. A listing of more than one response proves its end and
	// not an absence: a fact it does not hold is a candidate that needs the
	// provider's own answer. A listed team with no count is taken as not
	// read in one response.
	responses map[string]int
}

type ownershipCloseDecision struct {
	// read is every listed team: its open rows give the first-seen valid_from.
	read []string
	// closable is the part of read whose open rows a snapshot may close.
	closable []string
	legs     []DegradedLeg
	// proven counts the listed teams whose listing proved its end.
	proven int
	// scope is the scope gate's answer. It is asked only when a listing
	// proved its end; before that nothing can close and it stays not proven.
	scope ScopeProof
	// responses is the number of responses each team's listing took.
	responses map[string]int
}

// decideOwnershipClose is the one gate in front of every provider_access
// ownership close (GitHub and GitLab). A close runs only on a listing proven
// complete, and only when the org has no other active integration of the same
// provider. Rows carry no integration key, and two integrations' configured
// scopes cannot be compared safely (a subgroup, a numeric id, another spelling
// of one path), so any other integration of the provider may list the same
// teams: its rows stay open. Every skip is one WARN line and a DegradedLeg on
// the result, never silent.
func decideOwnershipClose(ctx context.Context, census OwnershipScopeCensus, request ownershipCloseRequest) ownershipCloseDecision {
	decision := ownershipCloseDecision{read: request.listed, responses: request.responses}
	if len(request.listed) == 0 {
		return decision
	}
	unproven := make(map[string]bool, len(request.unproven))
	for _, teamID := range request.unproven {
		unproven[teamID] = true
	}
	var reasons []string
	skip := func(reason, detail string) {
		reasons = append(reasons, reason)
		decision.legs = append(decision.legs, DegradedLeg{
			Dataset: "team_ownership", Leg: ownershipCloseLeg, Outcome: "skipped", Reason: reason, Detail: detail,
		})
	}
	for _, teamID := range request.listed {
		if !unproven[teamID] {
			decision.closable = append(decision.closable, teamID)
		}
	}
	decision.proven = len(decision.closable)
	unprovenListed := len(request.listed) - len(decision.closable)
	if unprovenListed > 0 {
		skip(OwnershipCloseSkippedListingIncomplete, strconv.Itoa(unprovenListed)+" team listings without a confirmed end")
	}
	var siblingsSharing int
	censusError := ""
	if len(decision.closable) > 0 {
		decision.scope, siblingsSharing, censusError = proveSoleScope(ctx, census, request.ref.OrgID, request.provider, request.ref.IntegrationID)
		for _, reason := range decision.scope.Missing() {
			detail := "no integration census for this run"
			switch reason {
			case OwnershipCloseSkippedCensusFailed:
				detail = censusError
			case OwnershipCloseSkippedScopeShared:
				detail = strconv.Itoa(siblingsSharing) + " other active integrations of this provider in the org"
			}
			skip(reason, detail)
		}
		if !decision.scope.Proven() {
			decision.closable = nil
		}
	}
	if len(reasons) > 0 {
		slog.Default().WarnContext(ctx, "ownership_close_skipped",
			"org_id", request.ref.OrgID, "provider", request.provider, "reasons", strings.Join(reasons, ","),
			"teams_listed", len(request.listed), "teams_unproven", unprovenListed,
			"teams_closable", len(decision.closable), "integrations_sharing_scope", siblingsSharing,
			"error", censusError)
	}
	return decision
}

// snapshot is the grant kind of the closable teams (kind makes it from the
// closable set) with the scope gate's proof and the proof of the listings: a
// team listing returned and at least one listing proved its end. A team
// outside closable is of no kind, so its open rows never close.
//
// The held set of a team's grants is ONE walk, the team's own listing (walk
// names it). The proof of an absence is that listing only when it was ONE
// response. For a team whose listing took more than one request, answer is
// asked for each open grant the listing does not hold (the provider's own
// answer for that one grant); a nil answer closes none of them.
func (decision ownershipCloseDecision) snapshot(
	kind func(closable []string) SnapshotKind[OwnershipSnapshotRow], walk string, answer func(OwnershipSnapshotRow) SnapshotAbsence,
) KindSnapshot[OwnershipSnapshotRow] {
	walks := func(row OwnershipSnapshotRow) []ListWalk {
		return []ListWalk{{Name: walk, Responses: decision.responses[row.TeamID]}}
	}
	return kind(decision.closable).Snapshot(decision.scope, ProveSnapshot(
		SnapshotTerm{Holds: len(decision.read) > 0, Reason: OwnershipCloseSkippedNoTeamListed},
		SnapshotTerm{Holds: len(decision.read) == 0 || decision.proven > 0, Reason: OwnershipCloseSkippedListingIncomplete},
	), AbsenceByListing(walks, answer))
}

// OwnershipAbsenceProver gives the provider's own answer for ONE ownership
// fact: a direct read of the link between the team and the project (or the
// repository). It answers SnapshotAbsenceProven only when the provider says,
// for that link, that it does not exist; SnapshotFactStillHeld when the
// provider says it does; and SnapshotAbsenceNotProven for every other answer
// (an error, a refusal, a rate limit, a body it cannot read).
type OwnershipAbsenceProver interface {
	OwnershipAbsence(ctx context.Context, row OwnershipSnapshotRow) SnapshotAbsence
}

// OwnershipAbsenceLookupBudget is the budget of direct answers of one run for
// the ownership kinds: the shared AbsenceLookupBudget.
const OwnershipAbsenceLookupBudget = AbsenceLookupBudget

// NewOwnershipAbsenceLookups makes the direct answers of one run for the
// ownership kinds (AbsenceLookups keyed by team, project and source). A nil
// prover answers nothing: every candidate stays open.
func NewOwnershipAbsenceLookups(ctx context.Context, prover OwnershipAbsenceProver) *AbsenceLookups[OwnershipSnapshotRow] {
	if prover == nil {
		return NewAbsenceLookups[OwnershipSnapshotRow](ctx, ownershipSnapshotKey, nil)
	}
	return NewAbsenceLookups(ctx, ownershipSnapshotKey, prover.OwnershipAbsence)
}

// The reasons of an ownership fact that stays open because its absence is not
// proven. Each one is a DegradedLeg reason on the run's result.
const (
	OwnershipAbsenceNotProven  = "absence_not_proven"
	OwnershipAbsenceOverBudget = "absence_lookup_budget_ended"
	// OwnershipAbsenceListingWrong: the provider's own answer says the fact
	// still holds: the listing lost it (the list changed while it was read).
	OwnershipAbsenceListingWrong = "absent_from_listing_still_held"

	ownershipAbsenceLeg = "ownership_absence"
)

// SnapshotAbsenceLegs is one DegradedLeg for each reason an open fact of the
// plan stayed open without a proof of its absence.
func SnapshotAbsenceLegs(plan SnapshotPlan) []DegradedLeg {
	var legs []DegradedLeg
	for _, outcome := range plan.Kinds {
		for _, part := range []struct {
			count  int
			reason string
		}{
			{outcome.AbsenceNotProven, OwnershipAbsenceNotProven},
			{outcome.AbsenceOverBudget, OwnershipAbsenceOverBudget},
			{outcome.StillHeld, OwnershipAbsenceListingWrong},
		} {
			if part.count > 0 {
				legs = append(legs, DegradedLeg{
					Dataset: "team_ownership", Leg: ownershipAbsenceLeg, Outcome: "skipped", Reason: part.reason,
					Detail: strconv.Itoa(part.count) + " open rows of " + outcome.Kind + " kept",
				})
			}
		}
	}
	return legs
}
