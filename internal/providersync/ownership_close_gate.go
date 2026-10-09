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
	decision := ownershipCloseDecision{read: request.listed}
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
func (decision ownershipCloseDecision) snapshot(kind func(closable []string) SnapshotKind[OwnershipSnapshotRow]) KindSnapshot[OwnershipSnapshotRow] {
	return kind(decision.closable).Snapshot(decision.scope, ProveSnapshot(
		SnapshotTerm{Holds: len(decision.read) > 0, Reason: OwnershipCloseSkippedNoTeamListed},
		SnapshotTerm{Holds: len(decision.read) == 0 || decision.proven > 0, Reason: OwnershipCloseSkippedListingIncomplete},
	))
}
