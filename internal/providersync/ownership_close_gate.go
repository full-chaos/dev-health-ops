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

	ownershipCloseLeg = "ownership_close"
)

// ownershipListingProvesEnd reports whether one team's listing may close that
// team's open ownership rows: the provider sent its end-of-list signal and no
// bound stopped the walk first.
func ownershipListingProvesEnd(pages providerfoundation.PageCollection) bool {
	return !pages.PageBudgetExhausted && !pages.ItemCapReached && !pages.EndUnconfirmed
}

// OwnershipSiblingIntegration is another active integration of the same
// provider in the same org, as configured: what the close gate needs to tell
// which scope it lists.
type OwnershipSiblingIntegration struct {
	IntegrationID    string
	CredentialConfig map[string]string
	SyncOptions      map[string]any
}

// OwnershipScopeCensus lists the active integrations of a provider in an org,
// other than the run's own. A failed read is an error, never an empty list.
type OwnershipScopeCensus interface {
	ActiveSiblingIntegrations(ctx context.Context, orgID, provider, integrationID string) ([]OwnershipSiblingIntegration, error)
}

type ownershipCloseRequest struct {
	ref      TeamCatalogReference
	provider string
	// scopeKey is what the run listed: the GitLab group path or the GitHub org login.
	scopeKey string
	// listed is every team whose listing returned; unproven is the part of it
	// whose end the provider did not confirm.
	listed, unproven []string
	// siblingScopeKey returns the scope another integration lists, and false
	// when its configuration does not tell.
	siblingScopeKey func(OwnershipSiblingIntegration) (string, bool)
}

type ownershipCloseDecision struct {
	// read is every listed team: its open rows give the first-seen valid_from.
	read []string
	// closable is the part of read whose open rows a snapshot may close.
	closable []string
	legs     []DegradedLeg
}

// decideOwnershipClose is the one gate in front of every provider_access
// ownership close (GitHub and GitLab). A close runs only on a listing proven
// complete, and only when no other active integration of the same provider in
// the org could list the same scope: rows carry no integration key, so a close
// there could remove a row the other integration still lists. Every skip is
// one WARN line and a DegradedLeg on the result, never silent.
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
	unprovenListed := len(request.listed) - len(decision.closable)
	if unprovenListed > 0 {
		skip(OwnershipCloseSkippedListingIncomplete, strconv.Itoa(unprovenListed)+" team listings without a confirmed end")
	}
	var siblingsSharing int
	censusError := ""
	if len(decision.closable) > 0 {
		switch {
		case census == nil || strings.TrimSpace(request.ref.IntegrationID) == "":
			skip(OwnershipCloseSkippedCensusUnavailable, "no integration census for this run")
			decision.closable = nil
		default:
			siblings, err := census.ActiveSiblingIntegrations(ctx, request.ref.OrgID, request.provider, request.ref.IntegrationID)
			if err != nil {
				censusError = err.Error()
				skip(OwnershipCloseSkippedCensusFailed, censusError)
				decision.closable = nil
				break
			}
			for _, sibling := range siblings {
				key, known := request.siblingScopeKey(sibling)
				if !known || strings.EqualFold(strings.TrimSpace(key), strings.TrimSpace(request.scopeKey)) {
					siblingsSharing++
				}
			}
			if siblingsSharing > 0 {
				skip(OwnershipCloseSkippedScopeShared, strconv.Itoa(siblingsSharing)+" other active integrations may list this scope")
				decision.closable = nil
			}
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

// retainClosableRetractions keeps only the retractions of open rows whose team
// may close. The first-seen valid_from of every fresh row stays as planned.
func retainClosableRetractions(plan OwnershipSnapshotPlan, open []OwnershipSnapshotRow, closable []string) OwnershipSnapshotPlan {
	allowed := make(map[string]bool, len(closable))
	for _, teamID := range closable {
		allowed[teamID] = true
	}
	var kept []OwnershipSnapshotRetraction
	for _, retraction := range plan.Retract {
		if allowed[open[retraction.Open].TeamID] {
			kept = append(kept, retraction)
		}
	}
	plan.Retract = kept
	return plan
}
