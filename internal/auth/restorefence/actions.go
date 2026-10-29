package restorefence

import (
	"fmt"
	"sort"
	"strings"
)

// ActionKind names what an operator must do after an apply.
type ActionKind string

const (
	// ActionForcePasswordReset: these users' passwords may have changed after the
	// restore target, so the restored (older) password hash is live again. The
	// operator forces a reset for each BEFORE readiness is acked back.
	ActionForcePasswordReset ActionKind = "force_password_reset"
	// ActionRequeueProviderRevocation: a provider token revocation queued after
	// the target is gone; the operator re-disconnects or revokes at the provider.
	ActionRequeueProviderRevocation ActionKind = "requeue_provider_revocation"
	// ActionUserTombstoned: the source deleted these legacy users after the target. The restored rows are
	// disabled and their token_version is bumped, not deleted. Nothing is required of the operator; the ids are
	// named so the deletion is visible.
	ActionUserTombstoned ActionKind = "user_tombstoned"
	// ActionReviewIPAllowlist: the source switched off or deleted every restored active allowlist entry of these orgs.
	// The fence leaves them active (0 active entries would accept every address); the operator reviews each org's list.
	ActionReviewIPAllowlist ActionKind = "review_ip_allowlist"
	// ActionReviewAfterBlindMargin: a blind margin has no source state, so the legacy access flags stay as restored.
	// These rows are active now; the operator reviews each one.
	ActionReviewAfterBlindMargin ActionKind = "review_after_blind_margin"
	// ActionReviewScopeEmptied: the source holds none of these workloads' or credentials' restored audiences, actions or
	// scopes (swapped after the target); the fence cut them to the empty set. The operator sets them again.
	ActionReviewScopeEmptied ActionKind = "review_scope_emptied"
	// ActionReviewMembershipOwner: the source demoted the last restored owner of these orgs; the fence kept the owner
	// (an org with no owner is locked out). The operator reviews each org's owners.
	ActionReviewMembershipOwner ActionKind = "review_membership_owner"
	// ActionReviewMembershipRole: a restored or source role of these memberships is outside the known ranks; the fence
	// left the row as restored.
	ActionReviewMembershipRole ActionKind = "review_membership_role"
)

// followUp names the follow-up leaf that automates each action; the operator
// action line prints it.
var followUp = map[ActionKind]string{
	ActionForcePasswordReset:        "CHAOS-11834",
	ActionRequeueProviderRevocation: "CHAOS-11835",
	ActionUserTombstoned:            "none",
	ActionReviewIPAllowlist:         "none",
	ActionReviewAfterBlindMargin:    "none",
	ActionReviewScopeEmptied:        "none",
	ActionReviewMembershipOwner:     "none",
	ActionReviewMembershipRole:      "none",
}

// OperatorAction is the ids a section names for one action. Ids only: never a
// hash, a token or a secret.
type OperatorAction struct {
	Kind   ActionKind `json:"kind"`
	Source string     `json:"source"`
	IDs    []string   `json:"ids"`
}

// mergeActions merges actions of one kind and source and sorts every id list.
func mergeActions(in []OperatorAction) []OperatorAction {
	type key struct {
		kind   ActionKind
		source string
	}
	seen := map[key]map[string]bool{}
	for _, a := range in {
		k := key{a.Kind, a.Source}
		if seen[k] == nil {
			seen[k] = map[string]bool{}
		}
		for _, id := range a.IDs {
			seen[k][id] = true
		}
	}
	out := make([]OperatorAction, 0, len(seen))
	for k, ids := range seen {
		a := OperatorAction{Kind: k.kind, Source: k.source, IDs: []string{}}
		for id := range ids {
			a.IDs = append(a.IDs, id)
		}
		sort.Strings(a.IDs)
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Source < out[j].Source
	})
	return out
}

// ActionLines prints one line per action that has ids.
func ActionLines(actions []OperatorAction) []string {
	var lines []string
	for _, a := range mergeActions(actions) {
		if len(a.IDs) == 0 {
			continue
		}
		lines = append(lines, fmt.Sprintf("operator-action %s source=%s count=%d ids=%s follow-up=%s", a.Kind, a.Source, len(a.IDs), strings.Join(a.IDs, ","), followUp[a.Kind]))
	}
	return lines
}
