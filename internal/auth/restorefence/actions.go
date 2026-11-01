package restorefence

import (
	"fmt"
	"sort"
	"strings"
)

// ActionKind names what an operator must do after an apply.
type ActionKind string

const (
	// ActionForcePasswordReset: these users' passwords may have changed after the restore target. From the sections
	// auth.users.password and public.users.password: the fence cleared the restored (older) hash; the user resets by the
	// forgot-password mail. From auth.users and public.users: the operator checks each user BEFORE readiness is acked.
	ActionForcePasswordReset ActionKind = "force_password_reset"
	// ActionRequeueProviderRevocation: a provider token revocation queued after
	// the target is gone; the operator re-disconnects or revokes at the provider.
	ActionRequeueProviderRevocation ActionKind = "requeue_provider_revocation"
	// ActionProviderRevocationRequeued: revocations queued after the target were put back from the source. A disconnect
	// or a reconnect of the credential of each named revocation (with the PagerDuty client id set) delivers them; a setup
	// revoke is drained by any disconnect or callback of its org.
	ActionProviderRevocationRequeued ActionKind = "provider_revocation_requeued"
	// ActionProviderRevocationRequeuedBlocked: revocations put back from the source whose credential was connected after
	// the target: the restored database has no credential row, so no disconnect delivers them. Delivery waits for the
	// revocation worker.
	ActionProviderRevocationRequeuedBlocked ActionKind = "provider_revocation_requeued_blocked"
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
	// ActionIdentityUnlinked: the source unlinked these external identities after the target; the fence deleted the
	// restored links. Nothing is required of the operator; a user who re-linked after the target links again.
	ActionIdentityUnlinked ActionKind = "identity_unlinked"
	// ActionProviderRevocationQueuedFromCredential: the fence deleted these bound PagerDuty credentials (disconnected, or
	// connected again under another binding, after the target) and queued each stored token for the revocation worker in
	// the same statement. Nothing is required of the operator.
	ActionProviderRevocationQueuedFromCredential ActionKind = "provider_revocation_queued_from_credential"
	ActionRegrantPlatformRole                    ActionKind = "regrant_platform_role"
	ActionNoActivePlatformAdmin                  ActionKind = "no_active_platform_admin"
)

// requeueRunbookStep is the follow-up of revocations an apply only named: the runbook step that sets the source. A
// second apply of the same manifest changes nothing, so the source must be set before the -apply run.
const requeueRunbookStep = "docs/operate/maintain/backup-and-restore.md#step-5:set-RESTORE_FENCE_SOURCE_URI-before-the-apply-run"

// credentialRevokeStep is the follow-up of provider credentials the fence deleted without queuing a revocation row (not
// oauthQueued): a token rotated by a refresh after the target has none anywhere, so nothing sends the old token's revoke.
const credentialRevokeStep = "docs/operate/maintain/backup-and-restore.md#step-6:revoke-the-old-token-at-the-provider-by-hand-then-reconnect-the-credential"

// followUp names the follow-up leaf that automates each action; the operator
// action line prints it.
var followUp = map[ActionKind]string{
	ActionForcePasswordReset:                     "none",
	ActionRequeueProviderRevocation:              requeueRunbookStep,
	ActionProviderRevocationRequeued:             "none",
	ActionProviderRevocationRequeuedBlocked:      "CHAOS-12010",
	ActionUserTombstoned:                         "none",
	ActionReviewIPAllowlist:                      "none",
	ActionReviewAfterBlindMargin:                 "none",
	ActionReviewScopeEmptied:                     "none",
	ActionReviewMembershipOwner:                  "none",
	ActionReviewMembershipRole:                   "none",
	ActionIdentityUnlinked:                       "none",
	ActionProviderRevocationQueuedFromCredential: "none",
	ActionRegrantPlatformRole:                    "a platform_admin grants each role again (admin route), except a platform_owner row: auth-migrate platform-owner-grant -principal <uuid> -confirm; with no active platform_admin run the break-glass verb first",
	ActionNoActivePlatformAdmin:                  "auth-migrate platform-role-bootstrap -principal <uuid> -confirm",
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

// followUpOf is the follow-up of one action. requeue_provider_revocation names two things: revocations an apply
// without a source only named (the runbook step that sets the source), and provider credentials the fence deleted
// (the manual revoke at the provider, then a reconnect).
func followUpOf(a OperatorAction) string {
	if a.Kind == ActionRequeueProviderRevocation && a.Source != revocationSection {
		return credentialRevokeStep
	}
	return followUp[a.Kind]
}

// ActionLines prints one line per action that has ids.
func ActionLines(actions []OperatorAction) []string {
	var lines []string
	for _, a := range mergeActions(actions) {
		if len(a.IDs) == 0 && a.Kind != ActionNoActivePlatformAdmin {
			continue
		}
		lines = append(lines, fmt.Sprintf("operator-action %s source=%s count=%d ids=%s follow-up=%s", a.Kind, a.Source, len(a.IDs), strings.Join(a.IDs, ","), followUpOf(a)))
	}
	return lines
}
