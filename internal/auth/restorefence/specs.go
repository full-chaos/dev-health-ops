package restorefence

import (
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// column is one carried column: its name and the PostgreSQL type the manifest
// value is cast back to. No column holds a credential, a hash or a secret.
type column struct {
	name, pgType string
	// keep copies the source value only into a NULL: a value already on the restored row stays (a write-once column).
	keep bool
	// narrow marks an array column: it travels as the PostgreSQL array literal text and the restored set is cut to its
	// intersection with the source (a NULL source is the empty set). Nothing is ever added.
	narrow bool
	// guardOnly marks a column the section's guard reads but never writes (a revoke section's SET leaves it).
	guardOnly bool
}

type kind int

const (
	// kindRevoke copies the carried columns onto a restored row that is still
	// in the active state, when the source row is in the revoked state.
	kindRevoke kind = iota
	// kindCounter raises users.token_version to the larger value and turns
	// is_active off when the source row is off.
	kindCounter
	// kindPresence deletes the restored rows whose id is not in the manifest: a
	// single-use token that was consumed after T is not usable again.
	kindPresence
	// kindInsert puts back replay evidence the restore lost: a row of the
	// source that the restored table lacks is inserted; an existing row is left.
	kindInsert
	// kindRetire moves the restored rows that pass a guard (alias t) and whose id is not in the manifest to a
	// closed state (retireSet). It never reopens a row and never deletes one.
	kindRetire
	// kindTombstone disables (is_active false) and bumps token_version of the restored rows whose id is not in
	// the manifest: a row the source deleted. It never deletes: foreign keys fan out from users.
	kindTombstone
	// kindReport writes nothing: it names the source rows the restored table
	// lacks, as an operator action.
	kindReport
)

// section is one table the fence carries.
type section struct {
	// name is the manifest section name: "auth.<table>" or "public.<table>", plus suffix when a table has two
	// sections.
	name   string
	suffix string
	legacy bool // a table of the legacy public schema
	table  string
	kind   kind
	key    column
	cols   []column
	// revoked is the predicate (alias s) on a source row that is in the state to
	// carry; active is the predicate (alias t) on a restored row that is not.
	revoked string
	active  string
	// touch sets updated_at on a changed row.
	touch bool
	// deleteGuard narrows what a presence section deletes, and selects what a retire section moves: an extra
	// predicate on the restored row (alias t).
	deleteGuard string
	// retireSet is the SET clause of a retire section.
	retireSet string
	// match is the predicate (aliases x, t) a retire or presence section uses to find the source row of a restored row;
	// empty means the keys are equal.
	match string
	// idExpr is the operator id of a restored row (alias t) when the key alone does not name it; empty means t.<key>::text.
	idExpr string
	// revokeSet replaces the SET clause a revoke section builds from cols: a section that carries ids only.
	revokeSet string
	// blindIDExpr is the operator id of a row a blind margin lists (alias t) when it differs from idExpr.
	blindIDExpr string
	// blindReview is the predicate (alias t) on the restored rows a blind margin leaves as they are but names for the
	// operator to review (ids only).
	blindReview string
	// manifestOnly marks a section that runs only with a manifest: with no manifest (the blind margin) nothing is
	// known about which rows the source kept, and a presence section would remove every row.
	manifestOnly bool
	// action is the operator action a report section (and the counter section)
	// raises for the ids it names.
	action ActionKind
	// queues marks the provider credential section: a deleted row oauthQueued holds has its stored token queued for the
	// revocation worker in the delete statement (ActionProviderRevocationQueuedFromCredential); action names the rest.
	queues bool
}

func (s section) runsWithoutManifest() bool { return !s.manifestOnly }

func (s section) qualified(schema authschema.ValidatedIdentifier) string {
	if s.legacy {
		return `public.` + s.table
	}
	return authschema.Quote(schema) + `.` + s.table
}

var (
	colUUID   = func(n string) column { return column{name: n, pgType: "uuid"} }
	colTS     = func(n string) column { return column{name: n, pgType: "timestamptz"} }
	colText   = func(n string) column { return column{name: n, pgType: "text"} }
	colBool   = func(n string) column { return column{name: n, pgType: "boolean"} }
	colTSKeep = func(n string) column { return column{name: n, pgType: "timestamptz", keep: true} }
	colTxKeep = func(n string) column { return column{name: n, pgType: "text", keep: true} }
	colSet    = func(n string) column { return column{name: n, pgType: "text", narrow: true} }
)

// roleRank is the rank of a membership role (viewer < member < admin < owner); an unknown or NULL role has none.
func roleRank(e string) string {
	return "array_position(ARRAY['viewer', 'member', 'admin', 'owner']::text[], " + e + ")"
}

// ownerKept holds (aliases t, m) while the org of t keeps another owner that the source also holds as owner: demoting
// t then leaves the org an owner. {tbl} is the membership table.
func ownerKept(orgCol, extra string) string {
	return "EXISTS (SELECT 1 FROM {tbl} k JOIN m y ON y.id = k.id WHERE k." + orgCol + " = t." + orgCol + " AND k.id <> t.id" + extra +
		" AND k.role = 'owner' AND y.role = 'owner')"
}

func roleLowered(orgCol, extra string) string {
	return roleRank("x.role") + " < " + roleRank("t.role") + " AND (t.role <> 'owner' OR " + ownerKept(orgCol, extra) + ")"
}

// scopesHeld holds (aliases t, x) while every carried array of t is a subset of the source's (an unchanged row is
// not written).
func scopesHeld(cols ...string) string {
	var held []string
	for _, c := range cols {
		held = append(held, "t."+c+"::text[] <@ COALESCE(x."+c+"::text[], '{}'::text[])")
	}
	return "NOT (" + strings.Join(held, " AND ") + ")"
}

// sections lists, in apply order, every table the fence carries. The auth
// sections come first, then the named legacy section (the same transaction).
var sections = []section{
	// The auth schema.
	{name: "auth.sessions", table: "sessions", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("revoked_at"), colText("revoked_reason")}, revoked: "s.revoked_at IS NOT NULL", active: "t.revoked_at IS NULL", touch: true},
	// A refresh credential is out of use when it is revoked or consumed (rotated). consumed_at is
	// write-once and revoked_at / revoked_reason never move: each is copied only into a NULL.
	{name: "auth.refresh_credentials", table: "refresh_credentials", kind: kindRevoke, key: colUUID("id"),
		cols:    []column{colTSKeep("revoked_at"), colTxKeep("revoked_reason"), colTSKeep("consumed_at")},
		revoked: "s.revoked_at IS NOT NULL OR s.consumed_at IS NOT NULL",
		active:  "(t.revoked_at IS NULL AND x.revoked_at IS NOT NULL) OR (t.consumed_at IS NULL AND x.consumed_at IS NOT NULL)", touch: true},
	{name: "auth.credential_registry", table: "credential_registry", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("revoked_at"), colText("revoked_reason")}, revoked: "s.revoked_at IS NOT NULL", active: "t.revoked_at IS NULL", touch: true},
	// An expiry the source shortened after T (RotateBootstrap shortens the parent's) is shortened again: the guard keeps
	// LEAST(restored, source), so a restore never extends an expiry. With no manifest it changes nothing.
	{name: "auth.credential_registry.expiry", suffix: ".expiry", table: "credential_registry", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("expires_at")}, revoked: "s.expires_at IS NOT NULL", active: "(t.expires_at IS NULL OR t.expires_at > x.expires_at)", touch: true},
	// Scopes and audiences narrowed in place after T (the 0007 trigger allows narrowing only) are cut to the source's,
	// on the credentials the source holds live. A credential cut to an empty set is named for review.
	{name: "auth.credential_registry.scope", suffix: ".scope", table: "credential_registry", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colSet("scopes"), colSet("audiences")}, revoked: "s.revoked_at IS NULL", active: scopesHeld("scopes", "audiences"), touch: true,
		action: ActionReviewScopeEmptied, blindReview: "t.revoked_at IS NULL AND (cardinality(t.scopes) > 0 OR cardinality(t.audiences) > 0)"},
	{name: "auth.signing_keys", table: "signing_keys", kind: kindRevoke, key: colText("kid"),
		cols: []column{colText("status"), colTS("revoked_at")}, revoked: "s.status = 'compromised'", active: "t.status <> 'compromised'", touch: true},
	{name: "auth.resource_grants", table: "resource_grants", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("revoked_at")}, revoked: "s.revoked_at IS NOT NULL", active: "t.revoked_at IS NULL"},

	// Disable switches: every session, token and grant of a disabled principal (and every binding of
	// a disabled workload, every member of a disabled organization) is void.
	{name: "auth.principals", table: "principals", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("disabled_at")}, revoked: "s.disabled_at IS NOT NULL", active: "t.disabled_at IS NULL", touch: true},
	{name: "auth.workload_bindings", table: "workload_bindings", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("disabled_at")}, revoked: "s.disabled_at IS NOT NULL", active: "t.disabled_at IS NULL", touch: true},
	{name: "auth.organizations", table: "organizations", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("disabled_at")}, revoked: "s.disabled_at IS NOT NULL", active: "t.disabled_at IS NULL", touch: true},

	// Invites (D6459 am.1): a restored PENDING invite that the source holds as accepted or revoked, or no longer
	// holds, ends revoked: never accepted (the membership made after T is not restored) and never pending. The
	// export carries the source's pending ids. With no manifest (blind margin) every pending invite is revoked:
	// fail closed, admins re-invite. An invite created after T is absent after the restore (expected).
	{name: "auth.invites.ids", suffix: ".ids", table: "invites", kind: kindRetire, key: colUUID("id"),
		revoked: "s.status = 'pending'", deleteGuard: "t.status = 'pending'", retireSet: "status = 'revoked', updated_at = clock_timestamp()"},
	{name: "auth.memberships", table: "memberships", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colText("status")}, revoked: "s.status IN ('suspended', 'removed')",
		active: "(t.status IN ('invited', 'active') OR (t.status = 'suspended' AND x.status = 'removed'))", touch: true},
	// A role lowered after T (ChangeRole) is lowered again; a role raised after T is not carried. The last owner of an
	// org is never demoted (an org with no owner is locked out): it stays and the org is named for review. An unknown
	// or NULL role on either side is left and named.
	{name: "auth.memberships.role", suffix: ".role", table: "memberships", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colText("role")}, active: roleLowered("organization_id", " AND k.status = 'active'"), touch: true,
		action: ActionReviewMembershipOwner, blindReview: "t.role IN ('admin', 'owner')"},
	{name: "auth.service_accounts", table: "service_accounts", kind: kindRevoke, key: colUUID("principal_id"),
		cols: []column{colText("status"), colTS("disabled_at")}, revoked: "s.status = 'disabled'", active: "t.status = 'active'", touch: true},
	{name: "auth.workloads", table: "workloads", kind: kindRevoke, key: colUUID("principal_id"),
		cols: []column{colTS("disabled_at")}, revoked: "s.disabled_at IS NOT NULL", active: "t.disabled_at IS NULL", touch: true},
	// Audiences and allowed actions (SetAudiences, SetAllowedActions replace them) are cut to the source's: a value
	// removed after T stays removed, one added after T is not granted, a swap leaves the empty set (named for review).
	{name: "auth.workloads.scope", suffix: ".scope", table: "workloads", kind: kindRevoke, key: colUUID("principal_id"),
		cols: []column{colSet("audiences"), colSet("allowed_actions")}, active: scopesHeld("audiences", "allowed_actions"), touch: true,
		action: ActionReviewScopeEmptied, blindReview: "(cardinality(t.audiences) > 0 OR cardinality(t.allowed_actions) > 0)"},
	{name: "auth.link_credentials", table: "link_credentials", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("consumed_at")}, revoked: "s.consumed_at IS NOT NULL", active: "t.consumed_at IS NULL"},
	// Link credentials the source replaced or removed: Issue deletes the old unconsumed row, so its restored copy
	// would stay usable. The ids of the source's unconsumed rows are carried and a restored unconsumed row absent
	// from them is deleted. The consumed section above runs first, so a link consumed after T stays as a consumed
	// row. A link issued after T is absent from the restored database (expected). With no manifest (blind margin)
	// every unconsumed link credential is removed: fail closed, users ask for a new link (D6459).
	{name: "auth.link_credentials.ids", suffix: ".ids", table: "link_credentials", kind: kindPresence, key: colUUID("id"),
		revoked: "s.consumed_at IS NULL", deleteGuard: "t.consumed_at IS NULL"},
	// An external identity unlinked after T (a DELETE) comes back with the restore, and its IdP subject can sign in as the
	// principal again. A restored link survives only if the source holds the same id: an unlink + re-link after T deletes
	// the restored row and the user links again (fail closed). A link made after T is never inserted. Only the id and the
	// principal travel; never the issuer, provider or subject. With no manifest nothing is deleted and every link is
	// listed for review (id, principal, provider).
	{name: "auth.external_identities", table: "external_identities", kind: kindPresence, key: colUUID("id"),
		cols: []column{colUUID("principal_id")}, manifestOnly: true, action: ActionIdentityUnlinked,
		blindReview: "true", blindIDExpr: identityReviewExpr},
	// Replay evidence: the identifier hash of a used SAML assertion is a hash of a
	// public assertion id, not a credential; it travels as hex.
	{name: "auth.saml_assertion_uses", table: "saml_assertion_uses", kind: kindInsert, key: colText("issuer"),
		cols: []column{colText("assertion_id_hash"), colTS("expires_at")}},
	// Report only: users whose password changed after the target (their
	// password_updated_at is newer in the source).
	{name: "auth.users", table: "users", kind: kindReport, key: colUUID("principal_id"),
		cols: []column{colTS("password_updated_at")}, revoked: "s.password_updated_at IS NOT NULL", action: ActionForcePasswordReset},
	// A password changed after the target (auth-service: password_updated_at; go-api: its token_version, mirrored as
	// legacy_token_version) comes back with the restore: the older hash works again. The restored hash and its algorithm
	// are cleared (fail closed: both logins refuse a NULL hash; the user resets by the forgot-password mail). Only a
	// timestamp and a counter travel, never the hash. A user without a password is never touched.
	{name: "auth.users.password", suffix: ".password", table: "users", kind: kindRevoke, key: colUUID("principal_id"),
		cols:    []column{{name: "password_updated_at", pgType: "timestamptz", guardOnly: true}, {name: "legacy_token_version", pgType: "bigint", guardOnly: true}},
		revoked: "s.password_updated_at IS NOT NULL OR s.legacy_token_version > 0",
		active: "t.password_hash IS NOT NULL AND ((x.password_updated_at IS NOT NULL AND (t.password_updated_at IS NULL OR t.password_updated_at < x.password_updated_at)) OR " +
			"t.legacy_token_version < x.legacy_token_version)",
		revokeSet: "password_hash = NULL, password_algorithm = NULL", touch: true, manifestOnly: true,
		action: ActionForcePasswordReset, blindReview: "t.password_hash IS NOT NULL"},

	// The legacy public schema: credential and revocation state the same
	// restore rolls back while legacy auth stays on.
	{name: "public.refresh_tokens", legacy: true, table: "refresh_tokens", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("revoked_at")}, revoked: "s.revoked_at IS NOT NULL", active: "t.revoked_at IS NULL"},
	{name: "public.internal_service_credentials", legacy: true, table: "internal_service_credentials", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("revoked_at")}, revoked: "s.revoked_at IS NOT NULL", active: "t.revoked_at IS NULL"},
	{name: "public.external_ingest_tokens", legacy: true, table: "external_ingest_tokens", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("revoked_at")}, revoked: "s.revoked_at IS NOT NULL", active: "t.revoked_at IS NULL"},
	{name: "public.pagerduty_webhook_bindings", legacy: true, table: "pagerduty_webhook_bindings", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("revoked_at"), colText("status")}, revoked: "s.revoked_at IS NOT NULL", active: "t.revoked_at IS NULL", touch: true},
	{name: "public.org_invites", legacy: true, table: "org_invites", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colText("status"), colTS("accepted_at")}, revoked: "s.status <> 'pending'", active: "t.status = 'pending'", touch: true},
	// A legacy invite still pending after the section above that the source no longer holds pending (deleted) ends
	// revoked; the legacy accept path reads this table. With no manifest every pending legacy invite is revoked (fail
	// closed, as auth.invites.ids).
	{name: "public.org_invites.ids", suffix: ".ids", legacy: true, table: "org_invites", kind: kindRetire, key: colUUID("id"),
		revoked: "s.status = 'pending'", deleteGuard: "t.status = 'pending'", retireSet: "status = 'revoked', updated_at = clock_timestamp()"},
	// The legacy membership mirror (D6358: a public row means an active member): a mirror row deleted after the
	// target is deleted again; a row the source still holds is left alone.
	{name: "public.memberships", legacy: true, table: "memberships", kind: kindPresence, key: colUUID("id"), manifestOnly: true},
	// The legacy mirror's role (go-api reads it): the same rank rule as auth.memberships.role, in the same transaction.
	{name: "public.memberships.role", suffix: ".role", legacy: true, table: "memberships", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colText("role")}, active: roleLowered("org_id", ""), touch: true,
		action: ActionReviewMembershipOwner, blindReview: "t.role IN ('admin', 'owner')"},
	// The legacy hash of a user whose password go-api (or the auth-service mirror) changed after the target: every such
	// write bumps token_version. Cleared like auth.users.password. It runs BEFORE the counter section below, which raises
	// the restored token_version to the source's. A deactivation also bumps token_version; that user is disabled anyway.
	{name: "public.users.password", suffix: ".password", legacy: true, table: "users", kind: kindRevoke, key: colUUID("id"),
		cols:    []column{{name: "token_version", pgType: "integer", guardOnly: true}},
		revoked: "s.token_version > 0", active: "t.password_hash IS NOT NULL AND t.token_version < x.token_version",
		revokeSet: "password_hash = NULL", touch: true, manifestOnly: true,
		action: ActionForcePasswordReset, blindReview: "t.password_hash IS NOT NULL"},
	{name: "public.users", legacy: true, table: "users", kind: kindCounter, key: colUUID("id"),
		cols: []column{{name: "is_active", pgType: "boolean"}, {name: "token_version", pgType: "integer"}}, revoked: "s.token_version > 0 OR NOT s.is_active", touch: true,
		action: ActionForcePasswordReset},
	// A legacy user the source force-deleted after the target: tombstoned, never deleted (restrict and no-action
	// foreign keys, cascades into sessions and tokens). Runs after the counter section above.
	{name: "public.users.ids", suffix: ".ids", legacy: true, table: "users", kind: kindTombstone, key: colUUID("id"), manifestOnly: true,
		action: ActionUserTombstoned},
	// A superuser flag the source removed after T stays removed; one granted after T is not carried.
	{name: "public.users.superuser", suffix: ".superuser", legacy: true, table: "users", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colBool("is_superuser")}, revoked: "NOT s.is_superuser", active: "t.is_superuser", touch: true,
		blindReview: "t.is_superuser"},
	{name: "public.provider_oauth_revocations", legacy: true, table: "provider_oauth_revocations", kind: kindReport, key: colUUID("id"),
		cols: []column{colText("org_id"), colText("provider"), colText("purpose")}, action: ActionRequeueProviderRevocation},
	// A provider credential rotated (refresh, reconnect) or removed (disconnect) after T comes back with its OLD token,
	// which may still be live at the provider. The restored row is deleted unless the source holds the same key with the
	// same version and binding (every token or scope change bumps version). A deleted row oauthQueued holds has its stored
	// token queued for the revocation worker in the same statement; the rest are named for the operator: revoke the old
	// token at the provider, reconnect. Only ids, the version and the binding id travel; never the token or the scopes.
	{name: "public.provider_oauth_credentials", legacy: true, table: "provider_oauth_credentials", kind: kindPresence, key: colText("org_id"),
		cols:         []column{colText("provider"), colText("credential_name"), {name: "version", pgType: "integer"}, colText("binding_id")},
		manifestOnly: true, match: oauthHeld, idExpr: oauthIDExpr, action: ActionRequeueProviderRevocation, blindReview: "true", queues: true},
	// The legacy SAML replay table: an assertion id used after the target must stay used.
	{name: "public.saml_assertion_replays", legacy: true, table: "saml_assertion_replays", kind: kindInsert, key: colUUID("provider_id"),
		cols: []column{colText("assertion_id"), colTS("expires_at")}},
	{name: "public.email_verification_tokens", legacy: true, table: "email_verification_tokens", kind: kindPresence, key: colUUID("id")},
	{name: "public.password_reset_tokens", legacy: true, table: "password_reset_tokens", kind: kindPresence, key: colUUID("id")},

	// Legacy access flags: a flag the source switched off after T is switched off again; nothing is switched on. A row
	// the source deleted is switched off (never deleted), only with a manifest. With no manifest (blind margin) nothing
	// is known: switching every row off is an outage (and opens every IP allowlist), so the flags stay as restored and
	// the active rows are named for review.
	{name: "public.organizations", legacy: true, table: "organizations", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colBool("is_active")}, revoked: "NOT s.is_active", active: "t.is_active", touch: true, blindReview: "t.is_active"},
	{name: "public.organizations.ids", suffix: ".ids", legacy: true, table: "organizations", kind: kindRetire, key: colUUID("id"), manifestOnly: true,
		deleteGuard: "t.is_active", retireSet: "is_active = false, updated_at = clock_timestamp()"},
	// 0 active allowlist entries means the org accepts every address, so an entry is switched off only while its org
	// keeps an active entry the source also holds active; otherwise it stays and the org is named for review. An entry
	// is held only with the SAME ip_range (the column is text in the DDL): a range edited after T is not held.
	{name: "public.org_ip_allowlist.ids", suffix: ".ids", legacy: true, table: "org_ip_allowlist", kind: kindRetire, key: colUUID("id"), manifestOnly: true,
		cols: []column{colText("ip_range")}, match: allowlistHeld,
		revoked: "s.is_active", deleteGuard: "t.is_active AND " + allowlistKeepsASharedEntry, retireSet: "is_active = false, updated_at = clock_timestamp()",
		action: ActionReviewIPAllowlist, blindReview: "t.is_active"},
	// An expiry the source shortened after T is shortened again (LEAST); an expired entry still counts as an entry,
	// so this never opens an org.
	{name: "public.org_ip_allowlist.expiry", suffix: ".expiry", legacy: true, table: "org_ip_allowlist", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("expires_at")}, revoked: "s.is_active AND s.expires_at IS NOT NULL", active: "(t.expires_at IS NULL OR t.expires_at > x.expires_at)", touch: true},
	{name: "public.integrations", legacy: true, table: "integrations", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colBool("is_active")}, revoked: "NOT s.is_active", active: "t.is_active", touch: true, blindReview: "t.is_active"},
	{name: "public.integrations.ids", suffix: ".ids", legacy: true, table: "integrations", kind: kindRetire, key: colUUID("id"), manifestOnly: true,
		deleteGuard: "t.is_active", retireSet: "is_active = false, updated_at = clock_timestamp()"},
	{name: "public.integration_credentials", legacy: true, table: "integration_credentials", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colBool("is_active")}, revoked: "NOT s.is_active", active: "t.is_active", touch: true, blindReview: "t.is_active"},
	// The stored provider secret the source cleared (a disconnect) is cleared again. Only ids travel, never the secret.
	{name: "public.integration_credentials.secret", suffix: ".secret", legacy: true, table: "integration_credentials", kind: kindRevoke, key: colUUID("id"),
		revoked: "s.credentials_encrypted IS NULL", active: "t.credentials_encrypted IS NOT NULL", revokeSet: "credentials_encrypted = NULL", touch: true},
	{name: "public.integration_credentials.ids", suffix: ".ids", legacy: true, table: "integration_credentials", kind: kindRetire, key: colUUID("id"), manifestOnly: true,
		deleteGuard: "(t.is_active OR t.credentials_encrypted IS NOT NULL)", retireSet: "is_active = false, credentials_encrypted = NULL, updated_at = clock_timestamp()"},
	// The webhook sets suspended_at on suspend and on uninstall; the row is never deleted. An earlier suspension stays.
	{name: "public.github_app_installations", legacy: true, table: "github_app_installations", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTSKeep("suspended_at")}, revoked: "s.suspended_at IS NOT NULL", active: "t.suspended_at IS NULL", touch: true,
		blindReview: "t.suspended_at IS NULL"},
}

// allowlistKeepsASharedEntry holds (alias t, manifest m) while the org of t keeps an active entry the source also
// holds active.
const allowlistKeepsASharedEntry = "EXISTS (SELECT 1 FROM public.org_ip_allowlist k JOIN m y ON y.id = k.id AND y.ip_range = k.ip_range WHERE k.org_id = t.org_id AND k.is_active)"

// oauthHeld matches a restored provider credential (t) to the source's (x): the same key, version and binding.
const oauthHeld = "x.org_id = t.org_id AND x.provider = t.provider AND x.credential_name = t.credential_name AND x.version = t.version AND x.binding_id IS NOT DISTINCT FROM t.binding_id"

// oauthQueued holds (manifest m) for a deleted provider credential (alias a) whose stored token the fence queues for
// the revocation worker: a bound PagerDuty row the source no longer holds (a disconnect) or holds under another binding
// (a replacement). The same binding at any other version (a refresh), or no binding on either side, is a manual revoke:
// the stored token may be the one PagerDuty already rotated, and the binding cannot tell.
func oauthQueued(a string) string {
	sameKey := "y.org_id = " + a + ".org_id AND y.provider = " + a + ".provider AND y.credential_name = " + a + ".credential_name"
	return "(" + a + ".provider = 'pagerduty' AND " + a + ".binding_id IS NOT NULL AND (NOT EXISTS (SELECT 1 FROM m y WHERE " + sameKey +
		") OR EXISTS (SELECT 1 FROM m y WHERE " + sameKey + " AND y.binding_id IS NOT NULL AND y.binding_id <> " + a + ".binding_id)))"
}

// oauthQueuedPurpose is the purpose of a queued row (alias a): replacement when the source holds the key, else disconnect.
func oauthQueuedPurpose(a string) string {
	return "CASE WHEN EXISTS (SELECT 1 FROM m y WHERE y.org_id = " + a + ".org_id AND y.provider = " + a + ".provider AND y.credential_name = " +
		a + ".credential_name) THEN 'replacement' ELSE 'disconnect' END"
}

// oauthIDExpr names a provider credential as org/provider/name, each part percent-encoded (%, /, comma, LF, CR): the three
// text columns have no CHECK, and the operator action line joins ids with commas.
// identityReviewExpr is the blind-margin id of an external identity: id/principal/provider, never the subject.
var identityReviewExpr = "t.id::text || '/' || t.principal_id::text || '/' || " + pctEncode("t.provider")

var oauthIDExpr = strings.Join([]string{pctEncode("t.org_id"), pctEncode("t.provider"), pctEncode("t.credential_name")}, " || '/' || ")

func pctEncode(e string) string {
	return "replace(replace(replace(replace(replace(" + e + ", '%', '%25'), '/', '%2F'), ',', '%2C'), chr(10), '%0A'), chr(13), '%0D')"
}

// allowlistHeld matches a restored entry (t) to the source's active entry (x): the same id and the same range.
const allowlistHeld = "x.id = t.id AND x.ip_range = t.ip_range"

// floorsSection is the manifest name of the revision floors.
const floorsSection = "auth.aggregate_revisions"

func (s section) columnNames() []string {
	names := []string{s.key.name}
	for _, c := range s.cols {
		names = append(names, c.name)
	}
	return names
}

func (s section) recordset() string {
	defs := []string{s.key.name + " " + s.key.pgType}
	for _, c := range s.cols {
		defs = append(defs, c.name+" "+c.pgType)
	}
	return "jsonb_to_recordset($1::jsonb) AS x(" + strings.Join(defs, ", ") + ")"
}

// sub puts the section's qualified table into a predicate that names it as {tbl}.
func (s section) sub(pred string, schema authschema.ValidatedIdentifier) string {
	return strings.ReplaceAll(pred, "{tbl}", s.qualified(schema))
}

// exportSQL returns one jsonb array of the source rows in the state to carry.
func (s section) exportSQL(schema authschema.ValidatedIdentifier) string {
	var pairs []string
	for _, c := range append([]column{s.key}, s.cols...) {
		n := c.name
		switch {
		case s.kind == kindInsert && n == "assertion_id_hash":
			pairs = append(pairs, "'assertion_id_hash', encode(s.assertion_id_hash, 'hex')")
		case c.narrow:
			pairs = append(pairs, fmt.Sprintf("'%s', s.%s::text", n, n))
		default:
			pairs = append(pairs, fmt.Sprintf("'%s', s.%s", n, n))
		}
	}
	where := ""
	if s.revoked != "" {
		where = " WHERE " + s.revoked
	}
	return fmt.Sprintf(`SELECT coalesce(jsonb_agg(jsonb_build_object(%s) ORDER BY s.%s), '[]'::jsonb) FROM %s s%s`,
		strings.Join(pairs, ", "), s.key.name, s.qualified(schema), where)
}

// applySQL returns a statement that takes the manifest rows as $1 and returns
// (changed, absent): rows it changed or deleted, and manifest rows that have no
// restored row (not inserted; the report names them).
func (s section) applySQL(schema authschema.ValidatedIdentifier) string {
	tbl := s.qualified(schema)
	switch s.kind {
	case kindRevoke:
		var sets []string
		if s.revokeSet != "" {
			sets = append(sets, s.revokeSet)
		}
		for _, c := range s.cols {
			switch {
			case c.guardOnly:
			case c.narrow:
				sets = append(sets, fmt.Sprintf("%s = ARRAY(SELECT a FROM unnest(t.%s) WITH ORDINALITY e(a, o) WHERE a::text = ANY(COALESCE(x.%s::text[], '{}'::text[])) ORDER BY o)",
					c.name, c.name, c.name))
			case c.keep:
				sets = append(sets, fmt.Sprintf("%s = COALESCE(t.%s, x.%s)", c.name, c.name, c.name))
			default:
				sets = append(sets, fmt.Sprintf("%s = x.%s", c.name, c.name))
			}
		}
		if s.touch {
			sets = append(sets, "updated_at = clock_timestamp()")
		}
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s),
u AS (UPDATE %s t SET %s FROM m x WHERE t.%s = x.%s AND (%s) RETURNING 1)
SELECT (SELECT count(*) FROM u), (SELECT count(*) FROM m x WHERE NOT EXISTS (SELECT 1 FROM %s t WHERE t.%s = x.%s))`,
			s.recordset(), tbl, strings.Join(sets, ", "), s.key.name, s.key.name, s.sub(s.active, schema), tbl, s.key.name, s.key.name)
	case kindCounter:
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s),
u AS (UPDATE %s t SET token_version = GREATEST(t.token_version, x.token_version), is_active = t.is_active AND x.is_active, updated_at = clock_timestamp()
      FROM m x WHERE t.%s = x.%s AND (t.token_version < x.token_version OR (t.is_active AND NOT x.is_active)) RETURNING 1)
SELECT (SELECT count(*) FROM u), (SELECT count(*) FROM m x WHERE NOT EXISTS (SELECT 1 FROM %s t WHERE t.%s = x.%s))`,
			s.recordset(), tbl, s.key.name, s.key.name, tbl, s.key.name, s.key.name)
	case kindInsert:
		names := s.columnNames()
		exprs := make([]string, len(names))
		for i, n := range names {
			exprs[i] = n
			if n == "assertion_id_hash" {
				exprs[i] = "decode(assertion_id_hash, 'hex')"
			}
		}
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s),
i AS (INSERT INTO %s (%s) SELECT %s FROM m ON CONFLICT DO NOTHING RETURNING 1)
SELECT (SELECT count(*) FROM i), 0::bigint`, s.recordset(), tbl, strings.Join(names, ", "), strings.Join(exprs, ", "))
	case kindRetire:
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s),
u AS (UPDATE %s t SET %s WHERE %s AND NOT EXISTS (SELECT 1 FROM m x WHERE %s) RETURNING 1)
SELECT (SELECT count(*) FROM u), 0::bigint`, s.recordset(), tbl, s.retireSet, s.deleteGuard, s.matchSQL())
	case kindTombstone:
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s),
u AS (UPDATE %s t SET is_active = false, token_version = t.token_version + 1, updated_at = clock_timestamp()
      WHERE NOT EXISTS (SELECT 1 FROM m x WHERE x.%s = t.%s) RETURNING 1)
SELECT (SELECT count(*) FROM u), 0::bigint`, s.recordset(), tbl, s.key.name, s.key.name)
	case kindReport:
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT 0::bigint, (SELECT count(*) FROM m x WHERE NOT EXISTS (SELECT 1 FROM %s t WHERE t.%s = x.%s))`, s.recordset(), tbl, s.key.name, s.key.name)
	case kindPresence:
		if !s.queues {
			break
		}
		// The queued token is copied as stored (the OAuthTokens JSON, sealed): key version oauth-v1 tells the senders to decode
		// it. Attempts 0 and no error: due at the first worker run.
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s),
d AS (DELETE FROM %s t WHERE NOT EXISTS (SELECT 1 FROM m x WHERE %s) RETURNING t.org_id, t.provider, t.credential_name, t.binding_id, t.token_encrypted),
q AS (INSERT INTO public.provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, last_error, created_at, updated_at)
  SELECT gen_random_uuid(), d.org_id, d.provider, d.credential_name, %s, d.token_encrypted, 'oauth-v1', 'pending', 0, NULL, clock_timestamp(), clock_timestamp()
  FROM d WHERE %s RETURNING 1)
SELECT (SELECT count(*) FROM d), (SELECT count(*) FROM q)`,
			s.recordset(), tbl, s.matchSQL(), oauthQueuedPurpose("d"), oauthQueued("d"))
	}
	// kindPresence
	guard := ""
	if s.deleteGuard != "" {
		guard = " AND " + s.deleteGuard
	}
	return fmt.Sprintf(`WITH m AS (SELECT * FROM %s),
d AS (DELETE FROM %s t WHERE NOT EXISTS (SELECT 1 FROM m x WHERE %s)%s RETURNING 1)
SELECT (SELECT count(*) FROM d), 0::bigint`,
		s.recordset(), tbl, s.matchSQL(), guard)
}

// queuedIDsSQL names the deleted rows whose stored token the apply queues (rows as $1), or "" for a section that queues
// none. It runs before the apply statement deletes them.
func (s section) queuedIDsSQL(schema authschema.ValidatedIdentifier) string {
	if !s.queues {
		return ""
	}
	return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT %s FROM %s t WHERE NOT EXISTS (SELECT 1 FROM m x WHERE %s) AND %s ORDER BY 1`, s.recordset(), s.idSQL(), s.qualified(schema), s.matchSQL(), oauthQueued("t"))
}

// matchSQL is the predicate (aliases x, t) that pairs a restored row with its manifest row.
func (s section) matchSQL() string {
	if s.match != "" {
		return s.match
	}
	return "x." + s.key.name + " = t." + s.key.name
}

// idsSQL returns the statement that names the ids of the operator action of
// this section (rows as $1), or "" when it has none.
func (s section) idsSQL(schema authschema.ValidatedIdentifier) string {
	tbl := s.qualified(schema)
	switch {
	case s.kind == kindRevoke && s.action == ActionForcePasswordReset:
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT t.%s::text FROM %s t JOIN m x ON x.%s = t.%s WHERE (%s) ORDER BY 1`, s.recordset(), s.key.name, tbl, s.key.name, s.key.name, s.sub(s.active, schema))
	case s.kind == kindPresence && (s.action == ActionRequeueProviderRevocation || s.action == ActionIdentityUnlinked):
		queued := ""
		if s.queues {
			queued = " AND NOT " + oauthQueued("t")
		}
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT %s FROM %s t WHERE NOT EXISTS (SELECT 1 FROM m x WHERE %s)%s ORDER BY 1`, s.recordset(), s.idSQL(), tbl, s.matchSQL(), queued)
	case s.action == ActionReviewScopeEmptied:
		var emptied []string
		for _, c := range s.cols {
			emptied = append(emptied, fmt.Sprintf("(cardinality(t.%s) > 0 AND NOT (t.%s::text[] && COALESCE(x.%s::text[], '{}'::text[])))", c.name, c.name, c.name))
		}
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT t.%s::text FROM %s t JOIN m x ON x.%s = t.%s WHERE %s ORDER BY 1`, s.recordset(), s.key.name, tbl, s.key.name, s.key.name, strings.Join(emptied, " OR "))
	case s.action == ActionReviewMembershipOwner:
		orgCol, extra := "org_id", ""
		if !s.legacy {
			orgCol, extra = "organization_id", " AND k.status = 'active'"
		}
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT DISTINCT t.%s::text FROM %s t JOIN m x ON x.id = t.id WHERE t.role = 'owner' AND %s < %s AND NOT %s ORDER BY 1`,
			s.recordset(), orgCol, tbl, roleRank("x.role"), roleRank("t.role"), s.sub(ownerKept(orgCol, extra), schema))
	case s.kind == kindReport && s.name == "auth.users":
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT t.%s::text FROM %s t JOIN m x ON x.%s = t.%s
WHERE t.password_updated_at IS NULL OR t.password_updated_at < x.password_updated_at ORDER BY 1`, s.recordset(), s.key.name, tbl, s.key.name, s.key.name)
	case s.kind == kindTombstone:
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT t.%s::text FROM %s t WHERE NOT EXISTS (SELECT 1 FROM m x WHERE x.%s = t.%s) ORDER BY 1`, s.recordset(), s.key.name, tbl, s.key.name, s.key.name)
	case s.kind == kindReport:
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT x.%s::text FROM m x WHERE NOT EXISTS (SELECT 1 FROM %s t WHERE t.%s = x.%s) ORDER BY 1`, s.recordset(), s.key.name, tbl, s.key.name, s.key.name)
	case s.action == ActionReviewIPAllowlist:
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT DISTINCT t.org_id::text FROM %s t WHERE t.is_active AND NOT EXISTS (SELECT 1 FROM m x WHERE %s) AND NOT %s ORDER BY 1`,
			s.recordset(), tbl, s.matchSQL(), allowlistKeepsASharedEntry)
	case s.kind == kindCounter && s.action != "":
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT t.%s::text FROM %s t JOIN m x ON x.%s = t.%s WHERE t.token_version < x.token_version ORDER BY 1`, s.recordset(), s.key.name, tbl, s.key.name, s.key.name)
	}
	return ""
}

// unknownRoleSQL names the memberships (rows as $1) whose role a role section leaves because the restored or the source
// value has no rank, or "" for any other section.
func (s section) unknownRoleSQL(schema authschema.ValidatedIdentifier) string {
	if s.suffix != ".role" {
		return ""
	}
	return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT t.id::text FROM %s t JOIN m x ON x.id = t.id WHERE t.role IS DISTINCT FROM x.role AND (%s IS NULL OR %s IS NULL) ORDER BY 1`,
		s.recordset(), s.qualified(schema), roleRank("t.role"), roleRank("x.role"))
}

// blindReviewSQL returns the statement that names the restored rows a blind margin leaves as they are, for the operator
// to review (ids only), or "" when the section has none.
func blindReviewSQL(s section, schema authschema.ValidatedIdentifier) string {
	if s.blindReview == "" {
		return ""
	}
	id := s.idSQL()
	if s.blindIDExpr != "" {
		id = s.blindIDExpr
	}
	return `SELECT ` + id + ` FROM ` + s.qualified(schema) + ` t WHERE ` + s.blindReview + ` ORDER BY 1`
}

// idSQL is the operator id of a restored row (alias t).
func (s section) idSQL() string {
	if s.idExpr != "" {
		return s.idExpr
	}
	return "t." + s.key.name + "::text"
}

// flowStatesSQL invalidates every SSO flow state the restored database holds: no per-row export
// exists, and a flow started before the fence must be started again.
func flowStatesSQL(schema authschema.ValidatedIdentifier) string {
	return `UPDATE ` + authschema.Quote(schema) + `.sso_flow_states SET consumed_at = clock_timestamp() WHERE consumed_at IS NULL`
}
