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
	// revokeSet replaces the SET clause a revoke section builds from cols: a section that carries ids only.
	revokeSet string
	// blindReview is the predicate (alias t) on the restored rows a blind margin leaves as they are but names for the
	// operator to review (ids only). Legacy sections only.
	blindReview string
	// manifestOnly marks a section that runs only with a manifest: with no manifest (the blind margin) nothing is
	// known about which rows the source kept, and a presence section would remove every row.
	manifestOnly bool
	// action is the operator action a report section (and the counter section)
	// raises for the ids it names.
	action ActionKind
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
)

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
	{name: "auth.service_accounts", table: "service_accounts", kind: kindRevoke, key: colUUID("principal_id"),
		cols: []column{colText("status"), colTS("disabled_at")}, revoked: "s.status = 'disabled'", active: "t.status = 'active'", touch: true},
	{name: "auth.workloads", table: "workloads", kind: kindRevoke, key: colUUID("principal_id"),
		cols: []column{colTS("disabled_at")}, revoked: "s.disabled_at IS NOT NULL", active: "t.disabled_at IS NULL", touch: true},
	{name: "auth.link_credentials", table: "link_credentials", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("consumed_at")}, revoked: "s.consumed_at IS NOT NULL", active: "t.consumed_at IS NULL"},
	// Link credentials the source replaced or removed: Issue deletes the old unconsumed row, so its restored copy
	// would stay usable. The ids of the source's unconsumed rows are carried and a restored unconsumed row absent
	// from them is deleted. The consumed section above runs first, so a link consumed after T stays as a consumed
	// row. A link issued after T is absent from the restored database (expected). With no manifest (blind margin)
	// every unconsumed link credential is removed: fail closed, users ask for a new link (D6459).
	{name: "auth.link_credentials.ids", suffix: ".ids", table: "link_credentials", kind: kindPresence, key: colUUID("id"),
		revoked: "s.consumed_at IS NULL", deleteGuard: "t.consumed_at IS NULL"},
	// Replay evidence: the identifier hash of a used SAML assertion is a hash of a
	// public assertion id, not a credential; it travels as hex.
	{name: "auth.saml_assertion_uses", table: "saml_assertion_uses", kind: kindInsert, key: colText("issuer"),
		cols: []column{colText("assertion_id_hash"), colTS("expires_at")}},
	// Report only: users whose password changed after the target (their
	// password_updated_at is newer in the source).
	{name: "auth.users", table: "users", kind: kindReport, key: colUUID("principal_id"),
		cols: []column{colTS("password_updated_at")}, revoked: "s.password_updated_at IS NOT NULL", action: ActionForcePasswordReset},

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
	{name: "public.users", legacy: true, table: "users", kind: kindCounter, key: colUUID("id"),
		cols: []column{{name: "is_active", pgType: "boolean"}, {name: "token_version", pgType: "integer"}}, revoked: "s.token_version > 0 OR NOT s.is_active", touch: true,
		action: ActionForcePasswordReset},
	// A legacy user the source force-deleted after the target: tombstoned, never deleted (restrict and no-action
	// foreign keys, cascades into sessions and tokens). Runs after the counter section above.
	{name: "public.users.ids", suffix: ".ids", legacy: true, table: "users", kind: kindTombstone, key: colUUID("id"), manifestOnly: true,
		action: ActionUserTombstoned},
	{name: "public.provider_oauth_revocations", legacy: true, table: "provider_oauth_revocations", kind: kindReport, key: colUUID("id"),
		cols: []column{colText("org_id"), colText("provider"), colText("purpose")}, action: ActionRequeueProviderRevocation},
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
	// keeps an active entry the source also holds active; otherwise it stays and the org is named for review.
	{name: "public.org_ip_allowlist.ids", suffix: ".ids", legacy: true, table: "org_ip_allowlist", kind: kindRetire, key: colUUID("id"), manifestOnly: true,
		revoked: "s.is_active", deleteGuard: "t.is_active AND " + allowlistKeepsASharedEntry, retireSet: "is_active = false, updated_at = clock_timestamp()",
		action: ActionReviewIPAllowlist, blindReview: "t.is_active"},
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
const allowlistKeepsASharedEntry = "EXISTS (SELECT 1 FROM public.org_ip_allowlist k JOIN m y ON y.id = k.id WHERE k.org_id = t.org_id AND k.is_active)"

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

// exportSQL returns one jsonb array of the source rows in the state to carry.
func (s section) exportSQL(schema authschema.ValidatedIdentifier) string {
	var pairs []string
	for _, n := range s.columnNames() {
		if s.kind == kindInsert && n == "assertion_id_hash" {
			pairs = append(pairs, "'assertion_id_hash', encode(s.assertion_id_hash, 'hex')")
			continue
		}
		pairs = append(pairs, fmt.Sprintf("'%s', s.%s", n, n))
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
			if c.keep {
				sets = append(sets, fmt.Sprintf("%s = COALESCE(t.%s, x.%s)", c.name, c.name, c.name))
				continue
			}
			sets = append(sets, fmt.Sprintf("%s = x.%s", c.name, c.name))
		}
		if s.touch {
			sets = append(sets, "updated_at = clock_timestamp()")
		}
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s),
u AS (UPDATE %s t SET %s FROM m x WHERE t.%s = x.%s AND (%s) RETURNING 1)
SELECT (SELECT count(*) FROM u), (SELECT count(*) FROM m x WHERE NOT EXISTS (SELECT 1 FROM %s t WHERE t.%s = x.%s))`,
			s.recordset(), tbl, strings.Join(sets, ", "), s.key.name, s.key.name, s.active, tbl, s.key.name, s.key.name)
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
u AS (UPDATE %s t SET %s WHERE %s AND NOT EXISTS (SELECT 1 FROM m x WHERE x.%s = t.%s) RETURNING 1)
SELECT (SELECT count(*) FROM u), 0::bigint`, s.recordset(), tbl, s.retireSet, s.deleteGuard, s.key.name, s.key.name)
	case kindTombstone:
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s),
u AS (UPDATE %s t SET is_active = false, token_version = t.token_version + 1, updated_at = clock_timestamp()
      WHERE NOT EXISTS (SELECT 1 FROM m x WHERE x.%s = t.%s) RETURNING 1)
SELECT (SELECT count(*) FROM u), 0::bigint`, s.recordset(), tbl, s.key.name, s.key.name)
	case kindReport:
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT 0::bigint, (SELECT count(*) FROM m x WHERE NOT EXISTS (SELECT 1 FROM %s t WHERE t.%s = x.%s))`, s.recordset(), tbl, s.key.name, s.key.name)
	default: // kindPresence
		guard := ""
		if s.deleteGuard != "" {
			guard = " AND " + s.deleteGuard
		}
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s),
d AS (DELETE FROM %s t WHERE NOT EXISTS (SELECT 1 FROM m x WHERE x.%s = t.%s)%s RETURNING 1)
SELECT (SELECT count(*) FROM d), 0::bigint`,
			s.recordset(), tbl, s.key.name, s.key.name, guard)
	}
}

// idsSQL returns the statement that names the ids of the operator action of
// this section (rows as $1), or "" when it has none.
func (s section) idsSQL(schema authschema.ValidatedIdentifier) string {
	tbl := s.qualified(schema)
	switch {
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
SELECT DISTINCT t.org_id::text FROM %s t WHERE t.is_active AND NOT EXISTS (SELECT 1 FROM m x WHERE x.%s = t.%s) AND NOT %s ORDER BY 1`,
			s.recordset(), tbl, s.key.name, s.key.name, allowlistKeepsASharedEntry)
	case s.kind == kindCounter && s.action != "":
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s)
SELECT t.%s::text FROM %s t JOIN m x ON x.%s = t.%s WHERE t.token_version < x.token_version ORDER BY 1`, s.recordset(), s.key.name, tbl, s.key.name, s.key.name)
	}
	return ""
}

// blindReviewSQL returns the statement that names the restored rows a blind margin leaves as they are, for the operator
// to review (ids only), or "" when the section has none.
func blindReviewSQL(s section) string {
	if s.blindReview == "" || !s.legacy {
		return ""
	}
	return `SELECT t.` + s.key.name + `::text FROM public.` + s.table + ` t WHERE ` + s.blindReview + ` ORDER BY 1`
}

// flowStatesSQL invalidates every SSO flow state the restored database holds: no per-row export
// exists, and a flow started before the fence must be started again.
func flowStatesSQL(schema authschema.ValidatedIdentifier) string {
	return `UPDATE ` + authschema.Quote(schema) + `.sso_flow_states SET consumed_at = clock_timestamp() WHERE consumed_at IS NULL`
}
