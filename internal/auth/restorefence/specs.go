package restorefence

import (
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// column is one carried column: its name and the PostgreSQL type the manifest
// value is cast back to. No column holds a credential, a hash or a secret.
type column struct{ name, pgType string }

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
)

// section is one table the fence carries.
type section struct {
	// name is the manifest section name: "auth.<table>" or "public.<table>".
	name   string
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
}

func (s section) qualified(schema authschema.ValidatedIdentifier) string {
	if s.legacy {
		return `public.` + s.table
	}
	return authschema.Quote(schema) + `.` + s.table
}

var (
	colUUID = func(n string) column { return column{n, "uuid"} }
	colTS   = func(n string) column { return column{n, "timestamptz"} }
	colText = func(n string) column { return column{n, "text"} }
)

// sections lists, in apply order, every table the fence carries. The auth
// sections come first, then the named legacy section (the same transaction).
var sections = []section{
	// The auth schema.
	{name: "auth.sessions", table: "sessions", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("revoked_at"), colText("revoked_reason")}, revoked: "s.revoked_at IS NOT NULL", active: "t.revoked_at IS NULL", touch: true},
	{name: "auth.refresh_credentials", table: "refresh_credentials", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("revoked_at"), colText("revoked_reason")}, revoked: "s.revoked_at IS NOT NULL", active: "t.revoked_at IS NULL", touch: true},
	{name: "auth.credential_registry", table: "credential_registry", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("revoked_at"), colText("revoked_reason")}, revoked: "s.revoked_at IS NOT NULL", active: "t.revoked_at IS NULL", touch: true},
	{name: "auth.signing_keys", table: "signing_keys", kind: kindRevoke, key: colText("kid"),
		cols: []column{colText("status"), colTS("revoked_at")}, revoked: "s.status = 'compromised'", active: "t.status <> 'compromised'", touch: true},
	{name: "auth.resource_grants", table: "resource_grants", kind: kindRevoke, key: colUUID("id"),
		cols: []column{colTS("revoked_at")}, revoked: "s.revoked_at IS NOT NULL", active: "t.revoked_at IS NULL"},

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
	{name: "public.users", legacy: true, table: "users", kind: kindCounter, key: colUUID("id"),
		cols: []column{{"is_active", "boolean"}, {"token_version", "integer"}}, revoked: "s.token_version > 0 OR NOT s.is_active", touch: true},
	{name: "public.email_verification_tokens", legacy: true, table: "email_verification_tokens", kind: kindPresence, key: colUUID("id")},
	{name: "public.password_reset_tokens", legacy: true, table: "password_reset_tokens", kind: kindPresence, key: colUUID("id")},
}

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
		for _, c := range s.cols {
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
	default: // kindPresence
		return fmt.Sprintf(`WITH m AS (SELECT * FROM %s),
d AS (DELETE FROM %s t WHERE NOT EXISTS (SELECT 1 FROM m x WHERE x.%s = t.%s) RETURNING 1)
SELECT (SELECT count(*) FROM d), 0::bigint`,
			s.recordset(), tbl, s.key.name, s.key.name)
	}
}
