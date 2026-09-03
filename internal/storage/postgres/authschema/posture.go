package authschema

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Violation is one effective privilege the runtime role holds that
// RuntimePosture does not declare.
//
// # Why this is a CLOSED-WORLD check
//
// The first version of this file enumerated escalation ROUTES -- role
// membership, database CREATE, ownership, CREATE on the schema -- and asked
// whether the runtime role had any of them. Two review rounds walked through
// it four times (out-of-schema grants, grants to PUBLIC, SECURITY DEFINER
// functions, and a non-inheritable membership it wrongly rejected), because an
// enumeration of bad things is an OPEN-WORLD problem: it is wrong by omission
// by construction, and every repair adds one more row to a list that can never
// be shown complete.
//
// The decisive argument is DEFAULT PRIVILEGES: `ALTER DEFAULT PRIVILEGES`
// grants on objects created LATER, so a route-enumeration that passes today is
// not a statement about tomorrow at all. No amount of adding routes fixes
// that.
//
// So the question is inverted. The manifest already says exactly what the role
// SHOULD hold. This asks the catalog what it ACTUALLY holds -- everywhere, from
// every source -- and reports the difference. `has_*_privilege` computes the
// EFFECTIVE privilege, which folds in direct grants, grants to PUBLIC, and
// grants reaching the role through an inheritable membership, so all three
// stop being separate cases that have to be thought of.
type Violation struct {
	Kind      string // relation | sequence | function | schema | database | default_acl | ownership | role_membership
	Schema    string
	Object    string
	Privilege string
	Detail    string
	// SystemRole marks a membership in a role the SERVER defines (OID below
	// systemRoleOIDBoundary). Those are detectors rather than context, because
	// they confer capability outside the object-privilege model that no
	// has_*_privilege check can observe.
	SystemRole bool
}

func (v Violation) String() string {
	target := v.Object
	if v.Schema != "" && v.Object != "" {
		target = v.Schema + "." + v.Object
	} else if v.Schema != "" {
		target = "schema " + v.Schema
	}
	if v.Detail != "" {
		return fmt.Sprintf("%s: %s on %s (%s)", v.Kind, v.Privilege, target, v.Detail)
	}
	return fmt.Sprintf("%s: %s on %s", v.Kind, v.Privilege, target)
}

// ErrRuntimeRoleCanEscalate reports a runtime role holding authority beyond
// RuntimePosture.
var ErrRuntimeRoleCanEscalate = fmt.Errorf(
	"%w: the runtime role holds privileges the manifest does not declare", ErrMigrationFailed,
)

// systemRoleOIDBoundary is PostgreSQL's own dividing line: every object with an
// OID below FirstNormalObjectId was created by initdb, so every ROLE below it
// is one the server defines rather than one an operator created.
//
// This replaces a hand-written list of six predefined role names. That list was
// the same defect this whole package exists to remove — a hand-written list
// standing beside a source of truth — and it would have silently missed
// `pg_signal_backend`, `pg_monitor`, `pg_checkpoint`, and whatever a future
// PostgreSQL adds. Asking the catalog which roles the SERVER defines is the
// closed-world form, and it cannot go stale on a version upgrade.
//
// Membership in any of them is a DETECTOR, not context: predefined roles
// confer capability outside the object-privilege model, so no has_*_privilege
// check can see the effect (codex round 3 executed `COPY ... TO PROGRAM`
// through `pg_execute_server_program` while the posture reported nothing).
const systemRoleOIDBoundary = 16384

// systemSchemaFilter excludes the catalogs// systemSchemaFilter excludes the catalogs, which every role can read and
// which no manifest describes.
const systemSchemaFilter = `n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\_%'`

// relationPrivileges are every table-level privilege PostgreSQL can grant.
// The list is exhaustive on purpose: checking only the four the manifest uses
// would let TRUNCATE, REFERENCES or TRIGGER through unexamined, and TRUNCATE
// in particular destroys an append-only table just as thoroughly as DELETE.
var relationPrivileges = []string{
	"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER",
}

var sequencePrivileges = []string{"USAGE", "SELECT", "UPDATE"}

// VerifyRuntimePosture reports every effective privilege the runtime role
// holds that RuntimePosture does not declare, across every non-system schema,
// plus the database, functions in the auth schema, schema-level privileges,
// default ACLs and object ownership.
//
// An empty result means the role's effective authority equals the manifest.
// That is a materially stronger claim than "none of the routes I thought of
// are present".
func VerifyRuntimePosture(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	if err := ValidateIdentifier(options.Schema); err != nil {
		return nil, err
	}
	if err := ValidateIdentifier(options.RuntimeRole); err != nil {
		return nil, err
	}

	allowed := make(map[string]map[string]struct{}, len(RuntimePosture()))
	for _, entry := range RuntimePosture() {
		granted := make(map[string]struct{}, 4)
		for _, privilege := range entry.privileges() {
			granted[privilege] = struct{}{}
		}
		allowed[entry.Table] = granted
	}
	allowedSequences := make(map[string]struct{}, len(runtimeSequences()))
	for _, sequence := range runtimeSequences() {
		allowedSequences[sequence] = struct{}{}
	}

	var violations []Violation
	collect := func(more []Violation, err error) error {
		if err != nil {
			return err
		}
		violations = append(violations, more...)
		return nil
	}

	if err := collect(relationViolations(ctx, conn, options, allowed)); err != nil {
		return nil, err
	}
	if err := collect(sequenceViolations(ctx, conn, options, allowedSequences)); err != nil {
		return nil, err
	}
	if err := collect(functionViolations(ctx, conn, options)); err != nil {
		return nil, err
	}
	if err := collect(schemaAndDatabaseViolations(ctx, conn, options)); err != nil {
		return nil, err
	}
	if err := collect(defaultACLViolations(ctx, conn, options)); err != nil {
		return nil, err
	}
	if err := collect(ownershipViolations(ctx, conn, options)); err != nil {
		return nil, err
	}
	// Membership in a PRIVILEGED PREDEFINED ROLE is a DETECTOR, unconditionally.
	//
	// This reverses a decision I made and defended one round earlier. I had
	// demoted membership to context-only -- reported only when some other
	// violation already existed -- on the reasoning that inherited OBJECT
	// privileges are already caught by the effective-privilege checks above,
	// and that re-promoting membership to a detector would restore the
	// open-world enumeration this file exists to replace.
	//
	// That reasoning is correct for ordinary roles and WRONG for PostgreSQL's
	// predefined roles, because they confer capabilities that live entirely
	// OUTSIDE the object-privilege model: `has_table_privilege` and its
	// siblings see nothing at all. Codex round 3 executed it — membership in
	// `pg_execute_server_program` let the runtime run
	// `COPY (...) TO PROGRAM 'sh -c ...'` and write a file, while this
	// function reported `extra_privileges: 0`.
	//
	// It does NOT reintroduce the open-world defect, and the distinction is
	// the whole reason this is safe: the predefined roles are a CLOSED set
	// fixed by PostgreSQL itself, not a list of attack routes I invented and
	// must keep complete. Membership in any OTHER role stays context-only,
	// because its effects are visible to the effective-privilege checks.
	memberships, err := roleMemberships(ctx, conn, options)
	if err != nil {
		return nil, err
	}
	for _, membership := range memberships {
		if membership.SystemRole {
			violations = append(violations, membership)
		}
	}
	if len(violations) > 0 {
		// The remaining memberships are appended as CONTEXT for a failure the
		// effective checks already found: a list of ninety inherited
		// privileges does not tell an operator WHY, and "REVOKE the
		// membership" is one statement where revoking ninety grants is not.
		for _, membership := range memberships {
			if !membership.SystemRole {
				violations = append(violations, membership)
			}
		}
	}

	sort.Slice(violations, func(i, j int) bool {
		if violations[i].Kind != violations[j].Kind {
			return violations[i].Kind < violations[j].Kind
		}
		if violations[i].Schema != violations[j].Schema {
			return violations[i].Schema < violations[j].Schema
		}
		if violations[i].Object != violations[j].Object {
			return violations[i].Object < violations[j].Object
		}
		return violations[i].Privilege < violations[j].Privilege
	})
	return violations, nil
}

// relationViolations walks EVERY relation in every non-system schema.
//
// Walking all schemas rather than only the auth schema is what catches a
// pre-existing `GRANT SELECT ON public.ops_owned_table TO <runtime>`, which a
// schema-scoped check reports as a clean posture (codex round 2, P1).
func relationViolations(
	ctx context.Context, conn *pgx.Conn, options Options, allowed map[string]map[string]struct{},
) ([]Violation, error) {
	rows, err := conn.Query(ctx, `
		SELECT n.nspname, c.relname, p.priv
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL unnest($2::text[]) AS p(priv)
		WHERE `+systemSchemaFilter+`
		  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
		  AND has_table_privilege($1, c.oid, p.priv)`,
		options.RuntimeRole, relationPrivileges,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: reading effective relation privileges", ErrMigrationFailed)
	}
	defer rows.Close()

	var violations []Violation
	for rows.Next() {
		var schema, object, privilege string
		if err := rows.Scan(&schema, &object, &privilege); err != nil {
			return nil, fmt.Errorf("%w: reading a relation privilege", ErrMigrationFailed)
		}
		if schema != options.Schema {
			violations = append(violations, Violation{
				Kind: "relation", Schema: schema, Object: object, Privilege: privilege,
				Detail: "outside the auth-owned schema",
			})
			continue
		}
		granted, declared := allowed[object]
		if !declared {
			detail := "table is not in RuntimePosture"
			if object == versionTable {
				detail = "the migration lineage table is deliberately unreachable"
			}
			violations = append(violations, Violation{
				Kind: "relation", Schema: schema, Object: object, Privilege: privilege, Detail: detail,
			})
			continue
		}
		if _, ok := granted[privilege]; !ok {
			violations = append(violations, Violation{
				Kind: "relation", Schema: schema, Object: object, Privilege: privilege,
				Detail: "not declared for this table",
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading effective relation privileges", ErrMigrationFailed)
	}
	return violations, nil
}

func sequenceViolations(
	ctx context.Context, conn *pgx.Conn, options Options, allowed map[string]struct{},
) ([]Violation, error) {
	rows, err := conn.Query(ctx, `
		SELECT n.nspname, c.relname, p.priv
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL unnest($2::text[]) AS p(priv)
		WHERE `+systemSchemaFilter+`
		  AND c.relkind = 'S'
		  AND has_sequence_privilege($1, c.oid, p.priv)`,
		options.RuntimeRole, sequencePrivileges,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: reading effective sequence privileges", ErrMigrationFailed)
	}
	defer rows.Close()

	var violations []Violation
	for rows.Next() {
		var schema, object, privilege string
		if err := rows.Scan(&schema, &object, &privilege); err != nil {
			return nil, fmt.Errorf("%w: reading a sequence privilege", ErrMigrationFailed)
		}
		if schema != options.Schema {
			violations = append(violations, Violation{
				Kind: "sequence", Schema: schema, Object: object, Privilege: privilege,
				Detail: "outside the auth-owned schema",
			})
			continue
		}
		if _, declared := allowed[object]; !declared {
			violations = append(violations, Violation{
				Kind: "sequence", Schema: schema, Object: object, Privilege: privilege,
				Detail: "sequence is not in runtimeSequences",
			})
			continue
		}
		// USAGE and SELECT are what a nextval/currval pair needs. UPDATE lets
		// a role setval the sequence, which is not something the runtime does.
		if privilege == "UPDATE" {
			violations = append(violations, Violation{
				Kind: "sequence", Schema: schema, Object: object, Privilege: privilege,
				Detail: "setval is not part of the runtime's work",
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading effective sequence privileges", ErrMigrationFailed)
	}
	return violations, nil
}

// functionViolations flags SECURITY DEFINER functions this migrator's own
// roles own, in ANY non-system schema.
//
// A SECURITY DEFINER function runs as its OWNER, and PostgreSQL grants EXECUTE
// on a new function to PUBLIC by default, so such a function is a standing
// offer of its owner's authority to anyone who can call it. No grant to the
// runtime role is needed for it to be reachable.
//
// SCOPED BY OWNERSHIP, NOT BY LOCATION, and that distinction is the fix for a
// real escalation (lane-auth-contracts, executed): the first version checked
// only the auth schema, on the reasoning that a deployment's extensions
// legitimately expose functions to PUBLIC everywhere and flagging them would
// bury the signal. The noise argument was right; the SCOPE was wrong. A
// SECURITY DEFINER function in `public` owned by the MIGRATION role is not
// third-party noise — it is this migrator's own blast radius, and it let the
// runtime role execute `CREATE TABLE auth.attacker` through a function call
// while every direct attempt was correctly refused.
//
// Ownership is the right axis because extension functions are owned by
// whoever installed them, normally a superuser, so they do not match and the
// signal stays clean.
//
// The temporal half is what makes this worth flagging rather than documenting:
// the route SURVIVES revocation of the grant that created it. Once the
// function exists, removing the migration role's CREATE on that schema changes
// nothing — the same shape as a default ACL, arriving from a second direction.
func functionViolations(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	rows, err := conn.Query(ctx, `
		SELECT n.nspname, p.proname, owner.rolname
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		JOIN pg_roles owner ON owner.oid = p.proowner
		WHERE `+systemSchemaFilter+`
		  AND p.prosecdef
		  AND (
		        owner.rolname = current_user
		     OR owner.oid = (SELECT nspowner FROM pg_namespace WHERE nspname = $2)
		  )
		  AND has_function_privilege($1, p.oid, 'EXECUTE')`,
		options.RuntimeRole, options.Schema,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: reading effective function privileges", ErrMigrationFailed)
	}
	defer rows.Close()

	var violations []Violation
	for rows.Next() {
		var schema, name, owner string
		if err := rows.Scan(&schema, &name, &owner); err != nil {
			return nil, fmt.Errorf("%w: reading a function privilege", ErrMigrationFailed)
		}
		violations = append(violations, Violation{
			Kind: "function", Schema: schema, Object: name + "()", Privilege: "EXECUTE",
			Detail: fmt.Sprintf(
				"SECURITY DEFINER owned by %q: executing it lends that role's authority, "+
					"and the route survives revoking the grant that created the function", owner,
			),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading effective function privileges", ErrMigrationFailed)
	}
	return violations, nil
}

func schemaAndDatabaseViolations(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	var violations []Violation

	// CREATE on any non-system schema lets the role own objects there.
	// USAGE on the auth schema is required and therefore expected.
	rows, err := conn.Query(ctx, `
		SELECT n.nspname, p.priv
		FROM pg_namespace n
		CROSS JOIN LATERAL unnest(ARRAY['CREATE','USAGE']) AS p(priv)
		WHERE `+systemSchemaFilter+`
		  AND has_schema_privilege($1, n.oid, p.priv)`,
		options.RuntimeRole,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: reading effective schema privileges", ErrMigrationFailed)
	}
	defer rows.Close()
	for rows.Next() {
		var schema, privilege string
		if err := rows.Scan(&schema, &privilege); err != nil {
			return nil, fmt.Errorf("%w: reading a schema privilege", ErrMigrationFailed)
		}
		if schema == options.Schema && privilege == "USAGE" {
			continue // required by the manifest.
		}
		if privilege == "USAGE" && schema != options.Schema {
			// USAGE alone reveals nothing without an object privilege, and
			// PostgreSQL grants it on `public` to PUBLIC by default. Flagging
			// it would fire on every deployment -- the TEMPORARY mistake
			// again. Object privileges in other schemas ARE flagged, above.
			continue
		}
		violations = append(violations, Violation{
			Kind: "schema", Schema: schema, Privilege: privilege,
			Detail: "lets the role create and own objects",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading effective schema privileges", ErrMigrationFailed)
	}

	// CREATE on the database. CONNECT is required; TEMPORARY is granted to
	// PUBLIC on every database and is not a route into a schema-qualified
	// object, so neither is flagged.
	var holdsCreate bool
	if err := conn.QueryRow(ctx,
		`SELECT has_database_privilege($1, current_database(), 'CREATE')`, options.RuntimeRole,
	).Scan(&holdsCreate); err != nil {
		return nil, fmt.Errorf("%w: reading database privileges", ErrMigrationFailed)
	}
	if holdsCreate {
		violations = append(violations, Violation{
			Kind: "database", Privilege: "CREATE",
			Detail: "lets the role create its own schema and own everything in it",
		})
	}
	return violations, nil
}

// defaultACLViolations reports any ALTER DEFAULT PRIVILEGES entry affecting
// the auth schema.
//
// This is the check that makes a point-in-time posture meaningful, and the
// reason a route-enumeration could never have worked. A default ACL grants on
// objects created LATER, so it is not a gap in a list of current grants — it
// is a statement in a different TENSE, and no enumeration of what is granted
// today can see it (lane-auth-contracts put it that way and it is the sharper
// framing). A rule sits dormant, the posture verifies clean because nothing
// has been created under it yet, and the escalation materialises on the next
// migration.
func defaultACLViolations(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	// The invariant is EMPTY, not "contains only the expected entries"
	// (lane-auth-contracts). A manifest of expected default ACLs would itself
	// become a fourth hand-written list standing beside a source of truth --
	// the exact defect this file exists to remove -- and "empty" is both
	// stronger and trivially defensible: this lineage sets no default
	// privileges, so any entry touching the auth schema arrived from outside
	// and is a statement about objects that do not exist yet.
	rows, err := conn.Query(ctx, `
		SELECT coalesce(n.nspname, '<all schemas>'), d.defaclobjtype, a.grantee::regrole::text, a.privilege_type
		FROM pg_default_acl d
		LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
		CROSS JOIN LATERAL aclexplode(d.defaclacl) AS a
		WHERE n.nspname = $1
		   OR (d.defaclnamespace = 0 AND has_schema_privilege(d.defaclrole, $1, 'CREATE'))`,
		options.Schema,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: reading default privileges", ErrMigrationFailed)
	}
	defer rows.Close()

	var violations []Violation
	for rows.Next() {
		var schema, objType, grantee, privilege string
		if err := rows.Scan(&schema, &objType, &grantee, &privilege); err != nil {
			return nil, fmt.Errorf("%w: reading a default privilege", ErrMigrationFailed)
		}
		if grantee == "-" {
			grantee = "PUBLIC"
		}
		violations = append(violations, Violation{
			Kind: "default_acl", Schema: schema, Object: "objtype " + objType,
			Privilege: privilege,
			Detail: fmt.Sprintf(
				"granted to %s on objects created LATER; a clean posture today is not one tomorrow", grantee,
			),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading default privileges", ErrMigrationFailed)
	}
	return violations, nil
}

// ownershipViolations reports objects the runtime role OWNS. An owner holds
// full rights over its own objects permanently and no REVOKE takes that away,
// so ownership is checked separately from privilege.
func ownershipViolations(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	var violations []Violation

	rows, err := conn.Query(ctx, `
		SELECT n.nspname, c.relname
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_roles r ON r.oid = c.relowner
		WHERE `+systemSchemaFilter+` AND r.rolname = $1`,
		options.RuntimeRole,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: reading object ownership", ErrMigrationFailed)
	}
	defer rows.Close()
	for rows.Next() {
		var schema, object string
		if err := rows.Scan(&schema, &object); err != nil {
			return nil, fmt.Errorf("%w: reading an owned object", ErrMigrationFailed)
		}
		violations = append(violations, Violation{
			Kind: "ownership", Schema: schema, Object: object, Privilege: "OWNER",
			Detail: "an owner holds DDL permanently; ownership cannot be revoked",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading object ownership", ErrMigrationFailed)
	}

	// Types and domains are owned objects too, and an owner keeps DDL over
	// them permanently. `pg_class` does not list them, so a runtime-owned
	// DOMAIN passed the ownership check while its owner could still
	// `ALTER DOMAIN ... ADD CONSTRAINT` — executed by codex round 3.
	typeRows, err := conn.Query(ctx, `
		SELECT n.nspname, t.typname
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_roles r ON r.oid = t.typowner
		WHERE `+systemSchemaFilter+` AND r.rolname = $1
		  AND t.typtype IN ('d', 'e', 'c', 'r')
		  AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.reltype = t.oid)`,
		options.RuntimeRole,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: reading type ownership", ErrMigrationFailed)
	}
	for typeRows.Next() {
		var schema, name string
		if err := typeRows.Scan(&schema, &name); err != nil {
			typeRows.Close()
			return nil, fmt.Errorf("%w: reading an owned type", ErrMigrationFailed)
		}
		violations = append(violations, Violation{
			Kind: "ownership", Schema: schema, Object: name, Privilege: "OWNER",
			Detail: "a type or domain owner keeps DDL over it permanently",
		})
	}
	typeRows.Close()
	if err := typeRows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading type ownership", ErrMigrationFailed)
	}

	var ownsSchema bool
	if err := conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner
			WHERE n.nspname = $2 AND r.rolname = $1
		)`, options.RuntimeRole, options.Schema,
	).Scan(&ownsSchema); err != nil {
		return nil, fmt.Errorf("%w: reading schema ownership", ErrMigrationFailed)
	}
	if ownsSchema {
		violations = append(violations, Violation{
			Kind: "ownership", Schema: options.Schema, Privilege: "OWNER",
			Detail: "a schema owner can CREATE in it regardless of explicit grants",
		})
	}
	return violations, nil
}

// roleMemberships reports the roles the runtime role belongs to, as context
// for a failure the effective-privilege checks already found.
func roleMemberships(ctx context.Context, conn *pgx.Conn, options Options) ([]Violation, error) {
	rows, err := conn.Query(ctx, `
		SELECT r.rolname, (r.oid < $2) AS system_role
		FROM pg_auth_members m
		JOIN pg_roles r ON r.oid = m.roleid
		WHERE m.member = (SELECT oid FROM pg_roles WHERE rolname = $1)
		ORDER BY r.rolname`, options.RuntimeRole, int32(systemRoleOIDBoundary))
	if err != nil {
		return nil, fmt.Errorf("%w: reading role memberships", ErrMigrationFailed)
	}
	defer rows.Close()

	var context []Violation
	for rows.Next() {
		var name string
		var systemRole bool
		if err := rows.Scan(&name, &systemRole); err != nil {
			return nil, fmt.Errorf("%w: reading a role membership", ErrMigrationFailed)
		}
		detail := fmt.Sprintf(
			"likely cause of the privileges above; remedy: REVOKE %q FROM %q", name, options.RuntimeRole,
		)
		if systemRole {
			// Not "the cause of the privileges above" — there are none. That
			// is the whole point: a predefined role confers capability
			// OUTSIDE the object-privilege model, so nothing else in this
			// report will mention it.
			detail = fmt.Sprintf(
				"server-defined role: confers capability no object-privilege check can see; "+
					"remedy: REVOKE %q FROM %q", name, options.RuntimeRole,
			)
		}
		context = append(context, Violation{
			Kind: "role_membership", Object: name, Privilege: "MEMBER",
			Detail: detail, SystemRole: systemRole,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading role memberships", ErrMigrationFailed)
	}
	return context, nil
}

// describeViolations renders violations for an error message, one per line.
func describeViolations(violations []Violation) string {
	rendered := make([]string, 0, len(violations))
	for _, violation := range violations {
		rendered = append(rendered, "  - "+violation.String())
	}
	return strings.Join(rendered, "\n")
}
