package postgres

// Posture SQL shared by the runtime-role readiness queries that bind to
// current_user: rolePostureQuery (domain and coordinator roles) and
// queueAuthorizationQuery (queue role). Each constant is spliced into a query
// verbatim, so every predicate has exactly one source. Each of those queries
// carries roleacl.IdentityPredicateSQL, which is what binds current_user to the
// role these fragments are read for.

// governedRelationsCTESQL opens with the ")" that closes the preceding CTE and
// ends inside the last CTE it declares, so the splice site continues with ")"
// or ", next_cte AS (". It reads the ACL columns of every relation in the
// public schema and in the River schema ($2) once, and exposes the privileges
// the caller holds on each relation (relation_held) and on each column
// (column_held), with "<PRIVILEGE> WITH GRANT OPTION" as its own element when
// the entry is grantable.
const governedRelationsCTESQL = `), current_role_identity(oid) AS (
	SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user
), governed_relations AS MATERIALIZED (
	-- CHAOS-6937: every privilege below is read from the ACL columns of the
	-- relations in the two schemas this posture governs, ONCE, instead of
	-- calling has_*_privilege per catalog row per predicate (and reading
	-- information_schema.column_privileges, which expands every ACL to every
	-- column of every relation in the database and sorted 21k rows on the
	-- production catalog). On that catalog the sweeps took 1.8 s of a 2 s
	-- readiness budget; the scoped read is a few thousand ACL entries.
	--
	-- "Held" below means: an ACL entry whose grantee is the caller by name or
	-- PUBLIC (0). That is the caller's whole effective privilege set only
	-- because the identity predicate above asserts the caller is a member of no
	-- role, and is not a superuser: nothing else can confer a privilege. A
	-- caller for whom that fails is refused by the identity predicate before any
	-- of this matters.
	SELECT class.oid, class.relkind, class.relacl, class.relowner,
		namespace.nspname = 'public' AS in_public
	FROM pg_catalog.pg_class AS class
	JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = class.relnamespace
	WHERE namespace.nspname IN ('public', $2)
		AND class.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
), relation_held AS MATERIALIZED (
	-- Table-level privileges held on each governed relation, with the
	-- "... WITH GRANT OPTION" form present as its own element when the entry is
	-- grantable, so a test for either form is one array membership test. A
	-- relation with no ACL carries its owner-only default (a sequence's is its
	-- own kind's), exactly as the has_*_privilege functions resolve it.
	SELECT governed.oid, array_agg(DISTINCT held.privilege) AS privileges
	FROM governed_relations AS governed
	CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(
		governed.relacl,
		pg_catalog.acldefault(
			CASE WHEN governed.relkind = 'S' THEN 's'::"char" ELSE 'r'::"char" END,
			governed.relowner
		)
	)) AS entry
	CROSS JOIN LATERAL (
		VALUES
			(entry.privilege_type),
			(CASE WHEN entry.is_grantable THEN entry.privilege_type || ' WITH GRANT OPTION' END)
	) AS held(privilege)
	WHERE entry.grantee IN (0, (SELECT oid FROM current_role_identity))
		AND held.privilege IS NOT NULL
	GROUP BY governed.oid
), column_held AS MATERIALIZED (
	-- Column-level privileges held on any live column of each governed
	-- relation (has_any_column_privilege's column half). attacl is NULL for the
	-- overwhelming majority of columns, so the filter comes first.
	SELECT attribute.attrelid AS oid, array_agg(DISTINCT held.privilege) AS privileges
	FROM pg_catalog.pg_attribute AS attribute
	CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) AS entry
	CROSS JOIN LATERAL (
		VALUES
			(entry.privilege_type),
			(CASE WHEN entry.is_grantable THEN entry.privilege_type || ' WITH GRANT OPTION' END)
	) AS held(privilege)
	WHERE attribute.attacl IS NOT NULL
		AND attribute.attnum > 0
		AND NOT attribute.attisdropped
		AND attribute.attrelid IN (SELECT oid FROM governed_relations)
		AND entry.grantee IN (0, (SELECT oid FROM current_role_identity))
		AND held.privilege IS NOT NULL
	GROUP BY attribute.attrelid
`

// ownsNothingPredicateSQL requires that the caller owns no database, schema,
// relation or function. It reads the current_role_identity CTE.
const ownsNothingPredicateSQL = `	-- Ownership is the one route that breaks every "holding a privilege WITH
	-- GRANT OPTION implies holding the plain privilege" argument this query
	-- otherwise relies on to treat required-ABSENT privileges as covered:
	-- an owner can REVOKE ALL on its own object (base has_*_privilege checks
	-- then read false) while PostgreSQL still treats ownership as carrying
	-- every grant option, letting it re-grant to itself, another role, or
	-- PUBLIC regardless of what it just revoked from itself. Confirmed
	-- empirically: CREATE SCHEMA s AUTHORIZATION owner; REVOKE ALL ON SCHEMA
	-- s FROM owner leaves has_schema_privilege(owner, s, 'CREATE') = false
	-- but has_schema_privilege(owner, s, 'CREATE WITH GRANT OPTION') = true.
	-- One "owns nothing at all" predicate closes the class across every
	-- object and privilege type at once, rather than adding an ownership
	-- carve-out to each option-form check individually, and is the more
	-- honest invariant regardless: a least-privilege runtime identity should
	-- own nothing. Verified both directions against a live server, not just
	-- the failing case: confirmed to reject a role that owns a schema and
	-- has revoked its own privileges on it (TestDomainAuthorizationRejectsSelfOwnedSchema),
	-- AND confirmed to pass for the posture runtimeGrantStatements actually
	-- provisions — both the domain and queue runtime roles were checked
	-- against a live server and found to own zero databases, schemas,
	-- relations, or functions, so this predicate does not fail closed on a
	-- working deployment.
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_database AS database, current_role_identity
		WHERE database.datdba = current_role_identity.oid
	)
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_namespace AS namespace, current_role_identity
		WHERE namespace.nspowner = current_role_identity.oid
	)
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_class AS class, current_role_identity
		WHERE class.relowner = current_role_identity.oid
			AND class.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
	)
	AND NOT EXISTS (
		SELECT 1 FROM pg_catalog.pg_proc AS procedure, current_role_identity
		WHERE procedure.proowner = current_role_identity.oid
	)
`

// ambientGrantOptionSQL requires that the caller cannot re-delegate CONNECT on
// the database, and holds USAGE on the public schema without the option.
const ambientGrantOptionSQL = `	-- CONNECT itself is never asserted true — the query is already running as
	-- this role, so it is implied — but the option to re-delegate CONNECT is
	-- an ambient, ungated privilege no other predicate here would ever see.
	AND NOT has_database_privilege(current_user, current_database(), 'CONNECT WITH GRANT OPTION')
	AND has_schema_privilege(current_user, 'public', 'USAGE')
	-- Same ambient-privilege shape as CONNECT above, for the one schema
	-- privilege this role is required to actually hold.
	AND NOT has_schema_privilege(current_user, 'public', 'USAGE WITH GRANT OPTION')
`
