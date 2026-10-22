package authschema

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// TablePrivilege is one table's entry in the runtime role's declared posture.
//
// The four booleans are the complete set of DML the runtime may perform. There
// is deliberately no field for a DDL privilege: ACP-ADR-04 states the runtime
// role owns no DDL, and the way to guarantee that is for this type to have no
// way to express it, rather than for a reviewer to notice its absence.
type TablePrivilege struct {
	Table  string
	Select bool
	Insert bool
	Update bool
	Delete bool
}

// privileges renders the SQL privilege list for one entry, in a stable order.
func (t TablePrivilege) privileges() []string {
	granted := make([]string, 0, 4)
	if t.Select {
		granted = append(granted, "SELECT")
	}
	if t.Insert {
		granted = append(granted, "INSERT")
	}
	if t.Update {
		granted = append(granted, "UPDATE")
	}
	if t.Delete {
		granted = append(granted, "DELETE")
	}
	return granted
}

// RuntimePosture is the complete, declared privilege manifest for the
// auth-service runtime role: exactly this, and nothing else, anywhere.
//
// It is DATA rather than a sequence of GRANT statements so that the same
// declaration drives three things that must agree — what auth-migrate grants,
// what the posture test asserts the role actually holds, and what a reader can
// audit — and so a drift between granting and checking is not expressible.
//
// The shape of each entry is a decision, not an accident:
//
//   - security_audit_events is INSERT and SELECT ONLY. An audit row the runtime
//     can UPDATE or DELETE is not an audit row, and the absence of those two
//     privileges is the only thing that actually makes the trail append-only —
//     a comment saying "append-only" would not.
//   - policy_revisions and entitlement_snapshots are likewise immutable once
//     written: a decision recorded against a revision stays explicable only if
//     the revision cannot be edited afterwards (ACP-ADR-05, ACP-ADR-07).
//   - signing_keys is SELECT ONLY. The runtime publishes JWKS and resolves a
//     kid; it does not create, rotate or revoke keys. Key lifecycle is an
//     operator action through a separate path (ACP-ADR-02 §6 makes rotation a
//     runbook, not a runtime feature), so a compromised runtime cannot mint
//     itself a new signing key and publish the matching public half.
//   - actions, roles and role_actions are SELECT ONLY: the policy vocabulary
//     is changed by a migration or an administrative path, never by the
//     service evaluating it. A service that can edit the actions it is
//     checked against can widen its own authority.
//   - platform_role_assignments has no UPDATE: changing an elevation is a
//     revoke followed by a grant, which leaves two audit rows instead of one
//     silent mutation.
func RuntimePosture() []TablePrivilege {
	return []TablePrivilege{
		// Identity.
		{Table: "principals", Select: true, Insert: true, Update: true},
		{Table: "users", Select: true, Insert: true, Update: true},
		{Table: "external_identities", Select: true, Insert: true, Update: true, Delete: true},

		// Tenancy and non-human principals.
		{Table: "organizations", Select: true, Insert: true, Update: true},
		{Table: "memberships", Select: true, Insert: true, Update: true, Delete: true},
		{Table: "platform_role_assignments", Select: true, Insert: true, Delete: true},
		{Table: "service_accounts", Select: true, Insert: true, Update: true},
		{Table: "workloads", Select: true, Insert: true, Update: true},

		// Session lifecycle.
		{Table: "sessions", Select: true, Insert: true, Update: true, Delete: true},
		{Table: "refresh_credentials", Select: true, Insert: true, Update: true, Delete: true},
		{Table: "link_credentials", Select: true, Insert: true, Update: true, Delete: true},
		{Table: "login_attempts", Select: true, Insert: true, Update: true, Delete: true},
		{Table: "sso_flow_states", Select: true, Insert: true, Update: true, Delete: true},
		{Table: "saml_assertion_uses", Select: true, Insert: true, Delete: true},

		// Credential material: metadata only, and keys are read-only.
		{Table: "signing_keys", Select: true},
		{Table: "credential_registry", Select: true, Insert: true, Update: true},

		// Authorization model: the vocabulary is read, the grants are written.
		{Table: "actions", Select: true},
		{Table: "roles", Select: true},
		{Table: "role_actions", Select: true},
		{Table: "resource_grants", Select: true, Insert: true, Update: true, Delete: true},
		{Table: "policy_revisions", Select: true, Insert: true},
		{Table: "entitlement_snapshots", Select: true, Insert: true},

		// Audit and outbox.
		{Table: "security_audit_events", Select: true, Insert: true},
		{Table: "auth_outbox_events", Select: true, Insert: true, Update: true, Delete: true},
		// Per-aggregate revision counter (0013): bumped, never deleted.
		{Table: "aggregate_revisions", Select: true, Insert: true, Update: true},
	}
}

// runtimeSequences are the sequences the runtime must be able to advance.
//
// A bigserial column's INSERT privilege is not sufficient on its own: the
// nextval() call needs USAGE on the backing sequence, and an INSERT that
// passes every table-privilege check still fails at runtime without it. That
// is a genuinely easy grant to forget, and the failure appears only when the
// first row is written -- which for the audit table means at the first
// security event, the worst possible moment to discover it.
func runtimeSequences() []string {
	return []string{
		"security_audit_events_id_seq",
		"auth_outbox_events_id_seq",
	}
}

// ApplyRuntimeGrants brings the runtime role's privileges to exactly
// RuntimePosture.
//
// It REVOKES ALL first and then grants back what the manifest declares. That
// order matters: granting alone is additive, so a privilege that was correct
// in an earlier release and has since been removed from the manifest would
// survive forever, and the posture would drift wider with every deployment
// while every check that only looks for MISSING privileges kept passing.
//
// SCOPE OF THAT CLAIM, stated precisely because an earlier version of this
// comment over-stated it. Revoke-then-grant makes the manifest authoritative
// for OBJECT-LEVEL privileges inside this schema -- tables, sequences, and the
// schema itself. It does NOT reach role memberships or database-level grants,
// and codex round 1 proved both survive a successful reapply: a pre-existing
// `GRANT <migration role> TO <runtime role>` let the runtime SET ROLE to the
// schema's owner and CREATE a table, and a direct `GRANT CREATE ON DATABASE`
// let it create a schema outside auth. Those are cluster and database scoped,
// deliberately not rewritten here, and are caught instead by
// VerifyRuntimePosture, which Apply calls and fails on.
func ApplyRuntimeGrants(ctx context.Context, conn *pgx.Conn, options Options) error {
	return applyRuntimeGrants(ctx, conn, RuntimePosture(), options)
}

// applyRuntimeGrants takes the manifest as a parameter so a test can hand it a
// manifest that has silently lost an entry, which is the failure the narrowing
// ratchet exists to catch.
//
// The revoke, the re-grant and the ratchet comparison run in ONE transaction.
// GRANT and REVOKE are transactional in PostgreSQL, so a refused narrowing
// rolls back to the grants the role held before the run instead of leaving it
// narrower than it was. Before this, a failure part-way through the statement
// list left the role with its grants revoked and only some re-granted.
//
// The ratchet is a witness against SILENT LOSS: a privilege the role held
// directly before the run and does not hold after is refused unless the
// operator acknowledges that exact set. It is not a proof of sufficiency -- it
// says the manifest has not regressed from the previous deployment, not that
// the manifest is right (CHAOS-4885 is the functional half). Only DIRECT grants
// are compared; see snapshotDirectGrants.
func applyRuntimeGrants(ctx context.Context, conn querier, manifest []TablePrivilege, options Options) error {
	schemaID, err := NewValidatedIdentifier(options.Schema)
	if err != nil {
		return err
	}
	roleID, err := NewValidatedIdentifier(options.RuntimeRole)
	if err != nil {
		return err
	}
	schema := quoteIdentifier(schemaID)
	role := quoteIdentifier(roleID)

	statements := []string{
		// Wipe the slate so the manifest is authoritative in both directions.
		fmt.Sprintf(`REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM %s`, schema, role),
		fmt.Sprintf(`REVOKE ALL ON ALL SEQUENCES IN SCHEMA %s FROM %s`, schema, role),
		fmt.Sprintf(`REVOKE ALL ON SCHEMA %s FROM %s`, schema, role),
		// USAGE lets the role resolve names in the schema. CREATE is
		// deliberately NOT granted: without it the role cannot create,
		// and therefore cannot own, any object here -- which is what
		// "the runtime role owns no DDL" means in practice.
		fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`, schema, role),
	}

	posture := append([]TablePrivilege(nil), manifest...)
	sort.Slice(posture, func(i, j int) bool { return posture[i].Table < posture[j].Table })
	for _, entry := range posture {
		tableID, err := NewValidatedIdentifier(entry.Table)
		if err != nil {
			return err
		}
		granted := entry.privileges()
		if len(granted) == 0 {
			return fmt.Errorf(
				"%w: %q declares no privileges; omit the table instead of declaring an empty entry",
				ErrInvalidOptions, entry.Table,
			)
		}
		statements = append(statements, fmt.Sprintf(
			`GRANT %s ON %s.%s TO %s`,
			strings.Join(granted, ", "), schema, quoteIdentifier(tableID), role,
		))
	}
	for _, sequence := range runtimeSequences() {
		sequenceID, err := NewValidatedIdentifier(sequence)
		if err != nil {
			return err
		}
		statements = append(statements, fmt.Sprintf(
			`GRANT USAGE, SELECT ON SEQUENCE %s.%s TO %s`, schema, quoteIdentifier(sequenceID), role,
		))
	}

	readerStatements, err := sessionReaderStatements(ctx, conn, options, schemaID)
	if err != nil {
		return err
	}
	statements = append(statements, readerStatements...)

	logger := options.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	publicStatements, err := publicReadStatements(ctx, conn, options, logger)
	if err != nil {
		return err
	}
	usersWrite, err := usersWriteStatements(ctx, conn, options, logger)
	if err != nil {
		return err
	}
	publicStatements = append(publicStatements, usersWrite...)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: beginning the runtime grants transaction", ErrMigrationFailed)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	before, err := snapshotDirectGrants(ctx, tx, options.Schema, options.RuntimeRole)
	if err != nil {
		return err
	}
	beforePublic, err := snapshotDirectGrants(ctx, tx, publicSchema, options.RuntimeRole)
	if err != nil {
		return err
	}
	statements = append(statements, publicStatements...)

	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			// The statement is this package's own text, built from validated
			// identifiers, so naming it is safe and is what makes a grant
			// failure diagnosable.
			return fmt.Errorf(
				"%w: %s: %v", ErrMigrationFailed, statement, redactPGError(err),
			)
		}
	}

	after, err := snapshotDirectGrants(ctx, tx, options.Schema, options.RuntimeRole)
	if err != nil {
		return err
	}
	afterPublic, err := snapshotDirectGrants(ctx, tx, publicSchema, options.RuntimeRole)
	if err != nil {
		return err
	}
	// One acknowledgement covers every narrowing in the run: the digest is over
	// the auth-schema losses and the ops-schema losses together, the latter
	// named "public.<object>" so a same-named table cannot collide.
	lost := narrowedGrants(before, after)
	for _, g := range narrowedGrants(beforePublic, afterPublic) {
		g.Object = publicSchema + "." + g.Object
		lost = append(lost, g)
	}
	sort.Slice(lost, func(i, j int) bool { return lost[i].less(lost[j]) })
	if len(lost) > 0 {
		digest := narrowingDigest(lost)
		if options.AcknowledgeNarrowing != digest {
			logger.ErrorContext(
				ctx, "runtime grants would narrow; refusing and rolling back",
				"schema", options.Schema, "runtime_role", options.RuntimeRole,
				"narrowed", len(lost), "digest", digest,
			)
			return &NarrowingError{Schema: options.Schema, Role: options.RuntimeRole, Lost: lost, Digest: digest}
		}
		logger.WarnContext(
			ctx, "runtime grants narrowed with operator acknowledgement",
			"schema", options.Schema, "runtime_role", options.RuntimeRole,
			"narrowed", len(lost), "digest", digest,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: committing the runtime grants: %v", ErrMigrationFailed, redactPGError(err))
	}
	return nil
}
