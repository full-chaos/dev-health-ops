package goapiproof

// CHAOS-5505: the routing verbs' append-only audit rows.
//
// WHY THIS EXISTS. `enable`, `disable` and `repoint` change which plane
// serves production traffic, and until now the only record any of them
// left was `recorded_by` + `review_evidence` ON THE ROUTING ROW ITSELF.
// That row is mutable: the NEXT write overwrites both. So the history of
// "who moved this, and when" was exactly one entry deep, and the entry was
// whoever touched it last.
//
// WHY ITS OWN TABLE. The first draft wrote into `worker_operator_audits`.
// chris ruled against it (2026-09-09): "this column is definitely for
// syncs to pass to workers" -- that table belongs to the sync -> worker
// operator plane (alembic 0047, CHAOS-3033), and its `principal_type` is
// pinned to a credential class issued and validated there. Routing writes
// are a different plane with a different credential class, and borrowing a
// table because its column names happen to fit is how two unrelated things
// become impossible to aggregate separately later.
//
// ONE ROW PER OPERATION, SHARING A CORRELATION ID. The resource an audit
// row is about is ONE routing row, not "the fifteen of them". A single
// summary row per invocation would make "was hotspots turned off on the
// 9th" a question you answer by parsing a list out of a text column.
// `correlation_id` is what re-groups an invocation.
//
// WRITTEN IN THE SAME TRANSACTION AS THE ROUTING WRITE. A rolled-back
// routing write changed nothing, and an audit row saying an operator
// disabled fifteen operations when the transaction rolled back would be a
// false entry in a table nothing can later correct.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The three WRITE verbs. `status` is a read-only diagnostic and writes no
// row: an audit entry for a read would make "this operation was touched on
// the 9th" untrue.
const (
	AuditActionEnable  = "enable"
	AuditActionDisable = "disable"
	AuditActionRepoint = "repoint"
)

// Credential classes, in the AUTH CONTROL PLANE's own vocabulary
// (`contracts/auth/v1/credential-classes.schema.json` is the closed
// registry; each class carries issuer, validator, lifecycle authority and
// allowed route set).
const (
	// CredentialClassEnvelope is `enable` and `repoint`. Both read the
	// deployed process's authenticated /buildinfo, which VERIFIES the
	// envelope (cmd/query-api/internal/principal/verifier.go) -- so by the
	// time a row is written the credential has been checked by the
	// verifier that owns it. This process never verifies it itself: one
	// validator per class is the ACP's rule, and this is not that
	// validator.
	//
	// This is the exact class_id the Auth Control Plane threat model
	// (Wave 0, §11.1 and §12 recommendation 2) proposes for the envelope.
	// Registering it in the credential-classes contract is filed against
	// the paused ACP project, deliberately not done here.
	CredentialClassEnvelope = "effective_principal_envelope"

	// CredentialClassOperatorDirect is `disable`. It presents NO
	// credential, by contract -- the off-ramp must work when the planes
	// disagree and the deployed process is down. Recording a weaker claim
	// accurately beats recording a stronger one nothing checked.
	CredentialClassOperatorDirect = "operator_direct"
)

// ErrAuditRowRefused reports that an audit row could not be written, so
// the routing write it describes must not commit either.
var ErrAuditRowRefused = errors.New("goapiproof: the routing audit row was refused")

// ErrEnvelopeSubjectMissing reports that the envelope carried no `sub`.
//
// It is a REFUSAL rather than a NULL: an envelope-class row whose
// principal_id is NULL is exactly the shape the pairing CHECK forbids,
// and writing `operator_direct` instead would claim no credential was
// presented when one was.
var ErrEnvelopeSubjectMissing = errors.New("goapiproof: the effective-principal envelope carries no subject, so no audit row can name who acted")

// auditReviewEvidenceMax mirrors alembic 0130's CHECK. Bounded rather
// than truncated: this table is append-only, so a silently trimmed
// explanation has no later remedy.
const auditReviewEvidenceMax = 2000

// auditRecordedByMax mirrors alembic 0130's CHECK.
const auditRecordedByMax = 128

// RoutingAuditEntry is one operation's row. Before-values are pointers so
// "there was no row" and "the row said python" are different facts.
type RoutingAuditEntry struct {
	DocumentDigest       string
	Operation            string
	CandidateBuildBefore *string
	CandidateBuildAfter  string
	ModeBefore           *string
	ModeAfter            string
}

// RoutingAudit is one invocation's worth of rows.
type RoutingAudit struct {
	Action          string
	CredentialClass string
	// PrincipalID is WHO THE CREDENTIAL SAYS is acting -- the envelope's
	// `sub`, a user id. Empty for operator_direct, which has no
	// credential to have carried one.
	PrincipalID string
	// RecordedBy is what the OPERATOR typed about themselves. Verified by
	// nothing, and required on every row. Deliberately separate from
	// PrincipalID: they answer different questions, and conflating them
	// makes both unreliable.
	RecordedBy     string
	ReviewEvidence string
	SchemaDigest   string
	Entries        []RoutingAuditEntry
	// CorrelationID ties one invocation's rows together; generated when
	// empty.
	CorrelationID string
}

var auditActions = map[string]bool{
	AuditActionEnable:  true,
	AuditActionDisable: true,
	AuditActionRepoint: true,
}

var auditCredentialClasses = map[string]bool{
	CredentialClassEnvelope:       true,
	CredentialClassOperatorDirect: true,
}

const insertRoutingAuditSQL = `
INSERT INTO public.go_api_routing_audits
	(correlation_id, action, credential_class, principal_id, recorded_by,
	 review_evidence, schema_digest, document_digest, selected_operation,
	 candidate_build_before, candidate_build_after, mode_before, mode_after,
	 recorded_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`

func (a RoutingAudit) validate() error {
	switch {
	case !auditActions[a.Action]:
		return fmt.Errorf("%w: %q is not a routing audit action; alembic 0130 admits %v",
			ErrAuditRowRefused, a.Action, []string{AuditActionEnable, AuditActionDisable, AuditActionRepoint})
	case !auditCredentialClasses[a.CredentialClass]:
		return fmt.Errorf("%w: %q is not a credential class; alembic 0130 admits %v",
			ErrAuditRowRefused, a.CredentialClass, []string{CredentialClassEnvelope, CredentialClassOperatorDirect})
	case a.SchemaDigest == "":
		return fmt.Errorf("%w: schema digest is required -- it is what CHAOS-5416 moved, and a row without it cannot tell a live write from one against a dead digest", ErrAuditRowRefused)
	case a.RecordedBy == "":
		return fmt.Errorf("%w: recorded-by is required on every row", ErrAuditRowRefused)
	case len(a.RecordedBy) > auditRecordedByMax:
		return fmt.Errorf("%w: recorded-by is %d characters, the column holds %d -- refusing rather than truncating an identity in an append-only table",
			ErrAuditRowRefused, len(a.RecordedBy), auditRecordedByMax)
	case a.ReviewEvidence == "":
		return fmt.Errorf("%w: review-evidence is required: a routing change is a decision, and a decision with no durable reason is unreadable weeks later", ErrAuditRowRefused)
	case len(a.ReviewEvidence) > auditReviewEvidenceMax:
		return fmt.Errorf("%w: review-evidence is %d characters, the column holds %d -- shorten it rather than have it silently trimmed in a table nothing can later correct",
			ErrAuditRowRefused, len(a.ReviewEvidence), auditReviewEvidenceMax)
	case len(a.Entries) == 0:
		// A verb that wrote nothing writes no audit rows, and reaching
		// here with none means a caller lost them between the write and
		// the record. Refusing turns that into a failed transaction
		// rather than a silently unaudited write.
		return fmt.Errorf("%w: an audit with no entries records nothing", ErrAuditRowRefused)
	}

	// The pairing the database also enforces, checked here so the refusal
	// names the reason instead of surfacing a constraint name.
	switch a.CredentialClass {
	case CredentialClassEnvelope:
		if a.PrincipalID == "" {
			return fmt.Errorf("%w: an %s row must name the subject its credential carried", ErrAuditRowRefused, CredentialClassEnvelope)
		}
	case CredentialClassOperatorDirect:
		if a.PrincipalID != "" {
			return fmt.Errorf("%w: an %s row must NOT name a principal -- there was no credential to have carried one, and recording an unverified identity as if a credential asserted it is the claim this class exists to avoid",
				ErrAuditRowRefused, CredentialClassOperatorDirect)
		}
	}

	for _, entry := range a.Entries {
		if entry.Operation == "" || entry.DocumentDigest == "" {
			return fmt.Errorf("%w: an audit entry with no operation or document digest names no row", ErrAuditRowRefused)
		}
		if entry.CandidateBuildAfter == "" || entry.ModeAfter == "" {
			return fmt.Errorf("%w: %s has no after-state, so the row records that something happened without saying what", ErrAuditRowRefused, entry.Operation)
		}
	}
	return nil
}

// writeRoutingAudit appends one row per entry, INSIDE the caller's
// transaction.
//
// It takes a pgx.Tx rather than a Querier on purpose: "in the same
// transaction as the routing write" is the entire contract, and a
// signature that accepted a pool would let a future caller satisfy the
// type while breaking the guarantee.
func writeRoutingAudit(ctx context.Context, tx pgx.Tx, audit RoutingAudit, now time.Time) (string, error) {
	if tx == nil {
		return "", fmt.Errorf("%w: nil transaction -- an audit row must commit with the routing write it describes", ErrAuditRowRefused)
	}
	if audit.CorrelationID == "" {
		audit.CorrelationID = uuid.NewString()
	}
	if err := audit.validate(); err != nil {
		return "", err
	}
	for _, entry := range audit.Entries {
		tag, err := tx.Exec(ctx, insertRoutingAuditSQL,
			audit.CorrelationID, audit.Action, audit.CredentialClass,
			nullIfEmpty(audit.PrincipalID), audit.RecordedBy, audit.ReviewEvidence,
			audit.SchemaDigest, entry.DocumentDigest, entry.Operation,
			entry.CandidateBuildBefore, entry.CandidateBuildAfter,
			entry.ModeBefore, entry.ModeAfter, now)
		if err != nil {
			return "", describeAuditWriteFailure(entry.Operation, err)
		}
		if tag.RowsAffected() != 1 {
			return "", fmt.Errorf("%w: the row for %s affected %d rows, want exactly 1", ErrAuditRowRefused, entry.Operation, tag.RowsAffected())
		}
	}
	return audit.CorrelationID, nil
}

// ErrAuditTableNotMigrated reports that alembic 0130 has not been applied,
// so no routing verb can record what it did.
var ErrAuditTableNotMigrated = errors.New("goapiproof: go_api_routing_audits does not exist: alembic 0130 has not been applied")

// undefinedTableSQLState is Postgres's "relation does not exist".
const undefinedTableSQLState = "42P01"

// describeAuditWriteFailure turns the one failure an operator is actually
// likely to hit into a message that says what to do.
//
// WHY THE VERB STILL FAILS rather than writing the routing change and
// warning. A routing write with no record is the exact state CHAOS-5505
// exists to end: the routing row's own review_evidence is overwritten by
// the next write, so an unaudited change is one nobody can find
// afterwards. That includes `disable` -- an off-ramp whose use leaves no
// trace is how "who turned this off, and when" becomes unanswerable. The
// condition that triggers it is a migration that was not applied, which
// the fleet's lockstep rule (migrate and every go-* image rebuild
// together) already forbids and which this message names outright.
func describeAuditWriteFailure(operation string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == undefinedTableSQLState {
		return fmt.Errorf("%w (writing the audit row for %s).\n"+
			"  Apply alembic 0130 and rebuild the go-* images together -- a migration and the binaries that depend on it never move separately.\n"+
			"  Nothing was written: the routing change and its audit row commit together or not at all",
			ErrAuditTableNotMigrated, operation)
	}
	return fmt.Errorf("%w: writing the row for %s: %w", ErrAuditRowRefused, operation, err)
}

// EnvelopeSubject reads the `sub` claim out of an effective-principal
// envelope.
//
// IT DOES NOT VERIFY THE ENVELOPE, and must not. The Auth Control Plane's
// rule is one validator per credential class, and this process is not that
// validator -- `cmd/query-api/internal/principal/verifier.go` is, and it
// holds the public key. What makes reading the subject here honest is the
// ORDER: `enable` and `repoint` both call the authenticated `/buildinfo`
// BEFORE they write, and that route runs the real verifier and answers 401
// on a bad envelope. So by the time a row is written, this exact token has
// already been accepted by the verifier that owns it, and this function is
// reading the subject out of a credential someone else checked -- not
// asserting one this process trusts.
//
// A caller that has NOT verified the envelope first must not use this.
func EnvelopeSubject(bearer string) (string, error) {
	token := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(bearer), "Bearer "))
	segments := strings.Split(token, ".")
	if len(segments) != 3 {
		return "", fmt.Errorf("%w: it is not a three-segment JWT", ErrEnvelopeSubjectMissing)
	}
	// RawURLEncoding: a JWT segment is unpadded base64url. The VALUE is
	// never included in any error below -- a decode failure names the
	// shape, never the token.
	payload, err := base64.RawURLEncoding.DecodeString(segments[1])
	if err != nil {
		return "", fmt.Errorf("%w: its payload segment is not base64url", ErrEnvelopeSubjectMissing)
	}
	var claims struct {
		Subject string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("%w: its payload is not JSON", ErrEnvelopeSubjectMissing)
	}
	if strings.TrimSpace(claims.Subject) == "" {
		return "", ErrEnvelopeSubjectMissing
	}
	return claims.Subject, nil
}

// EnvelopeSubject reads the `sub` claim out of the envelope this
// Credential carries, for an audit row's principal_id.
//
// This is a method rather than a caller reaching for the raw bearer,
// because Credential's whole design (credential.go) is that its value is
// asked for per request and never handed back -- adding a second way out
// of the type would undo that. The value never leaves this package: it is
// read here, fed straight into the package-level EnvelopeSubject above,
// and discarded.
//
// The same verification-order requirement applies: call this only after
// the credential has already been accepted by /buildinfo.
func (c *Credential) EnvelopeSubject(ctx context.Context) (string, error) {
	bearer, err := c.value(ctx)
	if err != nil {
		return "", err
	}
	return EnvelopeSubject(bearer)
}
