package migrationmatrix

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// enablementProofStage/TerminalState mirror
// src/dev_health_ops/api/graphql/go_api_routing_admin.py's
// ENABLEMENT_PROOF_STAGE / ENABLEMENT_PROOF_TERMINAL_STATE. "proven" is
// DERIVED from go_api_proof_run, never stored as a column -- so this is the
// only place the derivation lives on the Go side, and it is deliberately the
// same predicate the enablement command uses: a proof is evidence for one
// immutable (schema_digest, document_digest, selected_operation,
// candidate_build) tuple and is never carried forward across any of the four.
const (
	enablementProofStage         = "deployed_executed"
	enablementProofTerminalState = "match"
)

// routingStateQuery reads every routing row -- live AND dead. Dead rows (rows
// at a schema digest other than the current pin) are the whole reason this
// table is rendered at all: on 2026-09-01 a digest move killed twelve canary
// rows silently, and it was found six days later. A page that shows only the
// live rows would have shown nothing wrong.
const routingStateQuery = `
SELECT rs.selected_operation,
       rs.mode,
       rs.schema_digest,
       rs.current_candidate_build,
       (
         SELECT pr.id::text
         FROM go_api_proof_run pr
         WHERE pr.schema_digest = rs.schema_digest
           AND pr.document_digest = rs.document_digest
           AND pr.selected_operation = rs.selected_operation
           AND pr.candidate_build = rs.current_candidate_build
           AND pr.stage = $1
           AND pr.terminal_state = $2
         ORDER BY pr.observed_at DESC
         LIMIT 1
       ) AS proof_run_id
FROM go_api_routing_state rs
ORDER BY rs.schema_digest, rs.selected_operation
`

// ReadRoutingState reads go_api_routing_state, deriving `proven` per row.
// currentDigest decides which rows are Live.
func ReadRoutingState(ctx context.Context, dsn, currentDigest string) ([]OperationRow, int, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, 0, fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	rows, err := conn.Query(ctx, routingStateQuery, enablementProofStage, enablementProofTerminalState)
	if err != nil {
		return nil, 0, fmt.Errorf("read go_api_routing_state: %w", err)
	}
	defer rows.Close()

	var out []OperationRow
	for rows.Next() {
		var (
			row     OperationRow
			proofID *string
		)
		if err := rows.Scan(&row.Operation, &row.Mode, &row.SchemaDigest, &row.CandidateBuild, &proofID); err != nil {
			return nil, 0, fmt.Errorf("scan go_api_routing_state row: %w", err)
		}
		row.Live = row.SchemaDigest == currentDigest
		row.Proven = NoProof
		if proofID != nil && *proofID != "" {
			row.Proven = *proofID
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate go_api_routing_state: %w", err)
	}

	// The bare total is reported alongside the per-row derivation on
	// purpose. "0 rows in go_api_proof_run" is a single number that
	// falsifies every "proven" claim on the page at once, and it does not
	// depend on any join being right.
	var proofTotal int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM go_api_proof_run`).Scan(&proofTotal); err != nil {
		return nil, 0, fmt.Errorf("count go_api_proof_run: %w", err)
	}
	return out, proofTotal, nil
}

// SchemaDigestPin reads contracts/graphql/v1/schema-digest.json -- the pinned
// canonical digest every routing row is keyed by.
func SchemaDigestPin(path string) (string, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // caller-supplied repo path
	if err != nil {
		return "", fmt.Errorf("read schema digest pin: %w", err)
	}
	var pin struct {
		SchemaDigest string `json:"schema_digest"`
	}
	if err := json.Unmarshal(raw, &pin); err != nil {
		return "", fmt.Errorf("parse %s: %w", path, err)
	}
	if !digestRe.MatchString(pin.SchemaDigest) {
		return "", fmt.Errorf("%s: schema_digest %q is not sha256:<64 hex>", path, pin.SchemaDigest)
	}
	return pin.SchemaDigest, nil
}

// FleetReading is what one `docker inspect` sweep of the running fleet saw.
type FleetReading struct {
	// Revision is the single revision every inspected container agreed on,
	// or UnknownRevision. A DISAGREEMENT is not averaged away -- see
	// Disagreement.
	Revision string
	// PerContainer maps container name -> label value, verbatim.
	PerContainer map[string]string
	// Disagreement is set when the inspected containers do not all carry
	// the same revision. A split fleet is a real and dangerous state (the
	// migrate-ahead-of-workers lockstep failure, CHAOS-5437/5457), so it is
	// surfaced rather than collapsed.
	Disagreement string
	ReadAt       time.Time
	Source       string
}

// ReadFleetRevisions runs `docker inspect` over the named containers and
// reads org.opencontainers.image.revision from each.
//
// On a local compose build this returns "unknown", because compose never
// passes the COMMIT build-arg the Dockerfiles' LABEL interpolates. That is
// not a bug in this function and must not be worked around: "unknown" is the
// true answer to "what commit is the running fleet?", and a matrix that
// substituted a guess would be exactly the tracker that could say "done".
func ReadFleetRevisions(ctx context.Context, containers []string) (*FleetReading, error) {
	if len(containers) == 0 {
		return nil, fmt.Errorf("no containers named")
	}
	reading := &FleetReading{
		PerContainer: map[string]string{},
		ReadAt:       time.Now().UTC(),
		Source:       "docker inspect " + strings.Join(containers, " "),
	}
	args := append([]string{"inspect", "-f", `{{index .Config.Labels "org.opencontainers.image.revision"}}`}, containers...)
	cmd := exec.CommandContext(ctx, "docker", args...)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("docker inspect: %w", err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != len(containers) {
		return nil, fmt.Errorf("docker inspect returned %d lines for %d containers", len(lines), len(containers))
	}
	distinct := map[string]bool{}
	for i, name := range containers {
		value := strings.TrimSpace(lines[i])
		if value == "" || value == "<no value>" {
			value = UnknownRevision
		}
		reading.PerContainer[name] = value
		distinct[value] = true
	}
	if len(distinct) == 1 {
		for value := range distinct {
			reading.Revision = value
		}
		return reading, nil
	}
	values := make([]string, 0, len(distinct))
	for value := range distinct {
		values = append(values, value)
	}
	sort.Strings(values)
	reading.Revision = UnknownRevision
	reading.Disagreement = fmt.Sprintf("the fleet is split across %d revisions: %s", len(values), strings.Join(values, ", "))
	return reading, nil
}
