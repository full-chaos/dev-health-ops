package migrationmatrix

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// "proven" is DERIVED from go_api_proof_run, never stored as a column: a
// proof is evidence for one immutable (schema_digest, document_digest,
// selected_operation, candidate_build) tuple and is never carried forward
// across any of the four.
//
// The rule itself is NOT restated here. It used to be -- a local
// stage/terminal-state pair with a comment claiming it was "deliberately
// the same predicate the enablement command uses". That was true when
// written and false the moment CHAOS-5484 split the rule by target mode,
// and nothing failed: this page went on rendering a primary proof-route
// receipt PROVEN while enablement refused it, and a canary cited mismatch
// UNPROVEN while enablement accepted it (codex r2, P1, reproduced against
// live PostgreSQL).
//
// A copy of a rule is a copy that will drift. goapiproof.EnablementProofClause
// is the one source; this file composes it.

// routingStateQuery reads every routing row -- live AND dead. Dead rows (rows
// at a schema digest other than the current pin) are the whole reason this
// table is rendered at all: on 2026-09-01 a digest move killed twelve canary
// rows silently, and it was found six days later. A page that shows only the
// live rows would have shown nothing wrong.
// routingStateQuery is ONE statement, and that is the point.
//
// CHAOS-5484 briefly made it two -- one per target mode, partitioned by
// `WHERE (rs.mode = 'primary') = $1` -- because admissibility depends on
// where the row IS. opus r5 (P2) showed what that cost: two queries on
// one connection with no enclosing transaction are two snapshots, so a
// row whose mode changes between them is returned TWICE or NOT AT ALL.
// Measured with 60 rows and a concurrent updater: 60 duplicate reads, 4
// missing reads. Downstream that is not cosmetic -- ValidateRender's
// R8-duplicate-row rule fails the render, on the one page whose stated
// reason for existing is that a silent death should not look like health.
//
// The mode rule lives in a CASE instead, so every row is judged by its
// own mode inside a single snapshot. Both arms are
// goapiproof.EnablementProofClause, which is the one source the whole
// change is built around -- the CASE selects between them, it does not
// restate either.
//
// RoutingStateSQL is how an operator OBTAINS this statement, and it is the
// statement ReadRoutingState itself executes: `-print-routing-sql` prints
// it, the operator runs it with `psql -At`, and the file psql writes is the
// `-routing` snapshot, unedited.
//
// It emits the WHOLE snapshot payload -- `{"proof_run_total": n, "rows":
// [...]}` -- as one JSON value from one statement, and ReadRoutingState
// parses that value with ParseRoutingSnapshot, the same function the
// offline reader uses. So the live and offline paths share not only the
// rule but the statement, the snapshot and the parser.
//
// Why the whole payload (opus r6, P3-3). The r5 fix made the ROW statement
// printable, but the -routing reader parses a JSON object: executed
// literally, `-print-routing-sql | psql -At -f -` produced pipe-separated
// text the reader rejected (`invalid character 'e' in literal false`), and
// the operator still had to hand-write the JSON wrapper AND a second query
// for proof_run_total -- the retyping this function exists to end. Emitting
// the payload from SQL also puts proof_run_total in the SAME snapshot as
// the rows; it used to be a second query on the connection (Trap #141's
// shape, for the one number the page prints first).
//
// Before opus r5 the offline instruction named an unexported function, so
// the only way to comply was to retype the query by hand -- which is the
// copy-of-a-rule failure this file's header is about, moved into the
// operator's terminal.
func RoutingStateSQL() (string, error) {
	primaryClause, err := goapiproof.EnablementProofClause("pr", goapiproof.TargetModePrimary)
	if err != nil {
		return "", fmt.Errorf("build the primary enablement predicate: %w", err)
	}
	canaryClause, err := goapiproof.EnablementProofClause("pr", goapiproof.TargetModeCanary)
	if err != nil {
		return "", fmt.Errorf("build the canary enablement predicate: %w", err)
	}
	return routingSnapshotQuery(routingStateQuery(primaryClause, canaryClause)), nil
}

// routingSnapshotQuery wraps the row statement so it returns the snapshot
// payload as ONE text value. The keys are routingSnapshot's JSON tags; the
// column names of routingStateQuery ARE those keys, so json_agg(t) needs no
// mapping that could drift. `coalesce(..., '[]')` so an empty table yields
// `"rows": []`, which the offline reader then refuses BY NAME rather than
// as a JSON null.
func routingSnapshotQuery(rowQuery string) string {
	return `
SELECT json_build_object(
         'proof_run_total', (SELECT count(*) FROM go_api_proof_run),
         'rows', coalesce(
                   json_agg(t ORDER BY t.schema_digest, t.selected_operation, t.document_digest),
                   '[]'::json)
       )::text
FROM (` + rowQuery + `) AS t
`
}

func routingStateQuery(primaryClause, canaryClause string) string {
	return `
SELECT rs.selected_operation,
       rs.document_digest,
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
           AND CASE WHEN rs.mode = '` + goapiproof.TargetModePrimary + `'
                    THEN (` + primaryClause + `)
                    ELSE (` + canaryClause + `)
               END
         ORDER BY pr.observed_at DESC
         LIMIT 1
       ) AS proof_run_id
FROM go_api_routing_state rs
`
}

// routingSnapshot is the -routing file, and the value RoutingStateSQL
// returns: the offline equivalent of a live read, byte-for-byte the same
// shape. It exists because the operator running -render is often on a host
// where the compose Postgres is reachable only through `docker exec psql`,
// and teaching a docs generator to dig a password out of a container's
// environment is a credential-handling path it should not own.
type routingSnapshot struct {
	// ProofRunTotal is `SELECT count(*) FROM go_api_proof_run`, read in
	// the same statement as the rows.
	ProofRunTotal *int `json:"proof_run_total"`
	Rows          []struct {
		Operation string `json:"selected_operation"`
		// DocumentDigest is part of the routing-state KEY, not a
		// decoration: R8 keys rows by schema/document/operation and R14
		// judges DOCUMENT_DRIFT by it, so a snapshot that omits it renders
		// two distinct documents as one duplicated row and cannot say
		// whether either is dispatchable (Trap #120).
		DocumentDigest string `json:"document_digest"`
		Mode           string `json:"mode"`
		SchemaDigest   string `json:"schema_digest"`
		CandidateBuild string `json:"current_candidate_build"`
		// ProofRunID is the derived proof-run id; null or empty when no
		// receipt is admissible for the row's own mode.
		ProofRunID *string `json:"proof_run_id"`
	} `json:"rows"`
}

// ParseRoutingSnapshot parses the payload RoutingStateSQL emits into rows,
// deriving Live from currentDigest. ReadRoutingState and the offline
// -routing reader both call it, so they cannot read one snapshot two ways.
//
// Refuses a payload with no proof_run_total, or a row without the routing
// key's document digest: both are what a HAND-BUILT snapshot omits, and
// both used to be read as a zero and an empty key.
func ParseRoutingSnapshot(raw []byte, currentDigest string) ([]OperationRow, int, error) {
	var payload routingSnapshot
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&payload); err != nil {
		return nil, 0, fmt.Errorf("parse routing snapshot: %w (produce it with `dev-health-migration-matrix -print-routing-sql | psql -At -f -`, unedited)", err)
	}
	if payload.ProofRunTotal == nil {
		return nil, 0, fmt.Errorf("routing snapshot has no proof_run_total: that number falsifies every proven cell on the page at once, so it is never assumed to be 0 -- regenerate the snapshot with `-print-routing-sql`")
	}
	out := make([]OperationRow, 0, len(payload.Rows))
	for _, row := range payload.Rows {
		if strings.TrimSpace(row.DocumentDigest) == "" {
			return nil, 0, fmt.Errorf(
				"routing snapshot row %q at schema %q has no document_digest; "+
					"the routing key is (schema_digest, document_digest, selected_operation), "+
					"so a row without one cannot be told apart from another document's row. "+
					"Regenerate the snapshot with `-print-routing-sql`",
				row.Operation, row.SchemaDigest)
		}
		proven := NoProof
		if row.ProofRunID != nil && strings.TrimSpace(*row.ProofRunID) != "" {
			proven = strings.TrimSpace(*row.ProofRunID)
		}
		out = append(out, OperationRow{
			Operation:      row.Operation,
			DocumentDigest: row.DocumentDigest,
			Mode:           row.Mode,
			SchemaDigest:   row.SchemaDigest,
			CandidateBuild: row.CandidateBuild,
			Live:           row.SchemaDigest == currentDigest,
			Proven:         proven,
		})
	}

	// Stable: two rows sharing (schema digest, operation) differ only by
	// document, and an unstable sort would flip the render between runs on
	// identical data (opus r5, P2). The statement already orders by the
	// triple; a hand-assembled file need not, so the order is re-imposed
	// here rather than trusted.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].SchemaDigest != out[j].SchemaDigest {
			return out[i].SchemaDigest < out[j].SchemaDigest
		}
		if out[i].Operation != out[j].Operation {
			return out[i].Operation < out[j].Operation
		}
		return out[i].DocumentDigest < out[j].DocumentDigest
	})
	return out, *payload.ProofRunTotal, nil
}

// ReadRoutingState reads go_api_routing_state, deriving `proven` per row.
// currentDigest decides which rows are Live.
func ReadRoutingState(ctx context.Context, dsn, currentDigest string) ([]OperationRow, int, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, 0, fmt.Errorf("connect to postgres: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	// The SAME call the -print-routing-sql flag makes: an operator's
	// offline rows and this reader's rows cannot be produced by different
	// statements, because there is only one statement.
	statement, err := RoutingStateSQL()
	if err != nil {
		return nil, 0, err
	}

	// ONE statement, ONE value: the rows and the bare proof total come from
	// the same snapshot. The total is reported alongside the per-row
	// derivation on purpose -- "0 rows in go_api_proof_run" is a single
	// number that falsifies every "proven" claim on the page at once, and
	// it does not depend on any join being right.
	var payload string
	if err := conn.QueryRow(ctx, statement).Scan(&payload); err != nil {
		return nil, 0, fmt.Errorf("read go_api_routing_state: %w", err)
	}
	return ParseRoutingSnapshot([]byte(payload), currentDigest)
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
