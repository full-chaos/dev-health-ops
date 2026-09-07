package main

// GET /registry, and the startup routing-state drift log.
//
// Both exist because of one failure. On 2026-09-01 an SDL change (#2065,
// 33b3f3f21d) moved the canonical schema digest from sha256:67b87d38... to
// sha256:29d509cd.... Twelve go_api_routing_state rows had been seeded
// hours earlier at the OLD digest, every one of them mode=canary,
// rollout_percentage=100. PostgresSwitch keys its lookup on
// (schema_digest, document_digest, selected_operation), so from that
// commit onward every Enabled() call returned false, every request fell
// back to Python, and both planes did so EXACTLY AS DESIGNED -- a missing
// row is the documented safe default (see PostgresSwitch's doc comment).
//
// Nothing was wrong with the fail-closed behaviour. What was wrong is
// that "no operation is enabled" and "every enablement silently died"
// produced identical, invisible output. It took six days and a
// hand-written psql query to notice.
//
// The two surfaces here close that:
//
//   - logRoutingStateDrift runs once at route construction and says, in
//     one line, whether ANY row in the table is keyed to the digest this
//     binary actually computes.
//   - GET /registry lets `dev-hops go-api routing` ask this process --
//     not a checkout, not a checked-in mirror -- what it registers and
//     what digest it computed, so an enablement can REFUSE before writing
//     rows the running binary could never read.
//
// Both are built from the same digestByOperation map and schemaDigest
// newQueryHandler hands to PostgresSwitch, so neither can drift from the
// real registration set. That is deliberately the same by-construction
// discipline mountedRouteLogMessage already uses, and for the same reason:
// the hand-typed version of that log line went stale for three waves.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// registryDriftTimeout bounds the one startup drift query. This runs
// during route construction, before the server accepts connections, so an
// unbounded query against a sick registry would hang the process at boot
// with no diagnosis. A failure here is logged and ignored -- see
// logRoutingStateDrift's contract.
const registryDriftTimeout = 5 * time.Second

// registryResponse is GET /registry's body.
//
// Deliberately minimal: the schema digest this process computed, and the
// operation -> document digest map it registered. Nothing else. Every
// value here is already public in the repository (both are pure functions
// of checked-in files), which is why this route is unauthenticated
// alongside /healthz and /readyz -- but that is the reason to keep the
// body to exactly this, not an invitation to add build paths, env, or
// pool state later. It answers one question: what does the RUNNING binary
// serve?
type registryResponse struct {
	SchemaDigest string              `json:"schema_digest"`
	Operations   []registryOperation `json:"operations"`
}

type registryOperation struct {
	Operation      string `json:"operation"`
	DocumentDigest string `json:"document_digest"`
}

// newRegistryHandler serves GET /registry from the SAME schemaDigest and
// digestByOperation map newQueryHandler registers against.
//
// The response is built once, at construction, not per request: both
// inputs are fixed for the process lifetime (the digest is a pure
// function of the embedded SDL; the map is a composite literal), so
// rebuilding per request would be pure waste and would open the door to
// the two diverging. Operations are emitted in sorted order -- Go
// randomises map iteration, so an unsorted body would differ between
// requests and make a diff of two /registry responses useless.
func newRegistryHandler(schemaDigest string, digestByOperation map[string]string) http.HandlerFunc {
	operations := make([]registryOperation, 0, len(digestByOperation))
	for _, operation := range sortedOperationNames(digestByOperation) {
		operations = append(operations, registryOperation{
			Operation:      operation,
			DocumentDigest: digestByOperation[operation],
		})
	}
	body, err := json.Marshal(registryResponse{
		SchemaDigest: schemaDigest,
		Operations:   operations,
	})
	if err != nil {
		// Unreachable in practice (two string fields and a slice of
		// strings), but a silently-empty registry body would make the
		// enablement preflight refuse for the wrong reason -- and a
		// preflight that refuses for the wrong reason teaches an operator
		// to bypass it. Fail loudly at construction instead.
		log.Fatalf("query-api: /registry response could not be encoded: %v", err)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// This body is a marshalled struct of repo-derived digests -- never
		// HTML and never caller-supplied -- so the XSS-escaping advice a
		// scanner attaches to a direct ResponseWriter.Write does not apply
		// here. What DOES apply is content sniffing: nosniff makes a browser
		// honour the declared type instead of guessing one, which closes the
		// vector without pretending this endpoint renders a template.
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}
}

// logRoutingStateDrift reports, once, whether any go_api_routing_state row
// is reachable to THIS process.
//
// Best-effort by contract: /query serves requests correctly whether or not
// this query succeeds (an unreadable registry means PostgresSwitch answers
// false and everything stays on Python -- the safe default), so a failure
// here logs and returns rather than blocking startup. That is the same
// posture the Python edge's startup check takes, deliberately: this is a
// diagnostic, and a diagnostic that can take the service down is worse
// than no diagnostic.
//
// The three outcomes are kept distinct on purpose, because conflating
// them is what hid the September failure:
//
//   - rows exist at this digest       -> routing can work.
//   - table is empty                  -> nothing enabled. The legitimate
//     default posture, NOT an incident.
//   - rows exist, none at this digest -> every one of them is dead. Looks
//     identical from outside to the empty case, means the opposite.
func logRoutingStateDrift(pool *pgxpool.Pool, schemaDigest string) {
	if pool == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), registryDriftTimeout)
	defer cancel()

	rows, err := pool.Query(ctx,
		`SELECT schema_digest, count(*) FROM go_api_routing_state GROUP BY schema_digest`)
	if err != nil {
		log.Printf("query-api: routing-state drift check failed (dispatch is unaffected -- it fails closed to Python -- but this signal is absent): %v", err)
		return
	}
	defer rows.Close()

	counts := map[string]int64{}
	for rows.Next() {
		var digest string
		var count int64
		if scanErr := rows.Scan(&digest, &count); scanErr != nil {
			log.Printf("query-api: routing-state drift check failed to scan a row: %v", scanErr)
			return
		}
		counts[digest] = count
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		log.Printf("query-api: routing-state drift check failed mid-read: %v", rowsErr)
		return
	}

	for _, line := range classifyRoutingDrift(counts, schemaDigest) {
		log.Print(line)
	}
}

// classifyRoutingDrift turns a per-digest row census into the log lines that
// describe it.
//
// Split out from logRoutingStateDrift as a PURE function on purpose (codex
// r1, P3): while the decision lived inline behind a real pgxpool query, the
// only tests that could reach it were the Postgres-testcontainer ones under
// the integration tag, so inverting `live > 0` to `live == 0` left the
// unit-tier tests green. The three outcomes here are the entire point of
// this file, and the tier that runs on every `go test ./...` must be able
// to catch one being flipped.
//
// Returns one line for the live/empty cases and one per stale digest.
func classifyRoutingDrift(counts map[string]int64, schemaDigest string) []string {
	var total int64
	for _, count := range counts {
		total += count
	}

	if total == 0 {
		return []string{fmt.Sprintf("query-api: routing rows: table is empty at live schema digest %s -- no operation is enabled for Go (default posture, not an incident)", schemaDigest)}
	}
	if live := counts[schemaDigest]; live > 0 {
		return []string{fmt.Sprintf("query-api: routing rows: %d at live schema digest %s, %d at other digests", live, schemaDigest, total-live)}
	}

	// The failure this file exists for. Sorted, because Go randomises map
	// iteration and a log line whose order changes between restarts is
	// one an operator cannot diff.
	staleDigests := make([]string, 0, len(counts))
	for digest := range counts {
		staleDigests = append(staleDigests, digest)
	}
	sort.Strings(staleDigests)
	lines := make([]string, 0, len(staleDigests))
	for _, digest := range staleDigests {
		lines = append(lines, fmt.Sprintf("query-api: ROUTING ROWS STALE: %d rows at %s, 0 at %s -- these rows are keyed to a schema digest this binary does not compute, so NO operation is reachable and every request silently falls back to Python. The SDL moved after they were written. Re-run `dev-hops go-api routing enable` against THIS image; see docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md (section: When the schema digest moves)", counts[digest], digest, schemaDigest))
	}
	return lines
}
