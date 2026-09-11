package migrationmatrix

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

var (
	shaRe    = regexp.MustCompile(`^[0-9a-f]{40}$`)
	ticketRe = regexp.MustCompile(`^CHAOS-[0-9]{1,6}$`)
	digestRe = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// reachableModes are the modes in which a REAL client request can be served
// by Go. Kept in sync with cmd/query-api/internal/routeswitch's own
// reachableModes: "shadow" executes but the client still gets Python's
// answer, so it is not reachable in this sense.
var reachableModes = map[string]bool{"canary": true, "primary": true}

// Violation is one failed rule, addressed to the person who has to fix it.
type Violation struct {
	// Subject is the row the rule failed on (a family name, an operation
	// name, or a document field).
	Subject string
	// Rule is the short id of the rule, for grepping a CI log.
	Rule string
	// Detail says what was found and what was wanted.
	Detail string
}

func (v Violation) String() string {
	return fmt.Sprintf("[%s] %s: %s", v.Rule, v.Subject, v.Detail)
}

// ValidateLedger applies every "a row must not claim more than it can show"
// rule to the curated ledger, cross-checked against the executing-language
// authority. Returns every violation, not just the first -- one CI run should
// name every lie on the page.
func ValidateLedger(ledger *StatusLedger, families *NativeFamilies) []Violation {
	var out []Violation

	known := map[string]FamilyKey{}
	for _, key := range families.Keys() {
		known[key.ID()] = key
	}

	// R1 coverage, both directions. A family that exists in the worker but
	// has no status row is exactly how the old matrix stayed silent about
	// correctness: the row simply was not there to be wrong.
	for id := range known {
		if _, ok := ledger.Families[id]; !ok {
			out = append(out, Violation{
				Subject: id,
				Rule:    "R1-missing-status",
				Detail: "native-families.json has this family but status.json does not; " +
					"add a row (parity.status \"unverified\" is the honest default)",
			})
		}
	}
	for id := range ledger.Families {
		if _, ok := known[id]; !ok {
			out = append(out, Violation{
				Subject: id,
				Rule:    "R1-unknown-family",
				Detail: "status.json has this family but native-families.json does not; " +
					"the family was renamed, deleted, or the \"<scope>/<family>\" key is misspelled",
			})
		}
	}

	for _, name := range sortedKeys(ledger.Families) {
		out = append(out, validateFamily(name, ledger.Families[name])...)
	}
	return out
}

func validateFamily(name string, entry FamilyStatus) []Violation {
	var out []Violation

	// R2 parity evidence. The whole point: "verified" without a ticket, a
	// sha and an evidence path is the claim that was made twenty times.
	switch entry.Parity.Status {
	case ParityVerified:
		if !ticketRe.MatchString(entry.Parity.Ticket) {
			out = append(out, Violation{name, "R2-parity-ticket",
				fmt.Sprintf("parity.status=verified needs a CHAOS-<n> ticket, got %q", entry.Parity.Ticket)})
		}
		if !shaRe.MatchString(entry.Parity.Sha) {
			out = append(out, Violation{name, "R2-parity-sha",
				fmt.Sprintf("parity.status=verified needs the 40-hex sha of the A/B or regression run, got %q", entry.Parity.Sha)})
		}
		if strings.TrimSpace(entry.Parity.Evidence) == "" {
			out = append(out, Violation{name, "R2-parity-evidence",
				"parity.status=verified needs an evidence path or run id a reader can open"})
		}
	case ParityDiverged:
		if !ticketRe.MatchString(entry.Parity.Ticket) {
			out = append(out, Violation{name, "R2-parity-ticket",
				fmt.Sprintf("parity.status=diverged needs the CHAOS-<n> ticket tracking the divergence, got %q", entry.Parity.Ticket)})
		}
		if strings.TrimSpace(entry.Parity.Evidence) == "" {
			out = append(out, Violation{name, "R2-parity-evidence",
				"parity.status=diverged needs an evidence path or run id showing the mismatch"})
		}
		// R5 a diverged family must appear in its own open-regressions list.
		if !contains(entry.OpenRegressions, entry.Parity.Ticket) {
			out = append(out, Violation{name, "R5-diverged-not-open",
				fmt.Sprintf("parity.status=diverged but %q is not in open_regressions", entry.Parity.Ticket)})
		}
	case ParityNotApplicable:
		if strings.TrimSpace(entry.Parity.Evidence) == "" {
			out = append(out, Violation{name, "R2-parity-evidence",
				"parity.status=n/a needs an evidence note saying why there is nothing to compare against"})
		}
	case ParityUnverified:
		// The honest default. Ticket/sha/evidence optional.
	default:
		out = append(out, Violation{name, "R2-parity-status",
			fmt.Sprintf("unknown parity.status %q (want verified|diverged|unverified|n/a)", entry.Parity.Status)})
	}

	// R3 deployed revision. Either a real 40-hex sha or the literal
	// "unknown" -- never a branch name, never "main", never "latest", and
	// never a sha with no timestamp saying when the label was read.
	switch {
	case entry.Deployed.Revision == UnknownRevision:
		// Honest. Still needs a read time: "unknown" is a reading too.
	case shaRe.MatchString(entry.Deployed.Revision):
	default:
		out = append(out, Violation{name, "R3-deployed-revision",
			fmt.Sprintf("deployed.revision must be a 40-hex sha or the literal %q, got %q", UnknownRevision, entry.Deployed.Revision)})
	}
	if entry.Deployed.ReadAt.IsZero() {
		out = append(out, Violation{name, "R3-deployed-read-at",
			"deployed.read_at is empty; a revision with no read time is a memory, not a fact"})
	}
	if strings.TrimSpace(entry.Deployed.Source) == "" {
		out = append(out, Violation{name, "R3-deployed-source",
			"deployed.source must name what was read (e.g. \"docker inspect dev-health-go-worker-1\")"})
	}

	// R4 proof. "none" is allowed and expected; anything else must be a
	// substantive reference, and a proof cannot be claimed for a family
	// whose deployed revision is unknown -- proof is against a BUILD.
	proof := strings.TrimSpace(entry.Proof.Ref)
	switch {
	case proof == "":
		out = append(out, Violation{name, "R4-proof-empty",
			fmt.Sprintf("proof.ref is empty; write %q when there is no proof run", NoProof)})
	case proof == NoProof:
	default:
		if len(proof) < 8 {
			out = append(out, Violation{name, "R4-proof-ref",
				fmt.Sprintf("proof.ref %q is too short to identify a run; use a run id or an evidence path", proof)})
		}
		if entry.Deployed.Revision == UnknownRevision {
			out = append(out, Violation{name, "R4-proof-without-build",
				"proof.ref claims a deployed-executed run while deployed.revision is \"unknown\"; " +
					"a proof is evidence for one BUILD, so it cannot be recorded against a build nobody can name"})
		}
	}

	// R6 open regressions are ticket ids, not prose.
	for _, ticket := range entry.OpenRegressions {
		if !ticketRe.MatchString(ticket) {
			out = append(out, Violation{name, "R6-regression-ticket",
				fmt.Sprintf("open_regressions entry %q is not a CHAOS-<n> id", ticket)})
		}
	}
	return out
}

// ValidateRender applies the rules the live snapshot must satisfy. The one
// that matters: an operation reachable to a real client (canary/primary) with
// no proof run is allowed -- that is today's true state -- but it must be
// rendered as UNPROVEN, never as a bare mode.
func ValidateRender(render *Render) []Violation {
	var out []Violation

	if render.RenderedAt.IsZero() {
		out = append(out, Violation{"render", "R7-rendered-at", "rendered_at is empty"})
	}
	if !shaRe.MatchString(render.OpsSha) {
		out = append(out, Violation{"render", "R7-ops-sha",
			fmt.Sprintf("ops_sha must be a 40-hex sha, got %q", render.OpsSha)})
	}
	if !digestRe.MatchString(render.SchemaDigest) {
		out = append(out, Violation{"render", "R7-schema-digest",
			fmt.Sprintf("schema_digest must be sha256:<64 hex>, got %q", render.SchemaDigest)})
	}
	if strings.TrimSpace(render.FleetSource) == "" {
		out = append(out, Violation{"render", "R7-fleet-source", "fleet_source must name how the fleet was read"})
	}

	// Keyed on the TRIPLE, because go_api_routing_state is: two rows for
	// one operation at one schema digest, differing only by document, are
	// a real and named state -- the Python status surface calls it
	// DOCUMENT_DRIFT. Keyed on (digest, operation) this rule reported the
	// legitimate shape as a duplicate and failed the render (opus r5, P2),
	// which is the same silence one level down: the page whose job is to
	// show a row nobody noticed refused to show two.
	//
	// A genuine duplicate -- the SAME document twice -- is still a
	// violation, and still says so.
	seen := map[string]bool{}
	for _, row := range render.Operations {
		key := row.SchemaDigest + "/" + row.DocumentDigest + "/" + row.Operation
		if seen[key] {
			out = append(out, Violation{row.Operation, "R8-duplicate-row",
				fmt.Sprintf("two rows for operation %q at digest %s document %s", row.Operation, row.SchemaDigest, row.DocumentDigest)})
		}
		seen[key] = true

		if !digestRe.MatchString(row.SchemaDigest) {
			out = append(out, Violation{row.Operation, "R8-digest",
				fmt.Sprintf("schema_digest must be sha256:<64 hex>, got %q", row.SchemaDigest)})
		}
		// NamesNothing, not strings.TrimSpace: a proof id is a routing-proof
		// value, and every blank judgment over one uses the single definition
		// (opus r6 P2-1, swept as a class).
		if goapiproof.NamesNothing(row.Proven) {
			out = append(out, Violation{row.Operation, "R8-proven-empty",
				fmt.Sprintf("proven is empty; write %q when no proof run matches this exact tuple", NoProof)})
		}
		// A live, reachable, unproven row is NOT a violation: it is the
		// program's actual state today (15 routed operations, zero rows in
		// go_api_proof_run), and a gate that fails on the truth teaches
		// people to route around the gate. It is handled by RENDERING --
		// the mode cell reads "canary / UNPROVEN" and the provenance line
		// carries the count -- and the doc-drift rule makes that rendering
		// impossible to soften by hand. What IS a violation is a proof
		// reference too short to identify a run, checked below.
		if row.Proven != NoProof && !goapiproof.NamesNothing(row.Proven) && len(row.Proven) < 8 {
			out = append(out, Violation{row.Operation, "R8-proven-ref",
				fmt.Sprintf("proven %q is too short to identify a proof run", row.Proven)})
		}
	}
	return out
}

// DocumentDrift reports whether a LIVE row serves a document the catalog
// does not name: the DOCUMENT_DRIFT state `dev-hops go-api routing status`
// reports. The edge resolves a request to an operation through the
// catalog, so such a row can never be dispatched, whatever its mode says.
//
// A row with NO document digest is not judged here -- see
// DocumentUnjudged. Only a snapshot written before the reader carried the
// routing key can hold one: ReadRoutingState scans a NOT NULL column and
// the offline -routing reader refuses an empty one.
func DocumentDrift(row OperationRow, catalog Catalog) bool {
	return row.Live && !goapiproof.NamesNothing(row.DocumentDigest) && !catalog.Names(row.Operation, row.DocumentDigest)
}

// DocumentUnjudged reports whether a LIVE row carries no document digest,
// so whether it drifted cannot be decided from this snapshot. Counted on the
// page rather than assumed either way: calling such a row served would be
// the silence opus r6 (P2-2) found, and calling it drifted would fail every
// page rendered before the key was carried.
func DocumentUnjudged(row OperationRow) bool {
	return row.Live && goapiproof.NamesNothing(row.DocumentDigest)
}

// UnprovenReachable counts the rows a real client request can be served by
// at the CURRENT schema digest with no deployed-executed proof behind them.
// Rendered into the provenance line so the number is on the page rather than
// spread across rows a reader has to tally by eye -- the tally nobody did for
// six weeks.
//
// A DOCUMENT_DRIFT row is NOT counted: the edge cannot dispatch it, so no
// client reaches it. It has its own count (DriftedLive), because folding it
// in here would report an undispatchable row as a served one -- the exact
// misreading P2-2 found on this page.
func UnprovenReachable(rows []OperationRow, catalog Catalog) int {
	count := 0
	for _, row := range rows {
		if row.Live && reachableModes[row.Mode] && row.Proven == NoProof && !DocumentDrift(row, catalog) {
			count++
		}
	}
	return count
}

// The two states `dev-hops go-api routing status` names for a live row the
// edge cannot dispatch.
const (
	StateDocumentDrift = "DOCUMENT_DRIFT"
	StateUnregistered  = "UNREGISTERED"
)

// DispatchState names a row's state the way `routing status` does:
// UNREGISTERED when the catalog does not register the operation at all,
// DOCUMENT_DRIFT when it registers it at another document, "" otherwise.
// ONE function for the cell, the counts and R14 (opus r8 P3-2: the cell and
// the count said DOCUMENT_DRIFT for both while R14 and status did not).
func DispatchState(row OperationRow, catalog Catalog) string {
	if !DocumentDrift(row, catalog) {
		return ""
	}
	if len(catalog.Documents(row.Operation)) == 0 {
		return StateUnregistered
	}
	return StateDocumentDrift
}

// DriftedLive counts the live rows in the given dispatch state, in any
// mode.
func DriftedLive(rows []OperationRow, catalog Catalog, state string) int {
	count := 0
	for _, row := range rows {
		if DispatchState(row, catalog) == state {
			count++
		}
	}
	return count
}

// sqlLiteral renders a value as a standard SQL string literal, a quote
// doubled, for the remedy R14 prints for an operator to run (opus r8 P3-3:
// interpolated raw, an operation named `x' OR ”='` printed a statement
// that deleted every routing row). Row values are text columns, so there is
// no NUL to handle; with standard_conforming_strings on (PostgreSQL's
// default since 9.1) a backslash inside '...' is an ordinary character.
func sqlLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// UnjudgedLive counts the live rows whose drift cannot be judged.
func UnjudgedLive(rows []OperationRow) int {
	count := 0
	for _, row := range rows {
		if DocumentUnjudged(row) {
			count++
		}
	}
	return count
}

// ValidateDocumentDrift fails the render for every live row in
// DOCUMENT_DRIFT (rule R14).
//
// Loud on purpose, and not only rendered. At the base build two rows for
// one operation at one schema digest failed R8 and so failed the page --
// loud, if by accident. Keying R8 on the triple (correctly) removed that,
// and the drifted row then rendered as serving with -check green: a loud
// failure turned into a quiet false statement (opus r6, P2-2). A row the
// edge cannot dispatch while its mode says it serves is an operator action
// owed (disable it, or re-enable at the catalog's document) in the same
// way R12's moved digest is, so it fails the same way. `routing status`
// names the row; this page names it AND refuses to be committed with it.
func ValidateDocumentDrift(render *Render, catalog Catalog) []Violation {
	var out []Violation
	for _, row := range render.Operations {
		if !DocumentDrift(row, catalog) {
			continue
		}
		// The state `dev-hops go-api routing status` names for this row:
		// DOCUMENT_DRIFT when the catalog registers the operation at another
		// document, UNREGISTERED when it does not register the operation at
		// all (opus r7 P2-1: this message used to claim DOCUMENT_DRIFT for
		// both, and status named the second nowhere).
		state := DispatchState(row, catalog)
		want := "the catalog does not register this operation at all"
		if state == StateDocumentDrift {
			want = "the catalog names " + strings.Join(catalog.Documents(row.Operation), ", ")
		}
		// The remedy is the one the shipped verbs can perform (opus r7
		// P3-2): `disable` keys on the CATALOG's document, so it cannot
		// reach this row, and re-enabling writes the catalog's row beside
		// it. Until a disable-by-document verb exists, the row is removed by
		// its full key.
		out = append(out, Violation{row.Operation, "R14-document-drift",
			fmt.Sprintf("a live %s row at digest %s serves document %s, but %s: the edge resolves requests through the catalog, so this row cannot be dispatched and every request for it is served elsewhere. `dev-hops go-api routing status` reports it %s. No shipped verb reaches it (`routing disable` keys on the catalog's document): remove it by its full key -- DELETE FROM go_api_routing_state WHERE schema_digest = %s AND document_digest = %s AND selected_operation = %s -- then re-render",
				row.Mode, row.SchemaDigest, row.DocumentDigest, want, state, sqlLiteral(row.SchemaDigest), sqlLiteral(row.DocumentDigest), sqlLiteral(row.Operation))})
	}
	return out
}

// DeadReachable counts rows whose mode says a client can be served by Go but
// whose schema digest no longer matches the pin -- so the router can never
// match them and every request falls back to Python, silently. A non-zero
// count here is the 2026-09-01 failure still on the board.
func DeadReachable(rows []OperationRow) int {
	count := 0
	for _, row := range rows {
		if !row.Live && reachableModes[row.Mode] {
			count++
		}
	}
	return count
}

// CheckFreshness applies the anti-rot rule to the page's own verification
// stamp: the sha must be real, must be an ancestor of HEAD, and must not be
// older than maxAge. A page whose "Last verified" sha is five days and forty
// merges behind main is not verified, whatever it says.
//
// The ancestry and date lookups are injected so this stays a pure function --
// the git calls live in the caller.
func CheckFreshness(lastVerified string, commitDate, now time.Time, isAncestor bool, maxAge time.Duration) []Violation {
	var out []Violation
	if !shaRe.MatchString(lastVerified) {
		out = append(out, Violation{"Last verified", "R10-sha-shape",
			fmt.Sprintf("must be a full 40-hex sha, got %q (an abbreviated sha cannot be checked for ancestry)", lastVerified)})
		return out
	}
	if !isAncestor {
		out = append(out, Violation{"Last verified", "R10-not-ancestor",
			fmt.Sprintf("%s is not an ancestor of HEAD; the page was verified against a commit this branch does not contain", lastVerified)})
	}
	if commitDate.IsZero() {
		out = append(out, Violation{"Last verified", "R10-no-date",
			fmt.Sprintf("could not read a committer date for %s", lastVerified)})
		return out
	}
	if age := now.Sub(commitDate); age > maxAge {
		out = append(out, Violation{"Last verified", "R10-stale",
			fmt.Sprintf("%s is %s old, over the %s limit; re-verify the hand-curated rows and bump the stamp",
				lastVerified, roundDays(age), roundDays(maxAge))})
	}
	return out
}

// CheckRenderAge applies the same anti-rot rule to the live snapshot: a
// committed page carrying a two-week-old reading of the fleet and the routing
// table is telling you about a fleet that no longer exists.
func CheckRenderAge(renderedAt, now time.Time, maxAge time.Duration) []Violation {
	if renderedAt.IsZero() {
		return []Violation{{"last-render.json", "R11-no-timestamp", "rendered_at is empty"}}
	}
	if age := now.Sub(renderedAt); age > maxAge {
		return []Violation{{"last-render.json", "R11-stale",
			fmt.Sprintf("the live sources were last read %s ago, over the %s limit; "+
				"re-run `dev-health-migration-matrix -render`", roundDays(age), roundDays(maxAge))}}
	}
	return nil
}

func roundDays(d time.Duration) string {
	if d < 48*time.Hour {
		return d.Round(time.Hour).String()
	}
	return fmt.Sprintf("%.1fd", d.Hours()/24)
}

func contains(haystack []string, needle string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// FormatViolations renders violations as a CI-readable block.
func FormatViolations(violations []Violation) string {
	if len(violations) == 0 {
		return ""
	}
	var b strings.Builder
	for _, v := range violations {
		b.WriteString("  " + v.String() + "\n")
	}
	return b.String()
}
