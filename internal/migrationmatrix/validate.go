package migrationmatrix

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
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
		if strings.TrimSpace(row.Proven) == "" {
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
		if row.Proven != NoProof && len(strings.TrimSpace(row.Proven)) < 8 {
			out = append(out, Violation{row.Operation, "R8-proven-ref",
				fmt.Sprintf("proven %q is too short to identify a proof run", row.Proven)})
		}
	}
	return out
}

// UnprovenReachable counts the rows a real client request can be served by
// at the CURRENT schema digest with no deployed-executed proof behind them.
// Rendered into the provenance line so the number is on the page rather than
// spread across rows a reader has to tally by eye -- the tally nobody did for
// six weeks.
func UnprovenReachable(rows []OperationRow) int {
	count := 0
	for _, row := range rows {
		if row.Live && reachableModes[row.Mode] && row.Proven == NoProof {
			count++
		}
	}
	return count
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
