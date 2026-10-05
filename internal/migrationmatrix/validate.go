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
	return out
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

// CheckOpsShaAncestry applies the anti-rot rule to ops_sha itself -- the
// twin of CheckFreshness's rule for the "Last verified" stamp, but for the
// value a human cannot type by hand. ops_sha is the merge-base with main
// -render observed (see the Render.OpsSha field comment); it must still be
// an ancestor of the tree -check is running against, or the committed
// artefact is asserting a history this branch does not contain.
//
// Ancestry, not equality, is the rule on purpose. main moves on every push,
// so a branch whose recorded merge-base has fallen behind the CURRENT
// merge-base is the ordinary case -- it only means nobody has re-rendered
// since main last moved, which the separate render-age budget (see
// CheckRenderAge) already bounds, and failing it here would make every PR
// that sat for more than a few minutes fail for no real reason. What
// ancestry catches is the failure age cannot: a rebase that replays this
// branch's commits onto a new base whose own history does not contain the
// commit ops_sha names (most often because it was squashed into a
// different commit on main), or a render carried forward across an
// unrelated checkout. Either way the sha stops being an ancestor of HEAD
// the moment it happens, however fresh its timestamp still reads -- which
// is exactly how a rebase with "no conflicts at all" left a stale ops_sha
// sitting behind a green -check.
//
// currentMergeBase -- what a fresh -render would write right now -- is
// reported alongside the stale value purely so the failure names the fix
// instead of making the next reader diff it out by hand; it is never
// compared for equality, per the paragraph above.
func CheckOpsShaAncestry(opsSha, headSha, currentMergeBase string, isAncestor bool) []Violation {
	if isAncestor {
		return nil
	}
	return []Violation{{
		Subject: "ops_sha",
		Rule:    "R9-ops-sha-not-ancestor",
		Detail: fmt.Sprintf(
			"ops_sha %s is not an ancestor of HEAD %s; the committed render asserts a merge-base with main "+
				"that this branch's current history does not contain (a rebase after the last render, or a "+
				"squash on main, are the usual causes). A render right now would write ops_sha %s. "+
				"Re-render: go run ./cmd/dev-health-migration-matrix -render -root .",
			opsSha, headSha, currentMergeBase),
	}}
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
