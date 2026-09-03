// Package issueprlinks produces the PR<->issue MAPPING rows in ClickHouse
// `work_graph_issue_pr` from provider-attached `work_item_dependencies` rows.
//
// # Why this package exists
//
// The mapping is the provider's OWN statement that a pull request belongs to a
// tracker issue -- a Linear attachment, a GitHub closing reference, a Jira
// dev-status link. Text parsing of PR titles/bodies is a FALLBACK for when no
// such statement exists; it is not this package's job.
//
// Until CHAOS-4757 the mapping was produced by Python
// `work_graph/builder.py::_derive_issue_pr_links_from_dependencies`, even
// though the syncs and providers that capture the attachments are all Go
// (`internal/providersync`). This package is the Go producer; the Python one is
// retired in the same PR set. Exactly ONE implementation of this output may be
// write-capable at a time -- `work_graph_issue_pr` is a ReplacingMergeTree
// whose version column is `last_synced`, so two producers stamping different
// timestamps for one key makes the surviving row depend on execution order.
//
// # What this package does NOT do
//
// It writes ONLY `provenance = "native"` rows. The `explicit_text` and
// `heuristic` rows are still written by Python
// (`_build_issue_pr_edges`, `_build_heuristic_issue_pr_edges`) and move with
// lane-4752-go's port of the edge half of the builder. The two producers are
// disjoint by provenance, NOT by key: they can collide on
// (org_id, repo_id, work_item_id, pr_number), and because the fallback writers
// stamp build time while this one stamps the dependency row's earlier
// `last_synced`, a fallback row wins such a collision. That precedence
// inversion predates this package and is tracked as CHAOS-4769 -- it is
// deliberately NOT corrected here, because changing the stamp would hide a
// behaviour change inside a port whose whole claim is row-for-row parity with
// the producer it replaces.
package issueprlinks

import (
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Provider names carried through to the parse result. These are the values
// Python's _parse_pr_dependency_source returns as the third tuple element.
const (
	ProviderGitHub = "github"
	ProviderGitLab = "gitlab"
)

// ProvenanceNative is the only provenance this package writes. The other two
// values in the column's vocabulary (`explicit_text`, `heuristic`) belong to
// the Python fallback writers; see the package doc.
const ProvenanceNative = "native"

// NativeConfidence is the confidence every provider-attached row carries.
// The column is Float32 in ClickHouse (verified against system.columns, db
// `default`), so the value is held as float32 all the way to the insert -- a
// float64 that round-trips through the driver can differ in the last bit from
// the value Python wrote, and this package's parity claim is byte-level.
const NativeConfidence float32 = 1.0

// PRSource is a `work_item_dependencies.source_work_item_id` that names a pull
// request: the repo slug it lives in, its number, and which SCM minted the id.
type PRSource struct {
	RepoSlug string
	PRNumber uint32
	Provider string
}

// ParsePRSource ports work_graph/builder.py::_parse_pr_dependency_source
// (:256-278). It recognises the two SCM work-item id shapes Go providersync
// mints -- `ghpr:<owner>/<repo>#<n>` (github_work_items_rows.go:517,
// linear_work_items_route.go:892) and `gitlab:<group>/<project>!<n>`
// (linear_work_items_route.go:896) -- and returns ok=false for anything else,
// including tracker ids (`linear:`, `jira:`, `gh:`).
//
// The split is deliberately LAST-separator (Python's rsplit(sep, 1)), so a repo
// slug that itself contains the separator keeps it: `ghpr:a/b#c#12` parses as
// slug `a/b#c`, number 12.
//
// # Documented divergence from Python: non-ASCII digits
//
// Python gates the number with str.isdigit(), which is true for non-ASCII
// decimal digits (e.g. Arabic-Indic "٣") and for superscripts (e.g. "²"). It
// then calls int(), which accepts the former and RAISES ValueError on the
// latter -- so `ghpr:o/r#²` is an uncaught crash in the Python producer today,
// and `ghpr:o/r#٣` silently parses as 3. Go's strconv.ParseUint accepts ASCII
// only, so this function rejects both.
//
// This divergence is intentional and cannot fire on real data: every
// `ghpr:`/`gitlab:` id in `work_item_dependencies` is minted by Go providersync
// from a strconv.Itoa / URL path segment, so the number is always ASCII. It is
// recorded here (and covered by a test) rather than bug-compatibly reproduced,
// because reproducing it would mean either crashing on "²" or accepting a
// digit shape no producer emits.
//
// # Second, output-equivalent divergence: numbers above uint32
//
// Python's int() is unbounded, so `ghpr:o/r#4294967296` parses; the resulting
// lookup key can then never match a row, because `git_pull_requests.number` is
// `UInt32` -- so the dependency is dropped at the PR-existence gate. Go rejects
// it here instead. Both planes write NO link for such an id; only the rejection
// reason differs (`pr_not_found_or_out_of_window` vs `unparseable_source`).
// Recorded because the counters are load-bearing, and because "same output,
// different attribution" is the kind of difference an accounting assertion
// cannot see.
func ParsePRSource(value string) (PRSource, bool) {
	var (
		body      string
		separator string
		provider  string
	)
	switch {
	case strings.HasPrefix(value, "ghpr:"):
		body = strings.TrimPrefix(value, "ghpr:")
		separator = "#"
		provider = ProviderGitHub
	case strings.HasPrefix(value, "gitlab:"):
		body = strings.TrimPrefix(value, "gitlab:")
		separator = "!"
		provider = ProviderGitLab
	default:
		return PRSource{}, false
	}

	index := strings.LastIndex(body, separator)
	if index < 0 {
		return PRSource{}, false
	}
	repoSlug, number := body[:index], body[index+len(separator):]
	if repoSlug == "" || number == "" {
		return PRSource{}, false
	}
	// ParseUint rejects a sign, so "+1"/"-1" cannot reach the <= 0 gate the way
	// Python's isdigit()+int() pair does; both planes reject them.
	parsed, err := strconv.ParseUint(number, 10, 32)
	if err != nil || parsed == 0 {
		return PRSource{}, false
	}
	return PRSource{RepoSlug: repoSlug, PRNumber: uint32(parsed), Provider: provider}, true
}

// Admission is one accepted (relationship_type_raw -> target prefix) pair: the
// provider's own statement that a PR implements a tracker issue.
//
// Python gates this with a Linear LITERAL
// (_is_linear_pr_attachment_dependency, builder.py:282-288:
// relationship_type_raw == "linear_attachment" AND target starts with
// "linear:"). This package keeps that row admissible byte-for-byte and makes
// the rule a table, so CHAOS-4757's GitHub and Jira slices become data rather
// than new branches.
type Admission struct {
	// RelationshipTypeRaw is matched against the dependency row EXACTLY
	// (no trimming, no case folding) -- same as Python's == comparison.
	RelationshipTypeRaw string
	// TargetPrefix the dependency's target_work_item_id must start with, so a
	// raw kind cannot admit a row pointing at the wrong id space.
	TargetPrefix string
	// TargetValidator, when non-nil, is an ADDITIONAL grammar check beyond
	// TargetPrefix. nil for an admission whose id space has no narrower shape
	// to enforce here -- linear_attachment matches Python's own gate exactly
	// (prefix only), so it stays nil deliberately, not by oversight.
	//
	// github_closing_reference sets this (codex round 1 on #2174, P2): a
	// prefix-only check admits ANY "gh:..." string that happens to exist in
	// work_items, including one the real writer
	// (github_work_items_rows.go:779-780, node.Number < 1 skipped) could never
	// produce -- e.g. "gh:owner/repo#0". Without this, admission relies
	// entirely on an invariant (every "gh:"-prefixed work_items row is
	// well-formed) holding across every writer of that table forever, not
	// just this one. Confirmed reachable, not merely argued: Derive(inputs)
	// with a seeded gh:owner/repo#0 work_items row wrote a native link before
	// this fix.
	TargetValidator func(target string) bool
}

// isWellFormedGithubIssueTarget mirrors the grammar the real writer enforces
// (github_work_items_rows.go:779-780): gh:<owner>/<repo>#<positive-int>. The
// repo slug may itself contain "#" (same LAST-separator reasoning as
// ParsePRSource), so this splits on the LAST "#", not the first.
func isWellFormedGithubIssueTarget(target string) bool {
	body := strings.TrimPrefix(target, "gh:")
	index := strings.LastIndex(body, "#")
	if index < 0 {
		return false
	}
	repoSlug, number := body[:index], body[index+1:]
	if repoSlug == "" || number == "" {
		return false
	}
	parsed, err := strconv.ParseUint(number, 10, 64)
	return err == nil && parsed > 0
}

// DefaultAdmissions is the ACTIVE admission table: the raw kinds this producer
// will turn into mapping rows today.
//
// CHAOS-4771 (below) is why this table is no longer required to equal the live
// Python gate exactly: Go is SANCTIONED to lead Python's admission set, the
// same direction #2121's variant-C confidence policy already took (a Go
// post-step writing edges Python never wrote, ranked correctly by
// last-writer-wins on version). That is the intended shape of the cutover, not
// a divergence hazard -- as long as every admitted row carries its own
// provenance and the version ranking that decides collisions is proven, which
// CHAOS-4769 (below) is precisely what proves.
//
// github_closing_reference (CHAOS-4757 slice A) is activated here.
// Precondition, now satisfied: migration 084
// (`084_issue_pr_provenance_version_precedence.py`) makes `work_graph_issue_pr`
// rank by provenance before recency (`version_rank = rank(provenance)*2^45 +
// last_synced`, native=3 highest), so a row this admission writes -- stamped
// Provenance: ProvenanceNative, same as every other admitted kind -- now beats
// a colliding explicit_text/heuristic row unconditionally, regardless of which
// plane wrote which row first. Before that migration, admitting this kind
// would have let Python's text-parse fallback (`_build_issue_pr_edges`,
// builder.py:1352) discard the provider's own closing reference on a
// (org_id, repo_id, work_item_id, pr_number) collision -- inverting the
// standing rule that provider-attached is PRIMARY. See
// TestIssuePRProvenanceCollisionSurvivesMerge
// (provenance_collision_integration_test.go) for the mechanism proof; it is
// generic over provenance strings, not over RelationshipTypeRaw, so it already
// covers this admission -- Derive stamps ProvenanceNative for every admitted
// kind, github_closing_reference included, so no kind-specific collision test
// is needed on top of it.
//
// jira_dev_status stays reserved -- see ReservedAdmissions.
var DefaultAdmissions = []Admission{
	{RelationshipTypeRaw: "linear_attachment", TargetPrefix: "linear:"},
	{RelationshipTypeRaw: "github_closing_reference", TargetPrefix: "gh:", TargetValidator: isWellFormedGithubIssueTarget},
}

// ReservedAdmissions are the raw kinds whose shape is FROZEN and implemented
// but which this producer must not admit yet. Promoting one is a one-line move
// into DefaultAdmissions -- no other code changes.
//
//   - `jira_dev_status` (CHAOS-4757 slice B). A Go writer already exists
//     (`extractJiraDevStatusDependencies`, internal/providersync/jira_dev_status.go)
//     and stamps this raw kind today, same shape as github_closing_reference
//     before this PR -- so the CHAOS-4769 precondition this admission needs is
//     ALSO already satisfied (migration 084 is generic over provenance, not
//     per-kind). Left reserved here only because activating it was not part of
//     this PR's scope; it did not require new engineering to become ready and
//     is worth its own one-line-move PR, flagged separately rather than bundled
//     in silently.
//
// Declaring it here rather than leaving it undeclared is the point: the shape
// is agreed and tested, so activation is a decision, not an implementation.
var ReservedAdmissions = []Admission{
	{RelationshipTypeRaw: "jira_dev_status", TargetPrefix: "jira:"},
}

// DependencyRow is one `work_item_dependencies` row, trimmed to the columns the
// derivation reads.
type DependencyRow struct {
	OrgID               string
	SourceWorkItemID    string
	TargetWorkItemID    string
	RelationshipTypeRaw string
	LastSynced          time.Time
}

// RepoRow is one `repos` row: the slug -> id resolution a PR source needs.
type RepoRow struct {
	OrgID string
	ID    uuid.UUID
	Repo  string
}

// PullRequestRow is one `git_pull_requests` row inside the build's window. Its
// presence is the existence check; no column of it reaches the output.
type PullRequestRow struct {
	OrgID  string
	RepoID uuid.UUID
	Number uint32
}

// WorkItemRow is one `work_items` row: the target's existence check.
type WorkItemRow struct {
	OrgID      string
	WorkItemID string
}

// Link is one output row of `work_graph_issue_pr`.
type Link struct {
	OrgID      string
	RepoID     uuid.UUID
	WorkItemID string
	PRNumber   uint32
	Confidence float32
	Provenance string
	Evidence   string
	LastSynced time.Time
}

// Inputs is everything Derive reads. Holding them in a struct is what makes the
// derivation a pure function and therefore golden-testable without ClickHouse.
type Inputs struct {
	// OrgID is the build's org. It is the fallback for a dependency row with an
	// empty org_id (Python's _row_org_id, builder.py:279-280) and it is the
	// value stamped on every written row (Python's _issue_pr_to_record,
	// builder.py:194-206, writes config.org_id -- not the row's org_id).
	OrgID string

	Dependencies []DependencyRow
	Repos        []RepoRow
	PullRequests []PullRequestRow
	WorkItems    []WorkItemRow

	// Admissions defaults to DefaultAdmissions when empty.
	Admissions []Admission
}

// RejectionReason names why a candidate dependency row produced no link. The
// counts must account for EVERY dependency row that was not written, so
// len(Dependencies) == Written + sum(Rejected) exactly; TestDeriveAccountsFor
// EveryDependencyRow asserts it, and the live harness asserts the same identity
// against real data.
type RejectionReason string

const (
	// ReasonNotAdmissible covers a row no Admission matches: the wrong raw
	// kind, or the right raw kind with a target in the wrong id space.
	ReasonNotAdmissible RejectionReason = "not_admissible"
	// ReasonUnparseableSource covers a source that does not name a PR.
	ReasonUnparseableSource RejectionReason = "unparseable_source"
	// ReasonEmptyTarget covers an admissible row with a blank target id.
	ReasonEmptyTarget RejectionReason = "empty_target"
	// ReasonUnknownWorkItem covers a target absent from `work_items`.
	ReasonUnknownWorkItem RejectionReason = "unknown_work_item"
	// ReasonUnknownRepo covers a repo slug absent from `repos`.
	ReasonUnknownRepo RejectionReason = "unknown_repo"
	// ReasonPRNotFound covers a PR absent from `git_pull_requests` -- either it
	// does not exist, or it falls outside the build's from/to/repo window.
	ReasonPRNotFound RejectionReason = "pr_not_found_or_out_of_window"
	// ReasonDuplicateIdentity covers a second row for an identity already
	// linked. Python is first-wins in dependency-row order; so is this.
	ReasonDuplicateIdentity RejectionReason = "duplicate_identity"
)

// AllRejectionReasons is the fixed reason set, in report order. Ranging over a
// map would make the telemetry line's field order nondeterministic.
var AllRejectionReasons = []RejectionReason{
	ReasonNotAdmissible,
	ReasonUnparseableSource,
	ReasonEmptyTarget,
	ReasonUnknownWorkItem,
	ReasonUnknownRepo,
	ReasonPRNotFound,
	ReasonDuplicateIdentity,
}

// Result is Derive's output: the rows to write plus the full accounting of the
// rows that were not.
type Result struct {
	Links []Link

	// DependenciesRead is len(Inputs.Dependencies).
	DependenciesRead int
	// AdmittedByRawKind counts rows that passed the admission table, keyed by
	// relationship_type_raw -- the signal that lane-4757's GitHub/Jira slices
	// have started producing rows.
	AdmittedByRawKind map[string]int
	// Rejected counts every non-written row by reason.
	Rejected map[RejectionReason]int
	// ReservedSeenByRawKind counts rows matching a RESERVED admission -- the
	// shape is recognised but activation is deliberately withheld. Without it,
	// a provider starting to write a reserved kind would be invisible, buried
	// in `not_admissible` alongside every unrelated relation row, and the
	// decision to activate would have no evidence behind it.
	ReservedSeenByRawKind map[string]int
}

// Written is the number of rows Derive produced.
func (result Result) Written() int { return len(result.Links) }

// RejectedTotal is the number of dependency rows that produced no link.
func (result Result) RejectedTotal() int {
	total := 0
	for _, reason := range AllRejectionReasons {
		total += result.Rejected[reason]
	}
	return total
}

// Balanced reports whether every dependency row is accounted for exactly once.
// A false here means a rejection path was added without a counter, which would
// silently break the telemetry identity the live harness asserts.
func (result Result) Balanced() bool {
	return result.DependenciesRead == result.Written()+result.RejectedTotal()
}

type prKey struct {
	orgID  string
	repoID uuid.UUID
	number uint32
}

type identity struct {
	workItemID string
	repoID     uuid.UUID
	prNumber   uint32
}

// Derive is the whole mapping computation: pure, deterministic, no I/O.
//
// It ports work_graph/builder.py::_derive_issue_pr_links_from_dependencies
// (:644-798) gate for gate:
//
//  1. the row is admissible (Python: the Linear literal at :282-288);
//  2. its source parses as a PR (:746);
//  3. its target is non-empty (:749-751);
//  4. the target exists in `work_items` for the org (:754-755);
//  5. the repo slug resolves in `repos` (:758-760);
//  6. the PR exists in `git_pull_requests` -- already windowed by the caller's
//     from/to/repo filters (:761-762);
//  7. first-wins dedup on (target_work_item_id, repo_id, pr_number) (:764-767).
//
// Order is load-bearing and is the CALLER's responsibility: Python iterates
// whatever order ClickHouse returned, so the loader imposes an explicit
// ORDER BY on the identity key to make "first wins" reproducible. See
// dependencyQuery.
func Derive(inputs Inputs) Result {
	admissions := inputs.Admissions
	if len(admissions) == 0 {
		admissions = DefaultAdmissions
	}

	result := Result{
		AdmittedByRawKind:     make(map[string]int),
		Rejected:              make(map[RejectionReason]int),
		ReservedSeenByRawKind: make(map[string]int),
	}

	// Python's FIRST statement is `if not self.config.org_id: return 0`
	// (builder.py:645-646): an org-less build reads nothing and writes nothing.
	// Without this, Derive maps rows and stamps OrgID:"" on every link, and a
	// row written with an empty org_id lands in the wrong partition of
	// work_graph_issue_pr's (org_id, repo_id, work_item_id, pr_number) merge
	// key.
	//
	// Load already refuses an empty org, so the Service path was never exposed
	// -- but Derive is EXPORTED and is what the golden drives directly, so the
	// parity claim is about Derive itself, not only about the caller that
	// happens to reach it (codex round 7).
	//
	// DependenciesRead stays 0 deliberately: Python returns BEFORE its
	// dependency read, so "nothing was read" is the faithful accounting, and
	// the conservation identity holds trivially rather than by exception.
	if inputs.OrgID == "" {
		return result
	}

	result.DependenciesRead = len(inputs.Dependencies)
	if len(inputs.Dependencies) == 0 {
		return result
	}

	// Python returns early when the dependency read is empty, but performs the
	// lookup reads unconditionally otherwise -- so an org with dependencies but
	// no repos still walks every row and rejects it. Same here: the lookups are
	// built even when empty so the rejection accounting stays exact.
	repoLookup := make(map[[2]string]uuid.UUID, len(inputs.Repos))
	for _, repo := range inputs.Repos {
		// Only the slug is checked, matching Python's `if not repo_slug or not
		// repo_id` (builder.py:700) EXACTLY. An all-zero UUID must NOT be
		// excluded here: `uuid.UUID` defines no `__bool__`, so Python treats a
		// zero UUID as truthy and keeps the row -- only a missing (None)
		// repo_id is skipped, and `repos.id` is a non-nullable `UUID` in the
		// live schema, so None is unreachable from ClickHouse. A
		// `repo.ID == uuid.Nil` exclusion would therefore be a gate Python does
		// not have, turning a row Python maps into `unknown_repo`: a false
		// negative, which an accounting assertion cannot catch because the row
		// IS counted, just under the wrong outcome (codex round 6).
		if repo.Repo == "" {
			continue
		}
		repoLookup[[2]string{rowOrgID(repo.OrgID, inputs.OrgID), repo.Repo}] = repo.ID
	}

	prLookup := make(map[prKey]struct{}, len(inputs.PullRequests))
	for _, pull := range inputs.PullRequests {
		prLookup[prKey{
			orgID:  rowOrgID(pull.OrgID, inputs.OrgID),
			repoID: pull.RepoID,
			number: pull.Number,
		}] = struct{}{}
	}

	workItems := make(map[[2]string]struct{}, len(inputs.WorkItems))
	for _, item := range inputs.WorkItems {
		if item.WorkItemID == "" {
			continue
		}
		workItems[[2]string{rowOrgID(item.OrgID, inputs.OrgID), item.WorkItemID}] = struct{}{}
	}

	seen := make(map[identity]struct{}, len(inputs.Dependencies))
	for _, dependency := range inputs.Dependencies {
		admission, admissible := admit(admissions, dependency)
		if !admissible {
			if kind, ok := reservedRawKind(dependency.RelationshipTypeRaw); ok {
				result.ReservedSeenByRawKind[kind]++
			}
			result.Rejected[ReasonNotAdmissible]++
			continue
		}
		// Counted BEFORE the parse: this counter answers "is this provider
		// writing rows yet", so a provider whose rows all fail to parse must
		// look different from a provider that is not writing at all. Those two
		// states are operationally opposite and only one of them gets
		// investigated (codex round 1, F2).
		result.AdmittedByRawKind[admission.RelationshipTypeRaw]++

		source, parsed := ParsePRSource(dependency.SourceWorkItemID)
		if !parsed {
			result.Rejected[ReasonUnparseableSource]++
			continue
		}

		if dependency.TargetWorkItemID == "" {
			result.Rejected[ReasonEmptyTarget]++
			continue
		}
		orgID := rowOrgID(dependency.OrgID, inputs.OrgID)
		if _, ok := workItems[[2]string{orgID, dependency.TargetWorkItemID}]; !ok {
			result.Rejected[ReasonUnknownWorkItem]++
			continue
		}
		repoID, ok := repoLookup[[2]string{orgID, source.RepoSlug}]
		if !ok {
			result.Rejected[ReasonUnknownRepo]++
			continue
		}
		if _, ok := prLookup[prKey{orgID: orgID, repoID: repoID, number: source.PRNumber}]; !ok {
			result.Rejected[ReasonPRNotFound]++
			continue
		}
		key := identity{
			workItemID: dependency.TargetWorkItemID,
			repoID:     repoID,
			prNumber:   source.PRNumber,
		}
		if _, duplicate := seen[key]; duplicate {
			result.Rejected[ReasonDuplicateIdentity]++
			continue
		}
		seen[key] = struct{}{}

		result.Links = append(result.Links, Link{
			// The written org_id is the BUILD's org, not the row's -- Python's
			// _issue_pr_to_record stamps config.org_id (builder.py:205). The
			// two only differ for a row whose own org_id is empty, which the
			// org-filtered read cannot return anyway; matching Python exactly
			// keeps that irrelevance provable rather than assumed.
			OrgID:      inputs.OrgID,
			RepoID:     repoID,
			WorkItemID: dependency.TargetWorkItemID,
			PRNumber:   source.PRNumber,
			Confidence: NativeConfidence,
			Provenance: ProvenanceNative,
			// Python: str(row["relationship_type_raw"] or f"{provider}_attachment").
			// The fallback is unreachable here because the admission table
			// matched a non-empty raw kind, but the shape is preserved.
			Evidence:   evidenceFor(dependency.RelationshipTypeRaw, source.Provider),
			LastSynced: normalizeLastSynced(dependency.LastSynced),
		})
	}
	return result
}

// admit applies the admission table. Both conditions come from one entry, so a
// raw kind can never admit a target in another provider's id space.
func admit(admissions []Admission, dependency DependencyRow) (Admission, bool) {
	for _, admission := range admissions {
		if dependency.RelationshipTypeRaw != admission.RelationshipTypeRaw {
			continue
		}
		if !strings.HasPrefix(dependency.TargetWorkItemID, admission.TargetPrefix) {
			continue
		}
		if admission.TargetValidator != nil && !admission.TargetValidator(dependency.TargetWorkItemID) {
			continue
		}
		return admission, true
	}
	return Admission{}, false
}

// reservedRawKind matches on the RAW KIND ALONE, deliberately -- unlike admit,
// which additionally requires the target prefix.
//
// The two are asking different questions. Admission asks "may this row become a
// mapping row", and a raw kind pointing into the wrong id space must fail that.
// ReservedSeenByRawKind asks "has this provider started writing these rows at
// all", and the answer to that must not depend on the rows being well-formed:
// a provider emitting only malformed targets would otherwise report zero, which
// is indistinguishable from a provider that has not shipped yet.
//
// That is the same defect class as codex round-1 F2 (AdmittedByRawKind counted
// after parsing rather than after admission), applied to the counter added in
// response to it. Round 2 labelled the prefix requirement here "expected, not a
// finding"; recording the disagreement rather than deferring, because the
// counter exists precisely to make the activation decision evidence-based and a
// silent zero is the one answer it must never give wrongly.
func reservedRawKind(relationshipTypeRaw string) (string, bool) {
	for _, reserved := range ReservedAdmissions {
		if relationshipTypeRaw == reserved.RelationshipTypeRaw {
			return reserved.RelationshipTypeRaw, true
		}
	}
	return "", false
}

func evidenceFor(relationshipTypeRaw, provider string) string {
	if relationshipTypeRaw != "" {
		return relationshipTypeRaw
	}
	return provider + "_attachment"
}

// rowOrgID ports builder.py:279-280: the row's own org_id, falling back to the
// build's when it is empty.
func rowOrgID(rowOrg, buildOrg string) string {
	if rowOrg != "" {
		return rowOrg
	}
	return buildOrg
}

// normalizeLastSynced ports builder.py:768-785. Python coerces a naive
// timestamp to UTC and falls back to "now" for an unusable value; the driver
// hands us a time.Time, so the only cases left are the zero value (fallback is
// the caller's job -- see Service) and a non-UTC location, which is normalised
// so the written instant is stable regardless of the connection's timezone.
func normalizeLastSynced(value time.Time) time.Time {
	if value.IsZero() {
		return value
	}
	return value.UTC()
}
