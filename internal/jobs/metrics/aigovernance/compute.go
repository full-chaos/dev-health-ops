// Package aigovernance is the native Go port of the `ai_governance`
// metrics.daily family (CHAOS-4285).
//
// It ports three Python modules, which together are what
// build_governance_rows_for_day (audit/ai_governance/loaders.py:113) calls:
//
//   - audit/ai_governance/policy.py    -> EvaluateArtifact / EvaluateArtifacts
//   - audit/ai_governance/rollup.py    -> RollupCoverageDaily
//   - audit/ai_governance/models.py    -> the types below, incl. EvidenceJSON
//
// The ClickHouse loader half (_ARTIFACTS_SQL) lives in
// internal/jobs/metrics/daily/ai_governance_native_clickhouse.go, so this
// package stays pure and directly comparable against live Python.
//
// # Why there is no float in this file
//
// Every value this family persists is a COUNT (ai_artifacts,
// declared_artifacts, human_reviewed_prs, security_scanned_prs,
// in_policy_artifacts) or an identifier. models.py's declaration_coverage /
// human_review_coverage / security_scan_coverage / in_policy_coverage are
// read-side @property helpers on the dataclass; they are NOT columns of
// ai_governance_coverage_daily (migration 038:26-41) and are never written by
// this family. So neither CPython's Neumaier-compensated sum() nor arm64 FMA
// fusion has a live site here -- unlike ai_impact (CHAOS-4280), whose _avg IS
// a compensated sum. Stated explicitly because "no float rule cited" and "no
// float present" are indistinguishable to a later reader otherwise.
package aigovernance

import (
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// PolicyRule mirrors AIPolicyRule (models.py:17). The Python type is a
// StrEnum, so its members compare and serialize as these exact strings.
type PolicyRule string

const (
	RuleMissingAIDeclaration      PolicyRule = "MISSING_AI_DECLARATION"
	RuleMissingHumanReview        PolicyRule = "MISSING_HUMAN_REVIEW"
	RuleSensitiveRepoDisallowed   PolicyRule = "SENSITIVE_REPO_DISALLOWED"
	RuleDisallowedTool            PolicyRule = "DISALLOWED_TOOL"
	RuleMissingSecurityScan       PolicyRule = "MISSING_SECURITY_SCAN"
	RuleNewLicenseFindingFromAIPR PolicyRule = "NEW_LICENSE_FINDING_FROM_AI_PR"
)

// PolicySeverity mirrors AIPolicySeverity (models.py:28).
type PolicySeverity string

const (
	SeverityInfo     PolicySeverity = "info"
	SeverityWarning  PolicySeverity = "warning"
	SeverityHigh     PolicySeverity = "high"
	SeverityCritical PolicySeverity = "critical"
)

// ruleSeverity ports RULE_SEVERITY (policy.py:15). Held as a function over a
// switch rather than a map so that a rule added without a severity is a
// COMPILE-visible gap; Python's dict lookup would raise KeyError at runtime,
// inside the per-artifact loop, on whichever partition happened to contain
// the first matching artifact.
func ruleSeverity(rule PolicyRule) PolicySeverity {
	switch rule {
	case RuleMissingAIDeclaration:
		return SeverityWarning
	case RuleMissingHumanReview:
		return SeverityHigh
	case RuleSensitiveRepoDisallowed:
		return SeverityCritical
	case RuleDisallowedTool:
		return SeverityHigh
	case RuleMissingSecurityScan:
		return SeverityHigh
	case RuleNewLicenseFindingFromAIPR:
		return SeverityHigh
	}
	return SeverityInfo
}

// ToolAllowlistStatus mirrors ToolAllowlistStatus (models.py:36).
type ToolAllowlistStatus string

const (
	AllowlistAllowed    ToolAllowlistStatus = "allowed"
	AllowlistDisallowed ToolAllowlistStatus = "disallowed"
	AllowlistDeprecated ToolAllowlistStatus = "deprecated"
	AllowlistUnknown    ToolAllowlistStatus = "unknown"
)

// ParseToolAllowlistStatus ports _allowlist_status (loaders.py:191): an
// empty/absent/unrecognised value becomes UNKNOWN rather than raising.
func ParseToolAllowlistStatus(value string) ToolAllowlistStatus {
	switch ToolAllowlistStatus(value) {
	case AllowlistAllowed:
		return AllowlistAllowed
	case AllowlistDisallowed:
		return AllowlistDisallowed
	case AllowlistDeprecated:
		return AllowlistDeprecated
	case AllowlistUnknown:
		return AllowlistUnknown
	}
	return AllowlistUnknown
}

// ArtifactEvidence is the nested `evidence` dict _artifact_from_row builds
// (loaders.py:147-152). Every field is a pointer because Python stores None
// for an absent value and `null` is what reaches the persisted JSON.
//
// Confidence is *float64 and NOT *float32 even though the source column is
// narrower: clickhouse-connect hands Python a plain float (a float64) already
// widened from the wire value, and json.dumps then renders THAT. The loader
// is responsible for performing the same widening before filling this field
// -- see loadGovernanceArtifacts' scan comment.
type ArtifactEvidence struct {
	Source      *string
	Kind        *string
	Confidence  *float64
	ArtifactURL *string
}

// Artifact mirrors AIGovernanceArtifact (models.py:80).
type Artifact struct {
	OrgID                      string
	TeamID                     *string
	RepoID                     *uuid.UUID
	SubjectType                string
	SubjectID                  string
	ObservedAt                 time.Time
	AIDetected                 bool
	DeclaredAI                 bool
	HumanReviewed              *bool
	SensitiveRepo              bool
	RepoAllowsAI               bool
	SecurityScanned            *bool
	LicenseOrDependencyFinding bool
	ToolName                   *string
	ModelName                  *string
	ToolAllowlistStatus        ToolAllowlistStatus
	Evidence                   ArtifactEvidence
}

// Violation mirrors AIGovernanceViolation (models.py:102).
type Violation struct {
	EventID     uuid.UUID
	OrgID       string
	TeamID      *string
	RepoID      *uuid.UUID
	RuleID      PolicyRule
	Severity    PolicySeverity
	SubjectType string
	SubjectID   string
	ObservedAt  time.Time
	Evidence    map[string]any
}

// CoverageDaily mirrors AIGovernanceCoverageDaily (models.py:121). The five
// counters are uint64 to match the columns' declared UInt64 (migration
// 038:31-35), not uint32 as most other daily families use.
type CoverageDaily struct {
	OrgID              string
	TeamID             *string
	RepoID             *uuid.UUID
	Day                time.Time
	AIArtifacts        uint64
	DeclaredArtifacts  uint64
	HumanReviewedPRs   uint64
	SecurityScannedPRs uint64
	InPolicyArtifacts  uint64
}

// EvaluateArtifact ports evaluate_artifact (policy.py:25) exactly, INCLUDING
// the order in which the six rules append -- that order is observable, because
// EvaluateArtifacts flattens per-artifact lists in sequence and the resulting
// slice order is what the writer batches.
//
// Two predicates are deliberately `is not True` in Python, not `is False`:
// human_reviewed and security_scanned are Optional[bool], and an UNKNOWN
// (None) value counts as a violation. Go's *bool reproduces the three-valued
// logic; `ptr == nil || !*ptr` is the faithful spelling of `is not True`.
func EvaluateArtifact(artifact Artifact) []Violation {
	if !artifact.AIDetected {
		return nil
	}

	var violations []Violation
	if !artifact.DeclaredAI {
		violations = append(violations, newViolation(artifact, RuleMissingAIDeclaration))
	}
	if artifact.SubjectType == "pull_request" && !isTrue(artifact.HumanReviewed) {
		violations = append(violations, newViolation(artifact, RuleMissingHumanReview))
	}
	if artifact.SensitiveRepo && !artifact.RepoAllowsAI {
		violations = append(violations, newViolation(artifact, RuleSensitiveRepoDisallowed))
	}
	if artifact.ToolAllowlistStatus == AllowlistDisallowed {
		violations = append(violations, newViolation(artifact, RuleDisallowedTool))
	}
	if artifact.SubjectType == "pull_request" && !isTrue(artifact.SecurityScanned) {
		violations = append(violations, newViolation(artifact, RuleMissingSecurityScan))
	}
	if artifact.LicenseOrDependencyFinding {
		violations = append(violations, newViolation(artifact, RuleNewLicenseFindingFromAIPR))
	}
	return violations
}

// isTrue is Python's `x is True` for an Optional[bool]: nil (None) and false
// are both NOT true.
func isTrue(value *bool) bool { return value != nil && *value }

// EvaluateArtifacts ports evaluate_artifacts (policy.py:51).
func EvaluateArtifacts(artifacts []Artifact) []Violation {
	var violations []Violation
	for _, artifact := range artifacts {
		violations = append(violations, EvaluateArtifact(artifact)...)
	}
	return violations
}

// hasViolations answers rollup.py:54's `not evaluate_artifact(a)` without
// allocating the violation slice or deriving six event ids per artifact. It
// MUST stay a mirror of EvaluateArtifact's predicate set; the two are pinned
// together by TestHasViolationsAgreesWithEvaluateArtifact, which is a
// property over generated artifacts rather than a fixture, so a predicate
// added to one and not the other cannot pass.
func hasViolations(artifact Artifact) bool {
	if !artifact.AIDetected {
		return false
	}
	isPR := artifact.SubjectType == "pull_request"
	return !artifact.DeclaredAI ||
		(isPR && !isTrue(artifact.HumanReviewed)) ||
		(artifact.SensitiveRepo && !artifact.RepoAllowsAI) ||
		artifact.ToolAllowlistStatus == AllowlistDisallowed ||
		(isPR && !isTrue(artifact.SecurityScanned)) ||
		artifact.LicenseOrDependencyFinding
}

// aiGovernanceEventNamespace seeds the deterministic event_id below. A fixed,
// arbitrary, never-reused UUID -- the repo's established shape for derived
// ids (internal/scheduler/fixed/producers.go:50, materializer.go:956).
var aiGovernanceEventNamespace = uuid.MustParse("6f2b1c4e-9a13-4d77-8f52-1c0e4a9b7d63")

// newViolation ports _violation (policy.py:61) with ONE deliberate change,
// approved by team-lead 09-04 (design.md Q1).
//
// # THE CHANGE: event_id is derived, not random
//
// Python's AIGovernanceViolation.event_id defaults to `uuid4()`
// (models.py:110) -- and event_id is the LAST component of
// ai_policy_events' ORDER BY key (migration 038:23), which is also its
// ReplacingMergeTree dedup key. A random id therefore makes every re-run of a
// day append a full duplicate set that can NEVER merge away: the table grows
// without bound and no reader can tell a re-computed event from a second real
// one. The redundancy is worse than one copy per run, because
// build_governance_rows_for_day is called once per PARTITION while ignoring
// repo_id entirely (job_daily.py:1671 passes only org_id and day), so an org
// with N repo partitions writes N identical-but-for-event_id copies per day.
//
// This port derives event_id from the rest of the ORDER BY key instead:
// (org_id, team_id, repo_id, rule_id, subject_type, subject_id, observed_at).
// event_id becomes functionally dependent on the real key, so the RMT key
// collapses to the real key and dedup finally works as the schema intended.
// It also makes the differential oracle able to compare event_id itself
// rather than excluding a key column from the comparison.
//
// Pre-existing rows written by Python carry random ids and will never merge
// with these -- see this PR's RISK-NOTES and the Low ticket for the historical
// duplicates.
//
// The key components are joined with a delimiter that cannot occur in any of
// them and are LENGTH-PREFIXED where they are free-form, so that no two
// distinct keys can render to the same byte string (subject_id is opaque
// provider text and could otherwise contain the delimiter).
func newViolation(artifact Artifact, rule PolicyRule) Violation {
	return Violation{
		EventID:     deriveEventID(artifact, rule),
		OrgID:       artifact.OrgID,
		TeamID:      artifact.TeamID,
		RepoID:      artifact.RepoID,
		RuleID:      rule,
		Severity:    ruleSeverity(rule),
		SubjectType: artifact.SubjectType,
		SubjectID:   artifact.SubjectID,
		ObservedAt:  artifact.ObservedAt,
		Evidence:    violationEvidence(artifact),
	}
}

func deriveEventID(artifact Artifact, rule PolicyRule) uuid.UUID {
	var buf []byte
	appendKeyPart := func(value string) {
		buf = appendLengthPrefixed(buf, value)
	}
	appendKeyPart(artifact.OrgID)
	appendKeyPart(derefString(artifact.TeamID))
	appendKeyPart(repoIDKeyString(artifact.RepoID))
	appendKeyPart(string(rule))
	appendKeyPart(artifact.SubjectType)
	appendKeyPart(artifact.SubjectID)
	// RFC3339Nano is unambiguous and stable; the column is DateTime64(3), so
	// the value round-trips at millisecond precision either way.
	appendKeyPart(artifact.ObservedAt.UTC().Format(time.RFC3339Nano))
	return uuid.NewSHA1(aiGovernanceEventNamespace, buf)
}

// violationEvidence ports _violation's evidence dict (policy.py:64-71) key
// for key. Values are `any` so the tree can be handed to
// pythonparity.MarshalPythonJSONSorted, which reproduces
// json.dumps(..., sort_keys=True) byte for byte.
//
// str(artifact.tool_allowlist_status) on a StrEnum yields the VALUE
// ("allowed"/"disallowed"/...), not "ToolAllowlistStatus.ALLOWED" -- StrEnum
// inherits str.__str__. That is why this writes the bare status string.
func violationEvidence(artifact Artifact) map[string]any {
	return map[string]any{
		"subject_type":          artifact.SubjectType,
		"subject_id":            artifact.SubjectID,
		"tool_name":             optionalString(artifact.ToolName),
		"model_name":            optionalString(artifact.ModelName),
		"tool_allowlist_status": string(artifact.ToolAllowlistStatus),
		"artifact_evidence": map[string]any{
			"source":       optionalString(artifact.Evidence.Source),
			"kind":         optionalString(artifact.Evidence.Kind),
			"confidence":   optionalFloat(artifact.Evidence.Confidence),
			"artifact_url": optionalString(artifact.Evidence.ArtifactURL),
		},
	}
}

// EvidenceJSON ports AIGovernanceViolation.evidence_json (models.py:117):
//
//	json.dumps(self.evidence, default=str, sort_keys=True)
//
// sort_keys=True with DEFAULT separators (", " and ": "), which is exactly
// pythonparity.MarshalPythonJSONSorted's contract. This is deliberately NOT
// the same encoder as ai_workflow.py:54's _json, which passes
// separators=(",", ":") -- two different encoders in the same family cluster,
// and collapsing them would silently change persisted bytes.
//
// default=str never fires here: violationEvidence only ever puts strings,
// nil and float64 into the tree.
func (violation Violation) EvidenceJSON() (string, error) {
	encoded, err := pythonparity.MarshalPythonJSONSorted(violation.Evidence)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// RollupCoverageDaily ports rollup_coverage_daily (rollup.py:16).
//
// # Ordering
//
// Python sorts groups by `(org_id, team_id or "", str(repo_id))`
// (rollup.py:32-35). str() on a None repo_id is the LITERAL STRING "None",
// not an empty string -- and "None" sorts AFTER every digit and BEFORE every
// lowercase hex letter, so a null-repo group lands in the middle of the UUID
// range rather than at either end. repoIDSortString reproduces that exactly;
// getting it wrong would reorder the batch without changing any value, which
// no count-based assertion could detect.
func RollupCoverageDaily(artifacts []Artifact, day time.Time) []CoverageDaily {
	dayUTC := day.UTC()
	year, month, dayOfMonth := dayUTC.Date()

	type group struct {
		key       coverageGroupKey
		teamID    *string
		repoID    *uuid.UUID
		artifacts []Artifact
	}

	groups := make(map[coverageGroupKey]*group)
	var order []coverageGroupKey
	for _, artifact := range artifacts {
		if !artifact.AIDetected {
			continue
		}
		observedYear, observedMonth, observedDay := artifact.ObservedAt.UTC().Date()
		if observedYear != year || observedMonth != month || observedDay != dayOfMonth {
			continue
		}
		key := coverageGroupKey{orgID: artifact.OrgID}
		if artifact.TeamID != nil {
			key.teamID, key.hasTeam = *artifact.TeamID, true
		}
		if artifact.RepoID != nil {
			key.repoID, key.hasRepo = artifact.RepoID.String(), true
		}
		existing, ok := groups[key]
		if !ok {
			existing = &group{key: key, teamID: artifact.TeamID, repoID: artifact.RepoID}
			groups[key] = existing
			order = append(order, key)
		}
		existing.artifacts = append(existing.artifacts, artifact)
	}

	sort.SliceStable(order, func(i, j int) bool {
		left, right := order[i], order[j]
		if left.orgID != right.orgID {
			return left.orgID < right.orgID
		}
		// `team_id or ""` -- None and "" are the SAME sort value in Python.
		if left.teamID != right.teamID {
			return left.teamID < right.teamID
		}
		return left.repoSortString() < right.repoSortString()
	})

	rows := make([]CoverageDaily, 0, len(order))
	for _, key := range order {
		current := groups[key]
		row := CoverageDaily{
			OrgID:       current.key.orgID,
			TeamID:      current.teamID,
			RepoID:      current.repoID,
			Day:         time.Date(year, month, dayOfMonth, 0, 0, 0, 0, time.UTC),
			AIArtifacts: uint64(len(current.artifacts)),
		}
		for _, artifact := range current.artifacts {
			if artifact.DeclaredAI {
				row.DeclaredArtifacts++
			}
			if artifact.SubjectType == "pull_request" && isTrue(artifact.HumanReviewed) {
				row.HumanReviewedPRs++
			}
			if artifact.SubjectType == "pull_request" && isTrue(artifact.SecurityScanned) {
				row.SecurityScannedPRs++
			}
			if !hasViolations(artifact) {
				row.InPolicyArtifacts++
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// coverageGroupKey is rollup.py's grouping tuple
// `(artifact.org_id, artifact.team_id, artifact.repo_id)` in a form Go can
// use as a map key. The `has*` flags keep None distinguishable from "" for
// GROUPING purposes even though the SORT collapses them (see repoSortString
// and the team_id comparison in RollupCoverageDaily).
type coverageGroupKey struct {
	orgID   string
	teamID  string
	hasTeam bool
	repoID  string
	hasRepo bool
}

// repoSortString is Python's `str(repo_id)` used as a sort key: the literal
// "None" for a null repo, else the lowercase hyphenated UUID.
func (key coverageGroupKey) repoSortString() string {
	if !key.hasRepo {
		return "None"
	}
	return key.repoID
}

// repoIDKeyString is the event-id key component for a repo id. Distinct from
// repoIDSortString on purpose: this one is an identity component, so a null
// repo contributes an empty string that the length prefix already
// distinguishes from any real value.
func repoIDKeyString(repoID *uuid.UUID) string {
	if repoID == nil {
		return ""
	}
	return repoID.String()
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// optionalString turns a Go *string into the `any` Python would have put in
// the evidence dict: nil -> None -> JSON null.
func optionalString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func optionalFloat(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

// appendLengthPrefixed writes a decimal length, a colon, then the raw bytes,
// so concatenating parts is injective regardless of their content.
func appendLengthPrefixed(dst []byte, value string) []byte {
	dst = appendDecimal(dst, len(value))
	dst = append(dst, ':')
	return append(dst, value...)
}

func appendDecimal(dst []byte, value int) []byte {
	if value == 0 {
		return append(dst, '0')
	}
	var digits [20]byte
	index := len(digits)
	for value > 0 {
		index--
		digits[index] = byte('0' + value%10)
		value /= 10
	}
	return append(dst, digits[index:]...)
}
