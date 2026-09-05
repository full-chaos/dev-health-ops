package operationaledges

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

func float64Ptr(v float64) *float64 { return &v }

// TestSelectPreferredMappingsComparesFullFloat64Precision pins codex round
// chaos-4924-pr-a finding 1: the ranking comparison must happen at Python's
// own float64 precision, not after quantizing to float32 first. 0.5 and
// 0.50000001 are DISTINCT at float64 but IDENTICAL once narrowed to float32
// (float32 has ~7 decimal digits of precision) -- a comparison performed
// after quantizing would see a tie and keep the first-seen row; the correct
// (Python-matching) comparison sees a strict winner and keeps the second.
func TestSelectPreferredMappingsComparesFullFloat64Precision(t *testing.T) {
	repoID := uuid.MustParse("00000000-0000-0000-0000-0000000000aa")
	first := MappingRow{
		ServiceID: "svc-1", RepoID: &repoID,
		RelationshipConfidence: float64Ptr(0.5), Provider: "p1", RuleID: "rule-1",
	}
	second := MappingRow{
		ServiceID: "svc-1", RepoID: &repoID,
		RelationshipConfidence: float64Ptr(0.50000001), Provider: "p2", RuleID: "rule-2",
	}

	byKey, _ := selectPreferredMappings([]MappingRow{first, second})

	key := mappingKey{serviceID: "svc-1", repoID: repoID}
	got, ok := byKey[key]
	if !ok {
		t.Fatalf("expected a preferred mapping for %v", key)
	}
	if got.RuleID != "rule-2" {
		t.Fatalf("expected the higher-precision winner (rule-2, confidence 0.50000001), got %q (confidence source: %v)",
			got.RuleID, got.RelationshipConfidence)
	}
}

// TestSelectPreferredMappingsPreservesFirstSeenOrder pins finding 7: order
// must be the FIRST-SEEN order over the input slice (Python's dict
// insertion-order preservation), not whatever order a map range would
// produce.
func TestSelectPreferredMappingsPreservesFirstSeenOrder(t *testing.T) {
	repoA := uuid.MustParse("00000000-0000-0000-0000-0000000000aa")
	repoB := uuid.MustParse("00000000-0000-0000-0000-0000000000bb")
	repoC := uuid.MustParse("00000000-0000-0000-0000-0000000000cc")
	mappings := []MappingRow{
		{ServiceID: "svc-c", RepoID: &repoC, RelationshipConfidence: float64Ptr(0.9)},
		{ServiceID: "svc-a", RepoID: &repoA, RelationshipConfidence: float64Ptr(0.9)},
		{ServiceID: "svc-b", RepoID: &repoB, RelationshipConfidence: float64Ptr(0.9)},
	}

	_, order := selectPreferredMappings(mappings)

	want := []string{"svc-c", "svc-a", "svc-b"}
	if len(order) != len(want) {
		t.Fatalf("expected %d keys in order, got %d: %v", len(want), len(order), order)
	}
	for i, key := range order {
		if key.serviceID != want[i] {
			t.Fatalf("order[%d] = %s, want %s (full order: %v)", i, key.serviceID, want[i], order)
		}
	}
}

// TestSelectPreferredMappingsOrderIsStableAcrossRepeatedCalls is the
// multi-run half of finding 7's proof team-lead asked for: a single
// fixed-expected-order assertion (the test above) has only a 1-in-6 chance
// of catching a 3-element map-range mutant by luck (3! possible orders, one
// of them happens to match). Calling the SAME input many times and requiring
// EVERY run's order to match the FIRST is immune to that luck -- correct
// (first-seen-order) code trivially satisfies this always; a map-range
// mutant, whose iteration order is rehashed per call, will disagree with
// itself within a handful of the 30 runs below with overwhelming probability.
func TestSelectPreferredMappingsOrderIsStableAcrossRepeatedCalls(t *testing.T) {
	repoA := uuid.MustParse("00000000-0000-0000-0000-0000000000aa")
	repoB := uuid.MustParse("00000000-0000-0000-0000-0000000000bb")
	repoC := uuid.MustParse("00000000-0000-0000-0000-0000000000cc")
	repoD := uuid.MustParse("00000000-0000-0000-0000-0000000000dd")
	mappings := []MappingRow{
		{ServiceID: "svc-c", RepoID: &repoC, RelationshipConfidence: float64Ptr(0.9)},
		{ServiceID: "svc-a", RepoID: &repoA, RelationshipConfidence: float64Ptr(0.9)},
		{ServiceID: "svc-d", RepoID: &repoD, RelationshipConfidence: float64Ptr(0.9)},
		{ServiceID: "svc-b", RepoID: &repoB, RelationshipConfidence: float64Ptr(0.9)},
	}

	_, first := selectPreferredMappings(mappings)
	for i := 0; i < 30; i++ {
		_, order := selectPreferredMappings(mappings)
		if len(order) != len(first) {
			t.Fatalf("run %d: length changed (%d vs %d)", i, len(order), len(first))
		}
		for j := range order {
			if order[j] != first[j] {
				t.Fatalf("run %d disagreed with run 0 at position %d: %v vs %v (order is not "+
					"deterministic across calls -- a map was ranged instead of an ordered slice)",
					i, j, order, first)
			}
		}
	}
}

// TestSelectScopedIncidentsPreservesFirstSeenOrder is finding 7's other
// half: the incident-edge-building loop must also iterate in the query's
// own row-scan order, not map order.
func TestSelectScopedIncidentsPreservesFirstSeenOrder(t *testing.T) {
	incidents := []IncidentRow{
		{ID: "inc-3", ServiceID: "svc-1"},
		{ID: "inc-1", ServiceID: "svc-1"},
		{ID: "inc-2", ServiceID: "svc-1"},
	}

	_, order := selectScopedIncidents(incidents, nil, nil)

	want := []string{"inc-3", "inc-1", "inc-2"}
	if len(order) != len(want) {
		t.Fatalf("expected %d ids in order, got %d: %v", len(want), len(order), order)
	}
	for i, id := range order {
		if id != want[i] {
			t.Fatalf("order[%d] = %s, want %s (full order: %v)", i, id, want[i], order)
		}
	}
}

// TestBuildOperationalIncidentEdgesRejectsPrecisionSensitiveConfidence pins
// finding 2: heuristicConfidence must be validated at full float64
// precision. 1.00000001 is out of [0,1] at float64 but rounds to the VALID
// 1.0 once narrowed to float32 -- a validation performed on an
// already-float32 parameter could never see the difference. This call never
// reaches conn (the validation runs before any ClickHouse read), so a nil
// conn is safe and keeps the test hermetic.
func TestBuildOperationalIncidentEdgesRejectsPrecisionSensitiveConfidence(t *testing.T) {
	_, err := BuildOperationalIncidentEdges(
		context.Background(), nil, "70d529e0-3c06-4597-8480-794fd02328b6",
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		7, 1.00000001, nil, nil, nil,
	)
	if err == nil {
		t.Fatal("expected an error for heuristicConfidence=1.00000001 (outside [0,1] at float64 precision), got nil")
	}
}

// TestJiraKeyMatchesAcceptsUnicodeDecimalDigits and
// TestGithubPRURLMatchesAcceptsUnicodeDecimalDigits pin finding 4: RE2's
// bare `\d` is ASCII-only; Python's `\d` accepts any Unicode decimal digit
// (Nd category) -- e.g. Arabic-Indic ١٢ ("12"). Both patterns must match a
// key/URL built from such digits, the same as the deployed Python producer
// would.
func TestJiraKeyMatchesAcceptsUnicodeDecimalDigits(t *testing.T) {
	// "١٢" is Arabic-Indic for "12".
	got := jiraKeyMatches("See ABC-١٢ for details")
	if len(got) != 1 {
		t.Fatalf("expected 1 match for a Unicode-digit Jira key, got %d: %v", len(got), got)
	}
	if got[0] != "ABC-١٢" {
		t.Fatalf("expected the match to preserve the Unicode digits verbatim, got %q", got[0])
	}
}

func TestGithubPRURLMatchesAcceptsUnicodeDecimalDigits(t *testing.T) {
	got := githubPRURLMatches("see https://github.com/synthorg/webapp/pull/١٢ for the change")
	if len(got) != 1 {
		t.Fatalf("expected 1 PR URL match with Unicode digits, got %d: %v", len(got), got)
	}
	if got[0].Number != "١٢" {
		t.Fatalf("expected the raw Unicode digit string to be captured, got %q", got[0].Number)
	}
}

// TestNormalizeDigitsToASCIIHandlesOverflowSizedNumbers pins finding 5:
// parsePRNumber's old manual int accumulation silently wrapped Go's `int` on
// a PR number exceeding 64-bit range (Python's int() has arbitrary
// precision). normalizeDigitsToASCII + GeneratePRIDFromDigits must never
// route through a bounded integer type at all, so a huge digit string
// (larger than 2^63) comes through unchanged rather than silently becoming
// something small (like the old code's 0).
func TestNormalizeDigitsToASCIIHandlesOverflowSizedNumbers(t *testing.T) {
	const huge = "18446744073709551616" // 2^64, one past uint64's max
	got := normalizeDigitsToASCII(huge)
	if got != huge {
		t.Fatalf("expected an all-ASCII digit string to pass through unchanged, got %q", got)
	}

	repoID := uuid.MustParse("00000000-0000-0000-0000-0000000000aa")
	prID := edges.GeneratePRIDFromDigits(repoID, got)
	want := repoID.String() + "#pr" + huge
	if prID != want {
		t.Fatalf("GeneratePRIDFromDigits(%s) = %q, want %q (old code would have produced .../pr0 via int overflow)", huge, prID, want)
	}
}

// TestNormalizeDigitsToASCIIConvertsUnicodeDigitsToTheirValue confirms the
// Unicode-digit fix (finding 4) actually produces the RIGHT number, not just
// A number: "١٢" (Arabic-Indic) must normalize to ASCII "12", matching what
// Python's int("١٢") == 12 would format back to.
func TestNormalizeDigitsToASCIIConvertsUnicodeDigitsToTheirValue(t *testing.T) {
	got := normalizeDigitsToASCII("١٢")
	if got != "12" {
		t.Fatalf("normalizeDigitsToASCII(\"١٢\") = %q, want \"12\"", got)
	}
}

// TestAppendUserFallbackEvidenceUsesSourceFieldName pins finding 6:
// Python's `_append_user` fallback evidence is `row.get("source_url") or
// user_key` -- the SOURCE FIELD NAME, not a fixed literal. A timeline row's
// fallback must read "actor_id" and a responder row's must read "user_id",
// never both reading the same hardcoded string.
func TestAppendUserFallbackEvidenceUsesSourceFieldName(t *testing.T) {
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	b := &incidentEdgeBuilder{now: now}

	b.appendUser(directRow{IncidentID: "inc-1", PersonID: "u1"}, "actor_id")
	b.appendUser(directRow{IncidentID: "inc-1", PersonID: "u2"}, "user_id")

	if len(b.edges) != 2 {
		t.Fatalf("expected 2 edges, got %d", len(b.edges))
	}
	if b.edges[0].Evidence != "actor_id" {
		t.Errorf("timeline-row fallback evidence = %q, want \"actor_id\"", b.edges[0].Evidence)
	}
	if b.edges[1].Evidence != "user_id" {
		t.Errorf("responder-row fallback evidence = %q, want \"user_id\"", b.edges[1].Evidence)
	}
}

// TestReadIncidentsLogsConfigurationErrorWithOrgID pins codex round
// chaos-4924-pr-a-r3 finding 1: every read function returned a wrapped error
// on a query/scan/iteration/configuration failure but never logged it -- only
// the two ordering-contract InfoContext lines existed in the whole package.
// An invalid OPERATIONAL_ORDERING_CONTRACT value fails before any ClickHouse
// call, so this is hermetic (conn is never touched, nil is safe).
func TestReadIncidentsLogsConfigurationErrorWithOrgID(t *testing.T) {
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "not-a-valid-contract")

	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(previous)

	const orgID = "70d529e0-3c06-4597-8480-794fd02328b6"
	_, err := ReadIncidents(context.Background(), nil, orgID, nil, nil)
	if err == nil {
		t.Fatal("expected an error for an invalid ordering contract, got nil")
	}

	logged := buf.String()
	if !strings.Contains(logged, "level=ERROR") {
		t.Fatalf("expected an ERROR-level log record, got: %s", logged)
	}
	if !strings.Contains(logged, orgID) {
		t.Fatalf("expected the log record to carry org_id=%s, got: %s", orgID, logged)
	}
	if !strings.Contains(logged, "incident ordering contract") {
		t.Fatalf("expected the log record to name the failing operation, got: %s", logged)
	}
}

// TestReadServiceRepositoryMappingsLogsRepoIDOnFailure pins the r3
// confirmation-pass finding: a repo-scoped call's failure log must carry the
// requested repo_id, not just org_id -- otherwise a repo-scoped failure is
// indistinguishable in the logs from an org-wide one for the same org.
func TestReadServiceRepositoryMappingsLogsRepoIDOnFailure(t *testing.T) {
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "not-a-valid-contract")

	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(previous)

	const orgID = "70d529e0-3c06-4597-8480-794fd02328b6"
	repoID := uuid.MustParse("00000000-0000-0000-0000-0000000000aa")
	_, err := ReadServiceRepositoryMappings(context.Background(), nil, orgID, time.Now(), &repoID)
	if err == nil {
		t.Fatal("expected an error for an invalid ordering contract, got nil")
	}

	logged := buf.String()
	if !strings.Contains(logged, "repo_id="+repoID.String()) {
		t.Fatalf("expected the log record to carry the requested repo_id=%s, got: %s", repoID, logged)
	}
}
