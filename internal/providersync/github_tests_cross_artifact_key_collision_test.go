package providersync

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-4190: prod cicd/tests units terminalize after 5 attempts with the
// bare, unwrapped ErrInvalidConfiguration ("provider sync configuration is
// invalid"), category provider_unit_exhausted. Three mechanisms were already
// eliminated by test on that ticket (completion comparator, batch/comparator
// watermark agreement, Gate factory nil-per-claim).
//
// Root cause pinned here: SuiteID/CaseID/coverage SnapshotID are derived from
// (run_id, name[, path]) ONLY -- github_tests_reports.go:684 (`hashTestIdentifier(runID,
// name, "")`), :761 (`hashTestIdentifier(suite.SuiteID, name)`), :591/:877 for
// coverage -- with no artifact scoping. The chunked route accumulates
// suites/cases/coverage across EVERY artifact of one workflow run into a
// SINGLE effects batch (github_tests_chunked_route.go:634-789, cumulative
// append at ~745-747) with no cross-artifact dedup. A run whose artifacts
// both contribute a suite/case with the same name -- plausible for a matrix
// build where every job uploads an identically-named JUnit report --
// therefore reaches TestOpsClickHouseEffects.WriteEffect
// (github_tests_effects_clickhouse.go) with two rows sharing one natural
// key. recordGitHubTestsKey (same file, ~line 73/196) is a WORKING-AS-DESIGNED
// guard against exactly that (see
// TestGitHubTestsMultiRowEffectsAreAtomicAndRejectDuplicateNaturalKeys in
// github_tests_effects_integration_test.go) -- it correctly refuses the
// batch, bare, every time, because the content is deterministic. That is
// what burns all 5 attempts.
//
// The fixture below is NOT new: TestGitHubTestsPerRunArtifactsUnderCapIsUntouched
// already drives 2 artifacts each contributing 1 suite through this exact
// path and asserts the unit finalizes complete with an advanced watermark.
// These tests assert the CORRECT behavior for that same fixture -- distinct
// natural keys per artifact, and a batch WriteEffect actually accepts -- so
// they fail red against the unfixed producer and must pass green after it is
// fixed to scope every hashed id to the artifact it came from.

func TestGitHubTestsCrossArtifactSameNameSuiteGetsDistinctNaturalKeys(t *testing.T) {
	doer := &githubTestsOversizedRunDoer{t: t, jobs: 1, artifacts: 2, reportSuitesPerArtifact: 1}
	claim := nativeTestClaim("github", "cicd")
	final, effects := walkGitHubTestsChunksCapturingEffects(t, GitHubTestsRouteHandler{}, claim, githubTestsClient(t, doer), 4)

	// Anti-vacuity: the route must genuinely report this unit as a normal,
	// complete success -- otherwise a duplicate key inside a withheld/failed
	// unit would be an unsurprising, unrelated finding.
	if final.Watermark == nil {
		t.Fatal("fixture unit did not advance its watermark; not the success case under test")
	}
	if doer.artifacts != 2 || doer.archiveRequests != 2 {
		t.Fatalf("fixture must download exactly 2 artifacts for 1 run, got artifacts=%d downloaded=%d",
			doer.artifacts, doer.archiveRequests)
	}

	suiteRows := decodeGitHubTestsEffectRows[testSuiteResultRow](t, effects, "test_suite_results")
	if len(suiteRows) != 2 {
		t.Fatalf("test_suite_results rows=%d, want 2 (one per artifact)", len(suiteRows))
	}
	if suiteRows[0].SuiteID == "" || suiteRows[1].SuiteID == "" || suiteRows[0].SuiteID == suiteRows[1].SuiteID {
		t.Fatalf("CHAOS-4190: suite ids=%q,%q from two DISTINCT artifacts of the same run collided; "+
			"WriteEffect's recordGitHubTestsKey rejects the second as a duplicate",
			suiteRows[0].SuiteID, suiteRows[1].SuiteID)
	}

	caseRows := decodeGitHubTestsEffectRows[testCaseResultRow](t, effects, "test_case_results")
	if len(caseRows) != 2 {
		t.Fatalf("test_case_results rows=%d, want 2 (one per artifact)", len(caseRows))
	}
	if caseRows[0].CaseID == "" || caseRows[1].CaseID == "" || caseRows[0].CaseID == caseRows[1].CaseID {
		t.Fatalf("CHAOS-4190: case ids=%q,%q from two DISTINCT artifacts of the same run collided",
			caseRows[0].CaseID, caseRows[1].CaseID)
	}
}

func TestGitHubTestsCrossArtifactBatchCommitsBothArtifactsWithoutError(t *testing.T) {
	doer := &githubTestsOversizedRunDoer{t: t, jobs: 1, artifacts: 2, reportSuitesPerArtifact: 1}
	claim := nativeTestClaim("github", "cicd")
	_, effects := walkGitHubTestsChunksCapturingEffects(t, GitHubTestsRouteHandler{}, claim, githubTestsClient(t, doer), 4)

	suiteEffect, ok := findGitHubTestsEffect(effects, "test_suite_results")
	if !ok {
		t.Fatal("no non-empty test_suite_results effect emitted")
	}
	if len(suiteEffect.Rows) != 2 {
		t.Fatalf("test_suite_results effect rows=%d, want 2 (one per artifact)", len(suiteEffect.Rows))
	}

	batchConn := &githubTestsDuplicateKeyConn{}
	sink := TestOpsClickHouseEffects{
		Conn:  batchConn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	err := sink.WriteEffect(context.Background(), claim, suiteEffect)
	if errors.Is(err, ErrInvalidConfiguration) {
		// This is CHAOS-4190's exact prod log signature: the bare, unwrapped
		// sentinel text ("provider sync configuration is invalid", no
		// wrapping) that the cicd unit terminalizes on after 5 attempts. Two
		// genuinely distinct artifacts of one run must not collide into a
		// rejected duplicate.
		t.Fatalf("CHAOS-4190: WriteEffect rejected two genuinely distinct artifacts' rows as duplicates: %v "+
			"(err.Error()=%q)", err, err.Error())
	}
	if err != nil {
		t.Fatalf("WriteEffect unexpected error=%v", err)
	}
	if batchConn.batch == nil || batchConn.batch.appends != 2 {
		t.Fatalf("committed appends=%v, want both artifacts' rows written", batchConn.batch)
	}
}

// TestGitHubTestsWriteEffectDuplicateKeyErrorNamesTheCollidingKey pins the
// cause-erasure fix ordered alongside CHAOS-4190: a batch that DOES contain a
// genuine duplicate natural key (same content re-sent, a real upstream bug
// elsewhere, anything other than the cross-artifact case this ticket fixed)
// must still be refused -- recordGitHubTestsKey is a correct, deliberate
// guard -- but the refusal must name which destination and which key
// collided. The bare "provider sync configuration is invalid" this replaces
// is exactly the defect class CHAOS-4191 is chasing: an error with no
// context a reader can act on.
func TestGitHubTestsWriteEffectDuplicateKeyErrorNamesTheCollidingKey(t *testing.T) {
	claim := nativeTestClaim("github", "cicd")
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	row := testSuiteResultRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "9001",
		SuiteID: "duplicate-suite-id", SuiteName: "dup", LastSynced: now,
	}
	effect, err := effectBatchFromValues("test_suite_results", EffectReadbackRequired, []testSuiteResultRow{row, row})
	if err != nil {
		t.Fatal(err)
	}

	sink := TestOpsClickHouseEffects{
		Conn:  &githubTestsDuplicateKeyConn{},
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	err = sink.WriteEffect(context.Background(), claim, effect)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("WriteEffect error=%v, want ErrInvalidConfiguration", err)
	}
	for _, want := range []string{"test_suite_results", row.RunID, row.SuiteID} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error=%q does not name the colliding key: missing %q", err.Error(), want)
		}
	}
}

// TestGitHubTestsCrossArtifactSameReportPathCoverageGetsDistinctSnapshotIDs
// is codex's gap, closed: the suite/case tests above only exercised the
// JUnit path. Coverage SnapshotID is hashed independently
// (parseGitHubCoverageRow -> parseCoberturaRow / parseLCOVRow,
// github_tests_reports.go) and needs its own proof that two DISTINCT
// artifacts of one run, each contributing a coverage report at the SAME
// path (the ordinary shape of a matrix build where every leg uploads
// "coverage.info"), get distinct SnapshotIDs instead of colliding.
func TestGitHubTestsCrossArtifactSameReportPathCoverageGetsDistinctSnapshotIDs(t *testing.T) {
	repoID, runID, orgID := "c7198fbc-1945-3717-05d8-eb78866b4e79", "9001", "org-a"
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)

	lcovA := "SF:services/api/main.go\nDA:1,1\nDA:2,0\nLF:2\nLH:1\nend_of_record\n"
	lcovB := "SF:services/api/main.go\nDA:1,1\nDA:2,1\nLF:2\nLH:2\nend_of_record\n"

	rowsA, err := parseGitHubTestsArtifact(
		githubTestsZip(t, map[string]string{"coverage.info": lcovA}), "artifact-a", repoID, runID, orgID, &now, &now, now)
	if err != nil {
		t.Fatal(err)
	}
	rowsB, err := parseGitHubTestsArtifact(
		githubTestsZip(t, map[string]string{"coverage.info": lcovB}), "artifact-b", repoID, runID, orgID, &now, &now, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(rowsA.Coverage) != 1 || len(rowsB.Coverage) != 1 {
		t.Fatalf("coverage rows A=%+v B=%+v", rowsA.Coverage, rowsB.Coverage)
	}
	if rowsA.Coverage[0].SnapshotID == "" || rowsB.Coverage[0].SnapshotID == "" || rowsA.Coverage[0].SnapshotID == rowsB.Coverage[0].SnapshotID {
		t.Fatalf("CHAOS-4190: LCOV coverage snapshot ids=%q,%q from two DISTINCT artifacts at the same report path collided",
			rowsA.Coverage[0].SnapshotID, rowsB.Coverage[0].SnapshotID)
	}
}

// TestGitHubTestsCrossArtifactSameReportPathCoberturaGetsDistinctSnapshotIDs
// mirrors the LCOV case above for the Cobertura report format, the other
// branch inside parseGitHubCoverageRow.
func TestGitHubTestsCrossArtifactSameReportPathCoberturaGetsDistinctSnapshotIDs(t *testing.T) {
	repoID, runID, orgID := "c7198fbc-1945-3717-05d8-eb78866b4e79", "9001", "org-a"
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	cobertura := `<coverage lines-valid="2" lines-covered="1" branches-valid="2" branches-covered="1"><packages><package><classes><class filename="services/api/main.go"><lines><line number="1" hits="1"/><line number="2" hits="0"/></lines></class></classes></package></packages></coverage>`

	rowA, err := parseGitHubCoverageRow([]byte(cobertura), "coverage.xml", "artifact-a", repoID, runID, orgID, now)
	if err != nil {
		t.Fatal(err)
	}
	rowB, err := parseGitHubCoverageRow([]byte(cobertura), "coverage.xml", "artifact-b", repoID, runID, orgID, now)
	if err != nil {
		t.Fatal(err)
	}
	if rowA.SnapshotID == "" || rowB.SnapshotID == "" || rowA.SnapshotID == rowB.SnapshotID {
		t.Fatalf("CHAOS-4190: Cobertura coverage snapshot ids=%q,%q from two DISTINCT artifacts at the same report path collided",
			rowA.SnapshotID, rowB.SnapshotID)
	}
}

// TestGitLabTestsCrossJobSameReportPathCoverageGetsDistinctSnapshotIDs
// closes codex's gap for the GitLab path: two distinct jobs of one pipeline
// (GitLab's analogue of "artifact") each downloading a coverage report at
// the same path must not collide either.
func TestGitLabTestsCrossJobSameReportPathCoverageGetsDistinctSnapshotIDs(t *testing.T) {
	repoID, runID, orgID := "c7198fbc-1945-3717-05d8-eb78866b4e79", "9001", "org-a"
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	lcov := "SF:services/api/main.go\nDA:1,1\nDA:2,0\nLF:2\nLH:1\nend_of_record\n"

	rowA, err := parseLCOVRow([]byte(lcov), "reports/coverage.info", "job-11", repoID, runID, orgID, now)
	if err != nil {
		t.Fatal(err)
	}
	rowB, err := parseLCOVRow([]byte(lcov), "reports/coverage.info", "job-12", repoID, runID, orgID, now)
	if err != nil {
		t.Fatal(err)
	}
	if rowA.SnapshotID == "" || rowB.SnapshotID == "" || rowA.SnapshotID == rowB.SnapshotID {
		t.Fatalf("CHAOS-4190: GitLab coverage snapshot ids=%q,%q from two DISTINCT jobs at the same report path collided",
			rowA.SnapshotID, rowB.SnapshotID)
	}
}

func findGitHubTestsEffect(effects []EffectBatch, destination string) (EffectBatch, bool) {
	for _, effect := range effects {
		if effect.Destination == destination && len(effect.Rows) > 0 {
			return effect, true
		}
	}
	return EffectBatch{}, false
}

func decodeGitHubTestsEffectRows[T any](t *testing.T, effects []EffectBatch, destination string) []T {
	t.Helper()
	effect, ok := findGitHubTestsEffect(effects, destination)
	if !ok {
		t.Fatalf("no non-empty %s effect emitted", destination)
	}
	rows, err := decodeEffectRows[T](effect)
	if err != nil {
		t.Fatalf("decode %s: %v", destination, err)
	}
	return rows
}

// walkGitHubTestsChunksCapturingEffects drives CollectChunks exactly like
// walkGitHubTestsChunks (github_tests_page_budget_test.go), but additionally
// captures every emitted CompleteRouteBatch's Effects. walkGitHubTestsChunks
// only keeps the terminal metadata batch, which carries no data rows, so it
// cannot see the per-run effect batches this test needs to inspect.
func walkGitHubTestsChunksCapturingEffects(
	t *testing.T,
	handler GitHubTestsRouteHandler,
	claim Claim,
	client *providerfoundation.HTTPClient,
	maxChunks int,
) (CompleteRouteBatch, []EffectBatch) {
	t.Helper()
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	var final CompleteRouteBatch
	var effects []EffectBatch
	resume := ""
	for pass := 0; ; pass++ {
		if pass > 500 {
			t.Fatal("continuation walk never reached a final emission")
		}
		emitted := 0
		last := resume
		finalSeen := false
		err := handler.CollectChunks(
			context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				last = emission.CursorAfter
				if emission.Final {
					final = emission.Batch
					finalSeen = true
					return nil
				}
				effects = append(effects, emission.Batch.Effects...)
				emitted++
				if emitted >= maxChunks {
					return errGitHubTestsWalkContinuation
				}
				return nil
			},
		)
		if finalSeen {
			if err != nil {
				t.Fatalf("final emission returned err=%v", err)
			}
			return final, effects
		}
		if !errors.Is(err, errGitHubTestsWalkContinuation) {
			t.Fatalf("pass %d err=%v, want a continuation yield", pass, err)
		}
		resume = last
	}
}

// githubTestsDuplicateKeyConn is a driver.Conn whose PrepareBatch returns a
// batch that accepts every Append, so WriteEffect reaches its
// recordGitHubTestsKey natural-key check instead of stopping at the
// `sink.Conn == nil` wiring guard.
type githubTestsDuplicateKeyConn struct {
	driver.Conn
	batch *githubTestsDuplicateKeyBatch
}

func (c *githubTestsDuplicateKeyConn) PrepareBatch(
	context.Context, string, ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	c.batch = &githubTestsDuplicateKeyBatch{}
	return c.batch, nil
}

type githubTestsDuplicateKeyBatch struct {
	driver.Batch
	appends int
}

func (b *githubTestsDuplicateKeyBatch) Append(...any) error { b.appends++; return nil }
func (b *githubTestsDuplicateKeyBatch) Send() error         { return nil }
func (b *githubTestsDuplicateKeyBatch) Abort() error        { return nil }

// CHAOS-4392. RED on the pre-fix baseline: prod run 33149651369 for
// full-chaos/dev-health-ops carried TWO <testcase> elements sharing the
// identical name inside ONE suite of ONE artifact -- not the cross-artifact
// class CHAOS-4190 fixed above (that scoped SuiteID/CaseID by artifact ID;
// this collision is inside a single artifact's single suite, which artifact
// scoping cannot help). Both cases hashed to the same CaseID
// (hashTestIdentifier(suite.SuiteID, name) took no third input to
// disambiguate them), so this exact fixture reached WriteEffect's
// recordGitHubTestsKey and was rejected with the bare ErrInvalidConfiguration
// -- deterministically, since retrying re-parses the same archive bytes into
// the same colliding pair every time. River burned all 5 attempts before
// surfacing "Provider retries exhausted".
//
// The fix (parseJUnitRows' caseOccurrence, github_tests_reports.go): the
// SECOND-and-later case sharing a name within a suite gets an ordinal folded
// into its CaseID, so both rows are retained -- no test case is skipped or
// dropped, unlike the artifact-level skip-and-continue precedent this
// mirrors (CHAOS-4315) -- and the batch never collides at WriteEffect at
// all.
func TestGitHubTestsWithinSuiteDuplicateCaseNamesGetDistinctIDsAndWriteSucceeds(t *testing.T) {
	parseAndCommit := func(t *testing.T, fixture string, wantCases int) githubTestsReportRows {
		t.Helper()
		claim := nativeTestClaim("github", "cicd")
		rows, err := parseGitHubTestsArtifact(
			githubTestsZip(t, map[string]string{"reports/junit.xml": fixture}),
			"artifact-1", "c7198fbc-1945-3717-05d8-eb78866b4e79", "33149651369", claim.OrgID,
			nil, nil, time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC),
		)
		if err != nil {
			t.Fatalf("parse of a within-suite duplicate case name failed closed: %v", err)
		}
		if len(rows.Cases) != wantCases {
			t.Fatalf("cases=%+v, want %d rows retained (no skip, unlike the artifact-oversized precedent)", rows.Cases, wantCases)
		}
		ids := make(map[string]struct{}, len(rows.Cases))
		for _, row := range rows.Cases {
			if row.CaseID == "" {
				t.Fatalf("case=%+v has an empty CaseID", row)
			}
			if _, collided := ids[row.CaseID]; collided {
				t.Fatalf("cases=%+v, CaseID %q is not unique -- exactly CHAOS-4392's collision", rows.Cases, row.CaseID)
			}
			ids[row.CaseID] = struct{}{}
		}
		effect, err := effectBatchFromValues("test_case_results", EffectReadbackRequired, rows.Cases)
		if err != nil {
			t.Fatal(err)
		}
		batchConn := &githubTestsDuplicateKeyConn{}
		sink := TestOpsClickHouseEffects{
			Conn:  batchConn,
			Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		}
		if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
			t.Fatalf(
				"CHAOS-4392: WriteEffect rejected within-suite duplicate-named cases after disambiguation: %v", err,
			)
		}
		if batchConn.batch == nil || batchConn.batch.appends != wantCases {
			t.Fatalf("committed appends=%v, want all %d disambiguated case rows written", batchConn.batch, wantCases)
		}
		return rows
	}

	t.Run("two identically named cases get distinct ids", func(t *testing.T) {
		const fixture = `<testsuite name="matrix">` +
			`<testcase name="flaky" classname="pkg.TestA"/>` +
			`<testcase name="flaky" classname="pkg.TestB"><failure message="boom" type="AssertionError">trace</failure></testcase>` +
			`</testsuite>`
		rows := parseAndCommit(t, fixture, 2)
		if rows.Cases[0].CaseName != "flaky" || rows.Cases[1].CaseName != "flaky" {
			t.Fatalf("cases=%+v, want CaseName preserved verbatim on both rows despite the disambiguated ID", rows.Cases)
		}
		if rows.DuplicateCases != 1 {
			t.Fatalf("DuplicateCases=%d, want 1 (the telemetry input for dev_health_cicd_duplicate_test_case_total)", rows.DuplicateCases)
		}
	})

	// Codex review finding (P2): an empty name="" and a literal
	// name="unnamed" both normalize to "unnamed" in newJUnitCaseRow. Keying
	// caseOccurrence on the RAW pre-normalization name let each reach
	// occurrence=0 independently and still collide on the identical CaseID
	// -- the fix keys the occurrence map on the NORMALIZED name instead.
	t.Run("empty name and literal unnamed still get distinct ids", func(t *testing.T) {
		const fixture = `<testsuite name="matrix">` +
			`<testcase name="" classname="pkg.TestA"/>` +
			`<testcase name="unnamed" classname="pkg.TestB"/>` +
			`</testsuite>`
		parseAndCommit(t, fixture, 2)
	})

	// Codex review finding (P2): hashTestIdentifier joins parts with an
	// unescaped "::", so hashTestIdentifier(suiteID, "foo", "1") (a
	// duplicate "foo" at occurrence 1) and hashTestIdentifier(suiteID,
	// "foo::1") (a case genuinely named "foo::1") both join to the same
	// "suiteID::foo::1" before hashing -- a real collision class the ordinal
	// disambiguation introduced. The fix hashes the already-computed digest
	// with the ordinal instead of joining three raw parts.
	t.Run("an ordinal-shaped case name does not collide with a real duplicate", func(t *testing.T) {
		const fixture = `<testsuite name="matrix">` +
			`<testcase name="foo" classname="pkg.TestA"/>` +
			`<testcase name="foo" classname="pkg.TestB"/>` +
			`<testcase name="foo::1" classname="pkg.TestC"/>` +
			`</testsuite>`
		parseAndCommit(t, fixture, 3)
	})
}

// TestGitHubTestsSameArtifactSiblingSuitesSameNameCollide pins CHAOS-4487
// (local UI 2026-08-29 04:25 PT, unit 028a3088/aebe894c, sync_run a7b60282,
// repo full-chaos/dev-health-ops, run_id=33248832747): a SINGLE artifact
// whose JUnit report holds TWO SIBLING <testsuite> elements sharing the same
// `name` attribute -- an ordinary shape when one artifact bundles multiple
// report files or matrix legs that each emit a generically-named root suite
// (e.g. two files both producing <testsuite name="pytest">).
//
// SuiteID is hashTestIdentifier(runID, artifactID, name, "")
// (github_tests_reports.go, parseJUnitRows) -- scoped to run+artifact+name
// only, with NO discriminator for a suite's position among siblings in the
// same report. CHAOS-4190 scoped it to the artifact; CHAOS-4392 added
// caseOccurrence to disambiguate duplicate case names WITHIN one suite
// object -- but caseOccurrence is reset to a fresh map on every loop
// iteration over `flat` (parseJUnitRows), one iteration per suite OBJECT.
// Two suite objects that hash to the identical SuiteID therefore each start
// their first case at occurrence=0, so their first-named case also collides
// on CaseID = hashTestIdentifier(suite.SuiteID, name). Neither existing fix
// scoped an id to a suite's identity distinct from its (run, artifact, name)
// hash.
//
// This must fail red on this SHA (github_tests_reports.go still hashes
// SuiteID from (runID, artifactID, name, "") with no per-suite-object
// discriminator) and turn green once the producer disambiguates it -- e.g.
// folding the suite's index within `flat`, or a running per-name occurrence
// counter analogous to caseOccurrence, into the SuiteID hash.
func TestGitHubTestsSameArtifactSiblingSuitesSameNameCollide(t *testing.T) {
	const fixture = `<testsuites>` +
		`<testsuite name="pytest"><testcase name="test_health" classname="tests.test_api"/></testsuite>` +
		`<testsuite name="pytest"><testcase name="test_health" classname="tests.test_worker"/></testsuite>` +
		`</testsuites>`

	rows, err := parseGitHubTestsArtifact(
		githubTestsZip(t, map[string]string{"reports/junit.xml": fixture}),
		"artifact-1", "7b9583ee-4d24-2be7-4d09-34f815bebdd7", "33248832747", "70d529e0-3c06-4597-8480-794fd02328b6",
		nil, nil, time.Date(2026, 8, 29, 11, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("parse of two same-named sibling suites in one artifact failed closed: %v", err)
	}
	if len(rows.Suites) != 2 || len(rows.Cases) != 2 {
		t.Fatalf("suites=%+v cases=%+v, want 2 suite rows and 2 case rows retained", rows.Suites, rows.Cases)
	}
	if rows.Suites[0].SuiteID == rows.Suites[1].SuiteID {
		t.Fatalf("CHAOS-4487: sibling suites %q and %q in ONE artifact share SuiteID %q -- "+
			"hashTestIdentifier(runID, artifactID, name, \"\") has no per-suite-object discriminator",
			rows.Suites[0].SuiteName, rows.Suites[1].SuiteName, rows.Suites[0].SuiteID)
	}
	if rows.Cases[0].CaseID == rows.Cases[1].CaseID {
		t.Fatalf("CHAOS-4487: cases named %q in two sibling same-named suites share CaseID %q -- "+
			"this is the exact 'duplicate natural key in test_case_results batch' terminal failure "+
			"(sync_run a7b60282-4af1-58ad-a094-8dc8baab7a1f, run_id=33248832747)",
			rows.Cases[0].CaseName, rows.Cases[0].CaseID)
	}

	claim := nativeTestClaim("github", "cicd")
	effect, err := effectBatchFromValues("test_case_results", EffectReadbackRequired, rows.Cases)
	if err != nil {
		t.Fatal(err)
	}
	batchConn := &githubTestsDuplicateKeyConn{}
	sink := TestOpsClickHouseEffects{
		Conn:  batchConn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
		t.Fatalf(
			"CHAOS-4487: WriteEffect rejected the sibling-suite batch as a duplicate natural key, "+
				"exactly the prod terminal failure: %v", err,
		)
	}
}
