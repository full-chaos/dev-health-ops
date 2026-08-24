package providersync

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubTestsAllUnreadableDoer serves `runs` workflow runs, each with exactly
// one artifact whose download always answers with a 2xx body that is not a
// zip -- the shape a proxy or auth edge produces when it intercepts every
// artifact request with an error document (CHAOS-4185). It shares the
// runs-phase listing and the artifacts-phase listing (no `branch` query
// distinction), matching githubTestsCorruptArtifactDoer.
type githubTestsAllUnreadableDoer struct {
	t               *testing.T
	runs            int
	archiveRequests int
}

func (doer *githubTestsAllUnreadableDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, doer.runs)), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		return githubTestsHTTPResponse(request, header, githubTestsArtifactsFixture(1)), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		doer.archiveRequests++
		// A 200 whose body is not a zip -- not an HTTP error, so every status
		// guard in downloadGitHubTestsArtifact passes it through, and NOT an
		// empty body, which downloadGitHubTestsArtifact's caller treats as
		// "nothing to parse" rather than unreadable.
		return githubTestsHTTPResponse(request, header, githubTestsBlobErrorDocument), nil
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	}
}

// RED on main: ArchivesSeen/ArchivesUnreadable do not exist yet, and neither
// does ErrGitHubTestsAllArtifactsUnreadable, so this does not compile.
//
// Two runs, each with one unreadable artifact: seen=2, unreadable=2, floor
// met. The unit must fail closed with the new sentinel instead of finalizing
// a unit that ingested nothing.
func TestGitHubTestsAllArtifactsUnreadableFailsTheUnit(t *testing.T) {
	doer := &githubTestsAllUnreadableDoer{t: t, runs: 2}
	client := githubTestsClient(t, doer)

	walk, err := walkGitHubTestsChunksResult(t, client, 8)
	if !errors.Is(err, ErrGitHubTestsAllArtifactsUnreadable) {
		t.Fatalf("err=%v, want ErrGitHubTestsAllArtifactsUnreadable", err)
	}
	if doer.archiveRequests != 2 {
		t.Fatalf("downloaded %d archives, want 2 (both observed before failing)", doer.archiveRequests)
	}
	if !strings.Contains(err.Error(), "seen=2") || !strings.Contains(err.Error(), "unreadable=2") {
		t.Fatalf("err=%v, want it to carry seen=2 unreadable=2", err)
	}
	// No final emission: a totality failure must not persist a `done` cursor
	// or completion metadata, or the durable state would say success.
	if walk.final.Result != nil {
		t.Fatalf("final batch=%#v, want no final emission on a totality failure", walk.final)
	}
	// The dev_health_provider_all_artifacts_unreadable_total counter is
	// deliberately NOT asserted at the route level: it is recorded in
	// providerunit.Handler.observeAllArtifactsUnreadable, only after the
	// durable Fail transition succeeds, so one logical unit cannot be
	// double-counted if Fail itself errors and a later attempt re-detects
	// this same condition (CHAOS-4185 codex round 1). See
	// TestHandlerRecordsAllArtifactsUnreadableOnlyAfterDurableFail in
	// internal/jobs/providerunit.
}

// RED. A counter nothing logs is unqueryable in production: an operator
// reading logs for one org/repo cannot find this failure without the durable
// error text alone. Assertions are on the RECORD and its attributes, not
// rendered text (see membershipLogHandler's rationale), and specifically on
// org/repo/sync_run_id/seen/unreadable -- the exact standing-order fields.
func TestGitHubTestsAllArtifactsUnreadableLogsAStructuredLine(t *testing.T) {
	records := captureMembershipLogs(t)
	doer := &githubTestsAllUnreadableDoer{t: t, runs: 2}
	client := githubTestsClient(t, doer)

	if _, err := walkGitHubTestsChunksResult(t, client, 8); !errors.Is(err, ErrGitHubTestsAllArtifactsUnreadable) {
		t.Fatalf("err=%v, want ErrGitHubTestsAllArtifactsUnreadable", err)
	}

	var found *slog.Record
	for index := range *records {
		if (*records)[index].Level == slog.LevelError &&
			(*records)[index].Message == "provider unit failing: every observed cicd artifact was unreadable" {
			found = &(*records)[index]
		}
	}
	if found == nil {
		t.Fatalf("no ERROR record with the totality message; got %d records", len(*records))
	}
	attrs := membershipLogAttrs(*found)
	claim := nativeTestClaim("github", "cicd")
	want := map[string]any{
		"provider": claim.Provider, "dataset": claim.Dataset,
		"org": claim.OrgID, "sync_run_id": claim.SyncRunID, "unit": claim.ID,
		"repository": "Acme/API", "seen": int64(2), "unreadable": int64(2),
	}
	for key, wantValue := range want {
		if got := attrs[key]; got != wantValue {
			t.Fatalf("attr %q=%v (%T), want %v (%T)", key, got, got, wantValue, wantValue)
		}
	}
}

// RED. Sample floor: one repository with one workflow run and one corrupt
// archive must NOT satisfy totality -- "all observed artifacts" is not
// evidence of a systematic failure at n=1. This is the false-positive
// regression the reverted CHAOS-4177 attempt shipped.
func TestGitHubTestsSingleUnreadableArtifactStaysUnderTheFloor(t *testing.T) {
	doer := &githubTestsAllUnreadableDoer{t: t, runs: 1}
	client := githubTestsClient(t, doer)

	walk, err := walkGitHubTestsChunksResult(t, client, 8)
	if err != nil {
		t.Fatalf("a single unreadable artifact (below the floor) sank the unit: err=%v", err)
	}
	if walk.cursor.Phase != "done" {
		t.Fatalf("terminal phase=%q, want done", walk.cursor.Phase)
	}
	if walk.cursor.ArchivesSeen == nil || *walk.cursor.ArchivesSeen != 1 {
		t.Fatalf("ArchivesSeen=%v, want 1", intPtrString(walk.cursor.ArchivesSeen))
	}
	if walk.cursor.ArchivesUnreadable == nil || *walk.cursor.ArchivesUnreadable != 1 {
		t.Fatalf("ArchivesUnreadable=%v, want 1", intPtrString(walk.cursor.ArchivesUnreadable))
	}
}

func intPtrString(value *int) string {
	if value == nil {
		return "<nil>"
	}
	return strconv.Itoa(*value)
}

// RED. A repository with three artifacts where only two fail must not fire
// the gate -- partial degradation stays partial.
func TestGitHubTestsPartialUnreadabilityDoesNotFireTotality(t *testing.T) {
	doer := &githubTestsCorruptArtifactDoer{t: t, artifacts: 3, corrupt: map[int]bool{1: true, 2: true}}
	client := githubTestsClient(t, doer)

	walk, err := walkGitHubTestsChunksResult(t, client, 8)
	if err != nil {
		t.Fatalf("partial unreadability sank the unit: err=%v", err)
	}
	if walk.cursor.ArchivesSeen == nil || *walk.cursor.ArchivesSeen != 3 {
		t.Fatalf("ArchivesSeen=%v, want 3", intPtrString(walk.cursor.ArchivesSeen))
	}
	if walk.cursor.ArchivesUnreadable == nil || *walk.cursor.ArchivesUnreadable != 2 {
		t.Fatalf("ArchivesUnreadable=%v, want 2", intPtrString(walk.cursor.ArchivesUnreadable))
	}
}

// RED. decodeGitHubTestsChunkCursor must reject a corrupted counter pair
// where unreadable exceeds seen -- a decode-time invariant, not a runtime
// gate check.
func TestDecodeGitHubTestsChunkCursorRejectsUnreadableExceedingSeen(t *testing.T) {
	raw := `{"phase":"artifacts","next_url":"","index":0,"run_pages":1,"artifact_pages":1,` +
		`"repo":"acme/api","archives_seen":1,"archives_unreadable":2}`
	if _, err := decodeGitHubTestsChunkCursor(raw); !errors.Is(err, ErrChunkCheckpointConflict) {
		t.Fatalf("decode err=%v, want ErrChunkCheckpointConflict for unreadable(2) > seen(1)", err)
	}
}

// RED. A negative counter is corrupt regardless of the other invariant.
func TestDecodeGitHubTestsChunkCursorRejectsNegativeCounters(t *testing.T) {
	for _, raw := range []string{
		`{"phase":"artifacts","index":0,"run_pages":1,"artifact_pages":1,"archives_seen":-1,"archives_unreadable":0}`,
		`{"phase":"artifacts","index":0,"run_pages":1,"artifact_pages":1,"archives_seen":0,"archives_unreadable":-1}`,
	} {
		if _, err := decodeGitHubTestsChunkCursor(raw); !errors.Is(err, ErrChunkCheckpointConflict) {
			t.Fatalf("decode(%s) err=%v, want ErrChunkCheckpointConflict", raw, err)
		}
	}
}

// RED. A cursor carrying exactly one of the two counters is corrupt: the
// pair is either both known or both unknown, never split.
func TestDecodeGitHubTestsChunkCursorRejectsOneCounterWithoutTheOther(t *testing.T) {
	for _, raw := range []string{
		`{"phase":"artifacts","index":0,"run_pages":1,"artifact_pages":1,"archives_seen":1}`,
		`{"phase":"artifacts","index":0,"run_pages":1,"artifact_pages":1,"archives_unreadable":1}`,
	} {
		if _, err := decodeGitHubTestsChunkCursor(raw); !errors.Is(err, ErrChunkCheckpointConflict) {
			t.Fatalf("decode(%s) err=%v, want ErrChunkCheckpointConflict", raw, err)
		}
	}
}

// RED. A fresh walk (empty resume cursor) starts the counters at known zero,
// not unknown -- otherwise a brand-new unit could never detect totality at
// all.
func TestDecodeGitHubTestsChunkCursorFreshWalkStartsCountersKnown(t *testing.T) {
	cursor, err := decodeGitHubTestsChunkCursor("")
	if err != nil {
		t.Fatalf("decode empty cursor: %v", err)
	}
	if cursor.ArchivesSeen == nil || *cursor.ArchivesSeen != 0 {
		t.Fatalf("ArchivesSeen=%v, want known 0", intPtrString(cursor.ArchivesSeen))
	}
	if cursor.ArchivesUnreadable == nil || *cursor.ArchivesUnreadable != 0 {
		t.Fatalf("ArchivesUnreadable=%v, want known 0", intPtrString(cursor.ArchivesUnreadable))
	}
}

// RED. THE MANDATORY CASE (CHAOS-4185 part 3): a cursor written before these
// fields existed decodes with them UNKNOWN (nil), never zero. A walk that
// resumes from such a cursor mid-artifacts-phase and then observes only
// unreadable artifacts from here on must NOT fail -- it cannot know whether
// earlier, pre-deploy attempts already read good archives. This is the exact
// false positive the reverted CHAOS-4177 attempt shipped.
func TestGitHubTestsLegacyCursorWithoutCountersNeverFiresTheGate(t *testing.T) {
	legacyResume := `{"phase":"artifacts","next_url":"","index":0,"run_pages":1,"artifact_pages":1,"repo":"acme/api"}`
	decoded, err := decodeGitHubTestsChunkCursor(legacyResume)
	if err != nil {
		t.Fatalf("legacy cursor failed to decode: %v", err)
	}
	if decoded.ArchivesSeen != nil || decoded.ArchivesUnreadable != nil {
		t.Fatalf("legacy cursor decoded counters as known (%v/%v), want UNKNOWN (nil)",
			intPtrString(decoded.ArchivesSeen), intPtrString(decoded.ArchivesUnreadable))
	}

	doer := &githubTestsAllUnreadableDoer{t: t, runs: 3}
	client := githubTestsClient(t, doer)

	var finalCursor githubTestsChunkCursor
	var sawFinal bool
	err = GitHubTestsRouteHandler{}.CollectChunks(
		context.Background(), nativeTestClaim("github", "cicd"),
		providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), legacyResume,
		func(emission ChunkRouteEmission) error {
			if emission.Final {
				sawFinal = true
				decoded, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
				if decodeErr != nil {
					t.Fatalf("decode terminal cursor: %v", decodeErr)
				}
				finalCursor = decoded
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf(
			"a walk resuming from a legacy (pre-deploy) cursor failed: err=%v; "+
				"a walk spanning the deploy must not fail (CHAOS-4185 part 3)", err,
		)
	}
	if !sawFinal || finalCursor.Phase != "done" {
		t.Fatalf("walk did not reach a done final emission: sawFinal=%v phase=%q", sawFinal, finalCursor.Phase)
	}
	if finalCursor.ArchivesSeen != nil || finalCursor.ArchivesUnreadable != nil {
		t.Fatalf(
			"terminal cursor carries known counters (%v/%v) after resuming from an unknown state; "+
				"once unknown, a walk must stay unknown for its lifetime",
			intPtrString(finalCursor.ArchivesSeen), intPtrString(finalCursor.ArchivesUnreadable),
		)
	}
	// The all_artifacts_unreadable metric is recorded at the providerunit
	// level, not by this route, so it is not asserted here -- see
	// TestHandlerRecordsAllArtifactsUnreadableOnlyAfterDurableFail in
	// internal/jobs/providerunit.
}

// RED. Continuation: the gate must evaluate against counters ACCUMULATED
// across attempts, not reset per attempt. maxChunks=1 forces the walk to
// span multiple passes; a single-emission fixture (the reverted attempt's
// only test) cannot detect a reset.
func TestGitHubTestsAllArtifactsUnreadableAccumulatesAcrossContinuation(t *testing.T) {
	doer := &githubTestsAllUnreadableDoer{t: t, runs: 3}
	client := githubTestsClient(t, doer)

	walk, err := walkGitHubTestsChunksResult(t, client, 1)
	if walk.passes < 2 {
		t.Fatalf("passes=%d, want the walk to span multiple attempts (maxChunks=1, 3 runs)", walk.passes)
	}
	if !errors.Is(err, ErrGitHubTestsAllArtifactsUnreadable) {
		t.Fatalf("err=%v, want ErrGitHubTestsAllArtifactsUnreadable after %d passes", err, walk.passes)
	}
	if !strings.Contains(err.Error(), "seen=3") || !strings.Contains(err.Error(), "unreadable=3") {
		t.Fatalf("err=%v, want it to carry the FULL accumulated seen=3 unreadable=3, not a per-attempt reset", err)
	}
}

// RED. Done resume: a walk that finalized successfully (partial, not total,
// degradation) must not re-evaluate into a failure -- or double-count the
// metric -- when its `done` cursor is resumed to republish completion
// metadata (CHAOS-3820's resume path).
func TestGitHubTestsDoneResumeDoesNotReevaluateOrDoubleCount(t *testing.T) {
	doer := &githubTestsCorruptArtifactDoer{t: t, artifacts: 2, corrupt: map[int]bool{1: true}}
	client := githubTestsClient(t, doer)

	claim := nativeTestClaim("github", "cicd")
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	var terminal string
	finals := 0
	run := func(resume string) {
		if err := (GitHubTestsRouteHandler{}).CollectChunks(
			context.Background(), claim, providerfoundation.Credential{}, client, now, resume,
			func(emission ChunkRouteEmission) error {
				if emission.Final {
					finals++
					terminal = emission.CursorAfter
				}
				return nil
			},
		); err != nil {
			t.Fatalf("CollectChunks(%q): %v", resume, err)
		}
	}
	run("")
	if finals != 1 {
		t.Fatalf("first pass finals=%d, want 1", finals)
	}
	run(terminal)
	if finals != 2 {
		t.Fatalf("after done-resume finals=%d, want 2 (resume republishes, does not fail)", finals)
	}
}

// githubTestsReanchorAllUnreadableDoer combines a shrinking runs page (to
// force a re-anchor replay) with an artifact that always answers unreadable,
// so a re-anchor's whole-page replay re-processes an ALREADY-counted archive.
// This is the CHAOS-4185 part 6 re-verification: the counter lifecycle's
// no-double-count clearance must be re-checked against a re-anchoring walk.
type githubTestsReanchorAllUnreadableDoer struct {
	t                *testing.T
	items            int
	corrupt          bool
	artifactPhaseURL string
}

func (doer *githubTestsReanchorAllUnreadableDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		if request.URL.Query().Get("branch") != "" {
			doer.artifactPhaseURL = request.URL.String()
		}
		if doer.items == 0 {
			return githubTestsHTTPResponse(request, header, `{"workflow_runs":[]}`), nil
		}
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, doer.items)), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		if !doer.corrupt {
			// The discovery pass only needs to reach the artifacts phase and
			// capture its URL; it must not itself trip the totality gate.
			return githubTestsHTTPResponse(request, header, `{"artifacts":[]}`), nil
		}
		return githubTestsHTTPResponse(request, header, githubTestsArtifactsFixture(1)), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		return githubTestsHTTPResponse(request, header, githubTestsBlobErrorDocument), nil
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	}
}

// RED (codex round 1, HIGH). A re-anchor re-walks its WHOLE page from index
// 0, which re-downloads and re-counts every artifact on it -- including one
// an earlier attempt already reflected in ArchivesSeen/ArchivesUnreadable.
// Before this fix, a genuinely single real unreadable artifact (seen=1,
// unreadable=1, correctly BELOW the floor) could be replayed once by a
// re-anchor and become seen=2/unreadable=2 -- crossing the floor and firing
// totality on ONE distinct observation counted twice. This is the exact
// false-positive class the sample floor exists to prevent (CHAOS-4185 part
// 2), reached through a mechanism the original floor test never exercised.
//
// The fix: a genuine artifacts-phase re-anchor now poisons the totality
// counters to UNKNOWN for the rest of the walk (githubTestsResumeStart's new
// bool return), the same bounded/self-healing trade-off already accepted for
// a legacy pre-deploy cursor. This walk must therefore complete WITHOUT the
// totality sentinel even though every artifact it downloads, before and
// after the re-anchor, is unreadable.
func TestGitHubTestsAllArtifactsUnreadableReanchorReplayNeverFalselyCrossesTheFloor(t *testing.T) {
	discover := &githubTestsReanchorAllUnreadableDoer{t: t, items: 5}
	if err := (GitHubTestsRouteHandler{}).CollectChunks(
		context.Background(), nativeTestClaim("github", "cicd"), providerfoundation.Credential{},
		githubTestsClient(t, discover), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(ChunkRouteEmission) error { return nil },
	); err != nil {
		t.Fatalf("discovery pass: %v", err)
	}
	if discover.artifactPhaseURL == "" {
		t.Fatal("discovery pass never reached the artifacts phase; the resume cursor below would be meaningless")
	}

	// Simulates ONE genuinely distinct archive already durably committed as
	// unreadable (seen=1, unreadable=1, correctly below the floor) at index 4
	// of a page that has since shrunk to 2 items -- forcing a re-anchor that
	// replays those 2 items from index 0.
	resume := `{"phase":"artifacts","next_url":` + strconv.Quote(discover.artifactPhaseURL) +
		`,"index":4,"run_pages":1,"artifact_pages":1,"repo":"acme/api",` +
		`"archives_seen":1,"archives_unreadable":1}`

	doer := &githubTestsReanchorAllUnreadableDoer{t: t, items: 2, corrupt: true}
	client := githubTestsClient(t, doer)

	var finalCursor githubTestsChunkCursor
	sawFinal := false
	err := (GitHubTestsRouteHandler{}).CollectChunks(
		context.Background(), nativeTestClaim("github", "cicd"), providerfoundation.Credential{},
		client, time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume,
		func(emission ChunkRouteEmission) error {
			if emission.Final {
				sawFinal = true
				decoded, decodeErr := decodeGitHubTestsChunkCursor(emission.CursorAfter)
				if decodeErr != nil {
					t.Fatalf("decode terminal cursor: %v", decodeErr)
				}
				finalCursor = decoded
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf(
			"a re-anchor replay falsely crossed the totality floor: err=%v; "+
				"want the walk to complete (only one artifact was ever distinctly observed)", err,
		)
	}
	if !sawFinal || finalCursor.Phase != "done" {
		t.Fatalf("walk did not reach a done final emission: sawFinal=%v phase=%q", sawFinal, finalCursor.Phase)
	}
	if finalCursor.ArchivesSeen != nil || finalCursor.ArchivesUnreadable != nil {
		t.Fatalf(
			"terminal cursor carries known counters (%v/%v) after a re-anchor replay; "+
				"a genuine re-anchor must poison them to UNKNOWN for the rest of the walk",
			intPtrString(finalCursor.ArchivesSeen), intPtrString(finalCursor.ArchivesUnreadable),
		)
	}
}

// RED (codex round 1 companion). A page resumed WITHIN its stored index --
// no shrink, no re-anchor -- must keep its known counters and still detect a
// genuine totality failure. This is the control that proves the fix above
// poisons ONLY on a genuine re-anchor, not on every resume.
func TestGitHubTestsAllArtifactsUnreadableOrdinaryResumeKeepsCountersKnown(t *testing.T) {
	doer := &githubTestsAllUnreadableDoer{t: t, runs: 2}
	client := githubTestsClient(t, doer)

	// maxChunks=1 forces a resume between the two runs' artifacts, but the
	// page never shrinks (githubTestsAllUnreadableDoer always serves the
	// same 2 runs), so the stored index keeps addressing a real item and
	// githubTestsResumeStart never re-anchors.
	_, err := walkGitHubTestsChunksResult(t, client, 1)
	if !errors.Is(err, ErrGitHubTestsAllArtifactsUnreadable) {
		t.Fatalf("err=%v, want ErrGitHubTestsAllArtifactsUnreadable: an ordinary resume must not "+
			"lose its known counters", err)
	}
	if !strings.Contains(err.Error(), "seen=2") || !strings.Contains(err.Error(), "unreadable=2") {
		t.Fatalf("err=%v, want seen=2 unreadable=2", err)
	}
}

// githubTestsEmptyArtifactDoer serves `runs` workflow runs, each with one
// artifact whose download answers 200 OK with a TRULY EMPTY body -- not an
// HTTP error, and not even a malformed payload to reject. A real GitHub
// artifact download either has bytes or errors; a 2xx-with-nothing is the
// same "broken edge answers every request" condition the totality gate
// exists to catch, just via the emptiest possible non-answer.
type githubTestsEmptyArtifactDoer struct {
	t               *testing.T
	runs            int
	archiveRequests int
}

func (doer *githubTestsEmptyArtifactDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, doer.runs)), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		return githubTestsHTTPResponse(request, header, githubTestsArtifactsFixture(1)), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		doer.archiveRequests++
		return githubTestsHTTPResponse(request, header, ""), nil
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	}
}

// RED (codex round 2, MEDIUM). Two runs, each with one artifact whose
// download body is empty: before this fix, `len(archive) == 0` silently
// `continue`d without incrementing ArchivesUnreadable or recording any
// incomplete evidence, so ArchivesSeen grew (2) while ArchivesUnreadable
// stayed 0 -- the totality gate never fired, and a broken proxy/edge
// returning empty bodies for every artifact would finalize the unit as
// healthy having ingested zero report rows. The exact failure this whole
// ticket exists to close, reachable through the one path the corrupt-body
// fixtures never exercised.
func TestGitHubTestsEmptyArtifactBodiesCountAsUnreadable(t *testing.T) {
	doer := &githubTestsEmptyArtifactDoer{t: t, runs: 2}
	client := githubTestsClient(t, doer)

	walk, err := walkGitHubTestsChunksResult(t, client, 8)
	if !errors.Is(err, ErrGitHubTestsAllArtifactsUnreadable) {
		t.Fatalf("err=%v, want ErrGitHubTestsAllArtifactsUnreadable: empty artifact bodies "+
			"must count toward totality exactly like an unreadable archive", err)
	}
	if doer.archiveRequests != 2 {
		t.Fatalf("downloaded %d archives, want 2 (both observed before failing)", doer.archiveRequests)
	}
	if !strings.Contains(err.Error(), "seen=2") || !strings.Contains(err.Error(), "unreadable=2") {
		t.Fatalf("err=%v, want it to carry seen=2 unreadable=2", err)
	}
	if walk.final.Result != nil {
		t.Fatalf("final batch=%#v, want no final emission on a totality failure", walk.final)
	}
}
