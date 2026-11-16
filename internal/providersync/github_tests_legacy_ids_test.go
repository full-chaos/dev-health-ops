package providersync

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-13925: a slotted suite or coverage row lists the keys the pre-slot writer gives the same report, so a reader can tell
// which legacy row it replaces. The want values below are the suite_id and snapshot_id the pre-slot writer (ops 4444a99f, the
// writer live before CHAOS-13873) emitted for each artifact of this fixture through its real chunked route, one artifact per
// listing. They are recorded, not computed here, so a change of the key formula on either side turns these cells red. A suite is
// named by its suite name and its first case (the "dup" suites differ only there); a coverage report by its format.
var legacyIDsPreSlotSuites = map[int]map[string]string{
	102: {
		"dup/TestD1":    "2d324256c17946e799d52050b65b0cb527a8298c26ff2427b1f9da070ee25f31",
		"dup/TestD2":    "3391d4b952e7dabe2a4f2ff24c448944fa3d55deac3af3da090075929736af02",
		"dup/TestD3":    "0d32839f8cc27ac0183173046778f4bf66c3b48c34e77e3d3575e10431d14acc",
		"pkg/b/TestX":   "e1fc7a762df81aecc2381b023958c48493810dc5c46cf192ea05549cb777049f",
		"unnamed/TestU": "60f44bf38b76cdc35d004e8e602c75802fbf9af98be2ee2d518cc70a207ca85c",
	},
	104: {
		"pkg/c/TestC": "5e65cc6176b6f6aefe776fd9d81ba0814f53f8e2f31d8289b31fa309ba220f63",
	},
	203: {
		"dup/TestD1":    "0d93860302907385868f291defd0c8e451cea293c0c55c18f030a7b2c6309d48",
		"dup/TestD2":    "f611264d65dcee5af50c959437dcdf16e0b07b18731063100b29660fbb00b6a9",
		"dup/TestD3":    "15637923d19ee192f8f239a4c76b3f40dd61f88e80dcbaf9430266ebe3275621",
		"pkg/b/TestX":   "fd065faf1ed876a4509b930b05686ff5bde5103a38c8ab1e01b8aaa7c6e654af",
		"unnamed/TestU": "aad5cefe0bd5556b6a7f1b693c0fcd83d3b54f4d80498f6dd7d7bb5580c52fb2",
	},
}

var legacyIDsPreSlotCoverage = map[int]map[string]string{
	102: {
		"cobertura": "ddb0f860c1c3945567fa36af42c00a040b3756662d7ccaff519ffcfbec658596",
		"lcov":      "67c9ee3e4a2d30d5b48801f6179957d634dede55c48429837b43fe0b43f4f418",
	},
	203: {
		"cobertura": "356402b91d5d347e1ac1304d14516f84c718a088e58debb96258b6d1a2fa392e",
		"lcov":      "65e59657737912f3d06ac0e0a90f8454ce31111910a57e1f90f9bb8369745ca3",
	},
}

// legacyIDsWant gives, for each row, the pre-slot keys of the same suite (or coverage report) under each of the artifact ids, in
// that order.
func legacyIDsWant(t *testing.T, rows []map[string]any, key func(map[string]any) string, golden map[int]map[string]string, artifactIDs ...int) [][]string {
	t.Helper()
	want := make([][]string, len(rows))
	for index, row := range rows {
		for _, id := range artifactIDs {
			recorded, ok := golden[id][key(row)]
			if !ok {
				t.Fatalf("artifact %d: the pre-slot writer emitted no %q", id, key(row))
			}
			want[index] = append(want[index], recorded)
		}
	}
	return want
}

func legacyIDsSuiteKey(row map[string]any) string {
	name, _ := row["suite_name"].(string)
	first, _ := row["first_case"].(string)
	return name + "/" + first
}

func legacyIDsCoverageKey(row map[string]any) string {
	format, _ := row["report_format"].(string)
	return format
}

func legacyIDsGot(rows []map[string]any, key string) [][]string {
	got := make([][]string, len(rows))
	for index, row := range rows {
		values, _ := row[key].([]any)
		for _, value := range values {
			text, _ := value.(string)
			got[index] = append(got[index], text)
		}
	}
	return got
}

func legacyIDsOfSlot(rows []map[string]any, slot string) []map[string]any {
	var out []map[string]any
	for _, row := range rows {
		if row["report_slot"] == slot {
			out = append(out, row)
		}
	}
	return out
}

func legacyIDsCheck(t *testing.T, what string, got, want [][]string) {
	t.Helper()
	if !slices.EqualFunc(got, want, func(a, b []string) bool { return slices.Equal(a, b) }) {
		t.Fatalf("%s: legacy ids\n  got  %v\n  want %v", what, got, want)
	}
}

// A re-run job's artifact 203 replaces artifact 102 of the same name (102 is superseded and not downloaded); artifact 104 is the
// other job's. Each slotted suite row lists its pre-slot key under 203 and then under 102, the "dup" suites with their ordinal
// fold (it spans report members); the coverage rows the same per format; the go-test-c rows list 104 only.
func TestGitHubTestsSlottedRowsListThePreSlotKeysTheyReplace(t *testing.T) {
	t.Parallel()
	suites, coverage := legacyIDsRows(t,
		legacyIDsArtifact{102, "go-test-b", "2026-07-22T10:20:00Z", legacyIDsReportB(t)},
		legacyIDsArtifact{203, "go-test-b", "2026-07-22T11:30:00Z", legacyIDsReportB(t)},
		legacyIDsArtifact{104, "go-test-c", "2026-07-22T10:20:00Z", legacyIDsReportC(t)},
	)
	b, c := legacyIDsOfSlot(suites, "go-test-b"), legacyIDsOfSlot(suites, "go-test-c")
	if len(b) != 5 || len(c) != 1 || len(suites) != 6 {
		t.Fatalf("suite rows: go-test-b %d, go-test-c %d, all %d; want 5, 1, 6", len(b), len(c), len(suites))
	}
	legacyIDsCheck(t, "go-test-b suites", legacyIDsGot(b, "legacy_suite_ids"), legacyIDsWant(t, b, legacyIDsSuiteKey, legacyIDsPreSlotSuites, 203, 102))
	legacyIDsCheck(t, "go-test-c suites", legacyIDsGot(c, "legacy_suite_ids"), legacyIDsWant(t, c, legacyIDsSuiteKey, legacyIDsPreSlotSuites, 104))
	covB := legacyIDsOfSlot(coverage, "go-test-b")
	if len(covB) != 2 || len(coverage) != 2 {
		t.Fatalf("coverage rows: go-test-b %d, all %d; want 2, 2", len(covB), len(coverage))
	}
	legacyIDsCheck(t, "go-test-b coverage", legacyIDsGot(covB, "legacy_snapshot_ids"), legacyIDsWant(t, covB, legacyIDsCoverageKey, legacyIDsPreSlotCoverage, 203, 102))
}

// The one-batch route lists the same keys as the chunked route for the same listing.
func TestGitHubTestsSlottedRowsOfTheOneBatchRouteListThePreSlotKeysTheyReplace(t *testing.T) {
	t.Parallel()
	suites, coverage := legacyIDsRowsCollect(t,
		legacyIDsArtifact{102, "go-test-b", "2026-07-22T10:20:00Z", legacyIDsReportB(t)},
		legacyIDsArtifact{203, "go-test-b", "2026-07-22T11:30:00Z", legacyIDsReportB(t)},
		legacyIDsArtifact{104, "go-test-c", "2026-07-22T10:20:00Z", legacyIDsReportC(t)},
	)
	b, c := legacyIDsOfSlot(suites, "go-test-b"), legacyIDsOfSlot(suites, "go-test-c")
	if len(b) != 5 || len(c) != 1 || len(suites) != 6 {
		t.Fatalf("suite rows: go-test-b %d, go-test-c %d, all %d; want 5, 1, 6", len(b), len(c), len(suites))
	}
	legacyIDsCheck(t, "go-test-b suites", legacyIDsGot(b, "legacy_suite_ids"), legacyIDsWant(t, b, legacyIDsSuiteKey, legacyIDsPreSlotSuites, 203, 102))
	legacyIDsCheck(t, "go-test-c suites", legacyIDsGot(c, "legacy_suite_ids"), legacyIDsWant(t, c, legacyIDsSuiteKey, legacyIDsPreSlotSuites, 104))
	covB := legacyIDsOfSlot(coverage, "go-test-b")
	if len(covB) != 2 || len(coverage) != 2 {
		t.Fatalf("coverage rows: go-test-b %d, all %d; want 2, 2", len(covB), len(coverage))
	}
	legacyIDsCheck(t, "go-test-b coverage", legacyIDsGot(covB, "legacy_snapshot_ids"), legacyIDsWant(t, covB, legacyIDsCoverageKey, legacyIDsPreSlotCoverage, 203, 102))
}

// With no older artifact of the name in the listing, a row lists only the pre-slot key of the artifact it was read from.
func TestGitHubTestsSlottedRowsWithoutASupersededArtifactListOneKey(t *testing.T) {
	t.Parallel()
	suites, coverage := legacyIDsRows(t,
		legacyIDsArtifact{203, "go-test-b", "2026-07-22T11:30:00Z", legacyIDsReportB(t)},
	)
	if len(suites) != 5 || len(coverage) != 2 {
		t.Fatalf("rows: %d suites, %d coverage; want 5 and 2", len(suites), len(coverage))
	}
	legacyIDsCheck(t, "suites", legacyIDsGot(suites, "legacy_suite_ids"), legacyIDsWant(t, suites, legacyIDsSuiteKey, legacyIDsPreSlotSuites, 203))
	legacyIDsCheck(t, "coverage", legacyIDsGot(coverage, "legacy_snapshot_ids"), legacyIDsWant(t, coverage, legacyIDsCoverageKey, legacyIDsPreSlotCoverage, 203))
}

// legacyIDsArtifact is one listed artifact of the CHAOS-13925 cell.
type legacyIDsArtifact struct {
	id      int
	name    string
	created string
	archive []byte
}

// legacyIDsDoer serves run 43600000001 at attempt 2 with the listed artifacts.
type legacyIDsDoer struct {
	t         *testing.T
	artifacts []legacyIDsArtifact
}

func (doer *legacyIDsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	path := request.URL.Path
	status, header, body := http.StatusOK, http.Header{"Content-Type": {"application/json"}}, "{}"
	switch {
	case path == "/repos/acme/api":
		body = gitHubRepositoryFixture
	case path == "/repos/acme/api/actions/runs":
		body = `{"workflow_runs":[{"id":43600000001,"name":"Go","status":"completed","conclusion":"success",` +
			`"created_at":"2026-07-22T10:00:00Z","run_started_at":"2026-07-22T11:10:00Z","updated_at":"2026-07-22T11:40:00Z",` +
			`"run_attempt":2,"event":"push","head_sha":"abc","head_branch":"main",` +
			`"html_url":"https://github.com/acme/api/actions/runs/43600000001","pull_requests":[]}]}`
	case path == "/repos/acme/api/actions/runs/43600000001/jobs":
		body = `{"jobs":[]}`
	case path == "/repos/acme/api/actions/runs/43600000001/artifacts":
		items := make([]string, 0, len(doer.artifacts))
		for _, artifact := range doer.artifacts {
			items = append(items, fmt.Sprintf(`{"id":%d,"name":%q,"expired":false,"size_in_bytes":%d,"created_at":%q}`,
				artifact.id, artifact.name, len(artifact.archive), artifact.created))
		}
		body = `{"total_count":` + strconv.Itoa(len(items)) + `,"artifacts":[` + strings.Join(items, ",") + `]}`
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/repos/acme/api/actions/artifacts/"), "/zip")
		status, body = http.StatusFound, ""
		header.Set("Location", "https://blob.example/a"+id+".zip")
	case strings.HasPrefix(path, "/a") && strings.HasSuffix(path, ".zip"):
		id, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(path, "/a"), ".zip"))
		for _, artifact := range doer.artifacts {
			if artifact.id == id {
				header.Set("Content-Type", "application/zip")
				body = string(artifact.archive)
			}
		}
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

// legacyIDsZip builds an archive with its members in the given order (the suite ordinals follow the member order).
func legacyIDsZip(t *testing.T, members ...[2]string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	for _, member := range members {
		file, err := writer.Create(member[0])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write([]byte(member[1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

// legacyIDsReportB is the re-run job's report: pkg/b, "dup" three times (twice in report.xml, once in a second member: the
// ordinal fold spans members), an unnamed suite, and both coverage formats.
func legacyIDsReportB(t *testing.T) []byte {
	t.Helper()
	return legacyIDsZip(t,
		[2]string{"report.xml", `<testsuites>` +
			`<testsuite name="pkg/b" time="1.5" timestamp="2026-07-22T10:02:00Z"><testcase classname="pkg/b" name="TestX" time="0.5"/><testcase classname="pkg/b" name="TestY" time="1.0"/></testsuite>` +
			`<testsuite name="dup" time="1"><testcase classname="dup" name="TestD1" time="0.1"/></testsuite>` +
			`<testsuite name="dup" time="1"><testcase classname="dup" name="TestD2" time="0.1"/></testsuite>` +
			`<testsuite name="" time="1"><testcase classname="u" name="TestU" time="0.1"/></testsuite>` +
			`</testsuites>`},
		[2]string{"nested/other.xml", `<testsuite name="dup" time="1"><testcase classname="dup" name="TestD3" time="0.1"/></testsuite>`},
		[2]string{"coverage.info", "TN:\nSF:pkg/b/x.go\nLF:10\nLH:6\nend_of_record\n"},
		[2]string{"coverage.xml", `<coverage lines-valid="10" lines-covered="7"><packages><package name="pkg/b"><classes><class filename="pkg/b/y.go"><lines><line number="1" hits="1"/><line number="2" hits="0"/></lines></class></classes></package></packages></coverage>`},
	)
}

// legacyIDsReportC is the other job's report: one suite, no coverage.
func legacyIDsReportC(t *testing.T) []byte {
	t.Helper()
	return legacyIDsZip(t, [2]string{"report.xml", `<testsuites><testsuite name="pkg/c" time="1"><testcase classname="pkg/c" name="TestC" time="0.1"/></testsuite></testsuites>`})
}

// legacyIDsRows runs one tests unit over the listing through the chunked route and returns the emitted suite and coverage rows as
// JSON objects. Each suite row gets "first_case", the smallest case name among its case rows: the route emits rows in key order, so a
// suite is told apart by its content, not by its place.
func legacyIDsRows(t *testing.T, artifacts ...legacyIDsArtifact) (suites, coverage []map[string]any) {
	t.Helper()
	since := time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC)
	var effects []EffectBatch
	err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(),
		githubTestsWindowClaim(t, since, since.Add(time.Hour)), providerfoundation.Credential{},
		githubTestsClient(t, &legacyIDsDoer{t: t, artifacts: artifacts}), since.Add(65*time.Minute), "",
		func(emission ChunkRouteEmission) error {
			effects = append(effects, emission.Batch.Effects...)
			return nil
		})
	if err != nil {
		t.Fatalf("CollectChunks: %v", err)
	}
	return legacyIDsDecode(t, effects)
}

// legacyIDsRowsCollect is legacyIDsRows through the one-batch route (GitHubTestsRouteHandler.Collect).
func legacyIDsRowsCollect(t *testing.T, artifacts ...legacyIDsArtifact) (suites, coverage []map[string]any) {
	t.Helper()
	since := time.Date(2026, 7, 22, 11, 0, 0, 0, time.UTC)
	batch, err := GitHubTestsRouteHandler{}.Collect(context.Background(),
		githubTestsWindowClaim(t, since, since.Add(time.Hour)), providerfoundation.Credential{},
		githubTestsClient(t, &legacyIDsDoer{t: t, artifacts: artifacts}), since.Add(65*time.Minute))
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return legacyIDsDecode(t, batch.Effects)
}

func legacyIDsDecode(t *testing.T, effects []EffectBatch) (suites, coverage []map[string]any) {
	t.Helper()
	firstCase := map[string]string{}
	for _, effect := range effects {
		for _, raw := range effect.Rows {
			row := map[string]any{}
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			switch effect.Destination {
			case "test_suite_results":
				suites = append(suites, row)
			case "coverage_snapshots":
				coverage = append(coverage, row)
			case "test_case_results":
				suiteID, _ := row["suite_id"].(string)
				name, _ := row["case_name"].(string)
				if current, seen := firstCase[suiteID]; !seen || name < current {
					firstCase[suiteID] = name
				}
			}
		}
	}
	for _, row := range suites {
		suiteID, _ := row["suite_id"].(string)
		row["first_case"] = firstCase[suiteID]
	}
	return suites, coverage
}
