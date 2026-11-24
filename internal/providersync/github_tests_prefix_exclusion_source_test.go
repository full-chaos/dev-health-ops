package providersync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

const githubTestsPrefixSourceChunkMessage = "github tests chunk excluded artifacts by name prefix"

// githubTestsPrefixSourceChunkLines returns the source sets of the per-chunk prefix exclusion lines and checks each line's
// shape: INFO, a count >= 1, a sorted value from the closed set {code, config}, and only the closed attribute keys.
func githubTestsPrefixSourceChunkLines(t *testing.T, records []slog.Record) (sources [][]string, counts []int64) {
	t.Helper()
	keys := []string{"dataset", "excluded_non_report_artifact_prefix", "excluded_non_report_artifact_prefix_source", "provider", "repository", "unit"}
	for _, record := range records {
		if record.Message != githubTestsPrefixSourceChunkMessage {
			continue
		}
		if record.Level != slog.LevelInfo {
			t.Errorf("level = %s, want INFO", record.Level)
		}
		attrs := membershipLogAttrs(record)
		got := make([]string, 0, len(attrs))
		for key := range attrs {
			got = append(got, key)
		}
		slices.Sort(got)
		if !slices.Equal(got, keys) {
			t.Errorf("attribute keys = %v, want %v", got, keys)
		}
		count, _ := attrs["excluded_non_report_artifact_prefix"].(int64)
		if count < 1 {
			t.Errorf("excluded_non_report_artifact_prefix = %v, want >= 1", attrs["excluded_non_report_artifact_prefix"])
		}
		source, ok := attrs["excluded_non_report_artifact_prefix_source"].([]string)
		if !ok || len(source) == 0 || len(source) > 2 || !slices.IsSorted(source) || len(slices.Compact(slices.Clone(source))) != len(source) {
			t.Errorf("excluded_non_report_artifact_prefix_source = %v (ok=%t), want a sorted set", attrs["excluded_non_report_artifact_prefix_source"], ok)
		}
		for _, value := range source {
			if value != githubTestsPrefixExclusionSourceConfig && value != githubTestsPrefixExclusionSourceCode {
				t.Errorf("source value %q is outside the closed set {code, config}", value)
			}
		}
		sources = append(sources, source)
		counts = append(counts, count)
	}
	return sources, counts
}

func githubTestsPrefixSourceUnion(sources [][]string) []string {
	union := []string{}
	for _, source := range sources {
		for _, value := range source {
			if !slices.Contains(union, value) {
				union = append(union, value)
			}
		}
	}
	slices.Sort(union)
	return union
}

// The per-chunk line names the list each prefix exclusion of that chunk came from (closed set config | code; the
// configuration's list is checked first, on the full name). The skip summary line carries no source.
func TestGitHubTestsChunkLineNamesThePrefixExclusionSource(t *testing.T) {
	report := githubTestsPrefixGuardArtifact{name: "test-results", members: map[string]string{
		"junit.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/a", "TestA1", "TestA2") + "</testsuites>"}}
	extra := githubTestsPrefixGuardArtifact{name: "ci-extra-1", members: map[string]string{
		"x.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/x", "TestX") + "</testsuites>"}}
	image := githubTestsPrefixGuardArtifact{name: "go-images-amd64-9-1", members: map[string]string{"images.tar": "not-a-report"}}
	timings := githubTestsPrefixGuardArtifact{name: "go-test-timings-integration-packages-1", members: map[string]string{"t.tsv.gz": "pkg\t1.0"}}
	codeList := []string{"go-images-", "go-test-timings-", "go-junit-part-"}
	// A 40-byte configured prefix (Parse allows up to artifactexclude.MaxEntryBytes = 100): the sample cuts names at
	// githubTestsMaxArtifactNameBytes (24), so the source is taken at match time on the full name.
	const longConfig = "ci-long-artifact-family-for-a-forty-byte"
	long := githubTestsPrefixGuardArtifact{name: longConfig + "-1", members: map[string]string{"x.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/l", "TestL") + "</testsuites>"}}
	// A 41-byte configured prefix that starts with a code prefix: both lists match, so it reports config (a cut name would
	// match the code prefix only).
	const longBoth = "go-images-amd64-for-a-long-config-prefix-"
	longImage := githubTestsPrefixGuardArtifact{name: longBoth + "9-1", members: map[string]string{"images.tar": "not-a-report"}}
	for _, cell := range []struct {
		name           string
		artifacts      []githubTestsPrefixGuardArtifact
		configPrefixes []string
		want           []string
	}{
		{name: "A: only the configuration's list matches", artifacts: []githubTestsPrefixGuardArtifact{extra, report}, configPrefixes: []string{"ci-extra-"}, want: []string{"config"}},
		{name: "B: only the code list matches (a name cut at the sample bound too)", artifacts: []githubTestsPrefixGuardArtifact{image, timings, report}, want: []string{"code"}},
		{name: "C: a prefix in both lists reports config", artifacts: []githubTestsPrefixGuardArtifact{image, report}, configPrefixes: []string{"GO-IMAGES-"}, want: []string{"config"}},
		{name: "D: one exclusion from each list", artifacts: []githubTestsPrefixGuardArtifact{image, extra, report}, configPrefixes: []string{"ci-extra-"}, want: []string{"code", "config"}},
		{name: "E: a 40-byte configured prefix", artifacts: []githubTestsPrefixGuardArtifact{long, report}, configPrefixes: []string{longConfig}, want: []string{"config"}},
		// No code prefix is longer than 16 bytes ("go-test-timings-"); F is the longest code-only match: a 38-byte name.
		{name: "F: a long name matched only by the code list", artifacts: []githubTestsPrefixGuardArtifact{timings, report}, want: []string{"code"}},
		{name: "G: a 41-byte configured prefix that starts with a code prefix", artifacts: []githubTestsPrefixGuardArtifact{longImage, report}, configPrefixes: []string{longBoth}, want: []string{"config"}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			records := captureMembershipLogs(t)
			served, _ := githubTestsPrefixGuardRunWith(t, cell.artifacts, codeList, cell.configPrefixes)
			if served.excludedPrefix == 0 {
				t.Fatalf("premise: no prefix exclusion, got %+v", served)
			}
			sources, _ := githubTestsPrefixSourceChunkLines(t, *records)
			if len(sources) == 0 {
				t.Fatalf("no per-chunk prefix exclusion line; records: %+v", *records)
			}
			if got := githubTestsPrefixSourceUnion(sources); !slices.Equal(got, cell.want) {
				t.Errorf("sources over the chunk lines = %v, want %v", got, cell.want)
			}
			summaries := 0
			for _, record := range *records {
				if record.Message != "provider artifacts skipped this unit; inventory continued" {
					continue
				}
				summaries++
				if value, ok := membershipLogAttrs(record)["excluded_non_report_artifact_prefix_source"]; ok {
					t.Errorf("the skip summary carries a source %v: it is per chunk only", value)
				}
			}
			if summaries == 0 {
				t.Fatalf("no skip summary line; records: %+v", *records)
			}
		})
	}
}

// githubTestsPrefixSourceRunsDoer serves three runs, each with its own artifact listing (ids unique over the runs).
type githubTestsPrefixSourceRunsDoer struct {
	t    *testing.T
	runs map[string][]int
	all  []githubTestsPrefixGuardArtifact
}

func (doer *githubTestsPrefixSourceRunsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	header := http.Header{"Content-Type": {"application/json"}}
	path := request.URL.Path
	switch {
	case path == "/repos/acme/api":
		return githubTestsHTTPResponse(request, header, gitHubRepositoryFixture), nil
	case path == "/repos/acme/api/actions/runs":
		return githubTestsHTTPResponse(request, header, githubTestsWorkflowRunsFixture(1, 3)), nil
	case strings.HasSuffix(path, "/jobs"):
		return githubTestsHTTPResponse(request, header, `{"jobs":[]}`), nil
	case strings.HasSuffix(path, "/artifacts"):
		run := strings.TrimSuffix(strings.TrimPrefix(path, "/repos/acme/api/actions/runs/"), "/artifacts")
		var body strings.Builder
		body.WriteString(`{"artifacts":[`)
		for index, id := range doer.runs[run] {
			if index > 0 {
				body.WriteByte(',')
			}
			body.WriteString(`{"id":` + strconv.Itoa(id) + `,"name":"` + doer.all[id-1].name + `","expired":false}`)
		}
		body.WriteString(`]}`)
		return githubTestsHTTPResponse(request, header, body.String()), nil
	case strings.HasPrefix(path, "/repos/acme/api/actions/artifacts/") && strings.HasSuffix(path, "/zip"):
		id := githubTestsArtifactIDFromPath(doer.t, path)
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/zip"}},
			Body: io.NopCloser(bytes.NewReader(githubTestsZip(doer.t, doer.all[id-1].members))), Request: request,
		}, nil
	default:
		doer.t.Fatalf("unexpected request %s", request.URL.String())
		return nil, nil
	}
}

// Closed set per chunk: seven prefix exclusions from both lists over three runs (the middle run's are code-only), walked
// once yielding after every emission (each chunk a new pass) and once in one pass. Each chunk line holds THAT chunk's
// sorted set (at most the two values, never a set carried from an earlier chunk), at most one line per chunk with a
// prefix exclusion, and the line counts add up to the unit's counter.
func TestGitHubTestsChunkLinePrefixSourceIsAClosedSetPerChunk(t *testing.T) {
	doer := &githubTestsPrefixSourceRunsDoer{t: t, runs: map[string][]int{}}
	for run, names := range map[string][]string{
		"1": {"go-images-a-1", "ci-extra-1"},
		"2": {"go-test-timings-1"},
		"3": {"ci-extra-2", "go-images-a-2", "ci-extra-3", "go-test-timings-2"},
	} {
		for _, name := range append(names, "test-results-"+run) {
			members := map[string]string{"x.bin": "not-a-report"}
			if strings.HasPrefix(name, "test-results-") {
				members = map[string]string{"junit.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/"+run, "TestA") + "</testsuites>"}
			}
			doer.all = append(doer.all, githubTestsPrefixGuardArtifact{name: name, members: members})
			doer.runs[run] = append(doer.runs[run], len(doer.all))
		}
	}
	claim := nativeTestClaim("github", "cicd")
	claim.IntegrationConfig = map[string]any{"ci_artifact_exclude_prefixes": []any{"ci-extra-"}}
	for _, yieldEach := range []bool{true, false} {
		t.Run("yield after every emission "+strconv.FormatBool(yieldEach), func(t *testing.T) {
			client := githubTestsClient(t, fakehttp.Client(doer))
			records := captureMembershipLogs(t)
			resume, final, excluded, chunksWithExclusions := "", false, 0, 0
			for passes := 0; !final; passes++ {
				if passes > 200 {
					t.Fatal("the walk never finished")
				}
				err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
					time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), resume,
					func(emission ChunkRouteEmission) error {
						before := 0
						if emission.CursorBefore != "" {
							cursor, err := decodeGitHubTestsChunkCursor(emission.CursorBefore)
							if err != nil {
								return err
							}
							before = cursor.ExcludedNonReportPrefix
						}
						after, err := decodeGitHubTestsChunkCursor(emission.CursorAfter)
						if err != nil {
							return err
						}
						if after.ExcludedNonReportPrefix > before {
							chunksWithExclusions++
						}
						excluded = after.ExcludedNonReportPrefix
						resume = emission.CursorAfter
						if emission.Final {
							final = true
							return nil
						}
						if yieldEach {
							return errGitHubTestsWalkContinuation
						}
						return nil
					})
				if err != nil && !errors.Is(err, errGitHubTestsWalkContinuation) {
					t.Fatalf("pass %d: %v", passes, err)
				}
			}
			if excluded != 7 || chunksWithExclusions != 3 {
				t.Fatalf("premise: 7 prefix exclusions in 3 chunks, got %d in %d", excluded, chunksWithExclusions)
			}
			sources, counts := githubTestsPrefixSourceChunkLines(t, *records)
			got := make([]string, 0, len(sources))
			for _, source := range sources {
				got = append(got, strings.Join(source, " "))
			}
			slices.Sort(got)
			if want := []string{"code", "code config", "code config"}; !slices.Equal(got, want) {
				t.Errorf("chunk line sources = %v, want %v (one line per chunk with a prefix exclusion, its own set)", got, want)
			}
			total := int64(0)
			for _, count := range counts {
				total += count
			}
			if total != int64(excluded) {
				t.Errorf("chunk line counts add up to %d, want the unit's %d", total, excluded)
			}
		})
	}
}

// The field name and the value exactly as the production JSON handler (logging.NewJSON) renders them on the chunk line:
// a config-only unit, a code-only unit and a unit with both.
func TestGitHubTestsChunkLineRendersThePrefixExclusionSourceLiterals(t *testing.T) {
	report := githubTestsPrefixGuardArtifact{name: "test-results", members: map[string]string{
		"junit.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/a", "TestA1") + "</testsuites>"}}
	extra := githubTestsPrefixGuardArtifact{name: "ci-extra-1", members: map[string]string{
		"x.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/x", "TestX") + "</testsuites>"}}
	image := githubTestsPrefixGuardArtifact{name: "go-images-amd64-9-1", members: map[string]string{"images.tar": "not-a-report"}}
	codeList := []string{"go-images-", "go-test-timings-", "go-junit-part-"}
	for _, cell := range []struct {
		name           string
		artifacts      []githubTestsPrefixGuardArtifact
		configPrefixes []string
		literal        string
	}{
		{name: "config only", artifacts: []githubTestsPrefixGuardArtifact{image, report}, configPrefixes: codeList, literal: `"excluded_non_report_artifact_prefix_source":["config"]`},
		{name: "code only", artifacts: []githubTestsPrefixGuardArtifact{image, report}, literal: `"excluded_non_report_artifact_prefix_source":["code"]`},
		{name: "both", artifacts: []githubTestsPrefixGuardArtifact{image, extra, report}, configPrefixes: []string{"ci-extra-"}, literal: `"excluded_non_report_artifact_prefix_source":["code","config"]`},
	} {
		t.Run(cell.name, func(t *testing.T) {
			var out bytes.Buffer
			restore := logging.InstallDefault(logging.NewJSON(&out, slog.LevelDebug))
			githubTestsPrefixGuardRunWith(t, cell.artifacts, codeList, cell.configPrefixes)
			restore()
			lines := 0
			for _, line := range strings.Split(out.String(), "\n") {
				if !strings.Contains(line, `"msg":"`+githubTestsPrefixSourceChunkMessage+`"`) {
					continue
				}
				lines++
				if !strings.Contains(line, `"level":"INFO"`) || !strings.Contains(line, cell.literal) {
					t.Errorf("chunk line does not hold INFO and %s: %s", cell.literal, line)
				}
			}
			if lines == 0 {
				t.Fatalf("no chunk line in the JSON log: %s", out.String())
			}
		})
	}
}
