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

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

const githubTestsPrefixSourceChunkMessage = "github tests chunk excluded artifacts by name prefix"

// githubTestsPrefixSourceChunkLines returns the sources of the per-chunk prefix exclusion lines and checks each line's
// shape: INFO, a count >= 1, the source as the string "config", and only the closed attribute keys (the earlier array key
// is not emitted beside it).
func githubTestsPrefixSourceChunkLines(t *testing.T, records []slog.Record) (sources []string, counts []int64) {
	t.Helper()
	keys := []string{"dataset", "excluded_non_report_artifact_prefix", githubTestsPrefixSourceNameKey, "provider", "repository", "unit"}
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
		source, ok := attrs[githubTestsPrefixSourceNameKey].(string)
		if !ok || source != "config" {
			t.Errorf("%s = %#v, want the string \"config\"", githubTestsPrefixSourceNameKey, attrs[githubTestsPrefixSourceNameKey])
		}
		sources = append(sources, source)
		counts = append(counts, count)
	}
	return sources, counts
}

// The per-chunk line names the source of each prefix exclusion of that chunk: the integration's list, on the full name. The
// skip summary line carries no source. With no list there is no exclusion and no line.
func TestGitHubTestsChunkLineNamesThePrefixExclusionSource(t *testing.T) {
	report := githubTestsPrefixGuardArtifact{name: "test-results", members: map[string]string{
		"junit.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/a", "TestA1", "TestA2") + "</testsuites>"}}
	extra := githubTestsPrefixGuardArtifact{name: "ci-extra-1", members: map[string]string{
		"x.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/x", "TestX") + "</testsuites>"}}
	// A 40-byte configured prefix (Parse allows up to artifactexclude.MaxEntryBytes = 100): the sample cuts names at
	// githubTestsMaxArtifactNameBytes (24), so the match is taken on the full name.
	const longConfig = "ci-long-artifact-family-for-a-forty-byte"
	long := githubTestsPrefixGuardArtifact{name: longConfig + "-1", members: map[string]string{"x.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/l", "TestL") + "</testsuites>"}}
	for _, cell := range []struct {
		name           string
		artifacts      []githubTestsPrefixGuardArtifact
		configPrefixes []string
		wantLines      bool
	}{
		{name: "A: the configuration's list matches", artifacts: []githubTestsPrefixGuardArtifact{extra, report}, configPrefixes: []string{"ci-extra-"}, wantLines: true},
		{name: "E: a 40-byte configured prefix", artifacts: []githubTestsPrefixGuardArtifact{long, report}, configPrefixes: []string{longConfig}, wantLines: true},
		{name: "H: no list, no exclusion, no line", artifacts: []githubTestsPrefixGuardArtifact{extra, report}},
		{name: "I: a list that matches nothing, no line", artifacts: []githubTestsPrefixGuardArtifact{extra, report}, configPrefixes: []string{"ci-other-"}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			records := captureMembershipLogs(t)
			served, _ := githubTestsPrefixGuardRunWith(t, cell.artifacts, cell.configPrefixes)
			sources, _ := githubTestsPrefixSourceChunkLines(t, *records)
			if !cell.wantLines {
				if served.excludedPrefix != 0 || len(sources) != 0 {
					t.Errorf("excluded=%d chunk lines=%d, want 0 and 0", served.excludedPrefix, len(sources))
				}
				return
			}
			if served.excludedPrefix == 0 || len(sources) == 0 {
				t.Fatalf("premise: no prefix exclusion or no chunk line, got %+v, %d lines", served, len(sources))
			}
			summaries := 0
			for _, record := range *records {
				if record.Message != "provider artifacts skipped this unit; inventory continued" {
					continue
				}
				summaries++
				attrs := membershipLogAttrs(record)
				for _, key := range []string{githubTestsPrefixSourceNameKey, "excluded_non_report_artifact_prefix_source"} {
					if value, ok := attrs[key]; ok {
						t.Errorf("the skip summary carries %s = %v: it is per chunk only", key, value)
					}
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

// Seven prefix exclusions over three runs, walked once yielding after every emission (each chunk a new pass) and once in one
// pass. At most one line per chunk with a prefix exclusion, each holding the string "config", and the line counts add up
// to the unit's counter.
func TestGitHubTestsChunkLinePrefixSourceIsPerChunk(t *testing.T) {
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
	claim.IntegrationConfig = map[string]any{"ci_artifact_exclude_prefixes": []any{"ci-extra-", "go-images-", "go-test-timings-"}}
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
			if want := []string{"config", "config", "config"}; !slices.Equal(sources, want) {
				t.Errorf("chunk line sources = %v, want %v (one line per chunk with a prefix exclusion)", sources, want)
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
