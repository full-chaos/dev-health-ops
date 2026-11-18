package synccoverage

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// producerArtifact is one listed artifact of the fake run: expired (never
// downloaded), or served as body; a body over the route cap is served
// without range support.
type producerArtifact struct {
	expired bool
	body    []byte
}

type producerDoer struct {
	t         *testing.T
	artifacts []producerArtifact
}

func (doer *producerDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	path := request.URL.Path
	reply := func(body string) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
	switch {
	case path == "/repos/acme/api":
		return reply(`{"id":4567,"name":"api","full_name":"acme/api","html_url":"https://github.com/acme/api","default_branch":"main","archived":false}`)
	case path == "/repos/acme/api/actions/runs":
		return reply(`{"workflow_runs":[{"id":1,"name":"CI","status":"completed","conclusion":"success","created_at":"2026-07-22T10:00:00Z","run_started_at":"2026-07-22T10:01:00Z","updated_at":"2026-07-22T10:05:00Z","run_attempt":1,"event":"push","head_sha":"abc","head_branch":"main","html_url":"https://github.com/acme/api/actions/runs/1","pull_requests":[]}]}`)
	case strings.HasSuffix(path, "/jobs"):
		return reply(`{"jobs":[]}`)
	case strings.HasSuffix(path, "/artifacts"):
		entries := make([]string, 0, len(doer.artifacts))
		for index, artifact := range doer.artifacts {
			entries = append(entries, fmt.Sprintf(`{"id":%d,"name":"test-results-%d","expired":%t}`, index+1, index+1, artifact.expired))
		}
		return reply(`{"artifacts":[` + strings.Join(entries, ",") + `]}`)
	}
	for index, artifact := range doer.artifacts {
		if path != fmt.Sprintf("/repos/acme/api/actions/artifacts/%d/zip", index+1) || artifact.expired {
			continue
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/zip"}},
			ContentLength: int64(len(artifact.body)), Body: io.NopCloser(bytes.NewReader(artifact.body)), Request: request}, nil
	}
	doer.t.Fatalf("unexpected request %s", request.URL.String())
	return nil, nil
}

func producerZip(t *testing.T, member, content string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	file, err := writer.Create(member)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

const producerJUnit = `<testsuite name="s" tests="1"><testcase classname="c" name="t" time="0.1"/></testsuite>`

const producerMaxArtifactBytes = 4096

// producerIncomplete runs the real GitHub tests chunked route over the fake
// run and returns the unit result as the sink stores it (JSON) and its
// `incomplete` member, the text load.go selects as unit.result->'incomplete'.
func producerIncomplete(t *testing.T, artifacts []producerArtifact) (reportsComplete bool, incomplete *string, result map[string]json.RawMessage) {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient("github", "https://api.github.com", fakehttp.Client(&producerDoer{t: t, artifacts: artifacts}),
		func(request *http.Request) error { request.Header.Set("Authorization", "Bearer secret"); return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	capability, _ := providersync.Capability("github", "cicd")
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 7, 31, 23, 59, 59, 0, time.UTC)
	claim := providersync.Claim{
		Unit: providersync.Unit{
			ID: "11111111-1111-4111-8111-111111111111", SyncRunID: "22222222-2222-4222-8222-222222222222", OrgID: "org-acme",
			IntegrationID: "33333333-3333-4333-8333-333333333333", SourceID: "44444444-4444-4444-8444-444444444444",
			SourceExternalID: "acme/api", SourceName: "acme/api", Provider: "github", Dataset: "cicd", CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before,
			CredentialID: "55555555-5555-4555-8555-555555555555", AuthSource: "integration_credential",
		},
		Owner: "66666666-6666-4666-8666-666666666666", Attempt: 1,
		LeaseExpiresAt: time.Date(2026, 7, 23, 13, 0, 0, 0, time.UTC),
	}
	var final *providersync.CompleteRouteBatch
	err = providersync.GitHubTestsRouteHandler{MaxArtifactBytes: producerMaxArtifactBytes}.CollectChunks(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "",
		func(emission providersync.ChunkRouteEmission) error {
			if emission.Final {
				batch := emission.Batch
				final = &batch
			}
			return nil
		})
	if err != nil || final == nil {
		t.Fatalf("CollectChunks err=%v final=%v", err, final != nil)
	}
	encoded, err := json.Marshal(final.Result)
	if err != nil {
		t.Fatalf("marshal unit result: %v", err)
	}
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatalf("decode unit result: %v", err)
	}
	complete, _ := final.Result["reports_complete"].(bool)
	if raw, ok := result["incomplete"]; ok {
		text := string(raw)
		incomplete = &text
	}
	return complete, incomplete, result
}

// CHAOS-13988 with the real producer: the providersync GitHub tests route
// writes the `incomplete` list that closedWithReason reads, so the cause
// strings the reader mirrors are checked against the route's own output, not
// a hand-written list. The loss list is the per-(component, cause) count, not
// the skipped_artifacts sample, so a sample cut at its cap cannot hide a gap
// cause: 6 expired + 1 oversized leaves the oversized artifact out of the
// 5-entry sample and the window stays a gap.
func TestClosedWithReasonFromTheRealGitHubTestsProducer(t *testing.T) {
	healthy := producerArtifact{body: producerZip(t, "junit.xml", producerJUnit)}
	expired := producerArtifact{expired: true}
	oversized := producerArtifact{body: bytes.Repeat([]byte{'x'}, producerMaxArtifactBytes+512)}
	cells := []struct {
		name      string
		artifacts []producerArtifact
		want      []string
		sampleCut bool
	}{
		{name: "expired only", artifacts: []producerArtifact{healthy, expired}, want: []string{"artifact_expired"}},
		{name: "broken XML", artifacts: []producerArtifact{{body: producerZip(t, "junit.xml", `<testsuite name="s"><testcase`)}}, want: []string{"malformed"}},
		{name: "unsafe doctype", artifacts: []producerArtifact{{body: producerZip(t, "junit.xml",
			`<!DOCTYPE testsuite [ <!ELEMENT testsuite ANY> ]><testsuite name="x"><testcase name="t"/></testsuite>`)}}, want: []string{"unsafe_doctype"}},
		{name: "expired and broken XML", artifacts: []producerArtifact{expired, {body: producerZip(t, "junit.xml", `<testsuite`)}},
			want: []string{"artifact_expired", "malformed"}},
		{name: "6 expired and 1 oversized", artifacts: []producerArtifact{expired, expired, expired, expired, expired, expired, oversized},
			want: nil, sampleCut: true},
		{name: "oversized only", artifacts: []producerArtifact{oversized}, want: nil},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			complete, incomplete, result := producerIncomplete(t, cell.artifacts)
			if complete {
				t.Fatalf("reports_complete=true, want false: the cell loses a report")
			}
			if incomplete == nil {
				t.Fatalf("unit result has no incomplete list: %s", result)
			}
			t.Logf("incomplete=%s skipped_artifacts=%s", *incomplete, result["skipped_artifacts"])
			if got := closedWithReason(incomplete); !slices.Equal(got, cell.want) {
				t.Fatalf("closedWithReason(%s) = %v, want %v", *incomplete, got, cell.want)
			}
			if cell.sampleCut {
				var sample []struct {
					Cause string `json:"cause"`
				}
				if err := json.Unmarshal(result["skipped_artifacts"], &sample); err != nil {
					t.Fatalf("decode skipped_artifacts: %v", err)
				}
				for _, marker := range sample {
					if marker.Cause != "artifact_expired" {
						t.Fatalf("sample holds %q; the cell needs the oversized artifact cut from the sample", marker.Cause)
					}
				}
				if !strings.Contains(*incomplete, `"artifact_oversized"`) {
					t.Fatalf("incomplete=%s, want the oversized loss counted although the sample cut it", *incomplete)
				}
			}
		})
	}
}
