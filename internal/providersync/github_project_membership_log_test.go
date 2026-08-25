package providersync

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// membershipLogHandler captures slog records whole rather than as formatted
// text. Asserting on a rendered line would pin the handler's formatting, not
// the record, and would pass just as happily if every attribute collapsed into
// one string -- which is the failure mode that makes a "structured" log
// unqueryable in production.
type membershipLogHandler struct {
	records *[]slog.Record
}

func (h *membershipLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *membershipLogHandler) Handle(_ context.Context, record slog.Record) error {
	*h.records = append(*h.records, record.Clone())
	return nil
}

func (h *membershipLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *membershipLogHandler) WithGroup(string) slog.Handler { return h }

func captureMembershipLogs(t *testing.T) *[]slog.Record {
	t.Helper()
	records := []slog.Record{}
	previous := slog.Default()
	slog.SetDefault(slog.New(&membershipLogHandler{records: &records}))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &records
}

func membershipLogAttrs(record slog.Record) map[string]any {
	attrs := map[string]any{}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	return attrs
}

func fetchGitHubProjectV2ForLogging(t *testing.T, reply string) GitHubProjectV2FetchResult {
	t.Helper()
	doer := &gitHubProjectV2Doer{t: t, replies: []string{reply}}
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
		map[string]any{"org_login": "acme", "project_number": 3},
	}}
	credential := providerfoundation.Credential{Provider: "github", ID: claim.CredentialID}
	result, err := (GitHubProjectV2Fetcher{}).Fetch(
		context.Background(), claim, credential, githubProjectV2TestClient(t, doer),
		time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// TestGitHubProjectV2ReportsMembershipSkipsInAStructuredLog is the
// observability half of the skip counter.
//
// A count on a result struct is not observable in production: nothing reads
// GitHubProjectV2FetchResult except the caller that built it, and this
// collector owns no registration to publish a Prometheus series through (D18).
// Without this line the counter would be exactly what it replaced -- a drop
// nobody can see -- just with a nicer name in the source.
//
// The assertions are on the RECORD and its attributes, not on rendered text,
// because a line that concatenated every count into one message string would
// satisfy a substring check while being unqueryable by reason in a log
// backend, which is the entire point of emitting it.
//
// CHAOS-4193 retired "issue_deferred_to_snapshot_diff": an issue is no longer
// skipped, so it can no longer co-occur with a genuine skip reason to prove
// two DIFFERENT reasons are live at once. A draft issue (permanent, no
// subject to name) and an unidentifiable pull request (attention-worthy) fill
// that role instead.
func TestGitHubProjectV2ReportsMembershipSkipsInAStructuredLog(t *testing.T) {
	records := captureMembershipLogs(t)
	result := fetchGitHubProjectV2ForLogging(t, `{"data":{"organization":{"projectV2":{"items":{"nodes":[`+
		`{"id":"PVTI_1","content":{"__typename":"DraftIssue","title":"Ship it","createdAt":"2026-08-01T08:00:00Z","updatedAt":"2026-08-02T08:00:00Z"},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}},`+
		`{"id":"PVTI_2","content":{"__typename":"PullRequest","number":8,"title":"unidentifiable"},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}`+
		`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`)
	if result.MembershipSkips["draft_issue_has_no_subject"] != 1 ||
		result.MembershipSkips["pull_request_incomplete"] != 1 {
		t.Fatalf("premise failed, the fetch did not produce both skip reasons: %+v", result.MembershipSkips)
	}
	if len(*records) != 1 {
		t.Fatalf("membership skips produced %d log records, want exactly 1", len(*records))
	}
	record := (*records)[0]
	if record.Message != "github_projects_v2.membership_skips" {
		t.Fatalf("log event name = %q", record.Message)
	}
	// An unidentifiable pull request means the query or the payload changed
	// under us; that is not the same news as a draft issue, which is a
	// permanent, expected non-subject, and it must not arrive at the level
	// operators filter out.
	if record.Level != slog.LevelWarn {
		t.Fatalf("level = %s, want WARN when an attention-worthy reason is non-zero", record.Level)
	}
	attrs := membershipLogAttrs(record)
	claim := githubWorkItemOracleClaim()
	for key, want := range map[string]any{
		"provider": "github", "dataset": claim.Dataset, "org_id": claim.OrgID,
		"unit": claim.ID, "integration": claim.IntegrationID,
		"draft_issue_has_no_subject": int64(1),
		"pull_request_incomplete":    int64(1),
	} {
		if got, present := attrs[key]; !present || got != want {
			t.Errorf("attr %s = %#v (present=%t), want %#v", key, got, present, want)
		}
	}
	// A reason that did not occur must be ABSENT, not reported as zero. A
	// permanent zero series reads as coverage of a case nobody exercised. The
	// retired label must never be emitted again, whatever occurs.
	for _, absent := range []string{"unknown_content_type", "issue_deferred_to_snapshot_diff"} {
		if _, present := attrs[absent]; present {
			t.Errorf("attr %s was emitted with no occurrences", absent)
		}
	}
}

// TestGitHubProjectV2LogsNothingWhenEveryItemIsEmitted is the other half. A
// line on every sync regardless of content is noise that operators learn to
// filter, which costs the counter the attention it exists to buy.
func TestGitHubProjectV2LogsNothingWhenEveryItemIsEmitted(t *testing.T) {
	records := captureMembershipLogs(t)
	result := fetchGitHubProjectV2ForLogging(t, `{"data":{"organization":{"projectV2":{"items":{"nodes":[`+
		`{"id":"PVTI_PR","createdAt":"2026-08-01T08:00:00Z","content":{"__typename":"PullRequest","number":42,"title":"A PR","repository":{"nameWithOwner":"acme/api"}},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}`+
		`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`)
	if len(result.MembershipSkips) != 0 {
		t.Fatalf("premise failed, something was skipped: %+v", result.MembershipSkips)
	}
	if len(*records) != 0 {
		t.Fatalf("a clean fetch logged %d records: %+v", len(*records), *records)
	}
}

// TestGitHubProjectV2LogsDraftIssuesAtInfo separates the two levels using the
// vocabulary CHAOS-4193 leaves behind. A draft issue can never carry a
// membership -- that is permanent and expected on any real board, not news --
// so if it alone escalated to WARN the line would be permanently warning
// about working as designed, and the genuinely attention-worthy reasons would
// be invisible inside the noise. (The prior version of this test proved the
// same property of the issue-deferral reason, which CHAOS-4193 retired: an
// issue is no longer skipped at all.)
func TestGitHubProjectV2LogsDraftIssuesAtInfo(t *testing.T) {
	records := captureMembershipLogs(t)
	fetchGitHubProjectV2ForLogging(t, `{"data":{"organization":{"projectV2":{"items":{"nodes":[`+
		`{"id":"PVTI_1","content":{"__typename":"DraftIssue","title":"Ship it","createdAt":"2026-08-01T08:00:00Z","updatedAt":"2026-08-02T08:00:00Z"},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}`+
		`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`)
	if len(*records) != 1 {
		t.Fatalf("draft issues produced %d log records, want 1", len(*records))
	}
	if (*records)[0].Level != slog.LevelInfo {
		t.Fatalf("level = %s, want INFO for a deferral that happens every sync", (*records)[0].Level)
	}
}
