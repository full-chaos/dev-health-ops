package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushevent"
	"github.com/full-chaos/dev-health-ops/internal/auth/repoidentityevent"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/google/uuid"
)

const repositoryIdentityTestOrg = "0b6f3c3e-2d0e-4a8f-9c41-5a1e8e0f7a21"

// repoMetadataBatch runs the real repo-metadata route of provider and returns its one `repos` effect.
func repoMetadataBatch(t *testing.T, provider string, now time.Time) EffectBatch {
	t.Helper()
	claim := nativeTestClaim(provider, "repo-metadata")
	claim.OrgID = repositoryIdentityTestOrg
	var batch CompleteRouteBatch
	var err error
	switch provider {
	case "github":
		doer := &gitHubRepositoryDoer{t: t, body: gitHubRepositoryFixture}
		batch, err = (GitHubRepositoryRouteHandler{Now: func() time.Time { return now }}).Collect(
			context.Background(), claim, providerfoundation.Credential{},
			gitHubRepositoryClient(t, fakehttp.Client(doer), "https://api.github.com"), now)
	case "gitlab":
		doer := &gitLabRepositoryDoer{t: t, body: gitLabRepositoryFixture}
		batch, err = (GitLabRepositoryRouteHandler{}).Collect(
			context.Background(), claim, providerfoundation.Credential{},
			gitLabRepositoryClient(t, fakehttp.Client(doer), "https://gitlab.example"), now)
	default:
		t.Fatalf("no repo-metadata route for %s", provider)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "repos" {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	return batch.Effects[0]
}

func reposBatch(t *testing.T, destination string, rows ...map[string]any) EffectBatch {
	t.Helper()
	raw := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		encoded, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, encoded)
	}
	batch, err := BuildEffectBatch(destination, EffectReadbackRequired, raw)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func identityRow(provider, id, repo string) map[string]any {
	return map[string]any{
		"id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "org_id": repositoryIdentityTestOrg, "repo": repo,
		"provider": provider, "last_synced": "2026-11-05T07:30:00.123Z", "provider_repo_id": id,
	}
}

// TestRepositoryIdentityEventsOfTheRealRepoMetadataRoutes: the positive cells. The github and gitlab repo-metadata
// routes (the only routes that write `repos`) give one event each: organization, provider, the provider's stable id,
// the canonical path and the fetch instant.
func TestRepositoryIdentityEventsOfTheRealRepoMetadataRoutes(t *testing.T) {
	now := time.Date(2026, 11, 5, 7, 30, 0, 987654321, time.UTC)
	org := uuid.MustParse(repositoryIdentityTestOrg)
	for _, c := range []struct {
		provider, id string
		observed     time.Time
	}{
		{"github", "4567", now.Truncate(time.Millisecond)},
		{"gitlab", "123", now.Truncate(time.Millisecond)},
	} {
		events, skipped, err := repositoryIdentityEvents(repoMetadataBatch(t, c.provider, now))
		if err != nil || skipped != 0 {
			t.Fatalf("%s: skipped=%d err=%v", c.provider, skipped, err)
		}
		want := repoidentityevent.Event{OrganizationID: org, Provider: c.provider, ProviderRepoID: c.id, Path: "acme/api", ObservedAt: c.observed}
		if len(events) != 1 || events[0] != want {
			t.Fatalf("%s: events=%+v want %+v", c.provider, events, want)
		}
	}
}

// TestRepositoryIdentityEventsNegativeAndSkippedCells: a row without a provider id (the local git and External Push
// row shape) and any destination but `repos` give no event; a malformed row is skipped and counted, as the backfill
// skips it; an undecodable row fails closed.
func TestRepositoryIdentityEventsNegativeAndSkippedCells(t *testing.T) {
	cases := []struct {
		name    string
		batch   EffectBatch
		events  int
		skipped int
		path    string
	}{
		{"local git row: no provider id", reposBatch(t, "repos", identityRow("local", "", "acme/api")), 0, 0, ""},
		{"external push row: no provider id", reposBatch(t, "repos", identityRow("github", "", "acme/api")), 0, 0, ""},
		{"jira/linear destination: work_items", reposBatch(t, "work_items", identityRow("jira", "4567", "acme/api")), 0, 0, ""},
		{"another git destination", reposBatch(t, "git_commits", identityRow("github", "4567", "acme/api")), 0, 0, ""},
		{"upper case and spaces", reposBatch(t, "repos", identityRow("github", "4567", "  Acme/API  ")), 1, 0, "acme/api"},
		{"organization is not a uuid", reposBatch(t, "repos", func() map[string]any {
			row := identityRow("github", "4567", "acme/api")
			row["org_id"] = "org-acme"
			return row
		}()), 0, 1, ""},
		{"id is not a number", reposBatch(t, "repos", identityRow("github", "abc", "acme/api")), 0, 1, ""},
		{"provider is not a provider name", reposBatch(t, "repos", identityRow("Git-Hub", "4567", "acme/api")), 0, 1, ""},
		{"path of one segment", reposBatch(t, "repos", identityRow("github", "4567", "api")), 0, 1, ""},
		{"path is the wildcard", reposBatch(t, "repos", identityRow("github", "4567", "*")), 0, 1, ""},
		{"path is not ASCII", reposBatch(t, "repos", identityRow("github", "4567", "acme/\u212api")), 0, 1, ""},
		{"two rows, one without id", reposBatch(t, "repos", identityRow("github", "", "acme/web"), identityRow("github", "4567", "acme/api")), 1, 0, "acme/api"},
	}
	for _, c := range cases {
		events, skipped, err := repositoryIdentityEvents(c.batch)
		if err != nil || len(events) != c.events || skipped != c.skipped {
			t.Fatalf("%s: events=%+v skipped=%d err=%v", c.name, events, skipped, err)
		}
		if c.events == 1 && events[0].Path != c.path {
			t.Fatalf("%s: path=%q want %q", c.name, events[0].Path, c.path)
		}
	}
	bad := EffectBatch{Destination: "repos", Rows: []json.RawMessage{json.RawMessage(`[`)}}
	if _, _, err := repositoryIdentityEvents(bad); !errors.Is(err, ErrEffectRecoveryUnsafe) {
		t.Fatalf("undecodable row: err=%v", err)
	}
}

// TestRepositoryIdentityPathMatchesTheBackfill: the producer's path is the backfill's lowerUTF8(trimBoth(repo))
// (internal/auth/usersbackfill/repoidentity_command.go) restricted to repository keys: trimBoth trims spaces only.
func TestRepositoryIdentityPathMatchesTheBackfill(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"acme/api", "acme/api"},
		{"Acme/API", "acme/api"},
		{"  acme/api ", "acme/api"},
		{"group/sub/Project.Name", "group/sub/project.name"},
		{"\tacme/api", ""},
		{"acme/api\u00a0", ""},
		{"ACME/\u212aEY", ""},
		{"acme", ""},
		{"*", "*"},
		{"", ""},
	} {
		if got := repositoryIdentityPath(c.in); got != c.want {
			t.Fatalf("repositoryIdentityPath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestOnlyRepoMetadataRoutesWriteRepos: the identity producer hooks the `repos` destination, which only the github and
// gitlab repo-metadata routes write. jira, linear and every other route never write it, so they never emit.
func TestOnlyRepoMetadataRoutesWriteRepos(t *testing.T) {
	var writers []string
	for provider, datasets := range datasetCapabilities {
		for dataset := range datasets {
			descriptor, ok := Descriptor(provider, dataset)
			if !ok {
				continue
			}
			for _, destination := range descriptor.Destinations {
				if destination == repositoryIdentityDestination {
					writers = append(writers, provider+"/"+dataset)
				}
			}
		}
	}
	if len(writers) != 2 || !(writers[0] == "github/repo-metadata" && writers[1] == "gitlab/repo-metadata" ||
		writers[1] == "github/repo-metadata" && writers[0] == "gitlab/repo-metadata") {
		t.Fatalf("repos writers = %v, want github/repo-metadata and gitlab/repo-metadata", writers)
	}
	for _, provider := range []string{"jira", "linear"} {
		if len(datasetCapabilities[provider]) == 0 {
			t.Fatalf("%s has no datasets: the negative cell would be empty", provider)
		}
	}
}

func TestParseRepositoryIdentityOutbox(t *testing.T) {
	if EnvRepositoryIdentityOutbox != "WORKER_REPO_IDENTITY_OUTBOX" {
		t.Fatalf("env = %q", EnvRepositoryIdentityOutbox)
	}
	for _, c := range []struct {
		value   string
		set, on bool
		err     bool
	}{
		{"", false, false, false}, {"", true, false, false}, {"  ", true, false, false},
		{"true", true, true, false}, {" 1 ", true, true, false}, {"false", true, false, false},
		{"on", true, false, true}, {"yes", true, false, true},
	} {
		on, err := ParseRepositoryIdentityOutbox(func(string) (string, bool) { return c.value, c.set })
		if on != c.on || (err != nil) != c.err {
			t.Fatalf("%q set=%v: on=%v err=%v", c.value, c.set, on, err)
		}
	}
}

// TestRepositoryIdentityOutboxParsesLikeThePushLifecycleSwitch: the switch is parsed exactly as AUTH_PUSH_LIFECYCLE_EVENTS
// (pushevent.ParsePushLifecycleEvents): unset or empty is off, strconv.ParseBool values, any other value a start error.
func TestRepositoryIdentityOutboxParsesLikeThePushLifecycleSwitch(t *testing.T) {
	type cell struct {
		value string
		set   bool
	}
	cells := []cell{{"", false}}
	for _, v := range []string{"", "  ", "1", "0", "t", "f", "TRUE", "False", " true ", "false ", "true", "false", "yes", "on", "2"} {
		cells = append(cells, cell{v, true})
	}
	for _, c := range cells {
		lookup := func(want string) func(string) (string, bool) {
			return func(key string) (string, bool) {
				if key != want {
					return "", false
				}
				return c.value, c.set
			}
		}
		on, err := ParseRepositoryIdentityOutbox(lookup(EnvRepositoryIdentityOutbox))
		pushOn, pushErr := pushevent.ParsePushLifecycleEvents(lookup(pushevent.EnvPushLifecycleEvents))
		if on != pushOn || (err != nil) != (pushErr != nil) {
			t.Fatalf("%q set=%v: repository identity on=%v err=%v; push lifecycle on=%v err=%v", c.value, c.set, on, err, pushOn, pushErr)
		}
	}
}

// rowsRecordingLedger is the in-process ledger with the batch-aware flips, recording which flip received which batch.
type rowsRecordingLedger struct {
	*InProcessEffectLedger
	committed []string
	marked    []string
}

func (l *rowsRecordingLedger) CommitEffectRows(ctx context.Context, claim Claim, index int, digest string, batch EffectBatch, now time.Time) error {
	if batch.ContentDigest != digest {
		return ErrEffectLedgerConflict
	}
	l.committed = append(l.committed, batch.Destination)
	return l.InProcessEffectLedger.CommitEffect(ctx, claim, index, digest, now)
}

func (l *rowsRecordingLedger) MarkEffectCommittedRows(ctx context.Context, claim Claim, index int, digest string, batch EffectBatch, now time.Time) error {
	if batch.ContentDigest != digest {
		return ErrEffectLedgerConflict
	}
	l.marked = append(l.marked, batch.Destination)
	return l.InProcessEffectLedger.ResolveEffect(ctx, claim, index, digest, GenerationBlockMarkCommitted, now)
}

type scriptedSink struct{ fail bool }

func (s *scriptedSink) WriteEffect(context.Context, Claim, EffectBatch) error {
	if s.fail {
		return errors.New("sink down")
	}
	return nil
}

type scriptedReadback struct{ inspection EffectInspection }

func (r scriptedReadback) InspectEffect(context.Context, Claim, EffectBatch) (EffectInspection, error) {
	return r.inspection, nil
}

// TestEffectCommitterHandsTheBatchToEveryCommitFlip: the committer gives a rows ledger the batch on the write flip and
// on the recovery flip (readback exact), and on neither of the paths that do not commit (sink failure, readback absent
// before the rewrite).
func TestEffectCommitterHandsTheBatchToEveryCommitFlip(t *testing.T) {
	now := time.Date(2026, 11, 5, 7, 30, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "repo-metadata")
	batch := reposBatch(t, "repos", identityRow("github", "4567", "acme/api"))

	t.Run("write flip", func(t *testing.T) {
		ledger := &rowsRecordingLedger{InProcessEffectLedger: &InProcessEffectLedger{}}
		committer := EffectCommitter{Ledger: ledger, Sink: &scriptedSink{}, Now: func() time.Time { return now }}
		result, err := committer.Commit(context.Background(), claim, []EffectBatch{batch}, now)
		if err != nil || result.Written != 1 || len(ledger.committed) != 1 || ledger.committed[0] != "repos" || len(ledger.marked) != 0 {
			t.Fatalf("result=%+v err=%v committed=%v marked=%v", result, err, ledger.committed, ledger.marked)
		}
	})
	for _, c := range []struct {
		name               string
		inspection         EffectInspection
		committed, marked  int
		written, recovered int
	}{
		{"recovery flip: readback exact", EffectExact, 0, 1, 0, 1},
		{"readback absent: rewrite then write flip", EffectAbsent, 1, 0, 1, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			ledger := &rowsRecordingLedger{InProcessEffectLedger: &InProcessEffectLedger{}}
			sink := &scriptedSink{fail: true}
			committer := EffectCommitter{Ledger: ledger, Sink: sink, Readback: scriptedReadback{c.inspection}, Now: func() time.Time { return now }}
			if _, err := committer.Commit(context.Background(), claim, []EffectBatch{batch}, now); err == nil {
				t.Fatal("first attempt: want the sink failure")
			}
			if len(ledger.committed)+len(ledger.marked) != 0 {
				t.Fatalf("a failed write flipped: committed=%v marked=%v", ledger.committed, ledger.marked)
			}
			sink.fail = false
			result, err := committer.Commit(context.Background(), claim, []EffectBatch{batch}, now)
			if err != nil || len(ledger.committed) != c.committed || len(ledger.marked) != c.marked ||
				result.Written != c.written || result.MarkedCommitted != c.recovered {
				t.Fatalf("result=%+v err=%v committed=%v marked=%v", result, err, ledger.committed, ledger.marked)
			}
		})
	}
}
