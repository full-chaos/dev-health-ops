package providersync

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/storedversion"
)

// foldDeployment applies the deployment contract to one row against a held
// version, the same fold WriteEffect runs after its read.
func foldDeployment(t *testing.T, row deploymentRow, held map[string]any) deploymentRow {
	t.Helper()
	rows := []storedversion.Row{{Values: deploymentValues(row), Carry: deploymentCarry(row)}}
	if _, err := deploymentsContract.Fold(deploymentsInsert, rows, func([]any) (map[string]any, bool) { return held, held != nil }); err != nil {
		t.Fatal(err)
	}
	positions, err := storedversion.Positions(deploymentsInsert)
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range deploymentsContract.Kept() {
		if err := setDeploymentKept(&row, column, rows[0].Values[positions[column]]); err != nil {
			t.Fatal(err)
		}
	}
	return row
}

// Every cell of {nothing held, a held pair} x {lookup failed, succeeded with
// no pull request, succeeded with one}: a failed lookup keeps what is held,
// an empty lookup over a held merge keeps merged_at and pull_request_number
// as one unit, a found pull request is written, and nothing held is written
// as stated.
func TestDeploymentContractKeepsThePullRequestPairAsAUnit(t *testing.T) {
	claim := nativeTestClaim("github", "deployments")
	heldMerged := time.Date(2026, 7, 22, 9, 0, 0, 0, time.UTC)
	foundMerged := time.Date(2026, 7, 23, 9, 0, 0, 0, time.UTC)
	found := 7
	held := map[string]any{"status": "success", "started_at": nil, "finished_at": nil, "merged_at": heldMerged, "pull_request_number": uint32(42)}
	cases := []struct {
		name       string
		held       map[string]any
		failed     bool
		merged     *time.Time
		number     *int
		wantMerged *time.Time
		wantNumber *int
	}{
		{"nothing held, lookup failed", nil, true, nil, nil, nil, nil},
		{"nothing held, no pull request", nil, false, nil, nil, nil, nil},
		{"nothing held, pull request found", nil, false, &foundMerged, &found, &foundMerged, &found},
		{"pair held, lookup failed", held, true, nil, nil, &heldMerged, intPointer(42)},
		{"pair held, no pull request", held, false, nil, nil, &heldMerged, intPointer(42)},
		{"pair held, pull request found", held, false, &foundMerged, &found, &foundMerged, &found},
	}
	for _, tc := range cases {
		row := deploymentEffectsUnitRow(claim, "901")
		row.PullRequestLookupFailed = tc.failed
		row.MergedAt, row.PullRequestNumber = tc.merged, tc.number
		got := foldDeployment(t, row, tc.held)
		if !timePointersEqual(got.MergedAt, tc.wantMerged) || !intPointersEqual(got.PullRequestNumber, tc.wantNumber) {
			t.Errorf("%s: merged_at=%v pull_request_number=%v, want %v %v", tc.name, got.MergedAt, derefInt(got.PullRequestNumber), tc.wantMerged, derefInt(tc.wantNumber))
		}
	}
}

// The lifecycle columns are kept only when the statuses lookup failed; an
// honest empty lookup writes its nils.
func TestDeploymentContractCarriesTheLifecycleOnlyOnAFailedLookup(t *testing.T) {
	claim := nativeTestClaim("gitlab", "deployments")
	started := time.Date(2026, 7, 22, 9, 59, 0, 0, time.UTC)
	held := map[string]any{"status": "success", "started_at": started, "finished_at": started, "merged_at": nil, "pull_request_number": nil}
	row := deploymentEffectsUnitRow(claim, "902")
	row.Status, row.StartedAt, row.FinishedAt = nil, nil, nil
	row.LifecycleLookupFailed = true
	got := foldDeployment(t, row, held)
	if got.Status == nil || *got.Status != "success" || got.StartedAt == nil || !got.StartedAt.Equal(started) {
		t.Fatalf("failed lookup: status=%v started_at=%v, want the held lifecycle", got.Status, got.StartedAt)
	}
	row.LifecycleLookupFailed = false
	got = foldDeployment(t, row, held)
	if got.Status != nil || got.StartedAt != nil || got.FinishedAt != nil {
		t.Fatalf("honest empty lookup: status=%v started_at=%v, want nils", got.Status, got.StartedAt)
	}
}

func TestDeploymentContractWritesEveryKeptColumnBack(t *testing.T) {
	claim := nativeTestClaim("github", "deployments")
	row := deploymentEffectsUnitRow(claim, "903")
	positions, err := storedversion.Positions(deploymentsInsert)
	if err != nil {
		t.Fatal(err)
	}
	held := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	for _, column := range deploymentsContract.Kept() {
		value := any(held)
		switch column {
		case "status":
			value = "failure"
		case "pull_request_number":
			value = uint32(9)
		}
		if err := setDeploymentKept(&row, column, value); err != nil {
			t.Fatalf("%s: %v", column, err)
		}
		if got := deploymentValues(row)[positions[column]]; got != value {
			t.Errorf("%s = %#v after write-back, want %#v", column, got, value)
		}
		if err := setDeploymentKept(&row, column, nil); err != nil {
			t.Errorf("%s: a held null: %v", column, err)
		}
	}
	if err := setDeploymentKept(&row, "environment", "prod"); err == nil {
		t.Error("a column the contract does not keep was accepted")
	}
}

func intPointer(value int) *int { return &value }

func derefInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

// The named deployment events survive the contract: carried lifecycle and
// pull request pairs log their guard events, and an empty lookup over a held
// merge logs the pair refusal with the held scalars only.
func TestDeploymentOutcomesKeepTheNamedGuardEvents(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	claim := nativeTestClaim("github", "deployments")
	held := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	lifecycle := deploymentEffectsUnitRow(claim, "911")
	lifecycle.LifecycleLookupFailed = true
	pair := deploymentEffectsUnitRow(claim, "912")
	pair.PullRequestLookupFailed = true
	refused := deploymentEffectsUnitRow(claim, "913")
	refused.MergedAt, refused.PullRequestNumber = &held, intPointer(42)
	logDeploymentOutcomes(context.Background(), claim, []deploymentRow{lifecycle, pair, refused}, []storedversion.Outcome{
		{Key: []any{lifecycle.RepoID, "911"}, Carried: []string{"status", "started_at"}},
		{Key: []any{pair.RepoID, "912"}, Carried: []string{"merged_at", "pull_request_number"}},
		{Key: []any{refused.RepoID, "913"}, Refused: []string{"merged_at", "pull_request_number"}},
	})
	text := logs.String()
	for _, want := range []string{
		deploymentLifecycleRegressionGuardedEvent, deploymentPullRequestRegressionGuardedEvent,
		deploymentPullRequestRegressionRefusedEvent + " org_id=", "deployment_id=913", "stored_pull_request_number=42",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("log lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, storedVersionRefusedEvent) || strings.Contains(text, storedVersionCarriedEvent) {
		t.Errorf("a named event was also logged as a generic stored-version event:\n%s", text)
	}
}

// A failed held-version read fails the repository write before any insert
// and fails its recovery readback.
func TestRepositoryWriteAndInspectFailWhenTheHeldVersionReadFails(t *testing.T) {
	for _, provider := range []string{"github", "gitlab"} {
		claim := nativeTestClaim(provider, "repo-metadata")
		identity, err := repositoryIdentity("Acme/API")
		if err != nil {
			t.Fatal(err)
		}
		row := repositoryRow{ID: identity, OrgID: claim.OrgID, Repo: "Acme/API", Provider: provider,
			CreatedAt: time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC), LastSynced: time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
			Settings: `{}`, Tags: `[]`}
		effect, err := effectBatchFromValues("repos", EffectReadbackRequired, []repositoryRow{row})
		if err != nil {
			t.Fatal(err)
		}
		conn := &deploymentGuardReadFailsConn{}
		lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
		var sink deploymentEffectsIntegrationSinkForUnit = GitHubRepositoryClickHouseEffects{Conn: conn, Lease: lease}
		if provider == "gitlab" {
			sink = GitLabRepositoryClickHouseEffects{Conn: conn, Lease: lease}
		}
		if err := sink.WriteEffect(context.Background(), claim, effect); !errors.Is(err, errDeploymentGuardReadFailed) || conn.prepares != 0 {
			t.Fatalf("%s write err=%v prepares=%d want the read error and no insert", provider, err, conn.prepares)
		}
		if inspection, err := sink.InspectEffect(context.Background(), claim, effect); !errors.Is(err, errDeploymentGuardReadFailed) || inspection != EffectConflict {
			t.Fatalf("%s inspection=%s err=%v want the read error", provider, inspection, err)
		}
	}
}
