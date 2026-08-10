package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func gitLabSecurityEffect(t *testing.T, rows ...gitLabSecurityAlertRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("security_alerts", EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

func validGitLabSecurityRow(claim Claim) gitLabSecurityAlertRow {
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	fixedAt := now.Add(time.Minute)
	dismissedAt := now.Add(2 * time.Minute)
	return gitLabSecurityAlertRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79",
		AlertID: "gitlab_vuln:1", Source: "gitlab_vulnerability",
		Severity: ptrString("high"), State: ptrString("detected"),
		CVEID: ptrString("CVE-2026-0001"), URL: ptrString("https://example.invalid/1"),
		Title: ptrString("finding"), Description: ptrString("description"),
		CreatedAt: now, FixedAt: &fixedAt, DismissedAt: &dismissedAt, LastSynced: now,
	}
}

func ptrString(value string) *string { return &value }

func TestGitLabSecurityEffectsRejectCrossTenantAndDuplicateRowsBeforeClickHouse(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "security")
	sink := GitLabSecurityClickHouseEffects{
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	foreign := validGitLabSecurityRow(claim)
	foreign.OrgID = "other-org"
	if err := sink.WriteEffect(context.Background(), claim, gitLabSecurityEffect(t, foreign)); err == nil {
		t.Fatal("foreign row passed sink validation")
	}
	row := validGitLabSecurityRow(claim)
	err := sink.WriteEffect(context.Background(), claim, gitLabSecurityEffect(t, row, row))
	if !errors.Is(err, providerfoundation.ErrSinkDuplicate) {
		t.Fatalf("duplicate error=%v", err)
	}
}

func TestGitLabSecurityEffectsCheckLeaseBeforePrepareAndQuery(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "security")
	sink := GitLabSecurityClickHouseEffects{
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error {
			return providerfoundation.ErrLeaseLost
		}),
	}
	row := validGitLabSecurityRow(claim)
	if err := sink.WriteEffect(context.Background(), claim, gitLabSecurityEffect(t, row)); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("write error=%v", err)
	}
	inspection, err := sink.InspectEffect(context.Background(), claim, gitLabSecurityEffect(t, row))
	if !errors.Is(err, providerfoundation.ErrLeaseLost) || inspection != EffectConflict {
		t.Fatalf("inspection=%s error=%v", inspection, err)
	}
}

func TestGitLabSecurityEffectsAllowLegitimateEmptyEffect(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "security")
	sink := GitLabSecurityClickHouseEffects{
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	if err := sink.WriteEffect(context.Background(), claim, gitLabSecurityEffect(t)); err != nil {
		t.Fatal(err)
	}
	inspection, err := sink.InspectEffect(context.Background(), claim, gitLabSecurityEffect(t))
	if err != nil || inspection != EffectAbsent {
		t.Fatalf("inspection=%s error=%v", inspection, err)
	}
}

func TestCompareGitLabSecurityAlertVersionChecksFullPayloadAndTimestamp(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "security")
	expected := validGitLabSecurityRow(claim)
	if got := compareGitLabSecurityAlertVersion(expected, expected, true); got != EffectExact {
		t.Fatalf("exact=%s", got)
	}
	if got := compareGitLabSecurityAlertVersion(expected, gitLabSecurityAlertRow{}, false); got != EffectAbsent {
		t.Fatalf("absent=%s", got)
	}
	stale := expected
	stale.LastSynced = stale.LastSynced.Add(-time.Second)
	if got := compareGitLabSecurityAlertVersion(expected, stale, true); got != EffectAbsent {
		t.Fatalf("stale=%s", got)
	}
	newer := expected
	newer.LastSynced = newer.LastSynced.Add(time.Second)
	if got := compareGitLabSecurityAlertVersion(expected, newer, true); got != EffectConflict {
		t.Fatalf("newer=%s", got)
	}
	for _, test := range []struct {
		name   string
		mutate func(*gitLabSecurityAlertRow)
	}{
		{name: "org_id", mutate: func(row *gitLabSecurityAlertRow) { row.OrgID = "other-org" }},
		{name: "repo_id", mutate: func(row *gitLabSecurityAlertRow) { row.RepoID = "2f2d6f7c-2f28-4d6b-a0b7-48ec4d77a4f0" }},
		{name: "alert_id", mutate: func(row *gitLabSecurityAlertRow) { row.AlertID = "gitlab_vuln:changed" }},
		{name: "source", mutate: func(row *gitLabSecurityAlertRow) { row.Source = "gitlab_dependency" }},
		{name: "severity", mutate: func(row *gitLabSecurityAlertRow) { row.Severity = ptrString("low") }},
		{name: "state", mutate: func(row *gitLabSecurityAlertRow) { row.State = ptrString("resolved") }},
		{name: "package_name", mutate: func(row *gitLabSecurityAlertRow) { row.PackageName = ptrString("pkg") }},
		{name: "cve_id", mutate: func(row *gitLabSecurityAlertRow) { row.CVEID = ptrString("CVE-2026-0002") }},
		{name: "url", mutate: func(row *gitLabSecurityAlertRow) { row.URL = ptrString("https://example.invalid/changed") }},
		{name: "title", mutate: func(row *gitLabSecurityAlertRow) { row.Title = ptrString("changed") }},
		{name: "description", mutate: func(row *gitLabSecurityAlertRow) { row.Description = ptrString("changed") }},
		{name: "created_at", mutate: func(row *gitLabSecurityAlertRow) { row.CreatedAt = row.CreatedAt.Add(time.Second) }},
		{name: "fixed_at", mutate: func(row *gitLabSecurityAlertRow) { changed := row.FixedAt.Add(time.Second); row.FixedAt = &changed }},
		{name: "dismissed_at", mutate: func(row *gitLabSecurityAlertRow) {
			changed := row.DismissedAt.Add(time.Second)
			row.DismissedAt = &changed
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := expected
			test.mutate(&changed)
			if got := compareGitLabSecurityAlertVersion(expected, changed, true); got != EffectConflict {
				t.Fatalf("changed=%s", got)
			}
		})
	}
}

func TestGitLabSecurityReadbackRejectsAmbiguousRows(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("gitlab", "security")
	row := validGitLabSecurityRow(claim)
	if got := gitLabSecurityReadbackDecision(2, row, row); got != EffectConflict {
		t.Fatalf("ambiguous rows=%s", got)
	}
}
