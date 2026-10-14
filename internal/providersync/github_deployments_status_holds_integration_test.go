//go:build integration

package providersync

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-9678: the held-deployment read, against the migrated deployments
// table. The rows go through the real writer (so the stored shape is the one a
// sync leaves), and the read must return only the repository's recent
// deployments with no status, newest first, tenant-fenced, newest row per key.
func TestGitHubDeploymentStatusHoldsAgainstMigratedSchema(t *testing.T) {
	ctx, conn := newDeploymentsIntegrationConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitHubDeploymentsClickHouseEffects{Conn: conn, Lease: lease}
	holds := GitHubDeploymentStatusHoldsClickHouse{Conn: conn, Lease: lease}
	claim := nativeTestClaim("github", "deployments")
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	str := func(v string) *string { return &v }
	row := func(id string, deployedAgo time.Duration, status *string, lastSynced time.Time) deploymentRow {
		r := deploymentIntegrationRow(claim, id, lastSynced)
		deployedAt := now.Add(-deployedAgo)
		r.DeployedAt, r.Status = &deployedAt, status
		return r
	}
	write := func(c Claim, rows ...deploymentRow) {
		t.Helper()
		for _, r := range rows {
			if err := sink.WriteEffect(ctx, c, deploymentEffect(t, r)); err != nil {
				t.Fatal(err)
			}
		}
	}
	day := 24 * time.Hour
	write(claim,
		row("held-newest", 6*time.Hour, nil, now),
		row("held-older", 60*time.Hour, nil, now),
		row("held-empty-string", 36*time.Hour, str(""), now),
		row("has-status", 12*time.Hour, str("success"), now),
		row("held-too-old", 5*day, nil, now),
		// A status stored by a later write wins over the held row of the same key.
		row("filled-later", 2*day, nil, now.Add(-time.Hour)),
		row("filled-later", 2*day, str("failure"), now),
	)
	foreign := claim
	foreign.OrgID = "org-github-foreign"
	foreignRow := row("held-foreign-org", 6*time.Hour, nil, now)
	foreignRow.OrgID = foreign.OrgID
	write(foreign, foreignRow)
	otherRepo := row("held-other-repo", 6*time.Hour, nil, now)
	otherRepo.RepoID = "11111111-2222-4333-8444-555555555555"
	write(claim, otherRepo)

	got, err := holds.DeploymentsWithoutStatus(ctx, claim.OrgID, "c7198fbc-1945-3717-05d8-eb78866b4e79", now.Add(-deploymentStatusRetryHorizon), deploymentStatusRetryMax)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"held-newest", "held-empty-string", "held-older"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("held = %v, want %v: the repository's recent deployments with no status (NULL or empty), newest first, no other org, no other repository, none with a status, none past the horizon", got, want)
	}

	limited, err := holds.DeploymentsWithoutStatus(ctx, claim.OrgID, "c7198fbc-1945-3717-05d8-eb78866b4e79", now.Add(-deploymentStatusRetryHorizon), 2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(limited, want[:2]) {
		t.Fatalf("limited held = %v, want the newest two %v", limited, want[:2])
	}

	if _, err := (GitHubDeploymentStatusHoldsClickHouse{Conn: conn}).DeploymentsWithoutStatus(ctx, claim.OrgID, "c7198fbc-1945-3717-05d8-eb78866b4e79", now, 1); err == nil {
		t.Fatal("a read with no lease guard succeeded; the read must fail, never read as empty")
	}
}

// A run that looks a held deployment up again and finds it still empty writes
// it with a new last_synced, so the capped read moves on to the others: the
// newest deployments with no status never starve an older held one.
func TestGitHubDeploymentStatusHoldsRotateThroughMoreHeldDeploymentsThanTheLimit(t *testing.T) {
	ctx, conn := newDeploymentsIntegrationConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitHubDeploymentsClickHouseEffects{Conn: conn, Lease: lease}
	holds := GitHubDeploymentStatusHoldsClickHouse{Conn: conn, Lease: lease}
	claim := nativeTestClaim("github", "deployments")
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	repo := "c7198fbc-1945-3717-05d8-eb78866b4e79"
	write := func(id string, deployedAgo, syncedAgo time.Duration) {
		t.Helper()
		row := deploymentIntegrationRow(claim, id, now.Add(-syncedAgo))
		deployedAt := now.Add(-deployedAgo)
		row.DeployedAt = &deployedAt
		if err := sink.WriteEffect(ctx, claim, deploymentEffect(t, row)); err != nil {
			t.Fatal(err)
		}
	}
	read := func(limit int) []string {
		t.Helper()
		ids, err := holds.DeploymentsWithoutStatus(ctx, claim.OrgID, repo, now.Add(-deploymentStatusRetryHorizon), limit)
		if err != nil {
			t.Fatal(err)
		}
		return ids
	}
	// The newest deployment was synced last; the oldest deployment was synced first.
	write("oldest-deployment", 40*time.Hour, 3*time.Hour)
	write("middle-deployment", 20*time.Hour, 2*time.Hour)
	write("newest-deployment", 2*time.Hour, 1*time.Hour)
	if got, want := read(2), []string{"oldest-deployment", "middle-deployment"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("held (limit 2) = %v, want the two least recently synced %v", got, want)
	}
	// The run looked the oldest one up, found it empty and wrote it again.
	write("oldest-deployment", 40*time.Hour, 0)
	if got, want := read(2), []string{"middle-deployment", "newest-deployment"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("held (limit 2) after the oldest was written again = %v, want %v: a deployment just looked up moves behind the others", got, want)
	}
}
