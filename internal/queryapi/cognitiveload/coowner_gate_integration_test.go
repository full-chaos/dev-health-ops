//go:build integration

package cognitiveload

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The team + repository gate takes the canonical owner from the main row; a
// co-owner row (is_primary = 2) is never the canonical owner. A repository
// with co-owners only (a tie with no main owner) is owned by no team here, as
// before co-owner rows existed (the tie wrote no row then).
func TestTheRepoGateNeverTakesACoOwnerAsTheOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)
	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org = "org-coowner-gate"
	at := time.Now().UTC().Add(-time.Hour)
	shared, tied := uuid.New(), uuid.New()
	for id, name := range map[uuid.UUID]string{shared: "org/shared", tied: "org/tied"} {
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, 'github', ?)`, id, name, org, at); err != nil {
			t.Fatalf("insert repo: %v", err)
		}
	}
	for _, row := range []struct {
		team    string
		repo    uuid.UUID
		primary uint8
	}{
		{"team-main", shared, 1}, {"team-co", shared, 2},
		{"team-tied-a", tied, 2}, {"team-tied-b", tied, 2},
	} {
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
			(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
			VALUES (?, 'github', ?, ?, '', 'exact', 'inferred', ?, 150, 0, ?, NULL, ?)`,
			org, row.team, row.repo, row.primary, at, at); err != nil {
			t.Fatalf("insert ownership: %v", err)
		}
	}
	for _, tc := range []struct {
		team string
		repo uuid.UUID
		want bool
	}{
		{"team-main", shared, true},
		{"team-co", shared, false},
		{"team-tied-a", tied, false},
		{"team-tied-b", tied, false},
	} {
		_, owned, err := resolveOwnedRepoID(ctx, client, org, tc.team, tc.repo.String())
		if err != nil {
			t.Fatalf("resolveOwnedRepoID(%s): %v", tc.team, err)
		}
		if owned != tc.want {
			t.Errorf("%s owns %s = %v, want %v", tc.team, tc.repo, owned, tc.want)
		}
	}
}
