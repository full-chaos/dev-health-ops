//go:build integration

package teamownership

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// queryLog records which read each query of a connection is, in order.
type queryLog struct {
	conn  driver.Conn
	reads []string
}

func (q *queryLog) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "FROM teams WHERE org_id = ? GROUP BY id HAVING"):
		q.reads = append(q.reads, "activity")
	case strings.Contains(query, "FROM team_repo_ownership AS o FINAL"):
		q.reads = append(q.reads, "ownership")
	default:
		q.reads = append(q.reads, "other")
	}
	return q.conn.Query(ctx, query, args...)
}

// OwnerCandidatesByRepo lists an inactive owner and reads no activity;
// OwnersByRepo drops it, and reads the activity before the ownership.
func TestOwnerCandidatesByRepoKeepsInactiveOwnersAndOwnersByRepoDropsThem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const org = "00000000-0000-4000-8000-000000014429"
	asOf := time.Date(2026, 11, 20, 23, 59, 59, 0, time.UTC)
	long := asOf.AddDate(0, -1, 0)
	shared := uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+"shared"))
	inactiveOnly := uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+"inactive-only"))
	for _, id := range []uuid.UUID{shared, inactiveOnly} {
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, 'github', ?)`,
			id, "acme/"+id.String(), org, long); err != nil {
			t.Fatal(err)
		}
	}
	for _, team := range []struct {
		id     string
		active uint8
	}{{"team-live", 1}, {"team-retired", 0}} {
		if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, generateUUIDv4(), ?, [], ?, ?, ?, 'github', ?)`, team.id, team.id, long, long, org, team.active); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []struct {
		team string
		repo uuid.UUID
	}{{"team-live", shared}, {"team-retired", shared}, {"team-retired", inactiveOnly}} {
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, valid_to, updated_at)
    VALUES (?, 'github', ?, ?, ?, 'exact', 'provider_access', 0, 100, ?, NULL, ?)`,
			org, row.team, row.repo, "acme/"+row.repo.String(), long, long); err != nil {
			t.Fatal(err)
		}
	}

	candidatesLog := &queryLog{conn: conn}
	candidates, err := OwnerCandidatesByRepo(ctx, candidatesLog, org, asOf)
	if err != nil {
		t.Fatal(err)
	}
	wantCandidates := map[string][]string{
		shared.String():       {"team-live", "team-retired"},
		inactiveOnly.String(): {"team-retired"},
	}
	if !reflect.DeepEqual(candidates, wantCandidates) {
		t.Errorf("OwnerCandidatesByRepo = %v, want %v (an inactive owner is a candidate)", candidates, wantCandidates)
	}
	if want := []string{"ownership"}; !reflect.DeepEqual(candidatesLog.reads, want) {
		t.Errorf("OwnerCandidatesByRepo reads = %v, want %v (no activity read)", candidatesLog.reads, want)
	}

	ownersLog := &queryLog{conn: conn}
	owners, err := OwnersByRepo(ctx, ownersLog, org, asOf)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string][]string{shared.String(): {"team-live"}}; !reflect.DeepEqual(owners, want) {
		t.Errorf("OwnersByRepo = %v, want %v (an inactive team owns nothing)", owners, want)
	}
	if want := []string{"activity", "ownership"}; !reflect.DeepEqual(ownersLog.reads, want) {
		t.Errorf("OwnersByRepo reads = %v, want %v (the activity read runs first)", ownersLog.reads, want)
	}
}
