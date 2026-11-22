//go:build integration

package teamownership

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamactive"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// ownersByRepoStatementSHA256 is the digest of the statement OwnersByRepo sends,
// taken on the tree before OwnerCandidatesByRepo existed.
const ownersByRepoStatementSHA256 = "a22fcf29c7e6193e31c0a06cda4e71343e21c12b4bf61055e292d230d72abb58"

// queryLog records which read each query of a connection is, in order, and the
// text of each ownership read.
type queryLog struct {
	conn      driver.Conn
	reads     []string
	ownership []string
}

func (q *queryLog) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "FROM teams WHERE org_id = ? GROUP BY id HAVING"):
		q.reads = append(q.reads, "activity")
	case strings.Contains(query, "FROM team_repo_ownership AS o FINAL"):
		q.reads = append(q.reads, "ownership")
		q.ownership = append(q.ownership, query)
	default:
		q.reads = append(q.reads, "other")
	}
	return q.conn.Query(ctx, query, args...)
}

// OwnerCandidatesByRepo lists inactive owners and reads no teams row: the first
// record of its window follows the caller's snapshot only. OwnersByRepo drops
// an inactive owner, reads the activity before the ownership, and sends its
// statement unchanged.
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
	later := uuid.NewSHA1(uuid.NameSpaceURL, []byte(org+"first-record-after-as-of"))
	for _, id := range []uuid.UUID{shared, inactiveOnly, later} {
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
		from time.Time
	}{
		{"team-live", shared, long},
		{"team-retired", shared, long},
		{"team-retired", inactiveOnly, long},
		// The only row of this repository starts after asOf: it is the first
		// record, clamped back to asOf, only while its team is active.
		{"team-retired", later, asOf.Add(48 * time.Hour)},
	} {
		if err := conn.Exec(ctx, `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, valid_to, updated_at)
    VALUES (?, 'github', ?, ?, ?, 'exact', 'provider_access', 0, 100, ?, NULL, ?)`,
			org, row.team, row.repo, "acme/"+row.repo.String(), row.from, long); err != nil {
			t.Fatal(err)
		}
	}

	// The teams table says team-retired is inactive. Only the snapshot each
	// call is given decides the first record.
	for _, tc := range []struct {
		name     string
		snapshot teamactive.Inactive
		want     map[string][]string
	}{
		{"snapshot names the team inactive", teamactive.Inactive{"team-retired": {}}, map[string][]string{
			shared.String():       {"team-live", "team-retired"},
			inactiveOnly.String(): {"team-retired"},
		}},
		{"empty snapshot", teamactive.Inactive{}, map[string][]string{
			shared.String():       {"team-live", "team-retired"},
			inactiveOnly.String(): {"team-retired"},
			later.String():        {"team-retired"},
		}},
	} {
		log := &queryLog{conn: conn}
		candidates, err := OwnerCandidatesByRepo(ctx, log, org, asOf, tc.snapshot)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !reflect.DeepEqual(candidates, tc.want) {
			t.Errorf("%s: OwnerCandidatesByRepo = %v, want %v", tc.name, candidates, tc.want)
		}
		if want := []string{"ownership"}; !reflect.DeepEqual(log.reads, want) {
			t.Errorf("%s: OwnerCandidatesByRepo reads = %v, want %v (no activity read)", tc.name, log.reads, want)
		}
		if len(log.ownership) == 1 && strings.Contains(log.ownership[0], "FROM teams") {
			t.Errorf("%s: the candidate statement reads teams:\n%s", tc.name, log.ownership[0])
		}
	}
	if _, err := OwnerCandidatesByRepo(ctx, conn, org, asOf, nil); err == nil {
		t.Error("OwnerCandidatesByRepo with no snapshot: want an error")
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
	if len(ownersLog.ownership) != 1 {
		t.Fatalf("OwnersByRepo ownership statements = %d, want 1", len(ownersLog.ownership))
	}
	ownersStatement := ownersLog.ownership[0]
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(ownersStatement))); got != ownersByRepoStatementSHA256 {
		t.Errorf("the OwnersByRepo statement changed: sha256 %s, want %s", got, ownersByRepoStatementSHA256)
	}

	candidatesLog := &queryLog{conn: conn}
	if _, err := OwnerCandidatesByRepo(ctx, candidatesLog, org, asOf, teamactive.Inactive{}); err != nil {
		t.Fatal(err)
	}
	if swapped := strings.Replace(candidatesLog.ownership[0], ownershipWindowBoundInactive, ownershipWindow, 1); swapped != ownersStatement {
		t.Errorf("the candidate statement differs from the OwnersByRepo statement in more than the window:\n%s", candidatesLog.ownership[0])
	}
}
