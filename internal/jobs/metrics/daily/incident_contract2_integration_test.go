//go:build integration

package daily

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/opfixture"
)

// On the contract-2 schema FINAL keeps every revision of a key, so the daily
// incident loader must select the current row by revision. The fixture holds:
//
//	I1  a live version and a newer tombstone            -> not returned
//	I2  two live versions (open, then resolved)         -> returned once, resolved
//	I3  one live version                                -> returned
//	I4  live, behind a mapping whose newest version is inactive -> not returned
func TestLoadIncidentsStartedReadsTheCurrentRowOnContract2(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, conn := opfixture.Start(ctx, t)

	const org = "org-c2"
	repo := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	day := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	started := day.Add(3 * time.Hour)
	if err := conn.Exec(ctx, "INSERT INTO repos (id, repo, created_at, last_synced, org_id) VALUES ('"+repo.String()+"', 'acme/api', now64(3), now64(3), '"+org+"')"); err != nil {
		t.Fatal(err)
	}
	opfixture.InsertMapping(ctx, t, conn, opfixture.Mapping{Org: org, ID: "m-live", ServiceID: "svc-live", RepoID: repo.String(), Revision: 1, Active: true, At: started})
	opfixture.InsertMapping(ctx, t, conn, opfixture.Mapping{Org: org, ID: "m-gone", ServiceID: "svc-gone", RepoID: repo.String(), Revision: 1, Active: true, At: started})
	opfixture.InsertMapping(ctx, t, conn, opfixture.Mapping{Org: org, ID: "m-gone", ServiceID: "svc-gone", RepoID: repo.String(), Revision: 2, Active: false, At: started})

	inc := func(id, service, status string, revision int, deleted bool) {
		opfixture.InsertIncident(ctx, t, conn, opfixture.Incident{Org: org, ID: id, ServiceID: service, Status: status, Title: id, Revision: revision, Deleted: deleted, StartedAt: started})
	}
	inc("I1", "svc-live", "open", 1, false)
	inc("I1", "svc-live", "open", 2, true)
	inc("I2", "svc-live", "open", 1, false)
	inc("I2", "svc-live", "resolved", 2, false)
	inc("I3", "svc-live", "open", 1, false)
	inc("I4", "svc-gone", "open", 1, false)

	load := func() ([]IncidentRow, error) {
		return LoadIncidentsStarted(ctx, conn, org, []uuid.UUID{repo}, day, day.Add(24*time.Hour), day.Add(24*time.Hour), nil)
	}
	// "2" and unset are the same contract: the current row of each key.
	for name, configure := range map[string]func(){
		"two": func() { t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "2") },
		"unset": func() {
			t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "")
			_ = os.Unsetenv("OPERATIONAL_ORDERING_CONTRACT")
		},
	} {
		configure()
		got, err := load()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		statuses := map[string]string{}
		var ids []string
		for _, row := range got {
			ids = append(ids, row.IncidentID)
			statuses[row.IncidentID] = row.Status
		}
		sort.Strings(ids)
		if len(ids) != 2 || ids[0] != "I2" || ids[1] != "I3" {
			t.Fatalf("%s: returned %v (statuses %v), want exactly [I2 I3]: a tombstoned incident (I1) or an incident behind a deactivated mapping (I4) came back", name, ids, statuses)
		}
		if statuses["I2"] != "resolved" {
			t.Fatalf("%s: I2 status %q, want the newest version %q", name, statuses["I2"], "resolved")
		}
	}
	// Contract 1 is unsupported: refused, never read as legacy FINAL.
	for _, value := range []string{"1", "", "3"} {
		t.Setenv("OPERATIONAL_ORDERING_CONTRACT", value)
		if got, err := load(); err == nil {
			t.Fatalf("OPERATIONAL_ORDERING_CONTRACT=%q was accepted and returned %d rows", value, len(got))
		}
	}
}
