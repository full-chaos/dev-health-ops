package admin

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func purgeLog(t *testing.T) []string {
	t.Helper()
	db := &provisionDB{}
	h := &handlers{store: pgStore{Pool: db, Now: time.Now}}
	org := uuid.New()
	result := &orgDeletionResult{postgresTableCount: map[string]int64{"memberships": 1, "organizations": 1}}
	if err := h.orgDeletionPurgePostgres(context.Background(), org, org.String(), result); err != nil {
		t.Fatal(err)
	}
	return db.log
}

func touchesMemberships(log []string) bool {
	return slices.ContainsFunc(log, func(sql string) bool { return strings.Contains(sql, "FROM memberships") })
}

func TestPurgeSQLSkipsMembershipsWhileOwned(t *testing.T) {
	if log := purgeLog(t); !touchesMemberships(log) {
		t.Fatalf("switch off: no memberships statement: %q", log)
	}
	t.Setenv("AUTH_ORGS_OWNED", "true")
	if log := purgeLog(t); touchesMemberships(log) {
		t.Fatalf("switch on: a memberships statement: %q", log)
	}
}
