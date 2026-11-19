//go:build integration

package workgraph

import (
	"context"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The membership read scans two scalar count() subqueries. ClickHouse types a
// scalar subquery Nullable(UInt64), but a count() subquery always yields one
// row, so the scan never meets a NULL and the read reaches its answer.
func TestMembershipDegradedReasonScansItsCountSubqueries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	chschema.Apply(ctx, t, ch)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(ch.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	const orgID = "00000000-0000-4000-8000-000000014321"
	if err := conn.Exec(ctx, `INSERT INTO work_unit_investments (org_id, work_unit_id) VALUES (?, 'wu-1')`, orgID); err != nil {
		t.Fatal(err)
	}
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	reason := detectMembershipDegradedReason(ctx, client, orgID, &filterScope{})
	if reason == nil || *reason != MembershipNotMaterialized {
		t.Fatalf("reason = %v, want %s: investment rows and no membership run", reason, MembershipNotMaterialized)
	}
}
