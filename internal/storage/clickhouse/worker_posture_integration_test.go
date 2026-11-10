//go:build integration

package clickhouse

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func readWorkerGrants(ctx context.Context, t *testing.T, admin driver.Conn) []SystemGrantRow {
	t.Helper()
	rows, err := admin.Query(clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{"user": WorkerUser})), WorkerGrantsSelect)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var grants []SystemGrantRow
	for rows.Next() {
		var row SystemGrantRow
		if err := rows.Scan(&row.UserName, &row.RoleName, &row.AccessType, &row.Database, &row.Table, &row.Column,
			&row.IsPartialRevoke, &row.GrantOption); err != nil {
			t.Fatal(err)
		}
		grants = append(grants, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return grants
}

// The posture's own GRANT statements, applied to a real server, read back
// through WorkerGrantsSelect as exactly the declared rows: this pins what the
// server writes to system.grants (access type names, NULL table for db.*).
func TestWorkerPostureOnALiveServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouseIsolated(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := Open(ctx, DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	exec := func(statement string) {
		t.Helper()
		if err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	exec("CREATE USER " + WorkerUser + " IDENTIFIED WITH plaintext_password BY 'worker-posture-test-password'")
	for _, statement := range WorkerGrantStatements(WorkerUser) {
		exec(statement)
	}
	if err := CompareWorkerGrants(readWorkerGrants(ctx, t, admin)); err != nil {
		t.Fatalf("the posture's own grants must match: %v", err)
	}

	exec("GRANT ALTER UPDATE ON default.* TO " + WorkerUser)
	err = CompareWorkerGrants(readWorkerGrants(ctx, t, admin))
	if !errors.Is(err, ErrWorkerPostureMismatch) || !strings.Contains(err.Error(), "extra privilege: ALTER UPDATE ON default.*") {
		t.Fatalf("an extra ALTER UPDATE must fail naming it, got: %v", err)
	}
	exec("REVOKE ALTER UPDATE ON default.* FROM " + WorkerUser)

	exec("CREATE ROLE worker_posture_role")
	exec("GRANT SELECT ON system.query_log TO worker_posture_role")
	exec("GRANT worker_posture_role TO " + WorkerUser)
	err = CompareWorkerGrants(readWorkerGrants(ctx, t, admin))
	if !errors.Is(err, ErrWorkerPostureMismatch) || !strings.Contains(err.Error(), "role grant: role worker_posture_role holds SELECT ON system.query_log") {
		t.Fatalf("a role granted to the user must fail naming it, got: %v", err)
	}
}
