//go:build integration

package remaining

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// clickhouse-go v2.50+ refuses a NULL scanned into a destination that cannot
// hold NULL. fetchGitFiles scans contents into a string; its WHERE drops a NULL
// contents row before the scan, so the read holds on every driver version.
func TestFetchGitFilesNeverScansANullContents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	const orgID = "00000000-0000-4000-8000-000000014218"
	repoID := uuid.MustParse("00000000-0000-4000-8000-0000000142d1")
	synced := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	if err := conn.Exec(ctx, `INSERT INTO git_files (repo_id, path, executable, contents, last_synced, org_id)
		VALUES (?, 'empty.go', 0, NULL, ?, ?), (?, 'main.go', 0, 'package main', ?, ?)`,
		repoID, synced, orgID, repoID, synced, orgID); err != nil {
		t.Fatal(err)
	}
	files, err := clickhouseComplexitySource{conn: conn}.fetchGitFiles(ctx, repoID, orgID, []string{"empty.go", "main.go"})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].Path != "main.go" || files[0].Contents != "package main" {
		t.Fatalf("files = %+v, want only main.go: the NULL contents row is filtered, not scanned", files)
	}
}
