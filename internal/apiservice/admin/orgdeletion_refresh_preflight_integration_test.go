//go:build integration

package admin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// viewRefreshesDenied is the refusal a login without SELECT on
// system.view_refreshes gets where system tables need a grant: ClickHouse's
// ACCESS_DENIED text, format "{}: Not enough privileges. To execute this query,
// it's necessary to have the grant {}" (src/Access/ContextAccess.cpp,
// checkAccessImplHelper). view_refreshes is not in that file's
// always_accessible_tables.
const viewRefreshesDenied = "dho_app_shape: Not enough privileges. To execute this query, it's necessary to have the grant SELECT(database, view, status) ON system.view_refreshes"

// The purge waits on the refreshes of the views that fill a discovered table,
// so a login that cannot read their state is refused before any Postgres call:
// the route answers 503 and `dho admin orgs delete` gets the refusal. A
// database with no such table never reads the refresh state; the real read
// succeeds against a server that allows it.
func TestOrgDeletionRefusesALoginThatCannotReadTheRefreshState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouseIsolated(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	reads := 0
	denied := func(context.Context, driver.Conn, []string) (uint64, error) {
		reads++
		return 0, errors.New(viewRefreshesDenied)
	}

	exec("CREATE TABLE orgdel_refresh_probe (org_id String, n UInt8) ENGINE = MergeTree ORDER BY tuple()")
	ch, err := (&handlers{clickHouseDSN: instance.URI, readRefreshStates: denied}).orgDeletionClickHousePreflight(ctx)
	if err != nil {
		t.Fatalf("no refresh-fed table: preflight %v, want it to pass", err)
	}
	_ = ch.conn.Close()
	if reads != 0 {
		t.Fatalf("no refresh-fed table: the preflight read the refresh state %d time(s), want 0", reads)
	}

	exec("CREATE TABLE project_team_ownership_groups_log (org_id String, n UInt8) ENGINE = MergeTree ORDER BY tuple()")
	ch, err = (&handlers{clickHouseDSN: instance.URI}).orgDeletionClickHousePreflight(ctx)
	if err != nil {
		t.Fatalf("a refresh-fed table, the real refresh-state read: preflight %v, want it to pass", err)
	}
	_ = ch.conn.Close()

	t.Run("route", func(t *testing.T) {
		var logs bytes.Buffer
		pg := &touchRecorder{}
		h := &handlers{store: pgStore{Pool: pg}, logger: slog.New(slog.NewJSONHandler(&logs, nil)), clickHouseDSN: instance.URI, readRefreshStates: denied}
		for _, query := range []string{"", "?dry_run=true"} {
			orgID := uuid.NewString()
			request := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/orgs/"+orgID+query, nil)
			request.SetPathValue("org_id", orgID)
			recorder := httptest.NewRecorder()
			h.deleteOrganization(recorder, request)
			if want := fmt.Sprintf(`{"detail":%q}`, wantOrgDeletionRefusedDetail); recorder.Code != http.StatusServiceUnavailable || strings.TrimSpace(recorder.Body.String()) != want {
				t.Errorf("%q: status %d body %s, want 503 %s", query, recorder.Code, recorder.Body.String(), want)
			}
		}
		if calls := pg.touched(); len(calls) > 0 {
			t.Errorf("the refused delete made %d Postgres call(s): %v", len(calls), calls)
		}
		for _, want := range []string{`"reason":"privilege_missing"`, "read system.view_refreshes for project_team_ownership_groups_refresh", "ON system.view_refreshes"} {
			if !strings.Contains(logs.String(), want) {
				t.Errorf("log %q lacks %q", logs.String(), want)
			}
		}
	})
	t.Run("cli", func(t *testing.T) {
		pg := &touchRecorder{}
		operator := Operator{db: pg, readRefreshStates: denied, Provisioner: &fakeProvisioner{db: &provisionDB{}}}
		_, err := operator.DeleteOrg(ctx, uuid.NewString(), false, DeleteOrgConfig{ClickHouseDSN: instance.URI, Actor: operatorActorFlag})
		var refusal *OperatorError
		if !errors.As(err, &refusal) || refusal.Message != wantOrgDeletionRefusedDetail {
			t.Errorf("err = %v, want the refusal (the verb prints it and exits 1)", err)
		}
		if calls := pg.touched(); len(calls) > 0 {
			t.Errorf("the refused delete made %d Postgres call(s): %v", len(calls), calls)
		}
	})
}
