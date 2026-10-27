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
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// showGrantsFinal26_7DhoAppShape is SHOW GRANTS FOR CURRENT_USER FINAL as
// clickhouse-server 26.7.34.3 (ghcr.io/full-chaos/clickhouse/clickhouse-server:26.7)
// printed it for a login granted exactly prod dho_app's grants (ALTER DELETE,
// INSERT, OPTIMIZE, SELECT on default.*; SELECT on system.columns, parts,
// tables). Recorded by the fcs-11729 probe run, 2026-10-27; no prod value.
var showGrantsFinal26_7DhoAppShape = []string{
	"GRANT SELECT, INSERT, ALTER DELETE, OPTIMIZE ON default.* TO dho_app_shape",
	"GRANT SELECT ON system.columns TO dho_app_shape",
	"GRANT SELECT ON system.parts TO dho_app_shape",
	"GRANT SELECT ON system.tables TO dho_app_shape",
}

func TestOrgDeletionGrantFixturesFromALiveServer(t *testing.T) {
	if err := orgDeletionGrantProblem("default", showGrantsFinal26_7DhoAppShape); err != nil {
		t.Errorf("26.7 dho_app-shape grants: %v, want the purge allowed", err)
	}
	if err := orgDeletionGrantProblem("analytics", showGrantsFinal26_7DhoAppShape); err == nil {
		t.Error("26.7 dho_app-shape grants allowed a purge of database analytics; they name default only")
	}
}

// Every grant read the preflight cannot prove refuses the delete before any
// Postgres call: the route answers 503 and `dho admin orgs delete` (through
// Operator.DeleteOrg) gets the refusal it prints with exit 1. The cases run
// against a real ClickHouse; the two shapes no live server prints on demand (an
// unparseable answer, a role grant FINAL did not expand) replace the grant read.
func TestOrgDeletionRefusesEveryUnprovenGrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouseIsolated(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	db := containers.ClickHouseDatabase
	for _, statement := range []string{
		"CREATE TABLE orgdel_grants_probe (org_id String, n UInt8) ENGINE = MergeTree ORDER BY tuple()",
		"CREATE DATABASE IF NOT EXISTS orgdel_other",
		"CREATE USER orgdel_revoked IDENTIFIED WITH plaintext_password BY 'orgdel-revoked-pw'",
		"GRANT SELECT, ALTER DELETE ON " + db + ".* TO orgdel_revoked",
		"REVOKE ALTER DELETE ON " + db + ".orgdel_grants_probe FROM orgdel_revoked",
		"CREATE USER orgdel_otherdb IDENTIFIED WITH plaintext_password BY 'orgdel-otherdb-pw'",
		"GRANT SELECT, ALTER DELETE ON orgdel_other.* TO orgdel_otherdb",
	} {
		if err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	login := func(user, password string) string {
		parsed, err := url.Parse(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		parsed.User = url.UserPassword(user, password)
		return parsed.String()
	}
	stub := func(lines []string, err error) func(context.Context, driver.Conn) ([]string, error) {
		return func(context.Context, driver.Conn) ([]string, error) { return lines, err }
	}

	for _, tc := range []struct {
		name, dsn  string
		readGrants func(context.Context, driver.Conn) ([]string, error)
		wantLog    string
	}{
		{"unparseable SHOW GRANTS output", instance.URI, stub([]string{"GRANT", "SELECT ON " + db + ".* TO u", "<html>proxy error</html>", ""}, nil), "no SELECT on " + db + ".*"},
		{"grant only via a role FINAL did not expand", instance.URI, stub([]string{"GRANT orgdel_purger TO u"}, nil), "no SELECT on " + db + ".*"},
		{"grant read fails (FINAL not understood)", instance.URI, stub(nil, errors.New("Code: 62. DB::Exception: Syntax error")), "Syntax error"},
		{"partial revoke", login("orgdel_revoked", "orgdel-revoked-pw"), nil, "partial revoke: REVOKE ALTER DELETE ON " + db + ".orgdel_grants_probe"},
		{"grants on a database other than the DSN's", login("orgdel_otherdb", "orgdel-otherdb-pw"), nil, "no SELECT on " + db + ".*"},
	} {
		t.Run(tc.name+", route", func(t *testing.T) {
			var logs bytes.Buffer
			pg := &touchRecorder{}
			h := &handlers{store: pgStore{Pool: pg}, logger: slog.New(slog.NewJSONHandler(&logs, nil)), clickHouseDSN: tc.dsn, readGrants: tc.readGrants}
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
			for _, want := range []string{`"reason":"privilege_missing"`, tc.wantLog} {
				if !strings.Contains(logs.String(), want) {
					t.Errorf("log %q lacks %q", logs.String(), want)
				}
			}
			for _, secret := range []string{"orgdel-revoked-pw", "orgdel-otherdb-pw"} {
				if strings.Contains(logs.String(), secret) {
					t.Errorf("the log carries a ClickHouse password: %q", logs.String())
				}
			}
		})
		t.Run(tc.name+", cli", func(t *testing.T) {
			pg := &touchRecorder{}
			operator := Operator{db: pg, readGrants: tc.readGrants}
			_, err := operator.DeleteOrg(ctx, uuid.NewString(), false, DeleteOrgConfig{ClickHouseDSN: tc.dsn})
			var refusal *OperatorError
			if !errors.As(err, &refusal) || refusal.Message != wantOrgDeletionRefusedDetail {
				t.Errorf("err = %v, want the refusal (the verb prints it and exits 1)", err)
			}
			if calls := pg.touched(); len(calls) > 0 {
				t.Errorf("the refused delete made %d Postgres call(s): %v", len(calls), calls)
			}
		})
	}

	// Control: the same server, the dho_app-shaped fixture and the container
	// login pass the preflight, so the refusals above are the grants' doing.
	for name, h := range map[string]*handlers{
		"26.7 dho_app-shape fixture": {clickHouseDSN: instance.URI, readGrants: stub(rewriteDatabase(showGrantsFinal26_7DhoAppShape, db), nil)},
		"container admin, live read": {clickHouseDSN: instance.URI},
	} {
		ch, err := h.orgDeletionClickHousePreflight(ctx)
		if err != nil {
			t.Errorf("%s: preflight refused: %v", name, err)
			continue
		}
		_ = ch.conn.Close()
	}
}

// rewriteDatabase points the fixture's default.* grants at database.
func rewriteDatabase(lines []string, database string) []string {
	out := make([]string, len(lines))
	for i, line := range lines {
		out[i] = strings.ReplaceAll(line, " ON default.", " ON "+database+".")
	}
	return out
}
