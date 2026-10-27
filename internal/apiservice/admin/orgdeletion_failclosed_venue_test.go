//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	orgDeletionRefusedDetail    = "ClickHouse is not configured, not reachable or not permitted to purge; organization deletion refused. Nothing was deleted."
	orgDeletionIncompleteDetail = "Organization Postgres data was deleted, but the ClickHouse purge did not complete. Run the delete again."
	failClosedEncryptionKey     = "venue-orgdeletion-failclosed-fernet-key-32b"
)

// TestOrgDeletionFailsClosedWithoutAUsableClickHouse: an org delete never
// answers 200 while the org's ClickHouse rows stay. With no ClickHouse DSN, a
// DSN that cannot connect, or a login that may not ALTER DELETE, the route
// answers 503 BEFORE anything is touched: no PagerDuty revoke call, every
// Postgres and ClickHouse row of the org stays (real delete and dry run). A
// ClickHouse failure AFTER the Postgres commit answers 500, and the delete run
// again -- with the organization row already gone -- purges ClickHouse by
// org_id and answers 200.
func TestOrgDeletionFailsClosedWithoutAUsableClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	const jwtKey = "venue-oracle-test-secret-key-for-org-deletion-fail-closed-32b"

	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(failClosedEncryptionKey), "")
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := decryptor.Encrypt([]byte(`{"access_token":"venue-fc-access","refresh_token":"venue-fc-refresh","expires_at":"2099-01-01T00:00:00Z","granted_scopes":[]}`))
	if err != nil {
		t.Fatal(err)
	}

	orgs := map[string]uuid.UUID{}
	for _, name := range []string{"nodsn", "closed", "dryrun", "selectonly", "aftercommit", "control"} {
		orgs[name] = uuid.New()
	}
	superID := uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		GoOnly: true,
		Root:   repoRoot(t),
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, adminPool *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := adminPool.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			for name, org := range orgs {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, org, "venue-fc-"+name)
				exec(`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
VALUES ($1, $2, 'general', 'theme', 'dark', false, now(), now())`, uuid.New(), org.String())
				exec(`INSERT INTO scheduled_jobs (id, org_id, name, job_type, provider, schedule_cron, timezone, job_config, status, is_running, run_count, failure_count, next_run_at, created_at, updated_at)
VALUES ($1, $2, 'venue job', 'sync', '', '0 * * * *', 'UTC', '{}', 1, false, 0, 0, now(), now(), now())`, uuid.New(), org.String())
			}
			exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', 'default', $2, 1, now(), now(), true)`, orgs["selectonly"].String(), ciphertext.Reveal())
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-fc-super@example.com', true, true, true, 0, now(), now())`, superID)
			return map[string]map[string]any{
				"super": {"user_id": superID.String(), "email": "venue-fc-super@example.com", "is_superuser": true},
			}
		},
	})

	adminCH := venue.AdminClickHouseURI(t, venue.GoClickHouseDB)
	chConn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(adminCH))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = chConn.Close() })
	chExec := func(sql string, args ...any) {
		t.Helper()
		if err := chConn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("clickhouse: %v\n%s", err, sql)
		}
	}
	seedClickHouse := func(org uuid.UUID) {
		t.Helper()
		chExec(`INSERT INTO backfill_log (job_id, org_id, chunk_index, chunk_since, chunk_before, provider, items_synced, duration_ms, status) VALUES (?, ?, 0, today(), today(), 'github', 1, 1, 'complete')`,
			uuid.NewString(), org.String())
	}
	for _, org := range orgs {
		seedClickHouse(org)
	}

	// A login that may read the database but not ALTER DELETE in it.
	selectOnlyUser := "orgdel_fc_select_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	chExec(fmt.Sprintf("CREATE USER %s IDENTIFIED WITH plaintext_password BY 'orgdel-fc-select'", selectOnlyUser))
	t.Cleanup(func() { _ = chConn.Exec(context.Background(), "DROP USER IF EXISTS "+selectOnlyUser) })
	chExec(fmt.Sprintf("GRANT SELECT ON %s.* TO %s", venue.GoClickHouseDB, selectOnlyUser))
	selectOnlyURI := withClickHouseLogin(t, adminCH, selectOnlyUser, "orgdel-fc-select", "")
	closedURI := withClickHouseLogin(t, adminCH, "", "", "127.0.0.1:1")

	state := func(org uuid.UUID) string {
		t.Helper()
		pg := venue.AdminURI(t, venue.GoDB)
		id := org.String()
		return strings.Join([]string{
			"organizations=" + venueoracle.TableRows(t, ctx, pg, "SELECT count(*) FROM organizations WHERE id = '"+id+"'"),
			"settings=" + venueoracle.TableRows(t, ctx, pg, "SELECT count(*) FROM settings WHERE org_id = '"+id+"'"),
			"jobs_enabled=" + venueoracle.TableRows(t, ctx, pg, "SELECT count(*) FROM scheduled_jobs WHERE org_id = '"+id+"' AND status = 1 AND next_run_at IS NOT NULL"),
			"pagerduty=" + venueoracle.TableRows(t, ctx, pg, "SELECT count(*) FROM provider_oauth_credentials WHERE org_id = '"+id+"'"),
			"ch_backfill_log=" + venueoracle.CHRows(t, ctx, adminCH, "SELECT count() FROM backfill_log WHERE org_id = '"+id+"'"),
		}, " ")
	}
	bearer := map[string]string{"Authorization": "Bearer " + venue.Tokens["super"]}
	del := func(base string, org uuid.UUID, query string) venueoracle.Response {
		t.Helper()
		return venueoracle.Do(t, base, venueoracle.Request{
			Name: "delete org " + org.String() + query, Method: "DELETE",
			Path: "/api/v1/admin/orgs/" + org.String() + query, Headers: bearer,
		})
	}
	wantDetail := func(response venueoracle.Response, status int, detail string) {
		t.Helper()
		want := fmt.Sprintf(`{"detail":%q}`, detail)
		if response.Status != status || strings.TrimSpace(response.Body) != want {
			t.Errorf("status %d body %s, want %d %s", response.Status, response.Body, status, want)
		}
	}
	controlBefore := state(orgs["control"])

	noDSN, _ := startGoServer(t, ctx, venue, jwtKey)
	closed, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) { deps.ClickHouseDSN = closedURI })

	t.Run("no dsn, real delete", func(t *testing.T) {
		before := state(orgs["nodsn"])
		wantDetail(del(noDSN, orgs["nodsn"], ""), http.StatusServiceUnavailable, orgDeletionRefusedDetail)
		if after := state(orgs["nodsn"]); after != before {
			t.Errorf("the refused delete changed rows: %s -> %s", before, after)
		}
	})
	t.Run("no dsn, dry run", func(t *testing.T) {
		before := state(orgs["dryrun"])
		wantDetail(del(noDSN, orgs["dryrun"], "?dry_run=true"), http.StatusServiceUnavailable, orgDeletionRefusedDetail)
		if after := state(orgs["dryrun"]); after != before {
			t.Errorf("the refused dry run changed rows: %s -> %s", before, after)
		}
	})
	t.Run("dsn that cannot connect", func(t *testing.T) {
		before := state(orgs["closed"])
		wantDetail(del(closed, orgs["closed"], ""), http.StatusServiceUnavailable, orgDeletionRefusedDetail)
		if after := state(orgs["closed"]); after != before {
			t.Errorf("the refused delete changed rows: %s -> %s", before, after)
		}
	})
	t.Run("login without ALTER DELETE, before the PagerDuty revoke", func(t *testing.T) {
		fake := &fakePagerDutyRevokeServer{}
		fakeServer := httptest.NewServer(http.HandlerFunc(fake.handler))
		t.Cleanup(fakeServer.Close)
		selectOnly, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
			deps.ClickHouseDSN = selectOnlyURI
			deps.Decryptor = decryptor
			deps.PagerDuty = providerfoundation.PagerDutyRevokeConfig{ClientID: "venue-fc-pagerduty", RevokeURL: fakeServer.URL}
		})
		before := state(orgs["selectonly"])
		if !strings.Contains(before, "pagerduty=1") {
			t.Fatalf("seed: %s", before)
		}
		wantDetail(del(selectOnly, orgs["selectonly"], ""), http.StatusServiceUnavailable, orgDeletionRefusedDetail)
		if got := fake.count(); got != 0 {
			t.Errorf("the refused delete made %d PagerDuty revoke call(s), want 0", got)
		}
		if after := state(orgs["selectonly"]); after != before {
			t.Errorf("the refused delete changed rows: %s -> %s", before, after)
		}
	})
	t.Run("clickhouse fails after the postgres commit, then a re-run purges", func(t *testing.T) {
		org := orgs["aftercommit"]
		admin, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) { deps.ClickHouseDSN = adminCH })
		// A discovered org_id table whose count cannot bind a uuid string: the
		// purge of this one table fails after Postgres has committed.
		chExec("CREATE TABLE orgdel_fc_broken (org_id UInt64, n UInt8) ENGINE = MergeTree ORDER BY tuple()")
		t.Cleanup(func() { _ = chConn.Exec(context.Background(), "DROP TABLE IF EXISTS orgdel_fc_broken") })
		chExec("INSERT INTO orgdel_fc_broken VALUES (1, 1)")
		var probe uint64
		if err := chConn.QueryRow(ctx, "SELECT count() FROM orgdel_fc_broken WHERE org_id = ?", org.String()).Scan(&probe); err == nil {
			t.Fatal("setup: a count of orgdel_fc_broken by a uuid string succeeded; the table does not make the purge fail")
		}

		wantDetail(del(admin, org, ""), http.StatusInternalServerError, orgDeletionIncompleteDetail)
		if got := state(org); !strings.HasPrefix(got, "organizations=0 settings=0 jobs_enabled=0 ") {
			t.Errorf("after the 500 the Postgres purge is not committed: %s", got)
		}

		// The org row is gone; rows of the org that ClickHouse still holds are
		// purged by org_id when the delete runs again.
		chExec("DROP TABLE orgdel_fc_broken")
		seedClickHouse(org)
		if got := venueoracle.CHRows(t, ctx, adminCH, "SELECT count() FROM backfill_log WHERE org_id = '"+org.String()+"'"); got != "1" {
			t.Fatalf("seed before the re-run: backfill_log rows = %s, want 1", got)
		}
		again := del(admin, org, "")
		if again.Status != http.StatusOK {
			t.Fatalf("re-run: status %d body %s, want 200", again.Status, again.Body)
		}
		var plan struct {
			ClickHouse struct {
				Total int64 `json:"total"`
			} `json:"clickhouse"`
		}
		if err := json.Unmarshal([]byte(again.Body), &plan); err != nil || plan.ClickHouse.Total < 1 {
			t.Errorf("re-run: clickhouse total = %d (err %v), want >= 1; body %s", plan.ClickHouse.Total, err, again.Body)
		}
		if got := state(org); got != "organizations=0 settings=0 jobs_enabled=0 pagerduty=0 ch_backfill_log=0" {
			t.Errorf("after the re-run: %s", got)
		}
	})
	if after := state(orgs["control"]); after != controlBefore {
		t.Errorf("the control org changed: %s -> %s", controlBefore, after)
	}
	venueoracle.WriteGoOnlyProof(t, "Go's org delete refuses (503, nothing touched) without a usable ClickHouse and answers 500 when the purge fails after the Postgres commit; the re-run purges")
}

// withClickHouseLogin returns uri with its login (when user is set) or its
// host (when host is set) replaced.
func withClickHouseLogin(t *testing.T, uri, user, password, host string) string {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	if user != "" {
		parsed.User = url.UserPassword(user, password)
	}
	if host != "" {
		parsed.Host = host
	}
	return parsed.String()
}
