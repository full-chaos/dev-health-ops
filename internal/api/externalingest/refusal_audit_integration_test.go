//go:build integration

package externalingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type cancelAfterTokenRead struct {
	marker string
	cancel context.CancelFunc
}

type cancelAfterTokenReadKey struct{}

func (c cancelAfterTokenRead) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, c.marker) {
		return context.WithValue(ctx, cancelAfterTokenReadKey{}, true)
	}
	return ctx
}

func (c cancelAfterTokenRead) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if ctx.Value(cancelAfterTokenReadKey{}) != nil {
		c.cancel()
	}
}

type auditLogRow struct {
	orgID, action, resourceType, resourceID, changes, metadata, status string
	userID, description, errorMessage                                  *string
}

func readAuditRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orgID string) []auditLogRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT org_id::text, user_id::text, action, resource_type, resource_id, description, changes::text, request_metadata::text, status, error_message
		FROM audit_logs WHERE org_id = $1::uuid ORDER BY created_at, id`, orgID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []auditLogRow
	for rows.Next() {
		var row auditLogRow
		if err := rows.Scan(&row.orgID, &row.userID, &row.action, &row.resourceType, &row.resourceID, &row.description, &row.changes, &row.metadata, &row.status, &row.errorMessage); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func refusalRequest(ctx context.Context, peer, token string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/api/v1/external-ingest/validate", strings.NewReader(`{}`)).WithContext(ctx)
	request.Pattern = "POST /api/v1/external-ingest/validate"
	request.RemoteAddr = peer + ":4242"
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	return request
}

// TestKnownTokenRefusalAuditRealPostgres proves the refusal audit of a known ingest token against a real
// Postgres with the real schema: each of the four sites writes one row, read back exactly; a refused token
// refused twice has two rows; the failure throttle bounds the rows of one address at 30 and the 31st request
// is a 429 with no row; a cancelled request context still writes.
func TestKnownTokenRefusalAuditRealPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	createExternalIngestTables(t, ctx, pool)
	t.Setenv("TRUSTED_PROXIES", "")

	newDeps := func(pool *pgxpool.Pool) Deps {
		return Deps{Pool: pool, limiters: newAuthLimiters(nil, nil), routeLimiters: newRouteLimiters(nil, nil)}
	}
	deps := newDeps(pool)
	handler := deps.handleValidate()

	sites := []struct {
		name, reason string
		status       int
		arrange      func(orgID, token string)
		scopeSite    bool
	}{
		{"revoked", "token_revoked", http.StatusUnauthorized, func(orgID, token string) {
			exec(t, ctx, pool, `UPDATE external_ingest_tokens SET revoked_at = now() WHERE token_hash = $1`, mustHash(token))
		}, false},
		{"expired", "token_expired", http.StatusUnauthorized, func(orgID, token string) {
			exec(t, ctx, pool, `UPDATE external_ingest_tokens SET expires_at = now() - interval '1 hour' WHERE token_hash = $1`, mustHash(token))
		}, false},
		{"missing scope", "insufficient_scope", http.StatusForbidden, func(orgID, token string) {
			exec(t, ctx, pool, `UPDATE external_ingest_tokens SET scopes = '[]' WHERE token_hash = $1`, mustHash(token))
		}, true},
		{"feature off", "feature_not_enabled", http.StatusForbidden, func(orgID, token string) {
			exec(t, ctx, pool, `UPDATE organizations SET tier = 'community' WHERE id = $1::uuid`, orgID)
		}, false},
	}
	seed := func(token string) (string, uuid.UUID) {
		orgID := uuid.NewString()
		sourceID := seedIngestToken(t, ctx, pool, orgID, "github", "acme/"+orgID[:8], token)
		return orgID, sourceID
	}

	for index, site := range sites {
		t.Run("site "+site.name, func(t *testing.T) {
			token := "fcpush_" + site.reason + "-token-0123456789"
			orgID, sourceID := seed(token)
			site.arrange(orgID, token)
			recorder := httptest.NewRecorder()
			handler(recorder, refusalRequest(ctx, fmt.Sprintf("192.0.2.%d", 10+index), token))
			if recorder.Code != site.status {
				t.Fatalf("status %d, want %d; body %s", recorder.Code, site.status, recorder.Body.String())
			}
			rows := readAuditRows(t, ctx, pool, orgID)
			if len(rows) != 1 {
				t.Fatalf("%d audit rows, want 1", len(rows))
			}
			row := rows[0]
			var tokenID string
			if err := pool.QueryRow(ctx, `SELECT id::text FROM external_ingest_tokens WHERE token_hash = $1`, mustHash(token)).Scan(&tokenID); err != nil {
				t.Fatal(err)
			}
			if row.orgID != orgID || row.userID != nil || row.action != "ingest_auth_failed" || row.resourceType != "external_ingest_token" ||
				row.resourceID != tokenID || row.status != "failure" || row.changes != "{}" {
				t.Errorf("row %+v: wrong org/user/action/resource/status/changes (token %s)", row, tokenID)
			}
			if row.description == nil || *row.description != "Ingest authentication refused" || row.errorMessage == nil || *row.errorMessage != site.reason {
				t.Errorf("description %v, error_message %v, want the fixed text and %s", row.description, row.errorMessage, site.reason)
			}
			want := map[string]any{"method": "POST", "route": "/api/v1/external-ingest/validate", "source_id": sourceID.String()}
			if site.scopeSite {
				want["required_scope"] = "schema:read"
			}
			var got map[string]any
			if err := json.Unmarshal([]byte(row.metadata), &got); err != nil || !reflect.DeepEqual(got, want) {
				t.Errorf("request_metadata %s (%v), want %v", row.metadata, err, want)
			}
			if strings.Contains(row.metadata, token) || strings.Contains(row.metadata, "192.0.2") {
				t.Errorf("request_metadata carries the token or the address: %s", row.metadata)
			}
		})
	}

	t.Run("a revoked token refused twice has two rows", func(t *testing.T) {
		token := "fcpush_twice-token-0123456789"
		orgID, _ := seed(token)
		exec(t, ctx, pool, `UPDATE external_ingest_tokens SET revoked_at = now() WHERE token_hash = $1`, mustHash(token))
		for range 2 {
			recorder := httptest.NewRecorder()
			handler(recorder, refusalRequest(ctx, "192.0.2.50", token))
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status %d", recorder.Code)
			}
		}
		if rows := readAuditRows(t, ctx, pool, orgID); len(rows) != 2 {
			t.Fatalf("%d audit rows, want 2", len(rows))
		}
	})

	t.Run("one address writes at most 30 rows and the 31st request is the 429 with no row", func(t *testing.T) {
		token := "fcpush_throttle-token-0123456789"
		orgID, _ := seed(token)
		exec(t, ctx, pool, `UPDATE external_ingest_tokens SET revoked_at = now() WHERE token_hash = $1`, mustHash(token))
		for i := 1; i <= 30; i++ {
			recorder := httptest.NewRecorder()
			handler(recorder, refusalRequest(ctx, "192.0.2.60", token))
			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("request %d: status %d, want 401", i, recorder.Code)
			}
		}
		recorder := httptest.NewRecorder()
		handler(recorder, refusalRequest(ctx, "192.0.2.60", token))
		if recorder.Code != http.StatusTooManyRequests {
			t.Fatalf("request 31: status %d, want 429; body %s", recorder.Code, recorder.Body.String())
		}
		if rows := readAuditRows(t, ctx, pool, orgID); len(rows) != 30 {
			t.Fatalf("%d audit rows, want 30", len(rows))
		}
	})

	t.Run("a cancelled request context still writes the row", func(t *testing.T) {
		for _, site := range sites {
			token := "fcpush_cancel-" + strings.ReplaceAll(site.reason, "_", "-") + "-0123456789"
			orgID, _ := seed(token)
			site.arrange(orgID, token)
			exec(t, ctx, pool, `UPDATE external_ingest_tokens SET source_id = NULL WHERE token_hash = $1`, mustHash(token))
			config, err := pgxpool.ParseConfig(pg.URI)
			if err != nil {
				t.Fatal(err)
			}
			requestCtx, cancelRequest := context.WithCancel(ctx)
			marker := "external_ingest_tokens"
			if site.reason == "feature_not_enabled" {
				marker = "feature_flags"
			}
			config.ConnConfig.Tracer = cancelAfterTokenRead{marker: marker, cancel: cancelRequest}
			tracedPool, err := pgxpool.NewWithConfig(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			recorder := httptest.NewRecorder()
			newDeps(tracedPool).handleValidate()(recorder, refusalRequest(requestCtx, "192.0.2.70", token))
			if requestCtx.Err() == nil {
				t.Fatalf("%s: the request context was never cancelled", site.name)
			}
			if recorder.Code != site.status {
				t.Fatalf("%s: status %d, want %d; body %s", site.name, recorder.Code, site.status, recorder.Body.String())
			}
			rows := readAuditRows(t, ctx, pool, orgID)
			if len(rows) != 1 {
				t.Fatalf("%s: %d audit rows after a cancelled context, want 1", site.name, len(rows))
			}
			cancelRequest()
			tracedPool.Close()
		}
	})
}

func exec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}
