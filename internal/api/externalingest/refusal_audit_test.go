package externalingest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/api/externalingest"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/ingestpg"
)

const (
	auditCounter   = "dev_health_api_ingest_auth_audit_failures_total"
	auditOrgID     = "4d0a1b2c-3e4f-4a5b-8c6d-7e8f9a0b1c2d"
	auditTokenID   = "tok-audit-1"
	auditSourceID  = "5e1b2c3d-4f50-4b6c-9d7e-8f90a1b2c3d4"
	auditToken     = "fcpush_" + "audit-unit-token-0123456789"
	canonicalError = `{"error":{"code":"invalid_token","message":"Missing or invalid ingest token"}}`
)

var auditMeterReader = sync.OnceValue(func() *sdkmetric.ManualReader {
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	return reader
})

func auditFailureCounts(t *testing.T) map[string]int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := auditMeterReader().Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, series := range scope.Metrics {
			if series.Name != auditCounter {
				continue
			}
			for _, point := range series.Data.(metricdata.Sum[int64]).DataPoints {
				value, _ := point.Attributes.Value(attribute.Key("reason"))
				counts[value.AsString()] += point.Value
			}
		}
	}
	return counts
}

type auditRig struct {
	server *ingestpg.Server
	pool   *pgxpool.Pool
	logs   *bytes.Buffer
	deps   externalingest.Deps
}

func newAuditRig(t *testing.T, adjust func(*pgxpool.Config)) *auditRig {
	t.Helper()
	auditMeterReader()
	t.Setenv("TRUSTED_PROXIES", "")
	server, pool := ingestpg.StartWith(t, adjust)
	logs := &bytes.Buffer{}
	return &auditRig{server: server, pool: pool, logs: logs, deps: externalingest.Deps{
		Pool: pool, Logger: slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}}
}

type auditRoute struct {
	method, pattern, scope string
}

var auditRoutes = []auditRoute{
	{"GET", "/api/v1/external-ingest/availability", "schema:read"},
	{"POST", "/api/v1/external-ingest/validate", "schema:read"},
	{"POST", "/api/v1/external-ingest/batches", "ingest:write"},
	{"GET", "/api/v1/external-ingest/batches", "ingest:status"},
	{"GET", "/api/v1/external-ingest/batches/{ingestion_id}", "ingest:status"},
}

func (rig *auditRig) do(t *testing.T, ctx context.Context, rt auditRoute, authorization string) *httptest.ResponseRecorder {
	t.Helper()
	var handler http.Handler
	for _, candidate := range externalingest.Routes(rig.deps) {
		if candidate.Method == rt.method && candidate.Pattern == rt.pattern {
			handler = candidate.Handler
		}
	}
	if handler == nil {
		t.Fatalf("no route %s %s", rt.method, rt.pattern)
	}
	target := strings.ReplaceAll(rt.pattern, "{ingestion_id}", "11111111-1111-4111-8111-111111111111")
	request := httptest.NewRequest(rt.method, target, strings.NewReader(`{}`)).WithContext(ctx)
	request.SetPathValue("ingestion_id", "11111111-1111-4111-8111-111111111111")
	request.Pattern = rt.method + " " + rt.pattern
	request.RemoteAddr = "192.0.2.7:4242"
	request.Header.Set("Content-Type", "application/json")
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

type refusalSite struct {
	name   string
	token  func() *ingestpg.Token
	status int
	reason string
	code   string // error code in the body ("" = the canonical invalid_token body)
}

func refusalSites() []refusalSite {
	past, future := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	source := auditSourceID
	return []refusalSite{
		{"revoked", func() *ingestpg.Token {
			return &ingestpg.Token{ID: auditTokenID, OrgID: auditOrgID, Scopes: `["schema:read","ingest:write","ingest:status"]`, RevokedAt: &past}
		}, http.StatusUnauthorized, "token_revoked", ""},
		{"expired", func() *ingestpg.Token {
			return &ingestpg.Token{ID: auditTokenID, OrgID: auditOrgID, Scopes: `["schema:read","ingest:write","ingest:status"]`, ExpiresAt: &past, SourceID: &source}
		}, http.StatusUnauthorized, "token_expired", ""},
		{"missing scope", func() *ingestpg.Token {
			return &ingestpg.Token{ID: auditTokenID, OrgID: auditOrgID, Scopes: `[]`, ExpiresAt: &future, SourceID: &source}
		}, http.StatusForbidden, "insufficient_scope", "insufficient_scope"},
		{"feature off", func() *ingestpg.Token {
			return &ingestpg.Token{ID: auditTokenID, OrgID: auditOrgID, Scopes: `["schema:read","ingest:write","ingest:status"]`}
		}, http.StatusForbidden, "feature_not_enabled", "feature_not_enabled"},
	}
}

func wantMetadata(rt auditRoute, site refusalSite, hasSource bool) map[string]any {
	want := map[string]any{"method": rt.method, "route": rt.pattern}
	if site.reason == "insufficient_scope" {
		want["required_scope"] = rt.scope
	}
	if hasSource {
		want["source_id"] = auditSourceID
	}
	return want
}

// Each refusal of a token whose row was read writes one audit row with exactly the specified fields, and the
// answer is the one it was before.
func TestKnownTokenRefusalsWriteOneAuditRow(t *testing.T) {
	for _, site := range refusalSites() {
		for _, rt := range auditRoutes {
			if site.reason == "feature_not_enabled" && rt.pattern == auditRoutes[0].pattern {
				continue // GET /availability reports the feature state and does not require it
			}
			t.Run(site.name+"/"+rt.method+" "+rt.pattern, func(t *testing.T) {
				rig := newAuditRig(t, nil)
				token := site.token()
				rig.server.SetToken(token)
				answer := rig.do(t, context.Background(), rt, "Bearer "+auditToken)
				if answer.Code != site.status {
					t.Fatalf("status %d, want %d; body %s", answer.Code, site.status, answer.Body.String())
				}
				if site.code == "" && strings.TrimSpace(answer.Body.String()) != canonicalError {
					t.Errorf("body %q, want the canonical invalid_token body", answer.Body.String())
				}
				if site.code != "" && !strings.Contains(answer.Body.String(), `"`+site.code+`"`) {
					t.Errorf("body %q lacks %s", answer.Body.String(), site.code)
				}
				rows := rig.server.AuditRows()
				if len(rows) != 1 {
					t.Fatalf("%d audit rows, want 1", len(rows))
				}
				row := rows[0]
				if row.OrgID != auditOrgID || row.UserID != nil || row.Action != "ingest_auth_failed" || row.ResourceType != "external_ingest_token" ||
					row.ResourceID != auditTokenID || row.Status != "failure" {
					t.Errorf("row %+v: wrong org/user/action/resource/status", row)
				}
				if row.Description == nil || *row.Description != "Ingest authentication refused" {
					t.Errorf("description %v", row.Description)
				}
				if row.ErrorMessage == nil || *row.ErrorMessage != site.reason {
					t.Errorf("error_message %v, want %s", row.ErrorMessage, site.reason)
				}
				var got map[string]any
				if err := json.Unmarshal([]byte(row.RequestMetadata), &got); err != nil {
					t.Fatalf("request_metadata %q: %v", row.RequestMetadata, err)
				}
				if want := wantMetadata(rt, site, token.SourceID != nil); !reflect.DeepEqual(got, want) {
					t.Errorf("request_metadata %v, want %v", got, want)
				}
				for _, secret := range []string{auditToken, "192.0.2.7", "fcpush_"} {
					if strings.Contains(row.RequestMetadata+row.Changes, secret) || (row.Description != nil && strings.Contains(*row.Description, secret)) {
						t.Errorf("the row carries %q", secret)
					}
				}
			})
		}
	}
}

// A request that never read a token row writes nothing: no row found, a wrong class, no credential.
func TestRefusalsWithoutATokenRowWriteNoAudit(t *testing.T) {
	cases := map[string]string{
		"no row found":     "Bearer " + auditToken,
		"wrong class":      "Bearer eyJhbGciOiJIUzI1NiJ9.e30.sig",
		"no credential":    "",
		"svc class bearer": "Bearer svc_worker_xxxxxxxxxxxxxxxx",
	}
	for name, authorization := range cases {
		for _, rt := range auditRoutes {
			t.Run(name+"/"+rt.method+" "+rt.pattern, func(t *testing.T) {
				rig := newAuditRig(t, nil)
				rig.server.SetToken(nil)
				answer := rig.do(t, context.Background(), rt, authorization)
				if answer.Code != http.StatusUnauthorized || strings.TrimSpace(answer.Body.String()) != canonicalError {
					t.Fatalf("answer %d %s, want the canonical 401", answer.Code, answer.Body.String())
				}
				if rows := rig.server.AuditRows(); len(rows) != 0 {
					t.Errorf("%d audit rows, want 0: %+v", len(rows), rows)
				}
			})
		}
	}
}

// A write that fails changes nothing the caller sees: same status, same body, one count under the reason, and a
// log line with the reason and the token id and nothing of the error text or the row values.
func TestAuditWriteFailureKeepsTheAnswerAndIsCountedNotEchoed(t *testing.T) {
	const marker = "WRITER-MARKER-7c2d9e"
	for _, site := range refusalSites() {
		t.Run(site.name, func(t *testing.T) {
			rt := auditRoutes[2]
			rig := newAuditRig(t, nil)
			rig.server.SetToken(site.token())
			baseline := rig.do(t, context.Background(), rt, "Bearer "+auditToken)
			rig.server.FailAuditInsert("23503", "insert failed "+marker, "Key (org_id)=("+auditOrgID+") is not present; "+marker)
			rig.logs.Reset()
			before := auditFailureCounts(t)
			answer := rig.do(t, context.Background(), rt, "Bearer "+auditToken)
			if answer.Code != baseline.Code || answer.Body.String() != baseline.Body.String() {
				t.Errorf("answer changed: %d %q, was %d %q", answer.Code, answer.Body.String(), baseline.Code, baseline.Body.String())
			}
			after := auditFailureCounts(t)
			if after[site.reason]-before[site.reason] != 1 {
				t.Errorf("counter %v -> %v, want +1 under %s", before, after, site.reason)
			}
			logs := rig.logs.String()
			for _, banned := range []string{auditOrgID, marker, auditToken, "insert failed"} {
				if strings.Contains(logs, banned) {
					t.Errorf("the log carries %q:\n%s", banned, logs)
				}
			}
			for _, required := range []string{site.reason, auditTokenID, "23503", "level=WARN"} {
				if !strings.Contains(logs, required) {
					t.Errorf("the log lacks %q:\n%s", required, logs)
				}
			}
		})
	}
}

// cancelAfter cancels a request context when a query whose text holds marker ends: the request is gone at the
// moment the answer is decided, before the refusal is audited.
type cancelAfter struct {
	marker string
	cancel context.CancelFunc
}

type cancelAfterKey struct{}

func (c cancelAfter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, c.marker) {
		return context.WithValue(ctx, cancelAfterKey{}, true)
	}
	return ctx
}

func (c cancelAfter) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if ctx.Value(cancelAfterKey{}) != nil {
		c.cancel()
	}
}

// A client that has gone (the request context is cancelled once the last query that decides the answer is
// read) does not lose the row.
func TestAuditRowIsWrittenAfterTheRequestContextIsCancelled(t *testing.T) {
	for _, site := range refusalSites() {
		t.Run(site.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			marker := "external_ingest_tokens"
			if site.reason == "feature_not_enabled" {
				marker = "feature_flags"
			}
			rig := newAuditRig(t, func(config *pgxpool.Config) {
				config.ConnConfig.Tracer = cancelAfter{marker: marker, cancel: cancel}
			})
			token := site.token()
			token.SourceID = nil
			rig.server.SetToken(token)
			answer := rig.do(t, ctx, auditRoutes[1], "Bearer "+auditToken)
			if ctx.Err() == nil {
				t.Fatal("the request context was never cancelled: the test proves nothing")
			}
			if answer.Code != site.status {
				t.Fatalf("status %d, want %d; body %s", answer.Code, site.status, answer.Body.String())
			}
			if rows := rig.server.AuditRows(); len(rows) != 1 {
				t.Fatalf("%d audit rows after a cancelled request context, want 1", len(rows))
			}
		})
	}
}
