//go:build integration

package externalingest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestExternalIngestKeepsItsLegacy500ForALoneSurrogate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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
	valkey, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = valkey.Close(context.Background()) })
	options, _ := valkeygo.ParseURL(valkey.URI)
	client, err := valkeygo.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	orgID := uuid.New().String()
	const token = "fcpush_test-token-c1"
	seedIngestToken(t, ctx, pool, orgID, "github", "acme/repo", token)
	deps := newTestDeps(t, pool, client)
	post := func(h http.HandlerFunc, path, b string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec
	}
	show := func(b []byte) string {
		s := string(b)
		if len(s) > 260 {
			s = s[:260]
		}
		return "valid_utf8=" + map[bool]string{true: "true", false: "false"}[utf8.Valid(b)] + " body=" + s
	}
	env := func(idem, payload string) string {
		return `{"schemaVersion":"external-ingest.v1","idempotencyKey":"` + idem + `","source":{"system":"github","instance":"acme/repo"},"records":[{"kind":"repository.v1","externalId":"acme/repo","payload":` + payload + `}]}`
	}
	cases := []struct {
		name, body string
		clean      bool
	}{
		{"control clean", env("c0", `{"externalId":"acme/repo","sourceSystem":"github"}`), true},
		{"payload string externalId", env("c1", `{"externalId":"acme/\ud800repo","sourceSystem":"github"}`), false},
		{"payload extra str field", env("c2", `{"externalId":"acme/repo","sourceSystem":"github","name":"a\ud800b"}`), false},
		{"idempotencyKey", env("k\\ud800", `{"externalId":"acme/repo","sourceSystem":"github"}`), false},
		{"record externalId", `{"schemaVersion":"external-ingest.v1","idempotencyKey":"c4","source":{"system":"github","instance":"acme/repo"},"records":[{"kind":"repository.v1","externalId":"a\udc00","payload":{"externalId":"acme/repo","sourceSystem":"github"}}]}`, false},
		{"source.instance", `{"schemaVersion":"external-ingest.v1","idempotencyKey":"c5","source":{"system":"github","instance":"a\ud800"},"records":[]}`, false},
	}
	for _, c := range cases {
		wantBatch, wantValidate := http.StatusInternalServerError, http.StatusInternalServerError
		if c.clean {
			wantBatch, wantValidate = http.StatusAccepted, http.StatusOK
		}
		r := post(deps.handleAcceptBatch(), "/api/v1/external-ingest/batches", c.body)
		if r.Code != wantBatch {
			t.Errorf("POST /batches %s: %d %s, want %d", c.name, r.Code, show(r.Body.Bytes()), wantBatch)
		}
		v := post(deps.handleValidate(), "/api/v1/external-ingest/validate", c.body)
		if v.Code != wantValidate {
			t.Errorf("POST /validate %s: %d %s, want %d", c.name, v.Code, show(v.Body.Bytes()), wantValidate)
		}
	}
	var batches, payloads int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM external_ingest_batches`).Scan(&batches)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM external_ingest_batch_payloads`).Scan(&payloads)
	if batches != 1 || payloads != 1 {
		t.Errorf("stored %d batch row(s) and %d payload row(s), want only the clean control's 1 and 1", batches, payloads)
	}
	lr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/external-ingest/batches", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	deps.handleListBatches()(lr, req)
	if lr.Code != http.StatusOK {
		t.Errorf("later read GET /batches: %d %s, want 200", lr.Code, show(lr.Body.Bytes()))
	}
}
