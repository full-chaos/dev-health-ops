//go:build integration

package apiservice

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The two credential headers, named apart from their values.
const (
	keyHeader       = "X-API-" + "Key"
	signatureHeader = "X-Signature-" + "256"
)

var ingestionIDPattern = regexp.MustCompile(`"ingestion_id":"[0-9a-f-]{36}"`)

func legacySign(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// idempotencyKeys reads every idem:* key of one plane's Valkey with its
// value and whether its TTL is the day the Python api sets.
func idempotencyKeys(t *testing.T, ctx context.Context, uri string) string {
	t.Helper()
	options, err := valkeygo.ParseURL(uri)
	if err != nil {
		t.Fatal(err)
	}
	client, err := valkeygo.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	keys, err := client.Do(ctx, client.B().Keys().Pattern("idem:*").Build()).AsStrSlice()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	var lines []string
	for _, key := range keys {
		value, err := client.Do(ctx, client.B().Get().Key(key).Build()).ToString()
		if err != nil {
			t.Fatal(err)
		}
		ttl, err := client.Do(ctx, client.B().Ttl().Key(key).Build()).AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, key+"="+value+" ttl-is-a-day="+map[bool]string{true: "yes", false: "no"}[ttl > 86300 && ttl <= 86400])
	}
	return strings.Join(lines, " | ")
}

// TestLegacyIngestVenueOracle is the differential proof CHAOS-6499 needs: the
// real Python api and the real Go api answer the identical signed requests,
// each with its own Valkey, and the entries each wrote to the ingest:*
// streams (stream key and the model's JSON, byte for byte) and the
// idempotency keys each set are compared. The order of the dependencies
// (decode, credentials, idempotency key, validation) is measured here on
// the live app: a request refused with 422 has already claimed its key.
func TestLegacyIngestVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := webhookintakeRepoRoot(t)
	const (
		apiKey = "venue-ingest-key"
		secret = "venue-ingest-secret"
		jwtKey = "venue-oracle-jwt-signing-key-32-bytes-min"
	)
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root, JWTKey: jwtKey,
		PythonEnv: []string{"INGEST_API_KEYS=other-key, " + apiKey, "INGEST_SIGNING_SECRET=" + secret},
		Seed: func(*testing.T, context.Context, *pgxpool.Pool, *venueoracle.Venue) map[string]map[string]any {
			return nil
		},
	})
	t.Setenv("INGEST_API_KEYS", "other-key, "+apiKey)
	t.Setenv("INGEST_SIGNING_SECRET", secret)

	client, err := valkey.Open(ctx, valkey.DefaultConfig(venue.ValkeyURI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	logger := quietLogger()
	server, err := NewServer(config.Config{APIAddress: "127.0.0.1:0"}, logger,
		Routes(Deps{Valkey: client}, logger))
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() { _ = server.Shutdown(ctx) })
	goBase := "http://" + server.Address()

	commit := `{"hash":"abc","message":"m","author_name":"a","author_email":"a@example.test","author_when":"2026-01-01T00:00:00+02:00","committer_when":1767225600.5,"extra":true}`
	deployment := `{"deployment_id":"d1","status":"success","environment":"prod","started_at":"2026-01-01T00:00:00Z","pull_request_number":"7","release_ref_confidence":1}`
	incident := `{"incident_id":"i1","status":"open","started_at":"2026-01-01T00:00:00Z"}`
	body := func(items string) string {
		return `{"org_id":"org-venue","repo_url":"https://example.test/r.git","items":[` + items + `]}`
	}
	// signed builds a request with both credentials, each overridable.
	signed := func(name, route, text string, headers map[string]string) venueoracle.Request {
		all := map[string]string{"Content-Type": "application/json"}
		all[keyHeader] = apiKey
		all[signatureHeader] = legacySign(secret, text)
		for key, value := range headers {
			if value == "" {
				delete(all, key)
			} else {
				all[key] = value
			}
		}
		return venueoracle.Request{Name: name, Method: "POST", Path: "/api/v1/ingest/" + route, Headers: all, Body: venueoracle.B64(text)}
	}
	unicodeBody := `{"org_id":"org-é😀","repo_url":"r","items":[{"incident_id":"é\u0000😀","status":"open","started_at":"2026-01-01T00:00:00Z"}]}`
	requests := []venueoracle.Request{
		signed("commits accepted", "commits", body(commit), nil),
		signed("deployments accepted", "deployments", body(deployment), nil),
		signed("incidents accepted", "incidents", body(incident), nil),
		signed("two items, defaults", "commits", body(commit+`,{"hash":"h2","message":"","author_name":"","author_email":"","author_when":"2026-01-01"}`), nil),
		signed("unicode org and text", "incidents", unicodeBody, nil),
		signed("no api key", "commits", body(commit), map[string]string{"X-API-Key": ""}),
		signed("wrong api key", "commits", body(commit), map[string]string{"X-API-Key": "nope"}),
		signed("second key of the list", "commits", body(commit), map[string]string{"X-API-Key": "other-key"}),
		signed("no signature", "commits", body(commit), map[string]string{"X-Signature-256": ""}),
		signed("wrong signature", "commits", body(commit), map[string]string{"X-Signature-256": legacySign("other", body(commit))}),
		signed("signature of another body", "commits", body(commit), map[string]string{"X-Signature-256": legacySign(secret, body(incident))}),
		signed("signature without prefix", "commits", body(commit), map[string]string{"X-Signature-256": strings.TrimPrefix(legacySign(secret, body(commit)), "sha256=")}),
		// The order of the dependencies.
		signed("bad json beats credentials", "commits", `{`, map[string]string{"X-API-Key": "nope"}),
		signed("empty body, credentials wrong", "commits", ``, map[string]string{"X-API-Key": "nope", "X-Signature-256": legacySign(secret, "")}),
		signed("validation error only after credentials", "commits", `{}`, map[string]string{"X-API-Key": "nope"}),
		signed("validation error", "commits", `{}`, nil),
		signed("shape errors", "deployments", `{"org_id":1,"repo_url":null,"items":[{"deployment_id":1},{"status":"x","environment":"e","deployment_id":"d","started_at":"nope","pull_request_number":"x","release_ref_confidence":"inf"}]}`, nil),
		signed("empty items", "incidents", body(``), nil),
		signed("integer past jiter and json.loads digit limits", "commits", `{"org_id":"o","repo_url":"r","items":[`+commit+`],"n":`+strings.Repeat("1", 4301)+`}`, nil),
		// Idempotency: a repeat is a 409 and is not streamed; an invalid request
		// has already claimed its key.
		signed("idempotency first", "commits", body(commit), map[string]string{"X-Idempotency-Key": "venue-key-1"}),
		signed("idempotency repeat", "commits", body(commit), map[string]string{"X-Idempotency-Key": "venue-key-1"}),
		signed("idempotency other route, same key", "incidents", body(incident), map[string]string{"X-Idempotency-Key": "venue-key-1"}),
		signed("idempotency claimed by an invalid request", "commits", `{}`, map[string]string{"X-Idempotency-Key": "venue-key-2"}),
		signed("idempotency after the invalid one", "commits", body(commit), map[string]string{"X-Idempotency-Key": "venue-key-2"}),
		signed("idempotency claimed by a refused credential", "commits", body(commit), map[string]string{"X-Idempotency-Key": "venue-key-3", "X-API-Key": "nope"}),
		signed("idempotency after the refused credential", "commits", body(commit), map[string]string{"X-Idempotency-Key": "venue-key-3"}),
	}
	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Normalize: func(_ venueoracle.Request, text string) string {
			return ingestionIDPattern.ReplaceAllString(text, `"ingestion_id":"<id>"`)
		},
	})
	t.Log(receipt)

	pythonStream := venueoracle.StreamEntries(t, ctx, venue.PythonValkeyURI, "ingest:*", "ingestion_id")
	goStream := venueoracle.StreamEntries(t, ctx, venue.ValkeyURI, "ingest:*", "ingestion_id")
	if pythonStream != goStream || pythonStream == "" {
		t.Errorf("ingest stream entries differ:\n python: %s\n go:     %s", pythonStream, goStream)
	}
	pythonKeys := idempotencyKeys(t, ctx, venue.PythonValkeyURI)
	goKeys := idempotencyKeys(t, ctx, venue.ValkeyURI)
	if pythonKeys != goKeys || pythonKeys == "" {
		t.Errorf("idempotency keys differ:\n python: %s\n go:     %s", pythonKeys, goKeys)
	}
}
