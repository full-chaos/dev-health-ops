//go:build integration

package apiservice

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
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
	pullRequest := `{"number":"12","title":"t","body":null,"state":"merged","author_name":"a","created_at":"2026-01-01T00:00:00Z","merged_at":1767225600,"additions":3,"reviews":[{"review_id":"r1","reviewer":"u","state":"APPROVED","submitted_at":"2026-01-01T00:00:00.25Z"},{"review_id":"r2","reviewer":"v","state":"COMMENTED","submitted_at":"2026-01-01T01:00:00+05:30"}]}`
	workItem := `{"work_item_id":"jira:ABC-1","provider":"jira","title":"t","labels":["x","y"],"story_points":"2.5","created_at":"2026-01-01T00:00:00Z","url":"https://example.test/i"}`
	workItemsBody := func(items string) string { return `{"org_id":"org-venue","items":[` + items + `]}` }
	unicodeBody := `{"org_id":"org-é😀","repo_url":"r","items":[{"incident_id":"é\u0000😀","status":"open","started_at":"2026-01-01T00:00:00Z"}]}`
	requests := []venueoracle.Request{
		signed("commits accepted", "commits", body(commit), nil),
		signed("deployments accepted", "deployments", body(deployment), nil),
		signed("incidents accepted", "incidents", body(incident), nil),
		signed("two items, defaults", "commits", body(commit+`,{"hash":"h2","message":"","author_name":"","author_email":"","author_when":"2026-01-01"}`), nil),
		signed("unicode org and text", "incidents", unicodeBody, nil),
		signed("pull requests accepted", "pull-requests", body(pullRequest), nil),
		signed("pull request without reviews, defaults", "pull-requests", body(`{"number":1,"title":"t","state":"open","author_name":"a","created_at":"2026-01-01"}`), nil),
		signed("pull request shape errors", "pull-requests", body(`{"number":"x","reviews":[{"review_id":1},"y"],"created_at":"2026-13-01"}`), nil),
		signed("work items accepted", "work-items", workItemsBody(workItem), nil),
		signed("work item defaults", "work-items", workItemsBody(`{"work_item_id":"w","provider":"github","title":"t","created_at":"2026-01-01T00:00:00Z"}`), nil),
		signed("work item enum errors", "work-items", workItemsBody(`{"work_item_id":"w","provider":"bitbucket","title":"t","type":"Bug","status":"open","created_at":"2026-01-01T00:00:00Z"}`), nil),
		signed("work items need no repo_url", "work-items", `{"org_id":"o","items":[`+workItem+`]}`, nil),
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
		// CHAOS-6491: a 309-digit integer's nearest float64 is +Inf;
		// pydantic-core refuses it (float_type) rather than storing inf.
		signed("overflow integer in float field", "deployments",
			body(`{"deployment_id":"d2","status":"ok","environment":"prod","release_ref_confidence":`+strings.Repeat("9", 309)+`}`), nil),
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

// TestLegacyIngestTelemetryVenueOracle is the telemetry route's differential:
// the real Python api writes its signal buckets to its ClickHouse database,
// the real Go api to its own, through the api's ClickHouse login (whose
// posture now grants INSERT on the table), and the rows each stored are
// compared column by column (ingested_at excluded: each plane mints its
// own). A value the column cannot hold is Python's unhandled 500 and stores
// nothing, on both planes.
func TestLegacyIngestTelemetryVenueOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	const (
		apiKey = "venue-ingest-key"
		secret = "venue-ingest-secret"
	)
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(),
		PythonEnv: []string{"INGEST_API_KEYS=" + apiKey, "INGEST_SIGNING_SECRET=" + secret},
		Seed: func(*testing.T, context.Context, *pgxpool.Pool, *venueoracle.Venue) map[string]map[string]any {
			return nil
		},
	})
	t.Setenv("INGEST_API_KEYS", apiKey)
	t.Setenv("INGEST_SIGNING_SECRET", secret)
	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI:   secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIClickHouseURI: secrets.NewValue(venue.GoAPIClickHouseURI(t)),
		APIJWTSecret:     secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins: []string{"http://localhost:3000"},
		ValkeyURI:          secrets.NewValue(venue.ValkeyURI),
	}
	base := startVenueAPI(t, ctx, cfg, venue)

	// The table's TTL drops a bucket 90 days after its start, so every
	// timestamp is recent (fixed strings in the requests, so both planes
	// see the same ones).
	now := time.Now().UTC().Truncate(time.Second)
	recent := func(offset time.Duration, layout string) string { return `"` + now.Add(offset).Format(layout) + `"` }
	item := func(overrides ...string) string {
		fields := map[string]string{
			"signal_type": `"friction.rage_click"`, "signal_count": `4`, "session_count": `2`, "unique_pseudonymous_count": `3`,
			"endpoint_group": `"/api/x"`, "environment": `"prod"`, "repo_id": `"3F2504E0-4F89-11D3-9A0C-0305E82C3301"`, "release_ref": `"v1.2"`,
			"bucket_start": recent(-2*time.Hour, time.RFC3339), "bucket_end": recent(-time.Hour, "2006-01-02T15:04:05.123456Z"), "is_sampled": `true`,
			"schema_version": `"1.0"`, "dedupe_key": `"d-1"`,
		}
		for index := 0; index+1 < len(overrides); index += 2 {
			fields[overrides[index]] = overrides[index+1]
		}
		order := []string{"signal_type", "signal_count", "session_count", "unique_pseudonymous_count", "endpoint_group", "environment", "repo_id",
			"release_ref", "bucket_start", "bucket_end", "is_sampled", "schema_version", "dedupe_key"}
		var parts []string
		for _, name := range order {
			if value := fields[name]; value != "-" {
				parts = append(parts, `"`+name+`":`+value)
			}
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	batch := func(org string, items ...string) string {
		return `{"org_id":` + org + `,"items":[` + strings.Join(items, ",") + `]}`
	}
	request := func(name, text string, headers map[string]string) venueoracle.Request {
		all := map[string]string{"Content-Type": "application/json"}
		all[keyHeader] = apiKey
		all[signatureHeader] = legacySign(secret, text)
		for key, value := range headers {
			all[key] = value
		}
		return venueoracle.Request{Name: name, Method: "POST", Path: "/api/v1/ingest/telemetry", Headers: all, Body: venueoracle.B64(text)}
	}
	requests := []venueoracle.Request{
		request("accepted", batch(`"org-t1"`, item()), nil),
		request("two items, defaults and nulls", batch(`"org-t1"`, item("dedupe_key", `"d-2"`, "unique_pseudonymous_count", `null`, "endpoint_group", `-`, "repo_id", `""`),
			item("dedupe_key", `"d-3"`, "endpoint_group", `-`, "release_ref", `-`, "is_sampled", `-`, "schema_version", `-`, "repo_id", `null`, "unique_pseudonymous_count", `-`)), nil),
		request("milliseconds truncated, naive and aware times", batch(`"org-t2"`, item("dedupe_key", `"d-4"`, "bucket_start", recent(-3*time.Hour, "2006-01-02T15:04:05.987654"), "bucket_end", recent(-time.Hour, "2006-01-02T15:04:05.123999+00:00"))), nil),
		request("timestamps as numbers", batch(`"org-t2"`, item("dedupe_key", `"d-5"`, "bucket_start", fmt.Sprint(now.Add(-time.Hour).Unix()), "bucket_end", fmt.Sprint(now.Add(-time.Minute).Unix())+".5")), nil),
		request("lax ints and bool", batch(`"org-t3"`, item("dedupe_key", `"d-6"`, "signal_count", `"7"`, "session_count", `5.0`, "unique_pseudonymous_count", `true`, "is_sampled", `"yes"`)), nil),
		request("repo id forms", batch(`"org-t3"`, item("dedupe_key", `"d-7"`, "repo_id", `"{3f2504e0-4f89-11d3-9a0c-0305e82c3301}"`), item("dedupe_key", `"d-8"`, "repo_id", `0`)), nil),
		request("bad repo id", batch(`"org-t3"`, item("dedupe_key", `"d-9"`, "repo_id", `"x"`)), nil),
		request("non-string repo id", batch(`"org-t3"`, item("dedupe_key", `"d-10"`, "repo_id", `[1]`)), nil),
		request("unicode text", batch(`"org-é😀"`, item("dedupe_key", `"d-é"`, "signal_type", `"é😀"`)), nil),
		// A value the column cannot hold: the unhandled 500, nothing stored (one
		// insert for the whole request, so the valid item beside it is lost too).
		request("negative count", batch(`"org-t4"`, item("dedupe_key", `"d-11"`), item("dedupe_key", `"d-12"`, "signal_count", `-1`)), nil),
		request("count past UInt64", batch(`"org-t4"`, item("dedupe_key", `"d-13"`, "session_count", `18446744073709551616`)), nil),
		request("count at UInt64 max", batch(`"org-t5"`, item("dedupe_key", `"d-14"`, "signal_count", `18446744073709551615`)), nil),
		request("negative unique count", batch(`"org-t4"`, item("dedupe_key", `"d-15"`, "unique_pseudonymous_count", `-5`)), nil),
		request("lone surrogate text", batch(`"org-t4"`, item("dedupe_key", `"d-16"`, "signal_type", `"\ud800"`)), nil),
		request("no items", batch(`"org-t6"`), nil),
		request("bad credentials", batch(`"org-t6"`, item()), map[string]string{keyHeader: "nope"}),
	}
	python := venue.ServePython(t, requests)
	receipt := venueoracle.Diff(t, base, requests, python, venueoracle.DiffOptions{
		Normalize: func(_ venueoracle.Request, text string) string {
			return ingestionIDPattern.ReplaceAllString(text, `"ingestion_id":"<id>"`)
		},
	})
	t.Log(receipt)
	var out strings.Builder
	compareCHRows(t, ctx, venue, &out, "telemetry_signal_bucket",
		`SELECT org_id, signal_type, signal_count, session_count, ifNull(toString(unique_pseudonymous_count), '<null>'), endpoint_group, environment, repo_id, release_ref,
			toString(bucket_start), toString(bucket_end), is_sampled, schema_version, dedupe_key FROM telemetry_signal_bucket ORDER BY org_id, dedupe_key`)
	t.Log(out.String())
}
