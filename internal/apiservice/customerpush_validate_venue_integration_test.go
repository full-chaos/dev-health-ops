//go:build integration

package apiservice

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func customerPushValidateRequests(f customerPushWriteFixture, tokens map[string]string) []venueoracle.Request {
	bearer := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + tokens[name], "Content-Type": "application/json"}
	}
	var out []venueoracle.Request
	add := func(name, path, body string, headers map[string]string) {
		method := "POST"
		if strings.HasPrefix(name, "GET ") {
			method = "GET"
		}
		request := venueoracle.Request{Name: name, Method: method, Path: path, Headers: headers}
		if body != "\x00" {
			request.Body = venueoracle.B64(body)
		}
		out = append(out, request)
	}
	p := "/api/v1/admin/customer-push/sources/" + f.sourceGitHub.String() + "/validate"
	record := func(kind, payload string) string {
		return fmt.Sprintf(`{"kind": %q, "externalId": "x-%s", "payload": %s}`, kind, kind, payload)
	}
	envelope := func(version string, records ...string) string {
		return fmt.Sprintf(`{"schemaVersion": %q, "idempotencyKey": "k", "source": {"system": "github", "instance": "Acme/API"}, "records": [%s]}`,
			version, strings.Join(records, ", "))
	}
	good := record("repository.v1", `{"externalId": "acme/api", "sourceSystem": "github", "tags": ["a"], "settings": {"n": 1}}`)
	// Order: the admin check, the gate, the source scope, then the body.
	add("anon", p, envelope("external-ingest.v1", good), map[string]string{"Content-Type": "application/json"})
	add("member", p, envelope("external-ingest.v1", good), bearer("member"))
	add("community", p, `{`, bearer("community_owner"))
	add("disabled org", p, `{`, bearer("disabled_admin"))
	add("unknown source", "/api/v1/admin/customer-push/sources/"+f.sourceOther.String()+"/validate", `{`, bearer("admin"))
	add("bad source id", "/api/v1/admin/customer-push/sources/nope/validate", `{`, bearer("admin"))
	// Envelope failures: 200 rows.
	for name, body := range map[string]string{
		"no body": "\x00", "empty": ``, "eof in object": `{"a":1`, "trailing": `{} x`, "expected value": "\n\n  x",
		"trailing comma": `{"a":[1,]}`, "invalid escape": `{"a":"\q"}`, "bad utf-8": "{\"a\":\"\xff\"}", "array": `[]`,
		"object errors": `{"schemaVersion":1,"idempotencyKey":"","source":{"system":"x","instance":"","extra":1},"window":{"startedAt":"2026-01-02T00:00:00Z","endedAt":"2026-01-01T00:00:00Z"},"records":[1,{"kind":1,"externalId":"","payload":[],"q":1}],"zzz":1}`,
		"empty records": envelope("external-ingest.v1"),
		"field names":   `{"schema_version":"external-ingest.v1","idempotency_key":"k","source":{"system":"github","instance":"i","entity_family":"legacy"},"records":[{"kind":"repository.v1","external_id":"e","payload":{"externalId":"e","sourceSystem":"custom"}}]}`,
		"wrong version": envelope("it's \"v2\"", good),
		"too many":      envelope("external-ingest.v1", good, good, good, good),
		"valid":         envelope("external-ingest.v1", good),
		"record errors": envelope("external-ingest.v1", good, record("repository.v1", `{"tags": ["a", 1, "b", 2], "settings": {"a": [1]}, "extra": NaN}`),
			record("pull_request.v1", `{"repositoryExternalId": "r", "number": "0x1", "state": "merged", "createdAt": ".5", "additions": 1e300}`),
			record("nope.v1", `{}`), record("it's", `{}`)),
		"sixty errors": sixtyExtraKeys(),
		// jiter's integer-part limit (4300 chars, sign included): Python answers
		// a 200 row "number out of range" at the position 4301 chars in.
		"integer part at limit":         `{"extra":` + strings.Repeat("1", 4300) + `}`,
		"integer part over limit":       `{"extra":` + strings.Repeat("1", 4301) + `}`,
		"signed integer part over":      `{"extra":-` + strings.Repeat("1", 4300) + `}`,
		"float integer part over limit": `{"extra":` + strings.Repeat("1", 4301) + `.5}`,
	} {
		add("body: "+name, p, body, bearer("admin"))
	}
	add("naive vs aware window", p, `{"schemaVersion":"external-ingest.v1","idempotencyKey":"k","source":{"system":"github","instance":"i"},"window":{"startedAt":"2026-01-02T00:00:00","endedAt":"2026-01-01T00:00:00Z"},"records":[{"kind":"k","externalId":"e","payload":{}}]}`, bearer("admin"))
	add("too large", p, envelope("external-ingest.v1", good)+strings.Repeat(" ", 7000), bearer("admin"))
	add("form content type", p, envelope("external-ingest.v1", good), map[string]string{"Authorization": "Bearer " + tokens["admin"], "Content-Type": "text/plain"})
	add("GET validate", p, "\x00", bearer("admin"))
	return out
}

// sixtyExtraKeys is an object with 60 unknown keys: 60 extra_forbidden
// errors plus the missing fields, past the route's cap of 50 rows.
func sixtyExtraKeys() string {
	keys := make([]string, 60)
	for index := range keys {
		keys[index] = fmt.Sprintf(`"e%d": 1`, index)
	}
	return "{" + strings.Join(keys, ", ") + "}"
}

// TestVenueOracleCustomerPushValidate is the customer-push admin validate
// route's differential: status and body bytes and headers, on every
// envelope, record and limit outcome.
func TestVenueOracleCustomerPushValidate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	t.Setenv("EXTERNAL_INGEST_MAX_RECORDS", "3")
	t.Setenv("EXTERNAL_INGEST_MAX_BODY_BYTES", "6000")
	var seed customerPushWriteFixture
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: venueRoot(), JWTKey: venueKey, Logger: quietLogger(),
		PythonEnv: []string{"EXTERNAL_INGEST_MAX_RECORDS=3", "EXTERNAL_INGEST_MAX_BODY_BYTES=6000"},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			seed = customerPushWriteSeed(t, ctx, admin)
			return seed.tokenSpecs()
		},
	})
	cfg := config.Config{
		APIAddress: "127.0.0.1:0", RiverDatabaseSchema: "river", APIDatabaseRole: venue.Roles["api"],
		APIDatabaseURI: secrets.NewValue(venue.GoAPIDatabaseURI(t)),
		APIJWTSecret:   secrets.NewValue(venueKey), APIJWTIssuer: "dev-health-ops", APIJWTAudience: "dev-health-api",
		CORSAllowedOrigins: []string{"http://localhost:3000"},
	}
	base := startVenueAPI(t, ctx, cfg, venue)
	requests := customerPushValidateRequests(seed, venue.Tokens)
	receipt := venueoracle.Diff(t, base, requests, venue.ServePython(t, requests), venueoracle.DiffOptions{})
	if path := os.Getenv("DEV_HEALTH_VENUE_RECEIPT"); path != "" {
		_ = os.WriteFile(path, []byte(receipt), 0o600)
	}
	t.Log("\n" + receipt)
}
