//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestPagerDutyDisconnectWithoutClientIDVenueOracle is the venue proof for a
// disconnect while PAGER_DUTY_CLIENT_ID is not configured: both planes
// answer alike (503 "pending retry" while a stored token could not be
// revoked, 200 when there was nothing to revoke) and leave the same
// credential rows, and the one named difference is asserted on each side:
// Python drops the token (0 revocation rows) where Go queues a durable
// disconnect revocation, which a later disconnect of the credential revokes
// once the api is configured.
func TestPagerDutyDisconnectWithoutClientIDVenueOracle(t *testing.T) {
	ctx := context.Background()
	golden := venueoracle.OpenGolden(t, pagerDutyGolden("disconnect_noclient", "TestPagerDutyDisconnectWithoutClientIDVenueOracle", "f3b9597394d040093e5a08c6174f8ad01669b80eccad673be8e0a22ce1f1d560"))
	root := golden.PythonRoot(t, repoRoot(t))
	const jwtKey = "venue-oracle-test-secret-key-for-pagerduty-disconnect-32-by"

	fake := &pagerDutyDisconnectFakeServer{failTokens: map[string]bool{}}
	fakeServer := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(fakeServer.Close)

	orgs := map[string]uuid.UUID{}
	for _, slug := range []string{"connected", "empty", "bad-cipher", "racer"} {
		orgs[slug] = uuid.MustParse(venueoracle.StableUUID("pd-noclient-org-" + slug))
	}
	adminID := uuid.MustParse(venueoracle.StableUUID("pd-noclient-admin"))

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		// The Python api has no PagerDuty client id: the case under test.
		PythonEnv: []string{
			"PAGER_DUTY_CLIENT_ID=",
			"SETTINGS_ENCRYPTION_KEY=" + pagerDutyDisconnectVenueEncryptionKey,
			"VENUE_PAGERDUTY_REVOKE_URL_OVERRIDE=" + fakeServer.URL,
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			for slug, id := range orgs {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'team', 'stripe', true, now(), now())`, id, "pd-noclient-"+slug)
			}
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'pd-noclient-admin@example.com', true, true, false, 0, now(), now())`, adminID)
			encrypt := func(plaintext string) string {
				results := v.CallPython(t, venueoracle.PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{plaintext}})
				var ciphertext string
				if err := json.Unmarshal(results[0], &ciphertext); err != nil {
					t.Fatalf("decode encrypt_value result: %v", err)
				}
				return ciphertext
			}
			credential := func(org uuid.UUID) {
				exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', 'default', true, NULL, '{"auth_mode":"oauth","region":"us","subdomain":"acme"}'::json, now(), now())`, uuid.New(), org)
			}
			credential(orgs["connected"])
			exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', 'default', $2, 1, now(), now(), false)`, orgs["connected"],
				encrypt(`{"access_token":"venue-pd-noclient-token","refresh_token":null,"expires_at":"2099-01-01T00:00:00Z","granted_scopes":[]}`))
			credential(orgs["racer"])
			exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', 'default', $2, 1, now(), now(), false)`, orgs["racer"],
				encrypt(`{"access_token":"venue-pd-noclient-racer-token","refresh_token":null,"expires_at":"2099-01-01T00:00:00Z","granted_scopes":[]}`))
			credential(orgs["bad-cipher"])
			exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', 'default', 'garbage-not-fernet', 1, now(), now(), false)`, orgs["bad-cipher"])

			adminClaims := func(org uuid.UUID) map[string]any {
				return map[string]any{"user_id": adminID.String(), "email": "pd-noclient-admin@example.com", "org_id": org.String(), "role": "admin"}
			}
			return map[string]map[string]any{
				"admin_connected":  adminClaims(orgs["connected"]),
				"admin_empty":      adminClaims(orgs["empty"]),
				"admin_bad_cipher": adminClaims(orgs["bad-cipher"]),
				"admin_racer":      adminClaims(orgs["racer"]),
			}
		},
	})

	const path = "/api/v1/admin/integrations/pagerduty/disconnect"
	disconnect := func(name, token string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "POST", Path: path,
			Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens[token], "Content-Type": "application/json"}, Body: venueoracle.B64(`{}`)}
	}
	requests := []venueoracle.Request{
		disconnect("W disconnect with a stored token and no client id (pending retry)", "admin_connected"),
		disconnect("W disconnect again once nothing is stored", "admin_connected"),
		disconnect("W disconnect with an undecryptable token and no client id", "admin_bad_cipher"),
		disconnect("W disconnect with no credential and no client id", "admin_empty"),
	}

	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(pagerDutyDisconnectVenueEncryptionKey), "")
	if err != nil {
		t.Fatalf("build decryptor: %v", err)
	}
	python := golden.Python(t, venue, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Decryptor = decryptor
		// No ClientID: the Go api is unconfigured too. The revoke URL is
		// set so a plane that tried to revoke anyway would reach the fake.
		deps.PagerDuty = providerfoundation.PagerDutyRevokeConfig{RevokeURL: fakeServer.URL}
	})
	t.Log(venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{Golden: golden}))

	rows := func(db, query string) string {
		t.Helper()
		return venueoracle.TableRows(t, ctx, venue.AdminURI(t, db), query)
	}
	for name, query := range map[string]string{
		"integration_credentials":   `SELECT org_id, provider, name, is_active, (credentials_encrypted IS NULL)::text FROM integration_credentials ORDER BY org_id, name`,
		"provider_oauth_credential": `SELECT org_id, provider, credential_name FROM provider_oauth_credentials ORDER BY org_id`,
	} {
		source := golden.Rows(t, "rows: "+name, func() string { return rows(venue.SourceDB, query) })
		if goRows := rows(venue.GoDB, query); source != goRows {
			t.Errorf("%s rows differ after the disconnects:\n python: %s\n go:     %s", name, source, goRows)
		}
	}

	// The named difference, asserted on each side.
	const revocations = `SELECT count(*) FROM provider_oauth_revocations WHERE org_id = '%s' AND provider = 'pagerduty' AND purpose = 'disconnect' AND status = 'pending' AND attempts = 0`
	connected := orgs["connected"].String()
	if got := golden.Rows(t, "python revocations of the connected org", func() string {
		return rows(venue.SourceDB, strings.Replace(revocations, "%s", connected, 1))
	}); got != "0" {
		t.Errorf("Python queued %s revocation rows for the token it dropped, want 0", got)
	}
	if got := rows(venue.GoDB, strings.Replace(revocations, "%s", connected, 1)); got != "1" {
		t.Fatalf("Go queued %s revocation rows for the token it could not revoke, want 1", got)
	}
	for _, org := range []string{"bad-cipher", "empty"} {
		if got := rows(venue.GoDB, `SELECT count(*) FROM provider_oauth_revocations WHERE org_id = '`+orgs[org].String()+`'`); got != "0" {
			t.Errorf("Go queued %s revocation rows for %s, which had no usable token", got, org)
		}
	}
	var sealed string
	goAdmin, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(goAdmin.Close)
	if err := goAdmin.QueryRow(ctx, `SELECT token_encrypted FROM provider_oauth_revocations WHERE org_id = $1 AND purpose = 'disconnect'`, orgs["connected"]).Scan(&sealed); err != nil {
		t.Fatal(err)
	}
	if plain, err := decryptor.Decrypt(secrets.NewValue(sealed)); err != nil || string(plain) != "venue-pd-noclient-token" {
		t.Errorf("the queued revocation does not hold the dropped token (decrypt err %v)", err)
	}
	if fake.count() != 0 {
		t.Errorf("a plane without a client id still called PagerDuty's revoke endpoint %d time(s)", fake.count())
	}

	// Once the api is configured, the credential's next disconnect drains it.
	configured, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Decryptor = decryptor
		deps.PagerDuty = providerfoundation.PagerDutyRevokeConfig{ClientID: pagerDutyDisconnectVenueClientID, RevokeURL: fakeServer.URL}
	})
	// The credential row is kept (inactive) by a disconnect: reactivate it,
	// so the next disconnect of the name reaches the retry.
	if _, err := goAdmin.Exec(ctx, `UPDATE integration_credentials SET is_active = true WHERE org_id = $1 AND provider = 'pagerduty' AND name = 'default'`, orgs["connected"]); err != nil {
		t.Fatal(err)
	}
	drain := venueoracle.Do(t, configured, requests[0])
	if drain.Status != http.StatusOK {
		t.Errorf("the configured disconnect answered %d %s, want 200", drain.Status, drain.Body)
	}
	fake.mu.Lock()
	calls := strings.Join(fake.calls, ",")
	fake.mu.Unlock()
	if calls != "venue-pd-noclient-token" {
		t.Errorf("the configured disconnect revoked %q, want exactly the queued token", calls)
	}
	if got := rows(venue.GoDB, `SELECT count(*) FROM provider_oauth_revocations WHERE org_id = '`+connected+`'`); got != "0" {
		t.Errorf("%s revocation rows remain after the queued token was revoked", got)
	}

	// Two disconnects of one credential overlapping in time queue ONE
	// revocation: the second waits on the token row's lock and then finds
	// nothing to revoke. A trigger holds the first one's delete open so the
	// overlap is certain.
	if _, err := goAdmin.Exec(ctx, `CREATE FUNCTION pd_noclient_slow_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2); RETURN OLD; END $$;
CREATE TRIGGER pd_noclient_slow_delete BEFORE DELETE ON provider_oauth_credentials FOR EACH ROW EXECUTE FUNCTION pd_noclient_slow_delete()`); err != nil {
		t.Fatal(err)
	}
	racers := make(chan venueoracle.Response, 2)
	go func() { racers <- venueoracle.Do(t, goBase, disconnect("racer one", "admin_racer")) }()
	time.Sleep(500 * time.Millisecond)
	go func() { racers <- venueoracle.Do(t, goBase, disconnect("racer two", "admin_racer")) }()
	statuses := []int{(<-racers).Status, (<-racers).Status}
	if _, err := goAdmin.Exec(ctx, `DROP TRIGGER pd_noclient_slow_delete ON provider_oauth_credentials; DROP FUNCTION pd_noclient_slow_delete()`); err != nil {
		t.Fatal(err)
	}
	if got := rows(venue.GoDB, `SELECT count(*) FROM provider_oauth_revocations WHERE org_id = '`+orgs["racer"].String()+`'`); got != "1" {
		t.Errorf("two overlapping disconnects of one credential (statuses %v) queued %s revocations, want 1", statuses, got)
	}
	if _, err := goAdmin.Exec(ctx, `UPDATE integration_credentials SET is_active = true WHERE org_id = $1 AND provider = 'pagerduty' AND name = 'default'`, orgs["racer"]); err != nil {
		t.Fatal(err)
	}
	if again := venueoracle.Do(t, configured, disconnect("racer configured", "admin_racer")); again.Status != http.StatusOK {
		t.Errorf("the configured disconnect answered %d %s, want 200", again.Status, again.Body)
	}
	fake.mu.Lock()
	revoked := strings.Count(strings.Join(fake.calls, ","), "venue-pd-noclient-racer-token")
	fake.mu.Unlock()
	if revoked != 1 {
		t.Errorf("the racing credential's token was sent to PagerDuty %d times, want once", revoked)
	}
	golden.Finish(t)
}
