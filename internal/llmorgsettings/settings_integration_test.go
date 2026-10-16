//go:build integration

package llmorgsettings

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// newTestStore boots a real Postgres (containers.StartPostgres -- same
// harness internal/workerservice's entitlement integration tests use) with
// the settings/feature_flags/org_feature_overrides/org_licenses/
// organizations tables this package reads. One instance per test keeps
// each precedence-matrix case's fixtures independent.
func newTestStore(t *testing.T) (Store, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	// The migrated schema: the hand-written settings/organizations/feature_flags/
	// org_feature_overrides/org_licenses tables lacked the real tables' NOT NULL columns and keys
	// (CHAOS-6769 ledger).
	pgschema.Apply(ctx, t, pool)

	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("test-master-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	fixedNow := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	return Store{Pool: pool, Decryptor: decryptor, Now: func() time.Time { return fixedNow }}, pool
}

func seedByoLLMFeature(ctx context.Context, t *testing.T, pool *pgxpool.Pool, minTier string, globallyEnabled bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	// The migrations already register byo_llm; each test chooses its own floor and enabled state.
	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key = 'byo_llm'`); err != nil {
		t.Fatal(err)
	}
	pgseed.FeatureFlag(ctx, t, pool, id.String(), "byo_llm", minTier, globallyEnabled)
	return id
}

func seedOrg(ctx context.Context, t *testing.T, pool *pgxpool.Pool, tier string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	pgseed.Org(ctx, t, pool, id.String(), tier)
	return id
}

func insertSetting(ctx context.Context, t *testing.T, pool *pgxpool.Pool, orgID uuid.UUID, key, value string) {
	t.Helper()
	pgseed.Setting(ctx, t, pool, orgID.String(), "llm", key, value, false)
}

func insertEncryptedSetting(ctx context.Context, t *testing.T, store Store, pool *pgxpool.Pool, orgID uuid.UUID, key, plaintext string) {
	t.Helper()
	ciphertext, err := store.Decryptor.Encrypt([]byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	pgseed.Setting(ctx, t, pool, orgID.String(), "llm", key, ciphertext.Reveal(), true)
}

// TestResolveUsableProvider_PrecedenceMatrix is the bigboy proof team-lead
// asked for: seeds the same rows an equivalent Python fixture would and
// checks ResolveUsableProvider's answer at every point in the precedence
// matrix (flag off / override on / override off / tier below floor / tier
// at floor / no license), plus the encrypted-column and unknown-org paths
// this package's unit tests cannot reach without a real Postgres.
func TestResolveUsableProvider_PrecedenceMatrix(t *testing.T) {
	ctx := context.Background()

	t.Run("no settings at all -> not usable, no flag lookup needed", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\"", got)
		}
	})

	t.Run("enterprise org, flag globally enabled, no override -> usable (enabled by tier)", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
		insertSetting(ctx, t, pool, orgID, "base_url", "https://my-gateway.example.com/v1")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "openai" {
			t.Fatalf("got %q, want %q", got, "openai")
		}
	})

	t.Run("flag globally disabled -> not usable regardless of tier", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", false)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\" (flag globally disabled)", got)
		}
	})

	t.Run("community tier below the TEAM floor -> not usable even with a positive org override", func(t *testing.T) {
		store, pool := newTestStore(t)
		featureID := seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "community")
		pgseed.OrgOverride(ctx, t, pool, orgID.String(), featureID.String(), true)
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\" -- the TEAM floor must not be bypassed by an org override", got)
		}
	})

	t.Run("team tier at the floor, org override disabled -> not usable", func(t *testing.T) {
		store, pool := newTestStore(t)
		featureID := seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "team")
		pgseed.OrgOverride(ctx, t, pool, orgID.String(), featureID.String(), false)
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\" (org override disabled)", got)
		}
	})

	t.Run("no org_licenses row -> falls back to organizations.tier", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "team")
		insertSetting(ctx, t, pool, orgID, "provider", "ollama")
		insertSetting(ctx, t, pool, orgID, "base_url", "https://my-gateway.example.com/v1")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "ollama" {
			t.Fatalf("got %q, want %q", got, "ollama")
		}
	})

	t.Run("org_licenses.tier wins over organizations.tier", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		pgseed.OrgLicense(ctx, t, pool, orgID.String(), "community", `{}`)
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\" -- org_licenses.tier (community) must win over organizations.tier (enterprise)", got)
		}
	})

	t.Run("no feature_flags row at all -> unregistered, backward-compatible ungated", func(t *testing.T) {
		store, pool := newTestStore(t)
		// Deliberately no seedByoLLMFeature call.
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "openai" {
			t.Fatalf("got %q, want %q (unregistered feature treated as ungated)", got, "openai")
		}
	})

	t.Run("encrypted api_key column decrypts and is usable", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertEncryptedSetting(ctx, t, store, pool, orgID, "api_key", "sk-org-secret")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "openai" {
			t.Fatalf("got %q, want %q", got, "openai")
		}
	})

	t.Run("SSRF-unsafe base_url falls back, not usable", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "ollama")
		insertSetting(ctx, t, pool, orgID, "base_url", "http://169.254.169.254/latest")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\" (SSRF-unsafe base_url)", got)
		}
	})

	t.Run("malformed IPv6 bracket in base_url is an error, as Python raises", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "ollama")
		insertSetting(ctx, t, pool, orgID, "base_url", "https://[::1/v1")
		if got, err := store.ResolveUsableProvider(ctx, orgID.String()); err == nil {
			t.Fatalf("ResolveUsableProvider = %q, nil; want the urlsplit ValueError", got)
		}
		if _, ok, err := store.Credentials(ctx, orgID.String(), "ollama"); err == nil || ok {
			t.Fatalf("Credentials ok=%v err=%v; want the urlsplit ValueError", ok, err)
		}
	})

	t.Run("incomplete credentials (api-key-required provider, no key) not usable", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "base_url", "https://my-gateway.example.com/v1")
		got, err := store.ResolveUsableProvider(ctx, orgID.String())
		if err != nil {
			t.Fatal(err)
		}
		if got != "" {
			t.Fatalf("got %q, want \"\" (openai requires an api_key)", got)
		}
	})

	t.Run("non-UUID org_id with settings fails closed (error, not silent fallback)", func(t *testing.T) {
		store, pool := newTestStore(t)
		seedByoLLMFeature(ctx, t, pool, "team", true)
		insertSetting(ctx, t, pool, uuid.Nil, "provider", "openai")
		if _, err := pool.Exec(ctx,
			`UPDATE settings SET org_id = 'not-a-uuid' WHERE org_id = $1`, uuid.Nil.String()); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ResolveUsableProvider(ctx, "not-a-uuid"); err == nil {
			t.Fatal("expected an error for a BYO-configured org with a non-UUID org_id, got nil")
		}
	})
}

// TestCredentials_SourceBound proves Credentials()/Model() stay source-
// bound (CHAOS-2550): a request for a DIFFERENT provider than the org
// configured gets nothing, even though the org has usable BYO settings.
func TestCredentials_SourceBound(t *testing.T) {
	ctx := context.Background()
	store, pool := newTestStore(t)
	seedByoLLMFeature(ctx, t, pool, "team", true)
	orgID := seedOrg(ctx, t, pool, "enterprise")
	insertSetting(ctx, t, pool, orgID, "provider", "openai")
	insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")
	insertSetting(ctx, t, pool, orgID, "model", "gpt-5-mini")

	creds, ok, err := store.Credentials(ctx, orgID.String(), "anthropic")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("expected not-ok for a mismatched provider, got %+v", creds)
	}

	creds, ok, err = store.Credentials(ctx, orgID.String(), "openai")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || creds.APIKey.Reveal() != "sk-org" {
		t.Fatalf("expected the org's own api_key for a matching provider, got ok=%v creds=%+v", ok, creds)
	}

	model, err := store.Model(ctx, orgID.String(), "openai")
	if err != nil {
		t.Fatal(err)
	}
	if model != "gpt-5-mini" {
		t.Fatalf("got model %q, want %q", model, "gpt-5-mini")
	}
}

// TestCredentials_WhitespaceOnlyRequestedProviderNeverMatches is the
// codex round 2 (#2234), P2 regression: `?llm_provider=+` decodes to a
// single whitespace character -- categorize.normalizeProviderKind
// correctly treats this as an EXPLICIT, non-"auto" request that
// collapses to an empty kind (never matching a real provider name), but
// this package's own Credentials/Matches/Model/RawBaseURL used to run
// that same empty string back through normalizeProvider (which
// substitutes empty to "auto"), turning an explicitly-invalid request
// into a wildcard that matched ANY org-configured provider -- letting a
// malformed request pass the pre-stream availability check and fail
// LATE, mid-response, instead of cleanly rejecting up front. A genuinely
// empty string (never sent by ResolveProviderKindForOrg for a real
// request, but checked here for completeness) must ALSO not match --
// the fix removes the substitution entirely for the requested side, not
// just for whitespace specifically.
func TestCredentials_WhitespaceOnlyRequestedProviderNeverMatches(t *testing.T) {
	ctx := context.Background()
	store, pool := newTestStore(t)
	seedByoLLMFeature(ctx, t, pool, "team", true)
	orgID := seedOrg(ctx, t, pool, "enterprise")
	insertSetting(ctx, t, pool, orgID, "provider", "openai")
	insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")

	for _, requested := range []string{"", " ", "   "} {
		t.Run("requested="+requested, func(t *testing.T) {
			creds, ok, err := store.Credentials(ctx, orgID.String(), requested)
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				t.Fatalf("expected NOT ok for an empty/whitespace-only requested provider (must never wildcard-match), got %+v", creds)
			}
		})
	}
}

// TestResolveUsableProvider_NoFeatureFlagsTable is the codex round 1 P2
// regression: a pre-migration/minimal DB where the feature_flags TABLE
// itself does not exist (not just an absent row for this org) must
// resolve as "unregistered" -- backward-compatible, ungated -- matching
// feature_flag_state's own `if not has_table("feature_flags"): return
// "unregistered"` early return, which skips the TEAM-tier floor check
// entirely. Proven with a COMMUNITY-tier org specifically: if the table-
// missing case were (incorrectly) treated as a fatal error, or as a
// floor-check failure instead of an early "unregistered" return, this
// case would come back "" (not usable) instead of the org's own BYO
// setting.
func TestResolveUsableProvider_NoFeatureFlagsTable(t *testing.T) {
	ctx := context.Background()
	store, pool := newTestStore(t)
	if _, err := pool.Exec(ctx, `DROP TABLE feature_flags CASCADE`); err != nil {
		t.Fatal(err)
	}
	orgID := seedOrg(ctx, t, pool, "community")
	insertSetting(ctx, t, pool, orgID, "provider", "openai")
	insertSetting(ctx, t, pool, orgID, "api_key", "sk-org")

	got, err := store.ResolveUsableProvider(ctx, orgID.String())
	if err != nil {
		t.Fatal(err)
	}
	if got != "openai" {
		t.Fatalf("got %q, want %q (missing feature_flags table -> unregistered -> ungated, "+
			"even for a community-tier org)", got, "openai")
	}
}

// TestLoadRawSettings_WrongEncryptionKeyEmitsTelemetryNotSilence pins the
// behavior for a present-but-INCORRECT SETTINGS_ENCRYPTION_KEY: an
// encrypted row whose Decrypt() call fails is skipped exactly like a
// corrupt row (Python parity, `except ValueError: continue`, unchanged),
// so the org still resolves as if the setting were unset -- but the
// failure is not silent. recordDecryptFailure fires for the row, and
// checkKeyOnFirstDecrypt's once-per-process key-validation signal also
// fires, because this is the first (and only) decrypt attempt in the
// process.
func TestLoadRawSettings_WrongEncryptionKeyEmitsTelemetryNotSilence(t *testing.T) {
	ctx := context.Background()
	resetFirstDecryptAttemptForTest()

	rightStore, pool := newTestStore(t)
	seedByoLLMFeature(ctx, t, pool, "team", true)
	orgID := seedOrg(ctx, t, pool, "enterprise")
	insertSetting(ctx, t, pool, orgID, "provider", "openai")
	insertEncryptedSetting(ctx, t, rightStore, pool, orgID, "api_key", "sk-real-secret")

	wrongDecryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("totally-wrong-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	wrongStore := Store{Pool: pool, Decryptor: wrongDecryptor, Now: rightStore.Now}

	var decryptFailures []string
	var keyValidationFailures []string
	previousRecordDecryptFailure := recordDecryptFailure
	previousCheckKeyOnFirstDecrypt := checkKeyOnFirstDecrypt
	recordDecryptFailure = func(_ context.Context, orgID string) {
		decryptFailures = append(decryptFailures, orgID)
	}
	checkKeyOnFirstDecrypt = func(_ context.Context, orgID string, decryptErr error) {
		if decryptErr != nil {
			keyValidationFailures = append(keyValidationFailures, orgID)
		}
	}
	t.Cleanup(func() {
		recordDecryptFailure = previousRecordDecryptFailure
		checkKeyOnFirstDecrypt = previousCheckKeyOnFirstDecrypt
	})

	got, err := wrongStore.ResolveUsableProvider(ctx, orgID.String())
	if err != nil {
		t.Fatal(err)
	}
	if got != "" {
		t.Fatalf("got %q, want \"\" -- api_key must be unreadable with the wrong key, "+
			"so openai (api-key-required) is not usable", got)
	}

	if len(decryptFailures) != 1 || decryptFailures[0] != orgID.String() {
		t.Fatalf("recordDecryptFailure calls = %+v, want exactly [%s] -- "+
			"the wrong-key decrypt failure must be observable, not silent", decryptFailures, orgID.String())
	}
	if len(keyValidationFailures) != 1 || keyValidationFailures[0] != orgID.String() {
		t.Fatalf("checkKeyOnFirstDecrypt failing calls = %+v, want exactly [%s] -- "+
			"the FIRST decrypt attempt failing must flag the key itself", keyValidationFailures, orgID.String())
	}
}

// OrgConfig serves the decrypted stored truth and a version that moves when a row is written.
func TestOrgConfig_ServesDecryptedKeyAndMovingVersion(t *testing.T) {
	ctx := context.Background()
	store, pool := newTestStore(t)
	seedByoLLMFeature(ctx, t, pool, "team", true)
	orgID := seedOrg(ctx, t, pool, "enterprise")

	if _, ok, err := store.OrgConfig(ctx, orgID.String()); err != nil || ok {
		t.Fatalf("org with no settings: ok=%v err=%v, want ok=false", ok, err)
	}

	insertSetting(ctx, t, pool, orgID, "provider", "openai")
	insertSetting(ctx, t, pool, orgID, "model", "gpt-5-mini")
	insertSetting(ctx, t, pool, orgID, "base_url", "https://api.openai.com/v1")
	insertEncryptedSetting(ctx, t, store, pool, orgID, "api_key", "sk-org-secret")

	first, ok, err := store.OrgConfig(ctx, orgID.String())
	if err != nil || !ok {
		t.Fatalf("OrgConfig ok=%v err=%v", ok, err)
	}
	if first.Provider != "openai" || first.Model != "gpt-5-mini" || first.BaseURL != "https://api.openai.com/v1" {
		t.Fatalf("config = %+v", first)
	}
	if first.APIKey.Reveal() != "sk-org-secret" {
		t.Fatal("api_key was not decrypted to the stored plaintext")
	}
	if first.Version == "" {
		t.Fatal("version is empty")
	}

	if _, err := pool.Exec(ctx, `UPDATE settings SET value = 'gpt-5', updated_at = updated_at + interval '1 second'
		WHERE org_id = $1 AND category = 'llm' AND key = 'model'`, orgID.String()); err != nil {
		t.Fatal(err)
	}
	second, _, err := store.OrgConfig(ctx, orgID.String())
	if err != nil {
		t.Fatal(err)
	}
	if second.Version == first.Version {
		t.Fatalf("version did not move after a row write: %q", second.Version)
	}
}

// A closed byo_llm gate reads as no config, never as the stored key.
func TestOrgConfig_ClosedGateServesNothing(t *testing.T) {
	ctx := context.Background()
	store, pool := newTestStore(t)
	seedByoLLMFeature(ctx, t, pool, "team", false)
	orgID := seedOrg(ctx, t, pool, "free")
	insertSetting(ctx, t, pool, orgID, "provider", "openai")
	insertEncryptedSetting(ctx, t, store, pool, orgID, "api_key", "sk-org-secret")

	cfg, ok, err := store.OrgConfig(ctx, orgID.String())
	if err != nil || ok {
		t.Fatalf("closed gate: ok=%v err=%v cfg=%+v, want ok=false", ok, err, cfg)
	}
}

// A stored setting that cannot be decrypted must fail OrgConfig whenever the gate is open, whatever else
// is or is not readable beside it; a closed gate and an empty org stay "not configured".
func TestOrgConfig_UnreadableStoredSettingMatrix(t *testing.T) {
	ctx := context.Background()
	wrongDecryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("totally-wrong-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name       string
		gateOpen   bool
		plainRows  map[string]string
		cipherRows map[string]string
		wantOK     bool
		wantErr    bool
	}{
		{"only an unreadable api_key, gate open", true, nil, map[string]string{"api_key": "sk-real-secret"}, false, true},
		{"unreadable api_key beside a readable provider", true, map[string]string{"provider": "openai"}, map[string]string{"api_key": "sk-real-secret"}, false, true},
		{"unreadable api_key, gate closed", false, nil, map[string]string{"api_key": "sk-real-secret"}, false, false},
		{"no rows at all", true, nil, nil, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rightStore, pool := newTestStore(t)
			seedByoLLMFeature(ctx, t, pool, "team", tc.gateOpen)
			orgID := seedOrg(ctx, t, pool, "enterprise")
			for k, v := range tc.plainRows {
				insertSetting(ctx, t, pool, orgID, k, v)
			}
			for k, v := range tc.cipherRows {
				insertEncryptedSetting(ctx, t, rightStore, pool, orgID, k, v)
			}
			wrongStore := Store{Pool: pool, Decryptor: wrongDecryptor, Now: rightStore.Now}
			cfg, ok, err := wrongStore.OrgConfig(ctx, orgID.String())
			if ok != tc.wantOK || (err != nil) != tc.wantErr {
				t.Fatalf("ok=%v err=%v cfg=%+v, want ok=%v err=%v", ok, err, cfg, tc.wantOK, tc.wantErr)
			}
			if tc.wantErr {
				if !errors.Is(err, ErrStoredSettingUnreadable) {
					t.Fatalf("err = %v, want ErrStoredSettingUnreadable", err)
				}
				if strings.Contains(err.Error(), "sk-real-secret") {
					t.Fatal("error text carries the key")
				}
			}
		})
	}
}

// Settings and version come from one snapshot: a save landing between the row read and the version read
// must not pair the old values with the new version.
func TestOrgConfig_ValuesAndVersionShareOneSnapshot(t *testing.T) {
	ctx := context.Background()
	store, pool := newTestStore(t)
	seedByoLLMFeature(ctx, t, pool, "team", true)
	orgID := seedOrg(ctx, t, pool, "enterprise")
	insertSetting(ctx, t, pool, orgID, "provider", "openai")
	insertSetting(ctx, t, pool, orgID, "model", "model-before")
	before, ok, err := store.OrgConfig(ctx, orgID.String())
	if err != nil || !ok {
		t.Fatalf("baseline ok=%v err=%v", ok, err)
	}

	afterRowsRead = func() {
		if _, err := pool.Exec(ctx, `UPDATE settings SET value = 'model-after', updated_at = updated_at + interval '1 minute'
			WHERE org_id = $1 AND category = 'llm' AND key = 'model'`, orgID.String()); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { afterRowsRead = func() {} })
	interleaved, ok, err := store.OrgConfig(ctx, orgID.String())
	if err != nil || !ok {
		t.Fatalf("interleaved ok=%v err=%v", ok, err)
	}
	afterRowsRead = func() {}
	next, _, err := store.OrgConfig(ctx, orgID.String())
	if err != nil {
		t.Fatal(err)
	}
	if next.Model != "model-after" || next.Version == before.Version {
		t.Fatalf("after the save: model=%q version=%q (before %q)", next.Model, next.Version, before.Version)
	}
	if interleaved.Model != "model-before" || interleaved.Version != before.Version {
		t.Fatalf("interleaved read mixed snapshots: model=%q version=%q, want model-before at version %q", interleaved.Model, interleaved.Version, before.Version)
	}
}

// Settings of the llm category that are not BYO fields neither make an org "configured" nor move the version.
func TestOrgConfig_OnlyByoKeysCountAsConfig(t *testing.T) {
	ctx := context.Background()
	store, pool := newTestStore(t)
	seedByoLLMFeature(ctx, t, pool, "team", true)
	orgID := seedOrg(ctx, t, pool, "enterprise")
	insertSetting(ctx, t, pool, orgID, "platform_ask_dev_role_certification_profile:legacy_agent", "{}")

	if cfg, ok, err := store.OrgConfig(ctx, orgID.String()); err != nil || ok {
		t.Fatalf("only a platform setting: ok=%v err=%v cfg=%+v, want ok=false", ok, err, cfg)
	}

	insertSetting(ctx, t, pool, orgID, "provider", "openai")
	first, ok, err := store.OrgConfig(ctx, orgID.String())
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE settings SET updated_at = updated_at + interval '1 hour'
		WHERE org_id = $1 AND category = 'llm' AND key LIKE 'platform_%'`, orgID.String()); err != nil {
		t.Fatal(err)
	}
	second, _, err := store.OrgConfig(ctx, orgID.String())
	if err != nil {
		t.Fatal(err)
	}
	if second.Version != first.Version {
		t.Fatalf("a non-BYO setting moved the version: %q -> %q", first.Version, second.Version)
	}
}

// TestOrgConfig_InputDomainMatrix is the closing guard for OrgConfig: row kind x readability x gate x
// a concurrent save between the row read and the version read. Every cell asserts ok, error class,
// the served provider/model/key, and the version/value pairing.
func TestOrgConfig_InputDomainMatrix(t *testing.T) {
	ctx := context.Background()
	rightStore, pool := newTestStore(t)
	seedByoLLMFeature(ctx, t, pool, "team", true)
	wrongDecryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("totally-wrong-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	wrongStore := Store{Pool: pool, Decryptor: wrongDecryptor, Now: rightStore.Now}

	type rowKind string
	const (
		none     rowKind = "no rows"
		nonBYO   rowKind = "only a non-BYO llm row"
		byoPlain rowKind = "BYO plain rows"
		byoCiph  rowKind = "BYO model + encrypted api_key"
		byoMixed rowKind = "BYO provider + model + encrypted api_key"
		mockProv rowKind = "BYO provider mock"
		noneProv rowKind = "BYO provider none"
		blankKey rowKind = "BYO model only (no provider)"
	)
	seed := func(kind rowKind, orgID uuid.UUID) {
		switch kind {
		case nonBYO:
			insertSetting(ctx, t, pool, orgID, "platform_ask_dev_role_certification_profile:legacy_agent", "{}")
		case byoPlain:
			insertSetting(ctx, t, pool, orgID, "provider", "  OpenAI ")
			insertSetting(ctx, t, pool, orgID, "model", "m-before")
		case byoCiph:
			insertSetting(ctx, t, pool, orgID, "model", "m-before")
			insertEncryptedSetting(ctx, t, rightStore, pool, orgID, "api_key", "sk-secret")
		case byoMixed:
			insertSetting(ctx, t, pool, orgID, "provider", "openai")
			insertSetting(ctx, t, pool, orgID, "model", "m-before")
			insertEncryptedSetting(ctx, t, rightStore, pool, orgID, "api_key", "sk-secret")
		case mockProv:
			insertSetting(ctx, t, pool, orgID, "provider", "mock")
			insertSetting(ctx, t, pool, orgID, "model", "m-before")
		case noneProv:
			insertSetting(ctx, t, pool, orgID, "provider", "none")
			insertSetting(ctx, t, pool, orgID, "model", "m-before")
		case blankKey:
			insertSetting(ctx, t, pool, orgID, "model", "m-before")
		}
	}
	hasCipher := func(k rowKind) bool { return k == byoCiph || k == byoMixed }
	hasModel := func(k rowKind) bool {
		return k == byoPlain || k == byoCiph || k == byoMixed || k == mockProv || k == noneProv || k == blankKey
	}

	for _, kind := range []rowKind{none, nonBYO, byoPlain, byoCiph, byoMixed, mockProv, noneProv, blankKey} {
		for _, gateOpen := range []bool{true, false} {
			for _, rightKey := range []bool{true, false} {
				for _, save := range []bool{false, true} {
					name := fmt.Sprintf("%s/gate-open=%v/right-key=%v/save-between=%v", kind, gateOpen, rightKey, save)
					t.Run(name, func(t *testing.T) {
						tier := "enterprise"
						if !gateOpen {
							tier = "free"
						}
						orgID := seedOrg(ctx, t, pool, tier)
						seed(kind, orgID)
						store := wrongStore
						if rightKey {
							store = rightStore
						}
						var baseline OrgConfig
						if save && hasModel(kind) {
							b, _, err := rightStore.OrgConfig(ctx, orgID.String())
							if err != nil {
								t.Fatalf("baseline: %v", err)
							}
							baseline = b
							afterRowsRead = func() {
								if _, err := pool.Exec(ctx, `UPDATE settings SET value = 'm-after', updated_at = updated_at + interval '1 minute'
									WHERE org_id = $1 AND category = 'llm' AND key = 'model'`, orgID.String()); err != nil {
									t.Error(err)
								}
							}
							t.Cleanup(func() { afterRowsRead = func() {} })
						}
						cfg, ok, err := store.OrgConfig(ctx, orgID.String())
						afterRowsRead = func() {}

						unreadable := hasCipher(kind) && !rightKey
						wantErr := gateOpen && unreadable
						// "served" means: BYO rows present, a real provider (byoPlain, byoMixed), gate open, readable.
						served := gateOpen && !unreadable && (kind == byoPlain || kind == byoMixed)
						if (err != nil) != wantErr {
							t.Fatalf("err = %v, wantErr %v", err, wantErr)
						}
						if wantErr && !errors.Is(err, ErrStoredSettingUnreadable) {
							t.Fatalf("err = %v, want ErrStoredSettingUnreadable", err)
						}
						if ok != served {
							t.Fatalf("ok = %v, want %v (cfg %+v)", ok, served, cfg)
						}
						if !served {
							return
						}
						if cfg.Provider != "openai" {
							t.Fatalf("provider = %q, want canonical openai", cfg.Provider)
						}
						if cfg.Version == "" || len(cfg.Version) != 64 {
							t.Fatalf("version = %q, want 64 hex chars", cfg.Version)
						}
						if hasCipher(kind) && cfg.APIKey.Reveal() != "sk-secret" {
							t.Fatal("api_key not decrypted")
						}
						if save {
							// The pre-save snapshot: old value, old version, never a mixed pair.
							if cfg.Model != "m-before" || cfg.Version != baseline.Version {
								t.Fatalf("mixed snapshot: model=%q version=%q, want m-before at %q", cfg.Model, cfg.Version, baseline.Version)
							}
							next, _, err := rightStore.OrgConfig(ctx, orgID.String())
							if err != nil || next.Model != "m-after" || next.Version == cfg.Version {
								t.Fatalf("after the save: model=%q version=%q err=%v", next.Model, next.Version, err)
							}
						}
					})
				}
			}
		}
	}

	// Deleting a row that is not the newest still moves the version.
	t.Run("version moves when a non-newest row is deleted", func(t *testing.T) {
		orgID := seedOrg(ctx, t, pool, "enterprise")
		insertSetting(ctx, t, pool, orgID, "provider", "openai")
		insertSetting(ctx, t, pool, orgID, "base_url", "https://api.openai.com/v1")
		insertSetting(ctx, t, pool, orgID, "model", "m")
		before, ok, err := rightStore.OrgConfig(ctx, orgID.String())
		if err != nil || !ok {
			t.Fatalf("ok=%v err=%v", ok, err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM settings WHERE org_id = $1 AND category = 'llm' AND key = 'base_url'`, orgID.String()); err != nil {
			t.Fatal(err)
		}
		after, _, err := rightStore.OrgConfig(ctx, orgID.String())
		if err != nil || after.Version == before.Version || after.BaseURL != "" {
			t.Fatalf("version %q -> %q, base_url %q, err %v", before.Version, after.Version, after.BaseURL, err)
		}
	})
}
