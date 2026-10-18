//go:build integration

package llmorgsettings

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// seedPurposeOrg stores a default row (anthropic/big) and an override for each
// purpose, every one with a distinct provider, model and key.
func seedPurposeOrg(t *testing.T) (context.Context, Store, string, func(string, string)) {
	t.Helper()
	ctx := context.Background()
	store, pool := newTestStore(t)
	seedByoLLMFeature(ctx, t, pool, "team", true)
	orgID := seedOrg(ctx, t, pool, "enterprise")
	insertSetting(ctx, t, pool, orgID, "provider", "anthropic")
	insertSetting(ctx, t, pool, orgID, "model", "big-model")
	insertEncryptedSetting(ctx, t, store, pool, orgID, "api_key", "sk-default")
	set := func(key, value string) { insertSetting(ctx, t, pool, orgID, key, value) }
	return ctx, store, orgID.String(), set
}

// Each purpose reads its own override; a purpose with none reads the default;
// the default reader (no purpose) never sees an override.
func TestPurposeResolutionPerReader(t *testing.T) {
	ctx, store, org, set := seedPurposeOrg(t)
	set(PurposeKey(PurposeExplain, "provider"), "openai")
	set(PurposeKey(PurposeExplain, "model"), "small-explain")
	set(PurposeKey(PurposeExplain, "api_key"), "sk-explain")
	set(PurposeKey(PurposeCategorize, "provider"), "gemini")
	set(PurposeKey(PurposeCategorize, "model"), "small-categorize")
	set(PurposeKey(PurposeCategorize, "api_key"), "sk-categorize")

	for _, tc := range []struct {
		purpose      Purpose
		wantProvider string
		wantModel    string
		wantKey      string
	}{
		{"", "anthropic", "big-model", "sk-default"},
		{PurposeInvestigate, "anthropic", "big-model", "sk-default"},
		{PurposeExplain, "openai", "small-explain", "sk-explain"},
		{PurposeCategorize, "gemini", "small-categorize", "sk-categorize"},
	} {
		scoped := store.ForPurpose(tc.purpose)
		provider, err := scoped.ResolveUsableProvider(ctx, org)
		if err != nil || provider != tc.wantProvider {
			t.Errorf("%q: provider = %q, %v; want %q", tc.purpose, provider, err, tc.wantProvider)
		}
		model, err := scoped.Model(ctx, org, tc.wantProvider)
		if err != nil || model != tc.wantModel {
			t.Errorf("%q: model = %q, %v; want %q", tc.purpose, model, err, tc.wantModel)
		}
		creds, ok, err := scoped.Credentials(ctx, org, tc.wantProvider)
		if err != nil || !ok || creds.APIKey.Reveal() != tc.wantKey {
			t.Errorf("%q: credentials ok=%v err=%v (key matches=%v)", tc.purpose, ok, err, creds.APIKey.Reveal() == tc.wantKey)
		}
	}
}

// The acr llm-config source serves the investigate config: the override when
// stored, else the default row; the version follows the effective source only.
func TestOrgConfigServesInvestigateOverride(t *testing.T) {
	ctx, store, org, set := seedPurposeOrg(t)
	investigate := store.ForPurpose(PurposeInvestigate)

	base, ok, err := investigate.OrgConfig(ctx, org)
	if err != nil || !ok || base.Provider != "anthropic" || base.Model != "big-model" {
		t.Fatalf("default fallback: ok=%v err=%v cfg=%+v", ok, err, base)
	}

	// An explain override does not touch the investigate config or its version.
	set(PurposeKey(PurposeExplain, "provider"), "openai")
	set(PurposeKey(PurposeExplain, "model"), "small-explain")
	same, _, err := investigate.OrgConfig(ctx, org)
	if err != nil || same.Version != base.Version {
		t.Fatalf("explain override moved the investigate version: err=%v", err)
	}

	set(PurposeKey(PurposeInvestigate, "provider"), "openai")
	set(PurposeKey(PurposeInvestigate, "model"), "mid-model")
	got, ok, err := investigate.OrgConfig(ctx, org)
	if err != nil || !ok {
		t.Fatalf("override: ok=%v err=%v", ok, err)
	}
	if got.Provider != "openai" || got.Model != "mid-model" || got.APIKey.Reveal() != "" {
		t.Fatalf("override served %+v (the default api_key must not be borrowed)", got)
	}
	if got.Version == base.Version {
		t.Fatal("version did not move when the effective source changed")
	}

	// The default reader still serves the default row.
	def, ok, err := store.OrgConfig(ctx, org)
	if err != nil || !ok || def.Provider != "anthropic" || def.Version != base.Version {
		t.Fatalf("default reader: ok=%v err=%v cfg=%+v", ok, err, def)
	}
}

// An override whose stored key cannot be read fails closed; it never falls
// back to the readable default row.
func TestOrgConfigUnreadableOverrideKeyFailsClosed(t *testing.T) {
	ctx, store, org, set := seedPurposeOrg(t)
	set(PurposeKey(PurposeInvestigate, "provider"), "openai")
	set(PurposeKey(PurposeInvestigate, "model"), "mid-model")
	pool := store.Pool
	cipher, err := store.Decryptor.Encrypt([]byte("sk-override"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
		VALUES (gen_random_uuid(), $1, 'llm', $2, $3, true, now(), now())`,
		org, PurposeKey(PurposeInvestigate, "api_key"), cipher.Reveal()); err != nil {
		t.Fatal(err)
	}
	wrong, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("totally-wrong-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	wrongStore := Store{Pool: pool, Decryptor: wrong, Now: store.Now}.ForPurpose(PurposeInvestigate)
	cfg, ok, err := wrongStore.OrgConfig(ctx, org)
	if ok || !errors.Is(err, ErrStoredSettingUnreadable) {
		t.Fatalf("ok=%v err=%v cfg=%+v, want ErrStoredSettingUnreadable", ok, err, cfg)
	}
}

// A closed byo_llm gate still wins over every override.
func TestPurposeOverrideClosedGateServesNothing(t *testing.T) {
	ctx := context.Background()
	store, pool := newTestStore(t)
	seedByoLLMFeature(ctx, t, pool, "team", false)
	orgID := seedOrg(ctx, t, pool, "free")
	insertSetting(ctx, t, pool, orgID, PurposeKey(PurposeInvestigate, "provider"), "openai")
	insertSetting(ctx, t, pool, orgID, PurposeKey(PurposeInvestigate, "model"), "mid-model")
	if _, ok, err := store.ForPurpose(PurposeInvestigate).OrgConfig(ctx, orgID.String()); err != nil || ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if provider, err := store.ForPurpose(PurposeExplain).ResolveUsableProvider(ctx, orgID.String()); err != nil || provider != "" {
		t.Fatalf("provider=%q err=%v", provider, err)
	}
}

// One provider key, several models: an override for the default's provider
// with no key of its own is served the default key, and the version follows
// that key.
func TestOrgConfigInvestigateInheritsDefaultKeyForSameProvider(t *testing.T) {
	ctx, store, org, set := seedPurposeOrg(t)
	pool := store.Pool
	investigate := store.ForPurpose(PurposeInvestigate)
	set(PurposeKey(PurposeInvestigate, "provider"), "anthropic")
	set(PurposeKey(PurposeInvestigate, "model"), "mid-model")

	got, ok, err := investigate.OrgConfig(ctx, org)
	if err != nil || !ok || got.Model != "mid-model" || got.APIKey.Reveal() != "sk-default" {
		t.Fatalf("ok=%v err=%v model=%q inherited=%v", ok, err, got.Model, got.APIKey.Reveal() == "sk-default")
	}
	cipher, err := store.Decryptor.Encrypt([]byte("sk-rotated"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE settings SET value = $2, updated_at = updated_at + interval '1 second'
		WHERE org_id = $1 AND category = 'llm' AND key = 'api_key'`, org, cipher.Reveal()); err != nil {
		t.Fatal(err)
	}
	next, _, err := investigate.OrgConfig(ctx, org)
	if err != nil || next.Version == got.Version || next.APIKey.Reveal() != "sk-rotated" {
		t.Fatalf("version did not follow the inherited key: err=%v", err)
	}

	// The explain reader inherits the same way.
	set(PurposeKey(PurposeExplain, "provider"), "anthropic")
	set(PurposeKey(PurposeExplain, "model"), "small-explain")
	creds, ok, err := store.ForPurpose(PurposeExplain).Credentials(ctx, org, "anthropic")
	if err != nil || !ok || creds.APIKey.Reveal() != "sk-rotated" {
		t.Fatalf("explain credentials ok=%v err=%v", ok, err)
	}
	if model, _ := store.ForPurpose(PurposeExplain).Model(ctx, org, "anthropic"); model != "small-explain" {
		t.Fatalf("explain model = %q", model)
	}
}
