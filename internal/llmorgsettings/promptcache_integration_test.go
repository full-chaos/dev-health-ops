//go:build integration

package llmorgsettings

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func setPromptCacheRow(ctx context.Context, t *testing.T, store Store, org, value string) {
	t.Helper()
	tag, err := store.Pool.Exec(ctx, `UPDATE settings SET value = $3, updated_at = now() WHERE org_id = $1 AND category = 'llm' AND key = $2`, org, KeyPromptCache, value)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() == 0 {
		insertSetting(ctx, t, store.Pool, uuid.MustParse(org), KeyPromptCache, value)
	}
}

// The acr llm-config source serves the org's prompt-cache opt-in: off with no
// row, on only for the exact value "true" on an anthropic org, and a flip moves
// the version (acr keys the org's built runtime on it) while a removed row
// brings the version back to the one served before the row existed.
func TestOrgConfigServesThePromptCacheOptIn(t *testing.T) {
	ctx, store, org, set := seedPurposeOrg(t)
	investigate := store.ForPurpose(PurposeInvestigate)

	base, ok, err := investigate.OrgConfig(ctx, org)
	if err != nil || !ok || base.PromptCache {
		t.Fatalf("no row: ok=%v err=%v promptCache=%v", ok, err, base.PromptCache)
	}

	set(KeyPromptCache, "true")
	on, ok, err := investigate.OrgConfig(ctx, org)
	if err != nil || !ok || !on.PromptCache {
		t.Fatalf("opted in: ok=%v err=%v promptCache=%v", ok, err, on.PromptCache)
	}
	if on.Version == base.Version {
		t.Fatal("opting in did not move the version")
	}

	setPromptCacheRow(ctx, t, store, org, "false")
	off, ok, err := investigate.OrgConfig(ctx, org)
	if err != nil || !ok || off.PromptCache || off.Version == on.Version || off.Version == base.Version {
		t.Fatalf("flipped off: ok=%v err=%v promptCache=%v versions base=%s on=%s off=%s", ok, err, off.PromptCache, base.Version, on.Version, off.Version)
	}

	if _, err := store.Pool.Exec(ctx, `DELETE FROM settings WHERE org_id = $1 AND category = 'llm' AND key = $2`, org, KeyPromptCache); err != nil {
		t.Fatal(err)
	}
	gone, ok, err := investigate.OrgConfig(ctx, org)
	if err != nil || !ok || gone.PromptCache || gone.Version != base.Version {
		t.Fatalf("row removed: ok=%v err=%v promptCache=%v version %s, want %s", ok, err, gone.PromptCache, gone.Version, base.Version)
	}
}

// The opt-in follows the effective provider: an investigate override on another
// provider drops it, and the default reader still sees the anthropic default.
func TestOrgConfigPromptCacheFollowsTheEffectiveProvider(t *testing.T) {
	ctx, store, org, set := seedPurposeOrg(t)
	set(KeyPromptCache, "true")
	investigate := store.ForPurpose(PurposeInvestigate)

	set(PurposeKey(PurposeInvestigate, "provider"), "openai")
	set(PurposeKey(PurposeInvestigate, "model"), "mid-model")
	got, ok, err := investigate.OrgConfig(ctx, org)
	if err != nil || !ok || got.Provider != "openai" || got.PromptCache {
		t.Fatalf("openai override: ok=%v err=%v provider=%q promptCache=%v", ok, err, got.Provider, got.PromptCache)
	}
	def, ok, err := store.OrgConfig(ctx, org)
	if err != nil || !ok || def.Provider != "anthropic" || !def.PromptCache {
		t.Fatalf("default reader: ok=%v err=%v provider=%q promptCache=%v", ok, err, def.Provider, def.PromptCache)
	}
}

// An org that holds only the opt-in row is not a BYO org: every reader answers
// as it does for an org with no llm rows at all.
func TestPromptCacheRowAloneIsNotASavedConfig(t *testing.T) {
	ctx, store, _, _ := seedPurposeOrg(t)
	pool := store.Pool
	bare := seedOrg(ctx, t, pool, "enterprise")
	only := seedOrg(ctx, t, pool, "enterprise")
	insertSetting(ctx, t, pool, only, KeyPromptCache, "true")

	for _, purpose := range []Purpose{"", PurposeInvestigate, PurposeExplain} {
		scoped := store.ForPurpose(purpose)
		wantCfg, wantOK, wantErr := scoped.OrgConfig(ctx, bare.String())
		gotCfg, gotOK, gotErr := scoped.OrgConfig(ctx, only.String())
		if gotOK != wantOK || (gotErr == nil) != (wantErr == nil) || gotCfg.GateClosed != wantCfg.GateClosed || gotCfg.TurnedOff != wantCfg.TurnedOff || gotCfg.UnresolvedPurpose != wantCfg.UnresolvedPurpose || gotCfg.PromptCache {
			t.Errorf("%q OrgConfig: only-opt-in = (%+v, %v, %v), no rows = (%+v, %v, %v)", purpose, gotCfg, gotOK, gotErr, wantCfg, wantOK, wantErr)
		}
		wantProvider, wantErr := scoped.ResolveUsableProvider(ctx, bare.String())
		gotProvider, gotErr := scoped.ResolveUsableProvider(ctx, only.String())
		if gotProvider != wantProvider || (gotErr == nil) != (wantErr == nil) {
			t.Errorf("%q ResolveUsableProvider: only-opt-in = (%q, %v), no rows = (%q, %v)", purpose, gotProvider, gotErr, wantProvider, wantErr)
		}
	}
	saved, err := HasSavedBYO(ctx, pool, only.String())
	if err != nil || saved {
		t.Fatalf("HasSavedBYO = %v, %v; the opt-in row is not a saved config", saved, err)
	}
}
