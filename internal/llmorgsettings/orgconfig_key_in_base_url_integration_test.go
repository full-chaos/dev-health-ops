//go:build integration

package llmorgsettings

import "testing"

// OrgConfig flags a served base_url that carries any api key the org stores: the
// served key, the default row's key under an override that has its own, or an
// escaped form of either. A clean url is not flagged.
func TestOrgConfigFlagsAKeyInTheBaseURL(t *testing.T) {
	ctx, store, org, set := seedPurposeOrg(t)
	investigate := store.ForPurpose(PurposeInvestigate)

	clean, ok, err := investigate.OrgConfig(ctx, org)
	if err != nil || !ok || clean.KeyInBaseURL {
		t.Fatalf("no base_url: ok=%v err=%v flagged=%v", ok, err, clean.KeyInBaseURL)
	}

	set("base_url", "https://p.example.invalid/v1")
	cleanURL, ok, err := investigate.OrgConfig(ctx, org)
	if err != nil || !ok || cleanURL.KeyInBaseURL {
		t.Fatalf("clean base_url: ok=%v err=%v flagged=%v", ok, err, cleanURL.KeyInBaseURL)
	}
	if _, err := store.Pool.Exec(ctx, `UPDATE settings SET value = 'https://p.example.invalid/sk-default' WHERE org_id = $1 AND category = 'llm' AND key = 'base_url'`, org); err != nil {
		t.Fatal(err)
	}
	served, ok, err := investigate.OrgConfig(ctx, org)
	if err != nil || !ok || !served.KeyInBaseURL {
		t.Fatalf("served key in base_url: ok=%v err=%v flagged=%v", ok, err, served.KeyInBaseURL)
	}

	set(PurposeKey(PurposeExplain, "provider"), "openai")
	set(PurposeKey(PurposeExplain, "model"), "small-explain")
	set(PurposeKey(PurposeExplain, "api_key"), "sk-explain")
	set(PurposeKey(PurposeExplain, "base_url"), "https://p.example.invalid/%73k-default")
	explain, ok, err := store.ForPurpose(PurposeExplain).OrgConfig(ctx, org)
	if err != nil || !ok || explain.Provider != "openai" || !explain.KeyInBaseURL {
		t.Fatalf("override url holding the default row's key (escaped): ok=%v err=%v provider=%q flagged=%v", ok, err, explain.Provider, explain.KeyInBaseURL)
	}
}
