//go:build integration

package llmorgsettings

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

// CHAOS-14232 over real rows: a default, investigate or explain base_url that
// carries the org's encrypted categorize api key is refused by every reader of
// the non-categorize stores (OrgConfig/CheckOrgConfig, Credentials,
// ResolveUsableProvider, StoredBaseURLCarriesKey), with no lookup of a host
// that carries it.

const integrationCategorizeKey = "sk-integration-categorize-" + "0123456789abcdef"

func recordingPublicResolver(t *testing.T) *[]string {
	t.Helper()
	var mu sync.Mutex
	hosts := []string{}
	previous := hostResolver
	hostResolver = func(_ context.Context, host string) ([]net.IP, error) {
		mu.Lock()
		defer mu.Unlock()
		hosts = append(hosts, host)
		return []net.IP{net.ParseIP("8.8.8.8")}, nil
	}
	t.Cleanup(func() { hostResolver = previous })
	return &hosts
}

func seedCategorizeKeyOrg(t *testing.T) (context.Context, Store, string, func(string, string)) {
	t.Helper()
	ctx, store, org, set := seedPurposeOrg(t)
	set(PurposeKey(PurposeCategorize, "provider"), "openai")
	set(PurposeKey(PurposeCategorize, "model"), "cat-model")
	insertEncryptedSetting(ctx, t, store, store.Pool, uuid.MustParse(org), PurposeKey(PurposeCategorize, "api_key"), integrationCategorizeKey)
	return ctx, store, org, set
}

func assertRefusesCategorizeKeyURL(t *testing.T, ctx context.Context, name string, s Store, org, provider string, hosts *[]string) {
	t.Helper()
	before := len(*hosts)
	cfg, ok, err := s.OrgConfig(ctx, org)
	if err != nil || !ok || !cfg.KeyInBaseURL {
		t.Errorf("%s: OrgConfig ok=%v err=%v KeyInBaseURL=%v, want flagged", name, ok, err, cfg.KeyInBaseURL)
	}
	if ok {
		if reason, err := CheckOrgConfig(ctx, cfg); err != nil || reason != ConfigInvalidBaseURL {
			t.Errorf("%s: CheckOrgConfig = %q err=%v, want %q", name, reason, err, ConfigInvalidBaseURL)
		}
	}
	if _, served, err := s.Credentials(ctx, org, provider); err != nil || served {
		t.Errorf("%s: Credentials served=%v err=%v, want refused", name, served, err)
	}
	if usable, err := s.ResolveUsableProvider(ctx, org); err != nil || usable != "" {
		t.Errorf("%s: ResolveUsableProvider = %q err=%v, want refused", name, usable, err)
	}
	if hit, err := s.StoredBaseURLCarriesKey(ctx, org, provider); err != nil || !hit {
		t.Errorf("%s: StoredBaseURLCarriesKey = %v err=%v, want a key hit", name, hit, err)
	}
	for _, host := range (*hosts)[before:] {
		if BaseURLCarriesKey(host, integrationCategorizeKey) {
			t.Errorf("%s: looked up a host that carries the categorize key", name)
		}
	}
}

func TestInvestigateOverrideURLCarryingTheCategorizeKeyIsRefused(t *testing.T) {
	hosts := recordingPublicResolver(t)
	ctx, store, org, set := seedCategorizeKeyOrg(t)
	set(PurposeKey(PurposeInvestigate, "provider"), "openai")
	set(PurposeKey(PurposeInvestigate, "model"), "inv-model")
	set(PurposeKey(PurposeInvestigate, "api_key"), "sk-investigate-0123456789")
	set(PurposeKey(PurposeInvestigate, "base_url"), "https://llm.example.com/v1?k="+integrationCategorizeKey)
	assertRefusesCategorizeKeyURL(t, ctx, "investigate override", store.ForPurpose(PurposeInvestigate), org, "openai", hosts)
}

func TestExplainOverrideURLCarryingTheCategorizeKeyIsRefused(t *testing.T) {
	hosts := recordingPublicResolver(t)
	ctx, store, org, set := seedCategorizeKeyOrg(t)
	set(PurposeKey(PurposeExplain, "provider"), "openai")
	set(PurposeKey(PurposeExplain, "model"), "exp-model")
	set(PurposeKey(PurposeExplain, "api_key"), "sk-explain-0123456789")
	set(PurposeKey(PurposeExplain, "base_url"), "https://llm.example.com/v1/"+integrationCategorizeKey)
	assertRefusesCategorizeKeyURL(t, ctx, "explain override", store.ForPurpose(PurposeExplain), org, "openai", hosts)
}

func TestDefaultRowURLCarryingTheCategorizeKeyIsRefusedByEveryStore(t *testing.T) {
	hosts := recordingPublicResolver(t)
	ctx, store, org, set := seedCategorizeKeyOrg(t)
	set("base_url", "https://llm.example.com/v1?k="+integrationCategorizeKey)
	for _, purpose := range []Purpose{"", PurposeInvestigate, PurposeExplain} {
		assertRefusesCategorizeKeyURL(t, ctx, "default row, store "+string(purpose), store.ForPurpose(purpose), org, "anthropic", hosts)
	}
}

func TestHostLabelCarryingTheCategorizeKeyIsNeverLookedUp(t *testing.T) {
	hosts := recordingPublicResolver(t)
	ctx, store, org, set := seedCategorizeKeyOrg(t)
	set(PurposeKey(PurposeExplain, "provider"), "openai")
	set(PurposeKey(PurposeExplain, "model"), "exp-model")
	set(PurposeKey(PurposeExplain, "api_key"), "sk-explain-0123456789")
	set(PurposeKey(PurposeExplain, "base_url"), "https://"+integrationCategorizeKey+".example.com/v1")
	assertRefusesCategorizeKeyURL(t, ctx, "explain host label", store.ForPurpose(PurposeExplain), org, "openai", hosts)
}

// Controls: the investigate key in the explain url is refused as before, and a
// clean url serves every store with no trace of the categorize key in its
// config.
func TestCategorizeKeyCheckControls(t *testing.T) {
	hosts := recordingPublicResolver(t)
	ctx, store, org, set := seedCategorizeKeyOrg(t)
	set(PurposeKey(PurposeInvestigate, "provider"), "openai")
	set(PurposeKey(PurposeInvestigate, "model"), "inv-model")
	set(PurposeKey(PurposeInvestigate, "api_key"), "sk-investigate-0123456789")
	set(PurposeKey(PurposeExplain, "provider"), "openai")
	set(PurposeKey(PurposeExplain, "model"), "exp-model")
	set(PurposeKey(PurposeExplain, "api_key"), "sk-explain-0123456789")
	set(PurposeKey(PurposeExplain, "base_url"), "https://llm.example.com/v1?k=sk-investigate-0123456789")
	if _, served, err := store.ForPurpose(PurposeExplain).Credentials(ctx, org, "openai"); err != nil || served || len(*hosts) != 0 {
		t.Fatalf("investigate key in the explain url: served=%v err=%v lookups=%d", served, err, len(*hosts))
	}
	if _, err := store.Pool.Exec(ctx, `UPDATE settings SET value = 'https://llm.example.com/v1' WHERE org_id = $1 AND category = 'llm' AND key = $2`, org, PurposeKey(PurposeExplain, "base_url")); err != nil {
		t.Fatal(err)
	}
	set("base_url", "https://llm.example.com/v1")
	for _, purpose := range []Purpose{"", PurposeInvestigate, PurposeExplain} {
		s := store.ForPurpose(purpose)
		cfg, ok, err := s.OrgConfig(ctx, org)
		if err != nil || !ok || cfg.KeyInBaseURL {
			t.Errorf("clean url, store %q: ok=%v err=%v flagged=%v", purpose, ok, err, cfg.KeyInBaseURL)
		}
		if cfg.APIKey.Reveal() == integrationCategorizeKey {
			t.Errorf("clean url, store %q: OrgConfig serves the categorize key", purpose)
		}
		for label, text := range printsOf(cfg) {
			if strings.Contains(text, integrationCategorizeKey) {
				t.Errorf("clean url, store %q: OrgConfig %s holds the categorize key", purpose, label)
			}
		}
		if reason, err := CheckOrgConfig(ctx, cfg); err != nil || reason != ConfigActive {
			t.Errorf("clean url, store %q: CheckOrgConfig = %q err=%v", purpose, reason, err)
		}
	}
}
