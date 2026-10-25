//go:build integration

package llmorgsettings_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

type settingRow struct {
	category, key, value string
	encrypted            bool
	// sealed stores value as it is, marked encrypted: a row no key decrypts.
	sealed bool
}

func seedSettings(ctx context.Context, t *testing.T, admin *pgxpool.Pool, cipher providerfoundation.FernetDecryptor, orgID string, rows []settingRow) {
	t.Helper()
	for _, row := range rows {
		value := row.value
		if row.sealed {
			pgseed.Setting(ctx, t, admin, orgID, row.category, row.key, value, true)
			continue
		}
		if row.encrypted {
			sealed, err := cipher.Encrypt([]byte(row.value))
			if err != nil {
				t.Fatal(err)
			}
			value = sealed.Reveal()
		}
		pgseed.Setting(ctx, t, admin, orgID, row.category, row.key, value, row.encrypted)
	}
}

// functionRows is what the domain role reads through the definer function for
// orgID, as "category/key" sorted; every row must belong to wantOrg.
func functionRows(ctx context.Context, t *testing.T, domain *pgxpool.Pool, orgID any, wantOrg string) []string {
	t.Helper()
	rows, err := domain.Query(ctx, `SELECT org_id, category, key FROM public.categorize_byo_settings($1)`, orgID)
	if err != nil {
		t.Fatalf("categorize_byo_settings(%v) as the domain role: %v", orgID, err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var org, category, key string
		if err := rows.Scan(&org, &category, &key); err != nil {
			t.Fatal(err)
		}
		if org != wantOrg {
			t.Errorf("categorize_byo_settings(%v) returned a row of org %q", orgID, org)
		}
		got = append(got, category+"/"+key)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	return got
}

func llmRows(keys []string, value string, encrypted map[string]bool) []settingRow {
	rows := make([]settingRow, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, settingRow{category: "llm", key: key, value: value + "-" + key, encrypted: encrypted[key]})
	}
	return rows
}

// The definer function returns, for one org, the pinned llm rows while the org
// is opted in and the budget limit row always: no other key, category or org,
// whatever org id the caller passes.
func TestTheCategorizeSettingsFunctionReturnsOnlyThePinnedRowsOfOneOrg(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, domain, denials, _ := startDomainRoleDatabase(t, ctx)
	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("test-master-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	pinned := llmorgsettings.CategorizeBYODefinerKeys()
	decoys := []settingRow{
		{category: "llm", key: "purpose:investigate:api_key", value: "decoy", encrypted: true},
		{category: "llm", key: "purpose:investigate:model", value: "decoy", encrypted: false},
		{category: "llm", key: "purpose:investigate:base_url", value: "decoy", encrypted: false},
		{category: "llm", key: "purpose:explain:api_key", value: "decoy", encrypted: true},
		{category: "llm", key: "purpose:explain:model", value: "decoy", encrypted: false},
		{category: "llm", key: "purpose:explain:base_url", value: "decoy", encrypted: false},
		{category: "llm", key: "concurrency", value: "4", encrypted: false},
		{category: "llm", key: "certification_profile", value: "decoy", encrypted: false},
		{category: "llm_budget", key: "other_key", value: "1", encrypted: false},
		{category: "general", key: "api_key", value: "decoy", encrypted: true},
		{category: "integrations", key: "provider", value: "decoy", encrypted: false},
	}
	everything := func(optIn string) []settingRow {
		rows := append(llmRows(pinned, "v", map[string]bool{"api_key": true, "purpose:categorize:api_key": true}), decoys...)
		rows = append(rows, settingRow{category: "llm_budget", key: "limit_micro_usd", value: "1000000", encrypted: false})
		if optIn != "" {
			rows = append(rows, settingRow{category: "llm", key: llmorgsettings.KeyCategorizeBYOEnabled, value: optIn, encrypted: false})
		}
		return rows
	}
	optedIn, neighbour, notOptedIn, titleCase := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	seedSettings(ctx, t, admin, cipher, optedIn, everything("true"))
	seedSettings(ctx, t, admin, cipher, neighbour, everything("true"))
	seedSettings(ctx, t, admin, cipher, notOptedIn, everything(""))
	seedSettings(ctx, t, admin, cipher, titleCase, everything("True"))

	var want []string
	for _, key := range pinned {
		want = append(want, "llm/"+key)
	}
	want = append(want, "llm_budget/limit_micro_usd")
	sort.Strings(want)
	if got := functionRows(ctx, t, domain, optedIn, optedIn); !slices.Equal(got, want) {
		t.Fatalf("opted-in org: rows %v, want exactly %v", got, want)
	}
	for _, org := range []string{notOptedIn, titleCase} {
		if got := functionRows(ctx, t, domain, org, org); !slices.Equal(got, []string{"llm_budget/limit_micro_usd"}) {
			t.Fatalf("org not opted in (%s): rows %v, want the budget limit row only", org, got)
		}
	}
	// The argument is the only org filter: no value widens the result.
	for _, widen := range []any{"", "%", "*", optedIn + "' OR '1'='1", " " + optedIn, uuid.NewString(), nil} {
		if got := functionRows(ctx, t, domain, widen, fmt.Sprint(widen)); len(got) != 0 {
			t.Fatalf("categorize_byo_settings(%q) returned %v, want no row", widen, got)
		}
	}
	var others int
	if err := domain.QueryRow(ctx, `SELECT count(*) FROM public.categorize_byo_settings($1) AS s WHERE s.org_id <> $1`, optedIn).Scan(&others); err != nil || others != 0 {
		t.Fatalf("rows of another org through the function = %d, %v", others, err)
	}
	// The ciphertext comes back as stored; only a holder of the key reads it.
	var stored string
	var encrypted bool
	if err := domain.QueryRow(ctx, `SELECT value, is_encrypted FROM public.categorize_byo_settings($1) WHERE category = 'llm' AND key = 'api_key'`, optedIn).Scan(&stored, &encrypted); err != nil || !encrypted || stored == "v-api_key" {
		t.Fatalf("api_key through the function = encrypted %v, %v", encrypted, err)
	}
	if denied := denials.All(); len(denied) != 0 {
		t.Fatalf("the domain role was refused %d statement(s) on the function: %q", len(denied), denied)
	}
	var count int
	err = domain.QueryRow(ctx, `SELECT count(*) FROM settings`).Scan(&count)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
		t.Fatalf("SELECT on settings as the domain role = %v, want SQLSTATE 42501", err)
	}
}

// configView is an OrgConfig with its key revealed, for comparison.
func configView(cfg llmorgsettings.OrgConfig, found bool, err error) string {
	return fmt.Sprintf("found=%v unreadable=%v err=%v purpose=%q provider=%q model=%q base=%q key=%q cache=%v version=%q keyInBase=%v closed=%v unresolved=%q off=%v",
		found, errors.Is(err, llmorgsettings.ErrStoredSettingUnreadable), err != nil, cfg.Purpose, cfg.Provider, cfg.Model, cfg.BaseURL,
		cfg.APIKey.Reveal(), cfg.PromptCache, cfg.Version, cfg.KeyInBaseURL, cfg.GateClosed, cfg.UnresolvedPurpose, cfg.TurnedOff)
}

// The worker's readers on the definer function (domain role) give the same
// categorize config and the same model-gate inputs as the api's readers on the
// settings table, for every opted-in org shape. The named delta rows are the
// two places they differ by design: an org that did not opt in reads as no
// config on the worker, and the worker cannot test the base_url against the
// investigate and explain overrides' keys (the function does not return them).
func TestTheDefinerReadersMatchTheTableReadersForAnOptedInOrg(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, domain, denials, _ := startDomainRoleDatabase(t, ctx)
	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("test-master-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	optIn := settingRow{category: "llm", key: llmorgsettings.KeyCategorizeBYOEnabled, value: "true", encrypted: false}
	enc := func(key, value string) settingRow {
		return settingRow{category: "llm", key: key, value: value, encrypted: true}
	}
	plain := func(key, value string) settingRow {
		return settingRow{category: "llm", key: key, value: value, encrypted: false}
	}
	const clefURL = "https://api.cloudflare.com/client/v4/accounts/0123456789abcdef0123456789abcdef/ai"
	cases := []struct {
		name  string
		rows  []settingRow
		delta bool
	}{
		{name: "default config", rows: []settingRow{optIn, plain("provider", "openai"), plain("model", "gpt-5-mini"), enc("api_key", "sk-default")}},
		{name: "categorize override with its own key", rows: []settingRow{optIn, plain("provider", "openai"), plain("model", "gpt-5-mini"), enc("api_key", "sk-default"),
			plain("purpose:categorize:provider", "anthropic"), plain("purpose:categorize:model", "claude-x"), enc("purpose:categorize:api_key", "sk-cat")}},
		{name: "override inherits the default key", rows: []settingRow{optIn, plain("provider", "openai"), plain("model", "gpt-5-mini"), enc("api_key", "sk-default"),
			plain("purpose:categorize:provider", "openai"), plain("purpose:categorize:model", "gpt-5")}},
		{name: "clef override", rows: []settingRow{optIn, plain("purpose:categorize:provider", "clef"), plain("purpose:categorize:model", "clef"),
			enc("purpose:categorize:api_key", "cf-token"), plain("purpose:categorize:base_url", clefURL)}},
		{name: "prompt cache on anthropic", rows: []settingRow{optIn, plain("provider", "anthropic"), plain("model", "claude-x"), enc("api_key", "sk-a"), plain("prompt_cache", "true")}},
		{name: "own model turned off", rows: []settingRow{optIn, plain("provider", "openai"), plain("model", "gpt-5-mini"), enc("api_key", "sk-default"), plain("own_model_enabled", "false")}},
		{name: "only an investigate config", rows: []settingRow{optIn, plain("purpose:investigate:provider", "openai"), plain("purpose:investigate:model", "gpt-5"), enc("purpose:investigate:api_key", "sk-inv")}},
		{name: "unreadable default key", rows: []settingRow{optIn, plain("provider", "openai"), plain("model", "gpt-5-mini"), {category: "llm", key: "api_key", value: "not-a-token", sealed: true}}},
		{name: "no config at all", rows: []settingRow{optIn}},
		{name: "DELTA: not opted in", delta: true, rows: []settingRow{plain("provider", "openai"), plain("model", "gpt-5-mini"), enc("api_key", "sk-default")}},
		{name: "DELTA: base_url carries the investigate key", delta: true, rows: []settingRow{optIn, plain("provider", "openai"), plain("model", "gpt-5-mini"), enc("api_key", "sk-default"),
			plain("base_url", "https://llm.example.invalid/v1?key=sk-investigate-secret"), plain("purpose:investigate:provider", "openai"), enc("purpose:investigate:api_key", "sk-investigate-secret")}},
		{name: "DELTA: base_url carries the explain key", delta: true, rows: []settingRow{optIn, plain("provider", "openai"), plain("model", "gpt-5-mini"), enc("api_key", "sk-default"),
			plain("base_url", "https://llm.example.invalid/sk-explain-secret/v1"), plain("purpose:explain:provider", "openai"), enc("purpose:explain:api_key", "sk-explain-secret")}},
	}
	tableStore := llmorgsettings.Store{Pool: admin, Decryptor: cipher}.ForPurpose(llmorgsettings.PurposeCategorize)
	definerStore := llmorgsettings.Store{Pool: domain, Decryptor: cipher, Relation: llmorgsettings.CategorizeBYODefiner}.ForPurpose(llmorgsettings.PurposeCategorize)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			org := uuid.NewString()
			pgseed.Org(ctx, t, admin, org, "enterprise")
			seedSettings(ctx, t, admin, cipher, org, tc.rows)
			tableView := configView(tableStore.OrgConfig(ctx, org))
			definerView := configView(definerStore.OrgConfig(ctx, org))
			if !tc.delta {
				if tableView != definerView {
					t.Fatalf("OrgConfig differs:\n table   %s\n definer %s", tableView, definerView)
				}
				tableInputs, tableErr := llmorgsettings.PgSource{Licensing: licensing.PostgresStore{Pool: admin}, Queryer: admin, Config: tableStore}.Inputs(ctx, org)
				definerInputs, definerErr := llmorgsettings.PgSource{Licensing: licensing.PostgresStore{Pool: domain}, Queryer: domain, Config: definerStore, Relation: llmorgsettings.CategorizeBYODefiner}.Inputs(ctx, org)
				if fmt.Sprintf("%+v %v", tableInputs, tableErr != nil) != fmt.Sprintf("%+v %v", definerInputs, definerErr != nil) {
					t.Fatalf("model-gate inputs differ:\n table   %+v (%v)\n definer %+v (%v)", tableInputs, tableErr, definerInputs, definerErr)
				}
				return
			}
			switch tc.name {
			case "DELTA: not opted in":
				tableCfg, tableFound, _ := tableStore.OrgConfig(ctx, org)
				_, definerFound, definerErr := definerStore.OrgConfig(ctx, org)
				if !tableFound || tableCfg.Provider != "openai" || definerFound || definerErr != nil {
					t.Fatalf("not opted in: table found=%v, definer found=%v err=%v; want the worker to see no config", tableFound, definerFound, definerErr)
				}
			default:
				tableCfg, _, _ := tableStore.OrgConfig(ctx, org)
				definerCfg, definerFound, definerErr := definerStore.OrgConfig(ctx, org)
				if !tableCfg.KeyInBaseURL || !definerFound || definerErr != nil || definerCfg.KeyInBaseURL {
					t.Fatalf("%s: table KeyInBaseURL=%v, definer found=%v KeyInBaseURL=%v err=%v; want the documented delta (table true, definer false)",
						tc.name, tableCfg.KeyInBaseURL, definerFound, definerCfg.KeyInBaseURL, definerErr)
				}
				tableCfg.KeyInBaseURL = false
				if configView(tableCfg, true, nil) != configView(definerCfg, definerFound, definerErr) {
					t.Fatalf("%s: beyond KeyInBaseURL the configs differ:\n table   %s\n definer %s", tc.name, configView(tableCfg, true, nil), configView(definerCfg, definerFound, definerErr))
				}
			}
		})
	}
	if denied := denials.All(); len(denied) != 0 {
		t.Fatalf("the domain role was refused %d statement(s) on the definer readers: %q", len(denied), denied)
	}
}
