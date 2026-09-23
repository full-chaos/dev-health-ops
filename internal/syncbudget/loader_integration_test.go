//go:build integration

package syncbudget

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The loader runs its real SQL against Postgres here. Column types follow
// the alembic models: the JSON columns are json, ids are uuid, org_id is
// text. What each branch must produce is SyncTaskBootstrap.load's outcome;
// the estimators themselves are pinned by the live-Python oracle.

const loaderTestKey = "loader-integration-settings-key"

func createLoaderTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(ctx, `
CREATE TABLE public.integration_credentials (
 id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL,
 is_active boolean NOT NULL DEFAULT true, credentials_encrypted text NULL, config json NULL);
CREATE TABLE public.integrations (
 id uuid PRIMARY KEY, org_id text NOT NULL, provider text NOT NULL,
 credential_id uuid NULL, config json NOT NULL DEFAULT '{}');
CREATE TABLE public.integration_sources (
 id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL,
 provider text NOT NULL, external_id text NOT NULL, metadata json NOT NULL DEFAULT '{}');
CREATE TABLE public.integration_datasets (
 id uuid PRIMARY KEY, org_id text NOT NULL, integration_id uuid NOT NULL,
 dataset_key text NOT NULL, options json NOT NULL DEFAULT '{}');
CREATE TABLE public.sync_runs (
 id uuid PRIMARY KEY, org_id text NOT NULL, credential_id uuid NULL,
 credential_fingerprint text NULL, auth_source text NULL);
CREATE TABLE public.sync_run_units (
 id uuid PRIMARY KEY, sync_run_id uuid NOT NULL, org_id text NOT NULL,
 integration_id uuid NOT NULL, source_id uuid NOT NULL, provider text NOT NULL,
 dataset_key text NOT NULL, cost_class text NOT NULL DEFAULT 'light', mode text NOT NULL DEFAULT 'full',
 since_at timestamptz NULL, before_at timestamptz NULL, processor_flags json NULL);`)
	if err != nil {
		t.Fatal(err)
	}
}

type countingDecryptor struct {
	inner providerfoundation.FernetDecryptor
	calls atomic.Int64
}

func (d *countingDecryptor) Decrypt(value secrets.Value) ([]byte, error) {
	d.calls.Add(1)
	return d.inner.Decrypt(value)
}

type loaderFixture struct {
	t         *testing.T
	ctx       context.Context
	pool      *pgxpool.Pool
	cipher    providerfoundation.FernetDecryptor
	orgID     string
	runID     string
	integID   string
	sourceID  string
	decryptor *countingDecryptor
}

func (f *loaderFixture) exec(sql string, args ...any) {
	f.t.Helper()
	if _, err := f.pool.Exec(f.ctx, sql, args...); err != nil {
		f.t.Fatalf("%s: %v", strings.Fields(sql)[0], err)
	}
}

func (f *loaderFixture) credential(provider string, active bool, plaintext string) string {
	f.t.Helper()
	id := uuid.NewString()
	encrypted, err := f.cipher.Encrypt([]byte(plaintext))
	if err != nil {
		f.t.Fatal(err)
	}
	f.exec(`INSERT INTO public.integration_credentials (id, org_id, provider, is_active, credentials_encrypted) VALUES ($1, $2, $3, $4, $5)`,
		id, f.orgID, provider, active, encrypted.Reveal())
	return id
}

func (f *loaderFixture) unit(provider, dataset string, flags *string) string {
	f.t.Helper()
	id := uuid.NewString()
	since := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	f.exec(`INSERT INTO public.sync_run_units (id, sync_run_id, org_id, integration_id, source_id, provider, dataset_key, since_at, before_at, processor_flags)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::json)`,
		id, f.runID, f.orgID, f.integID, f.sourceID, provider, dataset, since, since.Add(72*time.Hour), flags)
	return id
}

func (f *loaderFixture) loader(env map[string]string) Loader {
	return Loader{
		DB: f.pool, Decryptor: f.decryptor, Logger: slog.New(slog.DiscardHandler),
		Getenv: func(name string) string { return env[name] },
	}
}

func withLoaderFixture(t *testing.T, provider string, run func(f *loaderFixture)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createLoaderTables(t, ctx, pool)
	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(loaderTestKey), "")
	if err != nil {
		t.Fatal(err)
	}
	f := &loaderFixture{
		t: t, ctx: ctx, pool: pool, cipher: cipher,
		orgID: uuid.NewString(), runID: uuid.NewString(), integID: uuid.NewString(), sourceID: uuid.NewString(),
		decryptor: &countingDecryptor{inner: cipher},
	}
	f.exec(`INSERT INTO public.integrations (id, org_id, provider) VALUES ($1, $2, $3)`, f.integID, f.orgID, provider)
	f.exec(`INSERT INTO public.integration_sources (id, org_id, integration_id, provider, external_id) VALUES ($1, $2, $3, $4, 'ext')`,
		f.sourceID, f.orgID, f.integID, provider)
	run(f)
}

// wantEstimate is what the estimators give for the context the loader
// should have built: the same function, fed by hand.
func wantEstimates(t *testing.T, context Context) []Estimate {
	t.Helper()
	estimates, err := EstimateProviderBudget(context)
	if err != nil {
		t.Fatal(err)
	}
	return estimates
}

func fingerprints(estimates []Estimate) []string {
	result := make([]string, len(estimates))
	for index, estimate := range estimates {
		result[index] = estimate.Bucket.CredentialFingerprint + "|" + estimate.Bucket.Host + "|" + estimate.RouteFamily
	}
	return result
}

func TestLoaderReferenceChecks(t *testing.T) {
	withLoaderFixture(t, "github", func(f *loaderFixture) {
		loader := f.loader(nil)
		unit := f.unit("github", "prs", nil)
		if _, err := loader.EstimateUnits(f.ctx, f.orgID, f.runID, []string{unit}); !errors.Is(err, ErrStaleRun) {
			t.Fatalf("no sync_runs row: err=%v, want ErrStaleRun", err)
		}
		f.exec(`INSERT INTO public.sync_runs (id, org_id) VALUES ($1, $2)`, f.runID, f.orgID)
		if _, err := loader.EstimateUnits(f.ctx, uuid.NewString(), f.runID, []string{unit}); !errors.Is(err, ErrStaleRun) {
			t.Fatalf("run of another org: err=%v, want ErrStaleRun", err)
		}
		foreign := uuid.NewString()
		f.exec(`INSERT INTO public.sync_run_units (id, sync_run_id, org_id, integration_id, source_id, provider, dataset_key)
VALUES ($1, $2, $3, $4, $5, 'github', 'prs')`, foreign, uuid.NewString(), f.orgID, f.integID, f.sourceID)
		if _, err := loader.EstimateUnits(f.ctx, f.orgID, f.runID, []string{unit, foreign}); !errors.Is(err, ErrUnitsOutsideRun) {
			t.Fatalf("unit of another run: err=%v, want ErrUnitsOutsideRun", err)
		}
		if _, err := loader.EstimateUnits(f.ctx, f.orgID, f.runID, []string{uuid.NewString()}); !errors.Is(err, ErrUnitsOutsideRun) {
			t.Fatalf("unknown unit: err=%v, want ErrUnitsOutsideRun", err)
		}
		for name, ids := range map[string][]string{"empty": nil, "not a uuid": {"unit-1"}} {
			if _, err := loader.EstimateUnits(f.ctx, f.orgID, f.runID, ids); !errors.Is(err, ErrInvalidReference) {
				t.Fatalf("%s: err=%v, want ErrInvalidReference", name, err)
			}
		}
		// A repeated id is one distinct unit, as len(set(unit_ids)).
		results, err := loader.EstimateUnits(f.ctx, f.orgID, f.runID, []string{unit, unit})
		if err != nil || len(results) != 1 {
			t.Fatalf("repeated id: results=%v err=%v", results, err)
		}
		if f.decryptor.calls.Load() != 0 {
			t.Fatalf("a refused reference decrypted %d credentials", f.decryptor.calls.Load())
		}
	})
}

func TestLoaderResolvesRunAuthLikeSyncTaskBootstrap(t *testing.T) {
	withLoaderFixture(t, "github", func(f *loaderFixture) {
		stamped := f.credential("github", true, `{"token": "stamped-token"}`)
		live := f.credential("github", true, `{"token": "live-token"}`)
		f.exec(`UPDATE public.integrations SET credential_id = $1`, live)
		flags := `{"sync_prs": 1}`
		unit := f.unit("github", "work-items", &flags)
		base := Context{
			Provider: "github", DatasetKey: "work-items", OrgID: f.orgID, IntegrationID: f.integID,
			ProcessorFlags: map[string]bool{"sync_prs": true},
		}
		since := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
		before := since.Add(72 * time.Hour)
		base.WindowStart, base.WindowEnd = &since, &before
		expect := func(credentialID *string, token string) []string {
			context := base
			context.CredentialID = credentialID
			mapping := newObject()
			mapping.set("token", token)
			context.Credentials = mapping
			return fingerprints(wantEstimates(t, context))
		}
		check := func(name string, loader Loader, want []string) {
			t.Helper()
			results, err := loader.EstimateUnits(f.ctx, f.orgID, f.runID, []string{unit})
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			result := results[unit]
			if result.Err != nil {
				t.Fatalf("%s: unit error %v", name, result.Err)
			}
			if got := fingerprints(result.Estimates); strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("%s:\n got %v\nwant %v", name, got, want)
			}
		}

		// Unstamped run (auth_source NULL): the integration's live credential.
		f.exec(`INSERT INTO public.sync_runs (id, org_id, credential_id) VALUES ($1, $2, $3)`, f.runID, f.orgID, stamped)
		check("unstamped", f.loader(nil), expect(&live, "live-token"))

		// Stamped run: the run's own credential, not the live one.
		f.exec(`UPDATE public.sync_runs SET auth_source = 'integration_credential'`)
		check("stamped", f.loader(nil), expect(&stamped, "stamped-token"))

		// Stamped with a matching witness: unchanged.
		witnessMapping := newObject()
		witnessMapping.set("token", "stamped-token")
		witness := RunAuthFingerprint(witnessMapping, &stamped, f.integID)
		f.exec(`UPDATE public.sync_runs SET credential_fingerprint = $1`, witness)
		check("matching witness strict", f.loader(map[string]string{"SYNC_RUN_AUTH_STRICT": "true"}), expect(&stamped, "stamped-token"))

		// A witness that no longer matches: warn and continue by default,
		// fail the unit under SYNC_RUN_AUTH_STRICT.
		f.exec(`UPDATE public.sync_runs SET credential_fingerprint = 'stale-witness'`)
		check("mismatch warn", f.loader(map[string]string{"SYNC_RUN_AUTH_STRICT": "no"}), expect(&stamped, "stamped-token"))
		results, err := f.loader(map[string]string{"SYNC_RUN_AUTH_STRICT": " ON "}).EstimateUnits(f.ctx, f.orgID, f.runID, []string{unit})
		if err != nil || results[unit].Err == nil || len(results[unit].Estimates) != 0 {
			t.Fatalf("mismatch strict: results=%+v err=%v, want a unit error and no estimates", results, err)
		}

		// Environment-stamped run (auth_source set, no credential):
		// environment credentials, credential_id None.
		f.exec(`UPDATE public.sync_runs SET credential_id = NULL, credential_fingerprint = NULL, auth_source = 'environment'`)
		check("environment stamped", f.loader(map[string]string{"GITHUB_TOKEN": "env-token"}), expect(nil, "env-token"))

		// Unstamped with no integration credential: environment too.
		f.exec(`UPDATE public.sync_runs SET auth_source = NULL`)
		f.exec(`UPDATE public.integrations SET credential_id = NULL`)
		check("unstamped environment", f.loader(map[string]string{"GITHUB_TOKEN": "env-token"}), expect(nil, "env-token"))

		// A stamped credential that is gone fails the unit only.
		f.exec(`UPDATE public.sync_runs SET auth_source = 'integration_credential', credential_id = $1`, uuid.NewString())
		results, err = f.loader(nil).EstimateUnits(f.ctx, f.orgID, f.runID, []string{unit})
		if err != nil || results[unit].Err == nil {
			t.Fatalf("missing credential: results=%+v err=%v, want a unit error", results, err)
		}
	})
}

func TestLoaderPerUnitFailuresStayPerUnit(t *testing.T) {
	withLoaderFixture(t, "pagerduty", func(f *loaderFixture) {
		active := f.credential("pagerduty", true, `{"subdomain": "acme", "region": "eu"}`)
		inactive := f.credential("pagerduty", false, `{"subdomain": "acme"}`)
		f.exec(`INSERT INTO public.sync_runs (id, org_id) VALUES ($1, $2)`, f.runID, f.orgID)
		f.exec(`UPDATE public.integrations SET credential_id = $1`, active)
		f.exec(`INSERT INTO public.integration_datasets (id, org_id, integration_id, dataset_key, options)
VALUES ($1, $2, $3, 'incident-alerts', '{"enrichment_cap": 250}')`, uuid.NewString(), f.orgID, f.integID)
		good := f.unit("pagerduty", "incident-alerts", nil)
		second := f.unit("pagerduty", "incidents", nil)

		results, err := f.loader(nil).EstimateUnits(f.ctx, f.orgID, f.runID, []string{good, second})
		if err != nil {
			t.Fatal(err)
		}
		alerts := results[good].Estimates
		if results[good].Err != nil || len(alerts) != 2 || alerts[1].EstimatedUnits != 600 || alerts[0].Bucket.Host != "api.eu.pagerduty.com" {
			t.Fatalf("dataset options not applied: %+v", results[good])
		}
		if calls := f.decryptor.calls.Load(); calls != 1 {
			t.Fatalf("two units on one credential decrypted %d times, want 1", calls)
		}

		// PagerDuty needs an active organization credential.
		f.exec(`UPDATE public.integrations SET credential_id = $1`, inactive)
		results, err = f.loader(nil).EstimateUnits(f.ctx, f.orgID, f.runID, []string{good})
		if err != nil || results[good].Err == nil {
			t.Fatalf("inactive PagerDuty credential: %+v %v", results, err)
		}
		f.exec(`UPDATE public.integrations SET credential_id = NULL`)
		results, err = f.loader(map[string]string{"PAGERDUTY_SUBDOMAIN": "env"}).EstimateUnits(f.ctx, f.orgID, f.runID, []string{good})
		if err != nil || results[good].Err == nil {
			t.Fatalf("PagerDuty without a credential: %+v %v", results, err)
		}
		f.exec(`UPDATE public.integrations SET credential_id = $1`, active)

		// Two dataset rows: .one_or_none() raises for that unit only.
		f.exec(`INSERT INTO public.integration_datasets (id, org_id, integration_id, dataset_key) VALUES ($1, $2, $3, 'incident-alerts')`,
			uuid.NewString(), f.orgID, f.integID)
		results, err = f.loader(nil).EstimateUnits(f.ctx, f.orgID, f.runID, []string{good, second})
		if err != nil || results[good].Err == nil || results[second].Err != nil || len(results[second].Estimates) == 0 {
			t.Fatalf("duplicate dataset rows: %+v %v", results, err)
		}

		// A ciphertext that does not decrypt fails the unit.
		f.exec(`UPDATE public.integration_credentials SET credentials_encrypted = 'v1:not-a-token' WHERE id = $1`, active)
		results, err = f.loader(nil).EstimateUnits(f.ctx, f.orgID, f.runID, []string{second})
		if err != nil || results[second].Err == nil || len(results[second].Estimates) != 0 {
			t.Fatalf("undecryptable credential: %+v %v", results, err)
		}

		// A source that is gone fails the unit.
		f.exec(`DELETE FROM public.integration_sources`)
		results, err = f.loader(nil).EstimateUnits(f.ctx, f.orgID, f.runID, []string{second})
		if err != nil || results[second].Err == nil {
			t.Fatalf("missing source: %+v %v", results, err)
		}
	})
}

func TestLoaderLinearPlaceholderInputs(t *testing.T) {
	withLoaderFixture(t, "linear", func(f *loaderFixture) {
		credential := f.credential("linear", true, `{"api_key": "k"}`)
		f.exec(`INSERT INTO public.sync_runs (id, org_id) VALUES ($1, $2)`, f.runID, f.orgID)
		f.exec(`UPDATE public.integrations SET credential_id = $1`, credential)
		unit := f.unit("linear", "work-items", nil)
		results, err := f.loader(nil).EstimateUnits(f.ctx, f.orgID, f.runID, []string{unit})
		if err != nil || results[unit].Err != nil || len(results[unit].Estimates) == 0 {
			t.Fatalf("linear unit: %+v %v", results, err)
		}
		// dict(source.metadata_) of a JSON list raises for a linear source.
		f.exec(`UPDATE public.integration_sources SET metadata = '[1]'`)
		results, err = f.loader(nil).EstimateUnits(f.ctx, f.orgID, f.runID, []string{unit})
		if err != nil || results[unit].Err == nil {
			t.Fatalf("non-mapping linear metadata: %+v %v", results, err)
		}
	})
}

type fakePagerDutyOAuth struct {
	calls atomic.Int64
	err   error
	seen  providerfoundation.Credential
}

func (f *fakePagerDutyOAuth) Hydrate(_ context.Context, _ providerfoundation.LeaseGuard, scope providerfoundation.TenantScope, credential providerfoundation.Credential) (providerfoundation.Credential, error) {
	f.calls.Add(1)
	f.seen = credential
	if scope.Provider != "pagerduty" || scope.OrgID == "" {
		return providerfoundation.Credential{}, errors.New("bad scope")
	}
	return credential, f.err
}

type fakeTokenDoer struct {
	calls  atomic.Int64
	status int
}

func (d *fakeTokenDoer) Do(request *http.Request) (*http.Response, error) {
	d.calls.Add(1)
	body := `{"access_token": "cc-token", "expires_in": 3600}`
	return &http.Response{
		StatusCode: d.status, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

// TestLoaderHydratesPagerDutyLikeResolveRunAuth pins hydrate_pagerduty_
// credentials' effect on the estimate: a unit whose hydration fails gets no
// estimate, one whose hydration succeeds gets the normal estimate, and a
// credential is hydrated once per batch.
func TestLoaderHydratesPagerDutyLikeResolveRunAuth(t *testing.T) {
	withLoaderFixture(t, "pagerduty", func(f *loaderFixture) {
		f.exec(`INSERT INTO public.sync_runs (id, org_id) VALUES ($1, $2)`, f.runID, f.orgID)
		first := f.unit("pagerduty", "incidents", nil)
		second := f.unit("pagerduty", "services", nil)
		use := func(plaintext string) {
			f.exec(`UPDATE public.integrations SET credential_id = $1`, f.credential("pagerduty", true, plaintext))
		}
		run := func(loader Loader) map[string]UnitResult {
			t.Helper()
			results, err := loader.EstimateUnits(f.ctx, f.orgID, f.runID, []string{first, second})
			if err != nil {
				t.Fatal(err)
			}
			return results
		}
		failed := func(results map[string]UnitResult) bool {
			return results[first].Err != nil && len(results[first].Estimates) == 0 &&
				results[second].Err != nil && len(results[second].Estimates) == 0
		}
		estimated := func(results map[string]UnitResult) bool {
			return results[first].Err == nil && len(results[first].Estimates) == 1 &&
				results[second].Err == nil && len(results[second].Estimates) == 1
		}

		oauth := &fakePagerDutyOAuth{}
		doer := &fakeTokenDoer{status: http.StatusOK}
		loader := func(env map[string]string) Loader {
			l := f.loader(env)
			l.PagerDutyOAuth, l.PagerDutyDoer = oauth, doer
			return l
		}

		// OAuth with PAGER_DUTY_CLIENT_ID unset: from_env() is None and
		// hydration raises, so neither unit is estimated (review round 1).
		use(`{"auth_mode": "oauth", "oauth_credential_name": "n", "oauth_binding_id": "b", "access_token": "stale", "subdomain": "acme"}`)
		if results := run(loader(nil)); !failed(results) || oauth.calls.Load() != 0 {
			t.Fatalf("oauth without app config: %+v, hydrator calls %d", results, oauth.calls.Load())
		}
		// App configured, token store answers: estimated, hydrated once for
		// two units, and without the stale access_token on the descriptor.
		appEnv := map[string]string{"PAGER_DUTY_CLIENT_ID": "app"}
		if results := run(loader(appEnv)); !estimated(results) || oauth.calls.Load() != 1 {
			t.Fatalf("oauth hydrated: %+v, hydrator calls %d", results, oauth.calls.Load())
		}
		if _, carried := oauth.seen.Secret("access_token"); carried {
			t.Fatal("the stored access_token reached the hydrator; Python always fetches a fresh one")
		}
		// The token store refuses (missing row, binding, scopes, refresh).
		oauth.err = errors.New("PagerDuty OAuth credential was not found")
		if results := run(loader(appEnv)); !failed(results) {
			t.Fatalf("oauth store refusal: %+v", results)
		}

		// Client credentials: every key present and the exchange succeeds.
		use(`{"auth_mode": "client_credentials", "client_id": "i", "client_secret": "s", "subdomain": "acme", "region": "us"}`)
		if results := run(loader(nil)); !estimated(results) || doer.calls.Load() != 1 {
			t.Fatalf("client credentials: %+v, token exchanges %d", results, doer.calls.Load())
		}
		// The exchange fails.
		doer.status = http.StatusUnauthorized
		if results := run(loader(nil)); !failed(results) {
			t.Fatalf("client credentials refused: %+v", results)
		}
		// region is indexed by Python (KeyError) even though Go defaults it.
		doer.status = http.StatusOK
		use(`{"auth_mode": "client_credentials", "client_id": "i", "client_secret": "s", "subdomain": "acme"}`)
		if results := run(loader(nil)); !failed(results) {
			t.Fatalf("client credentials without region: %+v", results)
		}

		// api_token needs no hydration.
		use(`{"auth_mode": "api_token", "api_token": "t", "subdomain": "acme"}`)
		before := oauth.calls.Load() + doer.calls.Load()
		if results := run(loader(nil)); !estimated(results) || oauth.calls.Load()+doer.calls.Load() != before {
			t.Fatalf("api token: %+v", results)
		}
	})
}
