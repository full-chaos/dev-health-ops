//go:build integration

// Package billingvenue holds the billing venue oracles (plans, checkout,
// portal, ledger, the Stripe webhook, and the test-mode leg), in a package of
// their own so they neither sit in nor extend internal/apiservice's venue
// time budget.
package billingvenue

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/webhookintake"
	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// venueKey is the venue's JWT signing key (a fixed test value).
const venueKey = "venue-oracle-signing-key-0123456789abcdef"

// venueUUID matches a UUID in a response body or a row.
var venueUUID = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

func venueRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// venueLogs is where the Go api's own log lines land in a venue: every test
// may read them (a test that does resets it first).
var venueLogs = &logSink{}

// logSink is a goroutine-safe buffer for slog.
type logSink struct {
	mu   sync.Mutex
	text strings.Builder
}

func (l *logSink) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.text.Write(p)
}

func (l *logSink) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.text.Reset()
}

// Lines are the log lines so far.
func (l *logSink) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Split(strings.TrimSpace(l.text.String()), "\n")
}

// startVenueAPI serves the Go plane for venue as dho api wires it (the
// routes, the org scope and impersonation middlewares, security headers and
// CORS), connected as the api role, after proving that role holds exactly
// its declared grants.
func startVenueAPI(t *testing.T, ctx context.Context, cfg config.Config, venue *venueoracle.Venue) string {
	t.Helper()
	return startBillingVenueAPI(t, ctx, cfg, venue, "")
}

// startBillingVenueAPI is startVenueAPI with the Go plane's Stripe client
// pointed at stripeBase ("" = Stripe's own base). now optionally injects the
// billing routes' clock (a frozen golden's webhook signature timestamp check
// must judge a replay at the instant it was recorded for, not the instant it
// happens to replay at, see recordedAt); omitted or nil, it is time.Now, same
// as every existing caller that does not pass one.
func startBillingVenueAPI(t *testing.T, ctx context.Context, cfg config.Config, venue *venueoracle.Venue, stripeBase string, now ...func() time.Time) string {
	t.Helper()
	pool, err := pgxpool.New(ctx, cfg.APIDatabaseURI.Reveal())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := postgres.CheckAPIAuthorization(ctx, pool, cfg.APIDatabaseRole, cfg.RiverDatabaseSchema); err != nil {
		t.Fatalf("dho api not ready as the api role: %v %s", err, venue.DiagnoseAPIRole(t, ctx))
	}
	logger := slog.New(slog.NewTextHandler(venueLogs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	verifier, err := edgetoken.New(cfg.APIJWTSecret.Reveal(), cfg.APIJWTIssuer, cfg.APIJWTAudience)
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := webhookintake.LoadJobRegistry(filepath.Join(venueRoot(), "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	producer, err := joboutbox.NewProducer(pool, registry)
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Now
	if len(now) > 0 && now[0] != nil {
		clock = now[0]
	}
	deps := apiservice.Deps{
		Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, logger), Producer: producer,
		Stripe:        stripeclient.New(stripeclient.Options{Key: cfg.StripeSecretKey.Reveal(), BaseURL: stripeBase}),
		BillingConfig: cfg.APIBilling, StripeWebhookSecret: cfg.StripeWebhookSecret, LicensePrivateKey: cfg.LicensePrivateKey,
		Now: clock,
	}
	scope := policy.NewScope(auth, logger)
	server, err := apiservice.NewServer(cfg, logger, apiservice.Routes(deps, logger), scope.OrgScope, scope.Impersonation)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

// venueNamespace makes the venue's fixture ids the same in every run: a
// recording of the Python plane (venueoracle.Frozen) holds the seeded ids
// literally, so the run that replays it must seed the same ones.
var venueNamespace = uuid.MustParse("6f6d0b50-8f0e-4a55-9c31-2a5cbe0a6817")

// venueID is the fixture id named label, stable across runs.
func venueID(label string) uuid.UUID { return uuid.NewSHA1(venueNamespace, []byte(label)) }

// resetSeedSequence starts the sequence the seed SQL draws its row ids from
// (md5(nextval(...))::uuid, in place of gen_random_uuid()), so the rows a seed
// inserts get the same ids in every run.
func resetSeedSequence(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	for _, statement := range []string{`DROP SEQUENCE IF EXISTS venue_seed_seq`, `CREATE SEQUENCE venue_seed_seq`} {
		if _, err := pool.Exec(ctx, statement); err != nil {
			t.Fatalf("seed sequence: %v", err)
		}
	}
}

// pythonBuild is the last main commit that still carried the Python billing
// route bodies: the build the frozen goldens' Python answers were executed on.
const pythonBuild = "023ae3e584ef6345a3bfaef1f0b30abd89f5dee8"

// webhookPythonBuild is the commit the CHAOS-7032 webhook goldens' Python
// answers were executed on: the last main commit before the Python Stripe
// webhook route (billing/router.py's stripe_webhook, CHAOS-6258) is deleted.
// Deliberately its OWN const, not pythonBuild above: the billing route bodies
// pythonBuild's goldens froze (CHAOS-6924/6859) may have changed between that
// pin and this one, and reusing one shared build for both would either force
// a pointless re-record of the four already-verified goldens or silently
// pin them to a build their own frozen files were never executed on.
const webhookPythonBuild = "c3a755abd3b30ba41625178c6bb00e1e2c219fcb"

// goldenOracles names the oracles that have a golden; goldenPins holds their
// digests in the same order (the digests the recording run printed). They are
// two lists, not a name-to-digest map: a "...Key" oracle name next to a
// 64-hex value on one line reads as a credential to the secret scanner.
var goldenOracles = []string{
	"TestVenueOracleBillingEdge",
	"TestVenueOracleBillingLedger",
	"TestVenueOracleBillingPlansCheckout",
	"TestVenueOracleBillingWithoutStripeKey",
	// CHAOS-7032: the four webhook oracles that still called venue.ServePython
	// directly, frozen before the Python Stripe webhook route is deleted
	// (CHAOS-6258/7033). Each pin below starts as goldenrecord's own
	// placeholder format (PIN:<test name>) and is find-and-replaced with the
	// real digest by the record verb at promotion time -- never hand-edited.
	"TestVenueOracleBillingWebhook",
	"TestInvoiceWebhookAppliesTestModeEvents",
	"TestRefundEventOwnershipGrid",
	"TestRefundEventSettledGrid",
}

var goldenPins = []string{
	"7076bb567067211c66de89db88d0a3a7d17d04ee65a033042a5b169ba45cd629",
	"11a31b73914c5ddbf3853383489d18a71f20efc090d136219321adbdc5aa88a8",
	"7bbaaf09dc400706d9b2c7de9d9428e14d7aedddfdf0e576d81350ce25a4f4ef",
	"8b5ff7eeb5f773ef9ce637e4e9ba1b120abe24aa0b539baddd0a14459eb5dd88",
	"64a08d0d9f7fe52820b233cf61408a3883a1652ed1bbcc4ce55dbd34401bca3a",
	"1927064902f678dd9c70d3e70340d1f31e2c9325f4948f8ff9a870245b6f3ddb",
	"ce76033bbc6141d2f562e1ac8e65bdfbf73f12ff53685d52a28db8f8bd51c608",
	"39c2ae7806d89991172e4cc1fe53c279b605a79dac52802192c2a400ca1046ba",
}

// goldenDigest is the pinned digest of the golden of the oracle called name.
func goldenDigest(name string) string {
	for index, oracle := range goldenOracles {
		if oracle == name {
			return goldenPins[index]
		}
	}
	return ""
}

// goldenSpec is the frozen Python golden of the test called name; sha is the
// digest of the file the test pins.
func goldenSpec(name, sha string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        filepath.Join("testdata", "golden", name+".json"),
		PythonBuild: pythonBuild,
		SHA256:      sha,
		Recipe: "git worktree add --detach <dir> " + pythonBuild + "; from internal/apiservice/billingvenue: DHO_VENUE_GOLDEN_UPDATE=1 " +
			"DHO_VENUE_GOLDEN_PYTHON_ROOT=<dir> DEV_HEALTH_LIVE_PYTHON_ORACLES=1 go test -tags=integration -count=1 -run '^" + name + "$' .",
	}
}

// webhookGoldenSpec is goldenSpec for the CHAOS-7032 webhook oracles: same
// shape, pinned to webhookPythonBuild instead of pythonBuild (see its own
// comment for why they must not share one build const).
func webhookGoldenSpec(name, sha string) venueoracle.GoldenSpec {
	return venueoracle.GoldenSpec{
		Path:        filepath.Join("testdata", "golden", name+".json"),
		PythonBuild: webhookPythonBuild,
		SHA256:      sha,
		Recipe: "git worktree add --detach <dir> " + webhookPythonBuild + "; from internal/apiservice/billingvenue: DHO_VENUE_GOLDEN_UPDATE=1 " +
			"DHO_VENUE_GOLDEN_PYTHON_ROOT=<dir> DEV_HEALTH_LIVE_PYTHON_ORACLES=1 go test -tags=integration -count=1 -run '^" + name + "$' .",
	}
}

// recordedAt is the wall clock of the run that recorded the golden (recorded
// itself, so a frozen run reads it back): the Python answers were normalized
// against it, the Go ones against this run's clock.
func recordedAt(t *testing.T, golden *venueoracle.Golden, now time.Time) time.Time {
	t.Helper()
	text := golden.InspectRows(t, "recorded_at", func() string { return now.Format(time.RFC3339Nano) })
	at, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		t.Fatalf("golden recorded_at %q: %v", text, err)
	}
	return at
}

// normalizeAnswers normalizes each Python answer's body with normalize.
func normalizeAnswers(answers []venueoracle.Response, normalize func(string) string) []venueoracle.Response {
	out := make([]venueoracle.Response, len(answers))
	for index, answer := range answers {
		out[index] = answer
		out[index].Body = normalize(answer.Body)
	}
	return out
}

// pythonCalls is the Stripe calls the Python plane made: live while recording,
// the golden's text otherwise.
func pythonCalls(t *testing.T, golden *venueoracle.Golden, fake *fakeStripe) []string {
	t.Helper()
	text := golden.InspectRows(t, "stripe_calls_py", func() string {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		return strings.Join(fake.calls["py"], "\n")
	})
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}
