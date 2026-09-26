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
// pointed at stripeBase ("" = Stripe's own base).
func startBillingVenueAPI(t *testing.T, ctx context.Context, cfg config.Config, venue *venueoracle.Venue, stripeBase string) string {
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
	deps := apiservice.Deps{
		Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, logger), Producer: producer,
		Stripe:        stripeclient.New(stripeclient.Options{Key: cfg.StripeSecretKey.Reveal(), BaseURL: stripeBase}),
		BillingConfig: cfg.APIBilling, StripeWebhookSecret: cfg.StripeWebhookSecret, LicensePrivateKey: cfg.LicensePrivateKey,
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

// goldenOracles names the oracles that have a golden; goldenPins holds their
// digests in the same order (the digests the recording run printed). They are
// two lists, not a name-to-digest map: a "...Key" oracle name next to a
// 64-hex value on one line reads as a credential to the secret scanner.
var goldenOracles = []string{
	"TestVenueOracleBillingLedger",
	"TestVenueOracleBillingPlansCheckout",
	"TestVenueOracleBillingWithoutStripeKey",
}

var goldenPins = []string{
	"086d39915f2bf1a97ce9a895c3e45d74496c0b24a4c1a91c4a55835468ff5f78",
	"62b01745c54ab42a2e714dab3140d895fbab9c83d978445003a0b185e0bbd0f4",
	"bc6f6a1cfd97cc65b0381e87742bd157b17306f0d9b77fa20cde18fad5464ff2",
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
