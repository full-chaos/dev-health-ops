package llmorgsettings

// loadRawSettings' Decrypt() failure branch used to be a bare `continue`:
// a wrong SETTINGS_ENCRYPTION_KEY and a genuinely corrupt row were
// indistinguishable from the rest of the org's settings resolving fine,
// with zero signal at any layer (see investment_explain_route.go's own
// doc comment, which named this exact gap as still open). This file
// closes it with two signals:
//
//   - decryptFailureCounter/defaultRecordDecryptFailure: every failed
//     Decrypt() call, by org, plus an ERROR log line -- the row is still
//     skipped (Python parity, `except ValueError: continue`), it is just
//     no longer silent about it.
//   - keyValidationFailedCounter/defaultCheckKeyOnFirstDecrypt: the
//     settings table has no fingerprint/version column to check the
//     configured key against without a schema change, so the closest
//     "known-good ciphertext" available is the first row this PROCESS
//     ever actually attempts to decrypt. If that first attempt fails,
//     that is the signature of a wrong key (every later row from the
//     same key fails the same way), not one bad row -- worth its own
//     elevated, once-per-process signal distinct from the ordinary
//     per-row one above. Gated on the first ATTEMPT (success or
//     failure), not the first failure, so a later corrupt row -- after
//     the key has already proven itself against an earlier row -- does
//     not retroactively look like a key problem.
import (
	"context"
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func mustLLMOrgSettingsCounter(name, description string) metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/internal/llmorgsettings")
	counter, err := meter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		// Same otel guarantee routeswitch/telemetry.go and
		// analytics/telemetry.go rely on: the instrument constructor
		// never returns a nil instrument even on error, so a broken
		// meter provider must not panic a request path over an
		// observability concern.
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}

var decryptFailureCounter = mustLLMOrgSettingsCounter(
	"devhealth_llmorgsettings_decrypt_failure_total",
	"Encrypted org LLM setting rows whose Decrypt() call failed (wrong SETTINGS_ENCRYPTION_KEY or a corrupt row), by org_id. loadRawSettings still skips the row (Python parity), but this used to be entirely silent.",
)

var keyValidationFailedCounter = mustLLMOrgSettingsCounter(
	"devhealth_llmorgsettings_key_validation_failed_total",
	"Fires at most once per process: the first encrypted org LLM setting this process attempted to decrypt failed -- the signature of a misconfigured SETTINGS_ENCRYPTION_KEY rather than one corrupt row.",
)

// recordDecryptFailure is a package var, not a plain func -- same
// reasoning as routeswitch.recordDigestMiss and
// analytics.recordDegradation: the metric+log firing is the only
// observable that distinguishes an instrumented decrypt failure from the
// silent skip this package used to do, so a test must be able to
// substitute a spy here.
var recordDecryptFailure = defaultRecordDecryptFailure

func defaultRecordDecryptFailure(ctx context.Context, orgID string) {
	decryptFailureCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("org_id", orgID)))
	slog.ErrorContext(ctx, "llmorgsettings: failed to decrypt an encrypted org LLM setting; skipping the row",
		"component", "llmorgsettings", "org_id", orgID)
}

var firstDecryptAttempt sync.Once

// checkKeyOnFirstDecrypt is called for EVERY decrypt attempt (success or
// failure) but only ever acts on the first one this process makes -- see
// the file doc comment above for why gating on the first attempt, not the
// first failure, matters.
var checkKeyOnFirstDecrypt = defaultCheckKeyOnFirstDecrypt

func defaultCheckKeyOnFirstDecrypt(ctx context.Context, orgID string, decryptErr error) {
	firstDecryptAttempt.Do(func() {
		if decryptErr == nil {
			return
		}
		keyValidationFailedCounter.Add(ctx, 1)
		slog.ErrorContext(ctx,
			"llmorgsettings: SETTINGS_ENCRYPTION_KEY failed to decrypt the first encrypted org LLM setting this process attempted; the configured key is likely wrong, not just this one row corrupt",
			"component", "llmorgsettings", "org_id", orgID)
	})
}

// resetFirstDecryptAttemptForTest lets a test observe
// defaultCheckKeyOnFirstDecrypt's first-attempt behavior more than once
// within the same test binary run; sync.Once has no public reset.
func resetFirstDecryptAttemptForTest() {
	firstDecryptAttempt = sync.Once{}
}
