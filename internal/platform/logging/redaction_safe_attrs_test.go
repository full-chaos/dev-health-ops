package logging_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

// expectedRedactedSafeAttrKeys are the only real, currently-emitted
// SafeAttrs()/Attrs() keys that a key-name marker should catch. Every other
// enumerated key must reach the log unredacted. This is the ground truth the
// ticket's acceptance test needs: the real config-key surface, not a
// hypothetical one.
var expectedRedactedSafeAttrKeys = map[string]bool{
	// A boolean flag announcing whether a PagerDuty client secret is
	// configured. It never carries the secret value itself, but its name
	// contains the whole segment "secret", and the ruling keeps that segment
	// redacting on purpose (defense in depth for anything named like a
	// credential).
	"pagerduty_oauth_secret_configured": true,
}

// TestSafeAttrsRealConfigKeySetOnlyRedactsSensitiveKeys enumerates every key
// the two Config.SafeAttrs() implementations and version.Info.Attrs() -- the
// only attr emitters that feed the redacting JSON handler -- actually emit,
// and asserts the marker match fires on exactly the sensitive ones: an
// assertion against the real key set, not a one-off probe of two named
// flags.
func TestSafeAttrsRealConfigKeySetOnlyRedactsSensitiveKeys(t *testing.T) {
	t.Parallel()

	authCfg, err := authconfig.Load(authconfig.Spec{
		LookupEnv: func(key string) (string, bool) {
			values := map[string]string{
				authconfig.EnvDatabaseURI:    "postgres://auth@localhost:5432/devhealth",
				authconfig.EnvSigningKeyFile: "/run/secrets/auth-signing-key.pem",
				authconfig.EnvSigningKeyID:   "auth-signing-key-v1",
			}
			value, ok := values[key]
			return value, ok
		},
	})
	if err != nil {
		t.Fatalf("authconfig.Load: %v", err)
	}

	workerCfg, err := config.Load(config.Spec{
		Service:   "probe-worker",
		LookupEnv: func(string) (string, bool) { return "", false },
	})
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	var attrs []slog.Attr
	attrs = append(attrs, authCfg.SafeAttrs()...)
	attrs = append(attrs, workerCfg.SafeAttrs()...)
	attrs = append(attrs, version.Current("probe-service").Attrs()...)
	if len(attrs) < 20 {
		t.Fatalf("only %d attrs enumerated -- SafeAttrs()/Attrs() likely changed shape", len(attrs))
	}

	for _, attr := range attrs {
		var output bytes.Buffer
		logger := logging.NewJSON(&output, slog.LevelDebug)
		logger.Info("probe", attr.Key, attr.Value.Resolve().Any())

		gotRedacted := strings.Contains(output.String(), fmt.Sprintf("%q:\"[REDACTED]\"", attr.Key))
		wantRedacted := expectedRedactedSafeAttrKeys[attr.Key]
		if gotRedacted != wantRedacted {
			t.Errorf(
				"key %q: redacted=%v, want %v (line: %s)",
				attr.Key, gotRedacted, wantRedacted, output.String(),
			)
		}
	}
}
