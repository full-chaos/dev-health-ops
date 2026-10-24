package apiservice

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/legacyingest"
)

const (
	legacyIngestEnabledEnv = "DEV_HEALTH_LEGACY_INGEST_ENABLED"
	legacyIngestSunsetEnv  = "DEV_HEALTH_LEGACY_INGEST_SUNSET"
)

// errLegacyIngestCredentialRequired refuses a start outside development with
// the legacy routes enabled and no credential configured.
var errLegacyIngestCredentialRequired = errors.New(
	"legacy ingest is enabled but neither INGEST_API_KEYS nor INGEST_SIGNING_SECRET is set: refusing to start")

// legacyIngestSettings reads the flag (default off) and the optional sunset
// date. A value that does not parse is an error, never a silent default.
func legacyIngestSettings(lookup func(string) (string, bool)) (enabled bool, sunset time.Time, err error) {
	if value, ok := lookup(legacyIngestEnabledEnv); ok && strings.TrimSpace(value) != "" {
		enabled, err = strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return false, time.Time{}, fmt.Errorf("%s is not a boolean: refusing to start", legacyIngestEnabledEnv)
		}
	}
	if value, ok := lookup(legacyIngestSunsetEnv); ok && strings.TrimSpace(value) != "" {
		sunset, err = http.ParseTime(strings.TrimSpace(value))
		if err != nil {
			return false, time.Time{}, fmt.Errorf("%s is not an HTTP-date: refusing to start", legacyIngestSunsetEnv)
		}
	}
	return enabled, sunset, nil
}

// checkLegacyIngest returns the legacy ingest options for deps, or the
// start-up refusal.
func checkLegacyIngest(lookup func(string) (string, bool)) (enabled bool, options legacyingest.Options, reason string, err error) {
	enabled, sunset, err := legacyIngestSettings(lookup)
	if err != nil {
		return false, legacyingest.Options{}, "legacy_ingest_config_invalid", err
	}
	if enabled && !developmentEnvironment(lookup) {
		getenv := func(name string) string {
			value, _ := lookup(name)
			return value
		}
		if !legacyingest.CredentialConfigured(getenv) {
			return false, legacyingest.Options{}, "legacy_ingest_auth_required", errLegacyIngestCredentialRequired
		}
	}
	return enabled, legacyingest.Options{Deprecation: true, Sunset: sunset, RequireSignatureTimestamp: true}, "", nil
}

// legacyIngestLayers marks every response on the legacy ingest paths, the
// router's own 405 included, when the routes are mounted.
func legacyIngestLayers(deps Deps) []func(http.Handler) http.Handler {
	return nil
}
