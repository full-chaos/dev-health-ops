package apiservice

import "github.com/full-chaos/dev-health-ops/internal/api/legacyingest"

const (
	legacyIngestEnabledEnv = "DEV_HEALTH_LEGACY_INGEST_ENABLED"
	legacyIngestSunsetEnv  = "DEV_HEALTH_LEGACY_INGEST_SUNSET"
)

func checkLegacyIngest(func(string) (string, bool)) (bool, legacyingest.Options, string, error) {
	return true, legacyingest.Options{}, "", nil
}
