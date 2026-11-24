package providersync

import (
	"github.com/full-chaos/dev-health-ops/internal/artifactexclude"
)

// githubTestsConfigExcludePrefixes is the sync configuration's own artifact name prefixes, from the integration config
// the unit carries (artifactexclude.Key, written by the admin API and the dho verb). A value the validator refuses
// excludes nothing and is reported as invalid, so the caller can log that once; the code list still applies.
func githubTestsConfigExcludePrefixes(claim Claim) []string {
	prefixes, _ := githubTestsConfigExcludePrefixesChecked(claim)
	return prefixes
}

func githubTestsConfigExcludePrefixesChecked(claim Claim) (prefixes []string, invalid bool) {
	value, present := claim.IntegrationConfig[artifactexclude.Key]
	if !present {
		return nil, false
	}
	change, err := artifactexclude.ParseDecoded(value)
	if err != nil {
		return nil, true
	}
	return change.Prefixes, false
}
