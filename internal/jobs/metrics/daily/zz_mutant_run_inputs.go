package daily

import "github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/compoundingrisk"

// MUTANT (never merged): each finalize family reuses the inputs it first
// loaded for a run id, the "reuse the original run's data" defect.
var (
	mutantCognitiveInputs  = map[string][]userMetricsCognitiveLoadInput{}
	mutantComplexityInputs = map[string][]repoComplexityInput{}
	mutantRiskInputs       = map[string][]compoundingrisk.RepoMetricsRow{}
	mutantCapabilityRun    string
	mutantCapabilityInputs = map[string][]workItemScopedRow{}
)
