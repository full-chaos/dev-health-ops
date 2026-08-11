// Package providerfamilycontract owns provider-neutral execution-family
// admission without changing each provider's D16 unit boundary.
package providerfamilycontract

import (
	"errors"
	"slices"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

type ExecutionMode string

const (
	AtomicCanonical ExecutionMode = "atomic_canonical"
	Independent     ExecutionMode = "independent"
)

var ErrInvalidClaim = errors.New("provider family claim is invalid")

const familyDatasetFlagPrefix = "family_dataset_"

// Policy declares one related dataset family. AtomicCanonical families admit
// exactly one canonical claim once their Go family switch is enabled.
// Independent families remain one claim per dataset; their membership is
// catalogued for ownership and tests, not used to collapse execution.
type Policy struct {
	Mode             ExecutionMode
	CanonicalDataset string
	Datasets         []string
	providers        map[string]struct{}
}

var policies = []Policy{
	{
		Mode:             AtomicCanonical,
		CanonicalDataset: "work-items",
		Datasets:         workitemcontract.FamilyDatasets(),
		providers: stringSet(
			"github", "gitlab", "jira", "linear",
		),
	},
	{
		Mode:             Independent,
		CanonicalDataset: "incidents",
		Datasets: []string{
			"incidents", "incident-alerts", "incident-log-entries", "incident-notes",
		},
		providers: stringSet("pagerduty"),
	},
}

func stringSet(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

// PolicyFor returns a defensive copy of the family policy containing the
// provider/dataset pair.
func PolicyFor(provider, dataset string) (Policy, bool) {
	provider = normalize(provider)
	dataset = normalize(dataset)
	for _, policy := range policies {
		if _, ok := policy.providers[provider]; !ok || !slices.Contains(policy.Datasets, dataset) {
			continue
		}
		policy.Datasets = slices.Clone(policy.Datasets)
		policy.providers = nil
		return policy, true
	}
	return Policy{}, false
}

// ValidateClaim enforces an atomic policy only after its Go family switch is
// enabled. Default-off claims keep their existing legacy shape. Independent
// families always preserve their per-dataset D16 boundary.
func ValidateClaim(
	provider string,
	dataset string,
	processorFlags map[string]bool,
	strictAtomic bool,
) error {
	policy, ok := PolicyFor(provider, dataset)
	if !ok || policy.Mode == Independent || !strictAtomic {
		return nil
	}
	if normalize(dataset) != policy.CanonicalDataset {
		return ErrInvalidClaim
	}
	expected := make(map[string]struct{}, len(policy.Datasets))
	for _, familyDataset := range policy.Datasets {
		flag := familyDatasetFlag(familyDataset)
		expected[flag] = struct{}{}
		if !processorFlags[flag] {
			return ErrInvalidClaim
		}
	}
	for flag := range processorFlags {
		if !strings.HasPrefix(flag, familyDatasetFlagPrefix) {
			continue
		}
		if _, known := expected[flag]; !known {
			return ErrInvalidClaim
		}
	}
	return nil
}

func familyDatasetFlag(dataset string) string {
	return familyDatasetFlagPrefix + strings.ReplaceAll(dataset, "-", "_")
}

func normalize(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
