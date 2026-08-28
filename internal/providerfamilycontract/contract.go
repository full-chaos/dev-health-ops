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
	// FoldContributing families (CHAOS-4078: PR-social prs/pr-reviews/
	// pr-comments -> prs, TestOps cicd/tests -> cicd) fold an alias-only
	// selection onto their canonical writer, but -- unlike AtomicCanonical --
	// membership is not all-or-nothing: a canonical claim may carry any
	// non-empty subset of its family's flags (including none, for a
	// canonical-only selection with no enabled aliases).
	FoldContributing ExecutionMode = "fold_contributing"
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
	{
		Mode:             FoldContributing,
		CanonicalDataset: "prs",
		Datasets:         []string{"prs", "pr-reviews", "pr-comments"},
		providers:        stringSet("github", "gitlab"),
	},
	{
		Mode:             FoldContributing,
		CanonicalDataset: "cicd",
		Datasets:         []string{"cicd", "tests"},
		providers:        stringSet("github", "gitlab"),
	},
}

// FamilyMembers returns the collapsible family's full canonical-order member
// list for a CANONICAL dataset key (e.g. "cicd" -> ["cicd", "tests"], or
// "prs" -> ["prs", "pr-reviews", "pr-comments"]), or false if `dataset` is
// not a fold/atomic family's canonical identity. Independent families (e.g.
// PagerDuty "incidents") are excluded: their membership is catalogued for
// ownership/tests only and was never meant to collapse execution or
// coverage math.
//
// Provider-agnostic on purpose (CHAOS-4393): coverage math sees only a
// persisted SyncRunUnit's raw dataset_key and processor_flags -- never its
// provider -- and no two providers disagree about what a given canonical
// dataset's family contains, so scanning every policy regardless of its
// provider set is safe.
func FamilyMembers(dataset string) ([]string, bool) {
	dataset = normalize(dataset)
	for _, policy := range policies {
		if policy.Mode == Independent {
			continue
		}
		if policy.CanonicalDataset == dataset {
			return slices.Clone(policy.Datasets), true
		}
	}
	return nil, false
}

// FamilyCanonical returns the canonical dataset key that owns `dataset`'s
// collapsible family, if `dataset` is a member of any fold/atomic family
// (including its own canonical identity, which maps to itself). See
// FamilyMembers for why this is provider-agnostic.
func FamilyCanonical(dataset string) (string, bool) {
	dataset = normalize(dataset)
	for _, policy := range policies {
		if policy.Mode == Independent {
			continue
		}
		for _, member := range policy.Datasets {
			if member == dataset {
				return policy.CanonicalDataset, true
			}
		}
	}
	return "", false
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
		expected[familyDatasetFlag(familyDataset)] = struct{}{}
	}
	// Any family_dataset_* flag present on this claim must belong to THIS
	// family -- a canonical "prs" claim carrying "family_dataset_tests"
	// (TestOps' own flag) fails closed here rather than sailing through to
	// provider execution and only being caught (if at all) by the
	// completion-side resolver, after work already happened.
	for flag := range processorFlags {
		if !strings.HasPrefix(flag, familyDatasetFlagPrefix) {
			continue
		}
		if _, known := expected[flag]; !known {
			return ErrInvalidClaim
		}
	}
	if policy.Mode == FoldContributing {
		// Non-atomic by design (CHAOS-4078): any subset of this family's own
		// flags is valid, including none (a canonical-only selection with no
		// enabled aliases). The cross-family check above already ran; each
		// PRESENT flag must still be literal true -- a Go map[string]bool can
		// store an explicit false, and a malformed claim carrying
		// "family_dataset_tests": false must fail closed here rather than
		// having completion silently treat it as absent.
		for flag := range expected {
			if value, present := processorFlags[flag]; present && !value {
				return ErrInvalidClaim
			}
		}
		return nil
	}
	for flag := range expected {
		if !processorFlags[flag] {
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
