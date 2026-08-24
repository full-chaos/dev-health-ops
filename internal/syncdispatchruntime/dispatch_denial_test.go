package syncdispatchruntime

import (
	"strings"
	"testing"
)

func TestIsAtomicProviderFamilyDirectAlias(t *testing.T) {
	t.Run("a non-canonical family member is a direct alias", func(t *testing.T) {
		if !isAtomicProviderFamilyDirectAlias("github", "work-item-labels") {
			t.Fatal("want true -- work-item-labels is a non-canonical atomic-family member")
		}
	})
	t.Run("the canonical dataset itself is NOT a direct alias", func(t *testing.T) {
		if isAtomicProviderFamilyDirectAlias("github", "work-items") {
			t.Fatal("want false -- work-items IS the canonical claim, not an alias of it")
		}
	})
	t.Run("an Independent-mode family is never a direct alias", func(t *testing.T) {
		if isAtomicProviderFamilyDirectAlias("pagerduty", "incident-notes") {
			t.Fatal("want false -- pagerduty's incident family is Independent mode, not atomic_canonical")
		}
	})
	t.Run("an unknown pair is not a direct alias", func(t *testing.T) {
		if isAtomicProviderFamilyDirectAlias("unknown-provider", "unknown-dataset") {
			t.Fatal("want false -- no policy exists for this pair")
		}
	})
}

func TestUnroutableReason(t *testing.T) {
	t.Run("an atomic-family alias names the family cause", func(t *testing.T) {
		reason := unroutableReason("github", "work-item-labels")
		if !strings.Contains(reason, "non-canonical member of an atomic provider family") {
			t.Fatalf("reason=%q missing the family-alias cause", reason)
		}
	})
	t.Run("a non-family pair names the capability-matrix cause", func(t *testing.T) {
		reason := unroutableReason("unknown-provider", "unknown-dataset")
		if !strings.Contains(reason, "capability matrix does not mark it route-ready") {
			t.Fatalf("reason=%q missing the capability-matrix cause", reason)
		}
	})
}
