package syncreconciler

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
)

// TestBucketAdvisoryLockKeyMatchesAcrossEveryGoCopy pins the cross-package
// invariant behind CHAOS-4586's round-5 fix (mutual exclusion between
// dispatch's claimUnits and this package's LeaseRepair.Step /
// UnreclaimableSweep.Step, via a shared pg_advisory_xact_lock key): all
// THREE independent Go implementations of the (orgID, provider, costClass)
// bucket advisory-lock key formula --
// syncdispatchruntime.bucketAdvisoryLockKey (exposed to this test only via
// BucketAdvisoryLockKeyForTest), this package's leaseRepairBucketAdvisoryID
// (lease_repair.go), and this package's unreclaimableBucketAdvisoryID
// (lock_order.go, added for round 5) -- must compute the IDENTICAL numeric
// key for the same input, or LeaseRepair.Step/UnreclaimableSweep.Step and
// dispatch's AuthorizeRun would take DIFFERENT Postgres advisory locks for
// what is meant to be the SAME bucket, achieving no mutual exclusion at all
// while looking like they do.
//
// Python needs no equivalent test: guard.py's _bucket_advisory_lock_key
// (src/dev_health_ops/sync/guard.py:415) is ONE function, imported and
// reused as-is by both dispatch (guard.py itself) and lease-repair
// (workers/sync_reconciler.py:26,198) -- there is only ever one Python
// value to begin with. Only the Go ports duplicated the formula, once per
// package/site, because dispatchBucket's fields are unexported and Go has
// no way to "import one private helper" the way Python imports one
// function; this test is what makes that duplication safe to keep instead
// of a byte-for-byte source reading (which caught nothing new when the
// third copy, unreclaimableBucketAdvisoryID, was added and could just as
// easily miss a future one-copy-only edit).
func TestBucketAdvisoryLockKeyMatchesAcrossEveryGoCopy(t *testing.T) {
	tests := []struct {
		name      string
		orgID     string
		provider  string
		costClass string
	}{
		{"lease-repair contract fixture", "org-a", "linear", "standard"},
		{"github provider", "org-acme", "github", "standard"},
		{"empty cost class", "org-b", "jira", ""},
		{"colon-bearing org id", "org:with:colons", "linear", "premium"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dispatchKey := syncdispatchruntime.BucketAdvisoryLockKeyForTest(test.orgID, test.provider, test.costClass)
			leaseRepairKey := leaseRepairBucketAdvisoryID(test.orgID, test.provider, test.costClass)
			unreclaimableKey := unreclaimableBucketAdvisoryID(test.orgID, test.provider, test.costClass)

			if dispatchKey != leaseRepairKey {
				t.Errorf("syncdispatchruntime.bucketAdvisoryLockKey = %d, leaseRepairBucketAdvisoryID = %d -- "+
					"dispatch's AuthorizeRun and LeaseRepair.Step would take DIFFERENT advisory locks for the same bucket",
					dispatchKey, leaseRepairKey)
			}
			if dispatchKey != unreclaimableKey {
				t.Errorf("syncdispatchruntime.bucketAdvisoryLockKey = %d, unreclaimableBucketAdvisoryID = %d -- "+
					"dispatch's AuthorizeRun and UnreclaimableSweep.Step would take DIFFERENT advisory locks for the same bucket, "+
					"reopening the codex round-5 deadlock this lock exists to close",
					dispatchKey, unreclaimableKey)
			}
		})
	}
}

// TestLeaseRepairAndUnreclaimableBucketAdvisoryIDsMatchThePinnedPythonContract
// extends TestLeaseRepairConfigDefaultsAndBucketHashMatchPythonContract's
// existing single-value pin (leaseRepairBucketAdvisoryID("org-a", "linear",
// "standard") == 3882165252103971925) to the round-5 sweep copy, so the
// literal Python-contract value is pinned against BOTH of this package's
// copies, not just the older one.
func TestLeaseRepairAndUnreclaimableBucketAdvisoryIDsMatchThePinnedPythonContract(t *testing.T) {
	const want = 3882165252103971925
	if got := leaseRepairBucketAdvisoryID("org-a", "linear", "standard"); got != want {
		t.Fatalf("leaseRepairBucketAdvisoryID(\"org-a\", \"linear\", \"standard\") = %d, want %d", got, want)
	}
	if got := unreclaimableBucketAdvisoryID("org-a", "linear", "standard"); got != want {
		t.Fatalf("unreclaimableBucketAdvisoryID(\"org-a\", \"linear\", \"standard\") = %d, want %d", got, want)
	}
}
