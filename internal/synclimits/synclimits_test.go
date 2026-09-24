package synclimits

import "testing"

// TestAdvisoryLockKeyMatchesPython pins the advisory-lock key against
// values computed with the Python api's own formula
// (`uuid.UUID(org_id).int & ((1<<63)-1)`, falling back to
// `uuid.uuid5(uuid.NAMESPACE_URL, org_id).int & ((1<<63)-1)` for an org id
// uuid.UUID() refuses). The key MUST agree across languages: the Python
// create path's repo-limit preflight and every Go writer serialize on it.
// The values are the ones this function returned before it moved here from
// the scheduler, extended to the mask and zero edges and to non-UUID ids.
func TestAdvisoryLockKeyMatchesPython(t *testing.T) {
	for _, test := range []struct {
		orgID string
		want  int64
	}{
		{"11111111-1111-1111-1111-111111111111", 1229782938247303441},
		{"70d529e0-3c06-4597-8480-794fd02328b6", 324392556872018102},
		{"not-a-uuid-org", 1247898447986800358},
		{"c6a38355-0000-4000-8000-ffffffffffff", 281474976710655},
		{"FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF", 9223372036854775807},
		{"00000000-0000-0000-0000-000000000000", 0},
		{"", 1288089823879393781},
		{"org é", 3513486620798708324},
	} {
		if got := AdvisoryLockKey(test.orgID); got != test.want {
			t.Errorf("AdvisoryLockKey(%q) = %d, want %d (Python parity)", test.orgID, got, test.want)
		}
	}
}
