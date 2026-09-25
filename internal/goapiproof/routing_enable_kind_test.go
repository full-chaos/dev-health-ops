package goapiproof

import (
	"strings"
	"testing"
)

// queryKinds declares every named operation a query: the kind the existing
// enable tests (all of them about query operations) state explicitly now that an
// unknown kind is refused.
func queryKinds(operations ...string) map[string]string {
	kinds := make(map[string]string, len(operations))
	for _, operation := range operations {
		kinds[operation] = OperationKindQuery
	}
	return kinds
}

// r1 P1: an operation with no known document kind must be refused before Enable
// reads or writes anything, never defaulted to a query (which would let a
// deployed_executed receipt authorize a mutation).
func TestEnableRefusesAnOperationWithNoKnownKind(t *testing.T) {
	base := func() EnableRequest {
		return EnableRequest{
			SchemaDigest: "sha256:s", RunningBuild: "b", Operations: []string{"saveReport"},
			DocumentDigest: map[string]string{"saveReport": "sha256:d"}, Mode: "canary",
			RolloutPercentage: EnforcedRolloutPercentage, RecordedBy: "r", ReviewEvidence: "e", PrincipalID: "p",
		}
	}
	good := base()
	good.OperationKinds = map[string]string{"saveReport": OperationKindMutation}
	if err := good.validate(); err != nil {
		t.Fatalf("a request with a known kind was refused: %v", err)
	}
	for name, kinds := range map[string]map[string]string{
		"no kind map":        nil,
		"empty kind map":     {},
		"operation missing":  {"other": OperationKindQuery},
		"unrecognised value": {"saveReport": "subscription"},
		"blank value":        {"saveReport": ""},
		"different case":     {"saveReport": "Mutation"},
	} {
		request := base()
		request.OperationKinds = kinds
		err := request.validate()
		if err == nil || !strings.Contains(err.Error(), "no known document kind") {
			t.Errorf("%s: want a refusal naming the missing kind, got %v", name, err)
		}
	}
}
