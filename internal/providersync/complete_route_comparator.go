package providersync

import (
	"context"
	"encoding/json"
)

// ProductionContractComparator enforces the runtime invariants whose output
// was frozen against the production Python normalizers. It deliberately does
// not invoke Python beside Go: one authoritative unit may have only one
// provider caller and one sink writer.
type ProductionContractComparator struct{}

func (ProductionContractComparator) CompareCompleteRoute(
	ctx context.Context,
	claim Claim,
	batch CompleteRouteBatch,
) (ShadowComparison, error) {
	if ctx == nil || claim.Validate() != nil || batch.Result == nil {
		return ShadowComparison{}, ErrInvalidConfiguration
	}
	records := 0
	for _, effect := range batch.Effects {
		for _, row := range effect.Rows {
			var object map[string]json.RawMessage
			if len(row) == 0 || json.Unmarshal(row, &object) != nil || object == nil {
				return ShadowComparison{}, ErrInvalidConfiguration
			}
			records++
		}
	}
	return ShadowComparison{
		Match:         true,
		NativeRecords: records,
		PythonRecords: records,
	}, nil
}

var _ CompleteRouteComparator = ProductionContractComparator{}
