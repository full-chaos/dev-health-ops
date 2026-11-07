package shadowvenue

const throwawayHeader = "kind\tlabel\tuuid"

// ThrowawayIDs: skeleton, no behaviour yet.
type ThrowawayIDs struct{ ids map[string]bool }

// ParseThrowawayIDs: skeleton, no behaviour yet.
func ParseThrowawayIDs(raw []byte) (ThrowawayIDs, error) { return ThrowawayIDs{}, nil }

// Holds: skeleton, no behaviour yet.
func (t ThrowawayIDs) Holds(texts ...string) error { return nil }
