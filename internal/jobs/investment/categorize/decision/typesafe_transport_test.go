package decision

import "github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"

// The TypeSafe client lives in package categorize and cannot import this
// package (this one imports categorize). The check that it still satisfies the
// Transport interface therefore lives here, where the import direction allows.
var _ Transport = (*categorize.TypeSafeClient)(nil)
