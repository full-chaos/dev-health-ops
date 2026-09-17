package sankey

import "time"

// teamScopeAsOf is the fixed instant this package's tests resolve team
// ownership at, so an assertion states the instant it is about rather than
// depending on when the test ran.
var teamScopeAsOf = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
