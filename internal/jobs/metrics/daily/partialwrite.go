package daily

import "github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/familyerr"

// ErrPartialWrite marks a native family executor that FAILED AFTER already
// writing at least one row. See familyerr.ErrPartialWrite for the full
// rationale.
//
// WHY THIS IS AN ALIAS rather than the definition. The sentinel has to be
// wrapped by the code that DETECTS a partial write, which is a family's writer
// -- and family writers live in subpackages of this one (daily imports
// daily/benchmarking, daily/compoundingrisk, ...), so they cannot import daily
// back without a cycle. Defining it here and wrapping it there is not
// expressible; defining it at a leaf and aliasing it here is.
//
// The alias keeps `daily.ErrPartialWrite` as the name computeNativeFamilies
// checks and the name other lanes are building against, so this placement
// constraint does not leak into their code.
var ErrPartialWrite = familyerr.ErrPartialWrite
