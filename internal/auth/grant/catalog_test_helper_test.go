package grant

import "github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"

func (s *Store) catalogForTest() { s.catalog = policycatalog.Default() }
