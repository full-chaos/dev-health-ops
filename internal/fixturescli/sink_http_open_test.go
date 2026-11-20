package fixturescli

import (
	"testing"

	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

func TestNormalizedHTTPSinkStillOpensForFixtures(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"clickhouse://ch:ch@127.0.0.1:8123/default", "http://ch:ch@127.0.0.1:8123/default"} {
		dsn := normalizeSink(in)
		if err := chstorage.DefaultConfig(dsn).Validate(); err != nil {
			t.Errorf("fixtures sink %q (normalized %q) is refused by the ClickHouse open path: %v", in, dsn, err)
		}
	}
}
