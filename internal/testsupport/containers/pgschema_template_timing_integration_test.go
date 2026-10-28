//go:build integration

package containers_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// TestTemplateFingerprintPrintsItsCostOnTheMigratedSchema prints how long one fingerprint of
// the migrated schema takes and how many rows it holds. It asserts no time: the figure is read
// from the output line "TEMPLATE-FINGERPRINT".
func TestTemplateFingerprintPrintsItsCostOnTheMigratedSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	runShared(t, func() {
		instance := pgschema.Start(ctx, t)
		defer func() { _ = instance.Close(context.Background()) }()
		for i := 1; i <= 3; i++ {
			began := time.Now()
			rows, err := containers.PgTemplateFingerprint(ctx, instance.URI)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) == 0 {
				t.Fatal("the fingerprint of the migrated schema is empty")
			}
			fmt.Printf("TEMPLATE-FINGERPRINT\trun=%d\trows=%d\tseconds=%.3f\n", i, len(rows), time.Since(began).Seconds())
		}
	})
}
