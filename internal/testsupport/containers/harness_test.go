package containers

import (
	"strings"
	"testing"
)

func TestDependencyImagesAreDigestPinned(t *testing.T) {
	t.Parallel()

	for _, image := range []string{PostgresImage, ClickHouseImage, ValkeyImage} {
		if !strings.Contains(image, "@sha256:") {
			t.Errorf("dependency image is not digest pinned: %s", image)
		}
	}
}
