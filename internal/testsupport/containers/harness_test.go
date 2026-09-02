package containers

import (
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func TestDependencyImagesAreDigestPinned(t *testing.T) {
	t.Parallel()

	for _, image := range []string{PostgresImage, ClickHouseImage, ValkeyImage} {
		if !strings.Contains(image, "@sha256:") {
			t.Errorf("dependency image is not digest pinned: %s", image)
		}
	}
}

// TestReaperImageMatchesTestcontainers is the only thing standing between a
// testcontainers-go bump and a silently useless pre-pull. CI warms ReaperImage
// by name before running container-backed tests; if a bump moves the library's
// reaper tag, the pre-pull would keep warming the old image, every job would go
// back to pulling the new one cold and anonymously from Docker Hub, and the
// only symptom would be the intermittent container-creation failure this
// pre-pull exists to prevent. Failing here instead makes that a build problem.
func TestReaperImageMatchesTestcontainers(t *testing.T) {
	t.Parallel()

	if ReaperImage != testcontainers.ReaperDefaultImage {
		t.Fatalf(
			"ReaperImage = %q but testcontainers-go starts %q: "+
				"CI would pre-pull the wrong image and leave the reaper pull cold",
			ReaperImage, testcontainers.ReaperDefaultImage,
		)
	}
}
