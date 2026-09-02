package containers

import (
	"regexp"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// clickHouseReference is the shape ClickHouseImage is allowed to take. The
// repository is FIXED and only the 26.x version floats, per chris's CHAOS-4854
// ruling ("It's major version MATCHING"). A digest stays legal so a future
// re-pin needs no change here.
//
// The previous assertion was `strings.Contains(ClickHouseImage, ":")`. Relaxing
// this image from a digest to a tag was a change to the VERSION predicate, but
// collapsing to "contains a colon" dropped the IMAGE domain with it: it equally
// accepted an empty tag, `:latest`, `: foo`, and
// `quay.io/other/clickhouse-server:latest`. A foreign registry accepted here
// then bypasses the ghcr mirror downstream.
// `[0-9]` rather than `\d` is deliberate and must not be "simplified" back.
// Go RE2's `\d` is already ASCII-only so this pair is equivalent HERE, but the
// same pattern is also written in bash ERE and in Python, and Python's `\d`
// matches non-ASCII digits. Spelling all three `[0-9]` makes them provably the
// same set without a reader having to know three dialects' rules.
var clickHouseReference = regexp.MustCompile(
	`^clickhouse/clickhouse-server(:26(\.[0-9]+)*|@sha256:[0-9a-f]{64})$`,
)

func TestDependencyImagesAreDigestPinned(t *testing.T) {
	t.Parallel()

	// ClickHouseImage is deliberately absent: it tracks the 26.7 MINOR by tag so
	// patch upgrades apply, ruled by chris (CHAOS-4854) and matching what
	// CHAOS-4851 did for the CI service containers. Adding it back here would
	// fail that pin, not protect it.
	for _, image := range []string{PostgresImage, ValkeyImage} {
		if !strings.Contains(image, "@sha256:") {
			t.Errorf("dependency image is not digest pinned: %s", image)
		}
	}
	if !clickHouseReference.MatchString(ClickHouseImage) {
		t.Errorf(
			"ClickHouseImage = %q but must match %s: the repository is fixed and "+
				"only the 26.x version floats",
			ClickHouseImage, clickHouseReference,
		)
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
