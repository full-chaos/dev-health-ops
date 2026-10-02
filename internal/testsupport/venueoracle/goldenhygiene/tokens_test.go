// Package goldenhygiene holds the checks that read every recorded golden in the repository. They are hygiene checks,
// not behaviour tests of the venueoracle package, and they scan about 150 MB: they live apart so the package of the
// harness stays fast under -race (CHAOS-7955).
package goldenhygiene

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func TestNoGoldenInTheRepoHoldsATokenShape(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	checked, violations, err := venueoracle.GoldenTokenViolations(root)
	if err != nil {
		t.Fatal(err)
	}
	if checked == 0 {
		t.Fatal("the walk found no golden: a gate that checks nothing passes everything")
	}
	if len(violations) > 0 {
		t.Fatalf("goldens hold token shapes:\n%s", strings.Join(violations, "\n"))
	}
}
