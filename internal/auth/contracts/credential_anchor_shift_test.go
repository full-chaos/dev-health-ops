package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/routescan"
)

// A line inserted above a pinned func moves every line below it and changes no code. The pin check must hold on such
// a tree: a pin is found by its symbol and its text, never by the line the registry cites.
func TestCredentialAnchorPinsSurviveALineShift(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(ContractsDir(root), "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	pins := loadAnchorPins(t)
	files := make([]string, 0, len(pins))
	for _, pin := range pins {
		files = append(files, pin.Path)
	}
	shifted := routescan.ShiftedTree(t, root, nil, files, map[string]string{"internal/auth/authapi/provisioning.go": "func ProvisioningRoutes("})
	report := checkCredentialAnchorLines(raw, pins, shifted)
	if len(report.failures) != 0 {
		t.Errorf("a line shift broke %d of %d pins:\n%s", len(report.failures), report.checked, strings.Join(report.failures, "\n"))
	}
	if report.checked < 180 {
		t.Fatalf("only %d pins were checked on the shifted tree: the measurement did not happen", report.checked)
	}
	if report.drift < 1 {
		t.Fatalf("no cited line drifted on the shifted tree (drift %d): the shift moved nothing the pins cite", report.drift)
	}
	t.Logf("shifted tree: %d pins checked, %d cited lines drifted", report.checked, report.drift)
}
