package storedversiontest

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storedversion"
)

func TestFingerprintNamesTheKeptWithRule(t *testing.T) {
	with := storedversion.Contract{Table: "t", Columns: []storedversion.Column{{Name: "c", Rule: storedversion.Unstated, Fields: []string{"f"}, With: "d"}}}
	without := storedversion.Contract{Table: "t", Columns: []storedversion.Column{{Name: "c", Rule: storedversion.Unstated, Fields: []string{"f"}}}}
	if Fingerprint(with) == Fingerprint(without) {
		t.Fatal("contracts differing only in With have one fingerprint")
	}
}
