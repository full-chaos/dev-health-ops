package decision

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"testing"
)

// The committed replay cases are v1d responses with v1d outcomes. They are
// replayed under the v1d rubric, which is test data only: production embeds
// v1f. Nothing here is hand-made v1f output.
//
//go:embed testdata/decision-support-v1d.json
var rubricV1dJSON []byte

const (
	rubricV1dVersion = "decision-support-v1d"
	rubricV1dSHA256  = "73ace2d4e437349155a933e826029807970d3b79e2608a5b519d48a9c6588803"
)

// newV1dCompleter is the completer of the evaluated v1d configuration.
func newV1dCompleter(t *testing.T, transport Transport, model string) *Completer {
	t.Helper()
	sum := sha256.Sum256(rubricV1dJSON)
	if got := hex.EncodeToString(sum[:]); got != rubricV1dSHA256 {
		t.Fatalf("testdata/decision-support-v1d.json has sha256 %s, want %s", got, rubricV1dSHA256)
	}
	r, err := parseRubric(rubricV1dJSON)
	if err != nil {
		t.Fatal(err)
	}
	if r.RubricVersion != rubricV1dVersion {
		t.Fatalf("rubric_version = %q", r.RubricVersion)
	}
	if model == "" {
		model = DefaultModel
	}
	return &Completer{rubric: r, transport: transport, model: model}
}
