package apiservice

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// issuerUnsetGolden was recorded on origin/main f831161db0, before go-api
// routed access tokens by endpoint profile. ACCESSROUTE_WRITE_GOLDEN=1
// rewrites it; that is only right on that commit.
var issuerUnsetGolden = filepath.Join("testdata", "accessroute", "issuer_unset.golden.json")

// TestIssuerUnsetResponsesAreByteIdentical: with AUTH_ACCESS_TOKEN_ISSUER
// unset (prod today) the api listener answers every request of the fixed set
// exactly as it did before profile routing: status, headers and body.
func TestIssuerUnsetResponsesAreByteIdentical(t *testing.T) {
	answers := probeAnswers(probeServer(t, newProbeStore(), nil), issuerUnsetRequests(t))
	got, err := json.MarshalIndent(answers, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	if os.Getenv("ACCESSROUTE_WRITE_GOLDEN") == "1" {
		if err := os.MkdirAll(filepath.Dir(issuerUnsetGolden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(issuerUnsetGolden, got, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Skip("golden rewritten")
	}
	want, err := os.ReadFile(issuerUnsetGolden)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		var wantAnswers []probeAnswer
		if err := json.Unmarshal(want, &wantAnswers); err != nil {
			t.Fatal(err)
		}
		byName := map[string]probeAnswer{}
		for _, answer := range wantAnswers {
			byName[answer.Name] = answer
		}
		for _, answer := range answers {
			wantAnswer := byName[answer.Name]
			a, _ := json.Marshal(answer)
			b, _ := json.Marshal(wantAnswer)
			if !bytes.Equal(a, b) {
				t.Errorf("%s:\n got %s\nwant %s", answer.Name, a, b)
			}
		}
		if len(answers) != len(wantAnswers) {
			t.Errorf("%d answers, golden has %d", len(answers), len(wantAnswers))
		}
		if !t.Failed() {
			t.Error("the answers differ from the golden in encoding only")
		}
	}
}
