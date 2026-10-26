package saml

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestSurrogateRefusalTableAuthServiceSAMLRows sends each saml row of
// ci/sso_surrogate_refusals.tsv to the auth service's initiate route and
// asserts the status, error type and loc its auth_service cell names. The
// twin has no sealed branch, so the setup column does not change its answer.
func TestSurrogateRefusalTableAuthServiceSAMLRows(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "ci", "sso_surrogate_refusals.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	driven := 0
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Split(line, "\t")
		if line == "" || strings.HasPrefix(line, "#") || len(f) != 9 || f[1] != "sso" || !strings.Contains(f[2], "/saml/") {
			continue
		}
		id, body, want := f[0], tableBody(t, f[3]), strings.Fields(f[6])
		rec, _ := initiateRequest(t, body)
		if status, _ := strconv.Atoi(want[0]); rec.Code != status {
			t.Errorf("%s: status=%d want %s body=%s", id, rec.Code, want[0], rec.Body.String())
		}
		if len(want) == 3 {
			var parsed struct {
				Detail []struct {
					Type string   `json:"type"`
					Loc  []string `json:"loc"`
				} `json:"detail"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &parsed); err != nil || len(parsed.Detail) != 1 || parsed.Detail[0].Type != want[1] ||
				strings.Join(parsed.Detail[0].Loc, ".") != want[2] {
				t.Errorf("%s: body=%s want %s %s", id, rec.Body.String(), want[1], want[2])
			}
		}
		driven++
	}
	if driven != 4 {
		t.Fatalf("drove %d saml rows, want 4", driven)
	}
}

// tableBody is the body cell: text, or "hex:" and the raw bytes in hex.
func tableBody(t *testing.T, cell string) string {
	t.Helper()
	encoded, isHex := strings.CutPrefix(cell, "hex:")
	if !isHex {
		return cell
	}
	raw, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatalf("body cell %q: %v", cell, err)
	}
	return string(raw)
}
