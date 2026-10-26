package oidc

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestSurrogateRefusalTableAuthServiceOIDCRows sends each oidc row of
// ci/sso_surrogate_refusals.tsv to the auth service's authorize route and
// asserts the status, error type and loc its auth_service cell names.
func TestSurrogateRefusalTableAuthServiceOIDCRows(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "ci", "sso_surrogate_refusals.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	c := routeCases[0]
	driven := 0
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Split(line, "\t")
		if line == "" || strings.HasPrefix(line, "#") || len(f) != 9 || f[1] != "sso" || !strings.Contains(f[2], "/oidc/") {
			continue
		}
		id, body, want := f[0], f[3], strings.Fields(f[6])
		row := c.activeRow()
		env := newUnitEnv(t, &fakeProviders{before: row})
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/oidc/"+row.ID.String()+"/authorize", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := env.serve(c, req)
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
				strings.Join(parsed.Detail[0].Loc, ".") != want[2] || strings.Contains(rec.Body.String(), "app.example") {
				t.Errorf("%s: body=%s want %s %s", id, rec.Body.String(), want[1], want[2])
			}
		}
		driven++
	}
	if driven != 1 {
		t.Fatalf("drove %d oidc rows, want 1", driven)
	}
}
