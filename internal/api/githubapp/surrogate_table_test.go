package githubapp

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

// TestSurrogateRefusalTableGithubappRowsAnswerAsTheTableSays sends each
// githubapp row of ci/sso_surrogate_refusals.tsv to the install-callback
// handler and asserts the status and the error loc the table names.
func TestSurrogateRefusalTableGithubappRowsAnswerAsTheTableSays(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "ci", "sso_surrogate_refusals.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	driven := 0
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Split(line, "\t")
		if line == "" || strings.HasPrefix(line, "#") || f[1] != "githubapp" {
			continue
		}
		id, request, body, wantCell := f[0], f[2], f[3], strings.Fields(f[5])
		if f[4] != "none" {
			t.Fatalf("%s: setup %q, this driver runs rows with setup none", id, f[4])
		}
		method, path, _ := strings.Cut(request, " ")
		if path != "/api/v1/admin/integrations/github/install-callback" {
			t.Fatalf("%s: no handler for %s", id, request)
		}
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		decoded, outcome, _, err := pybody.Read(req)
		if err != nil || outcome != pybody.Ready {
			t.Fatalf("%s: read: %v %v", id, outcome, err)
		}
		req = req.WithContext(policy.WithBody(req.Context(), decoded))
		recorder := httptest.NewRecorder()
		func() {
			defer func() {
				if recover() != nil {
					t.Errorf("%s: the handler went on past validation", id)
				}
			}()
			handlers{}.installCallback(recorder, req)
		}()
		if want, _ := strconv.Atoi(wantCell[0]); recorder.Code != want {
			t.Errorf("%s: status=%d want %s body=%s", id, recorder.Code, wantCell[0], recorder.Body.String())
		}
		if len(wantCell) == 3 {
			var parsed struct {
				Detail []struct {
					Type string `json:"type"`
					Loc  []any  `json:"loc"`
				} `json:"detail"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &parsed); err != nil || len(parsed.Detail) != 1 || parsed.Detail[0].Type != wantCell[1] ||
				fmt.Sprint(parsed.Detail[0].Loc) != "["+strings.ReplaceAll(wantCell[2], ".", " ")+"]" {
				t.Errorf("%s: body=%s want %s %s", id, recorder.Body.String(), wantCell[1], wantCell[2])
			}
		}
		driven++
	}
	if driven != 1 {
		t.Fatalf("drove %d githubapp rows, want 1", driven)
	}
}
