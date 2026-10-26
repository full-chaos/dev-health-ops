package sso

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

type surrogateRow struct{ id, request, body, setup, goAPI, kind string }

func surrogateRows(t *testing.T, pkg string) []surrogateRow {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "ci", "sso_surrogate_refusals.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []surrogateRow
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 9 {
			t.Fatalf("row %q has %d columns, want 9", line, len(f))
		}
		if f[1] == pkg {
			rows = append(rows, surrogateRow{id: f[0], request: f[2], body: f[3], setup: f[4], goAPI: f[5], kind: f[7]})
		}
	}
	if len(rows) == 0 {
		t.Fatalf("no rows for %s", pkg)
	}
	return rows
}

// wantRefusal reads a go_api cell: "<status>" or "<status> <type> <loc>".
func (r surrogateRow) wantRefusal(t *testing.T) (status int, errType, loc string) {
	t.Helper()
	cells := strings.Fields(r.goAPI)
	status, err := strconv.Atoi(cells[0])
	if err != nil {
		t.Fatalf("%s: go_api %q", r.id, r.goAPI)
	}
	if len(cells) == 3 {
		return status, cells[1], cells[2]
	}
	return status, "", ""
}

func TestSurrogateRefusalTableUnitRowsAnswerAsTheTableSays(t *testing.T) {
	driven := 0
	for _, row := range surrogateRows(t, "sso") {
		switch row.setup {
		case "saml_forwarded", "saml_sealed":
			continue // driven against a real provider row by TestSAMLInitiateSurrogateRefusalTable (integration)
		case "none":
		default:
			t.Fatalf("%s: unknown setup %q", row.id, row.setup)
		}
		method, path, _ := strings.Cut(row.request, " ")
		request := httptest.NewRequest(method, path, strings.NewReader(row.body))
		request.Header.Set("Content-Type", "application/json")
		request.SetPathValue("provider_id", "00000000-0000-4000-8000-000000000001")
		body, outcome, _, err := pybody.Read(request)
		if err != nil || outcome != pybody.Ready {
			t.Fatalf("%s: read: %v %v", row.id, outcome, err)
		}
		request = request.WithContext(policy.WithBody(request.Context(), body))
		recorder := httptest.NewRecorder()
		func() {
			defer func() {
				if recover() != nil {
					t.Errorf("%s: the handler went on to the provider lookup", row.id)
				}
			}()
			switch {
			case strings.HasSuffix(path, "/oidc/00000000-0000-4000-8000-000000000001/authorize"):
				handlers{}.initiateOIDCAuth(recorder, request)
			default:
				t.Fatalf("%s: no handler for %s", row.id, row.request)
			}
		}()
		status, errType, loc := row.wantRefusal(t)
		assertRefusal(t, row.id, recorder.Code, recorder.Body.Bytes(), status, errType, loc)
		driven++
	}
	if driven == 0 {
		t.Fatal("the table drove no unit row")
	}
}

func assertRefusal(t *testing.T, id string, gotStatus int, raw []byte, wantStatus int, wantType, wantLoc string) {
	t.Helper()
	if gotStatus != wantStatus {
		t.Errorf("%s: status=%d want %d body=%s", id, gotStatus, wantStatus, raw)
		return
	}
	if wantType == "" {
		return
	}
	var parsed struct {
		Detail []struct {
			Type string `json:"type"`
			Loc  []any  `json:"loc"`
		} `json:"detail"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil || len(parsed.Detail) != 1 {
		t.Errorf("%s: body=%s", id, raw)
		return
	}
	parts := make([]string, len(parsed.Detail[0].Loc))
	for i, part := range parsed.Detail[0].Loc {
		parts[i], _ = part.(string)
	}
	if parsed.Detail[0].Type != wantType || strings.Join(parts, ".") != wantLoc {
		t.Errorf("%s: type=%s loc=%v want %s %s", id, parsed.Detail[0].Type, parsed.Detail[0].Loc, wantType, wantLoc)
	}
	if strings.Contains(string(raw), "app.example") {
		t.Errorf("%s: the refusal echoes the input: %s", id, raw)
	}
}
