package sso

import (
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// tableBody is the body cell: text, or "hex:" and the raw bytes in hex (a
// table cannot hold a byte that is not text).
func tableBody(t testing.TB, cell string) string {
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
			rows = append(rows, surrogateRow{id: f[0], request: f[2], body: tableBody(t, f[3]), setup: f[4], goAPI: f[5], kind: f[7]})
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
		case "saml_forwarded", "saml_sealed", "oauth_gitlab":
			continue // driven against a real provider row by TestSSOSurrogateRefusalTableStackRows (integration)
		case "none":
		default:
			t.Fatalf("%s: unknown setup %q", row.id, row.setup)
		}
		method, path, _ := strings.Cut(row.request, " ")
		request := httptest.NewRequest(method, path, strings.NewReader(row.body))
		request.Header.Set("Content-Type", "application/json")
		request.SetPathValue("provider_id", "00000000-0000-4000-8000-000000000001")
		recorder := httptest.NewRecorder()
		guard := policy.NewGuard(&policy.Authenticator{}, slog.New(slog.DiscardHandler))
		func() {
			defer func() {
				if recover() != nil {
					t.Errorf("%s: the handler went on to the provider lookup", row.id)
				}
			}()
			switch {
			case strings.HasSuffix(path, "/oidc/00000000-0000-4000-8000-000000000001/authorize"):
				guard.BodyFirst(policy.Public, http.HandlerFunc(handlers{}.initiateOIDCAuth)).ServeHTTP(recorder, request)
			default:
				t.Fatalf("%s: no handler for %s", row.id, row.request)
			}
		}()
		status, errType, loc := row.wantRefusal(t)
		assertRefusal(t, row.id, recorder.Code, recorder.Body.Bytes(), status, errType, loc)
		driven++
	}
	if driven != 3 {
		t.Fatalf("the table drove %d unit rows, want 3", driven)
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

// TestSurrogateRefusalTableIsWellFormed checks every column of every row of
// ci/sso_surrogate_refusals.tsv, whatever package names it: a row no driver
// owns, a repeated id, an unknown request, setup or body, a go_api or
// auth_service cell that is not a status, a kind outside the three kinds or
// one that contradicts the cells, and an empty reason are all refused.
func TestSurrogateRefusalTableIsWellFormed(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "ci", "sso_surrogate_refusals.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	owners := map[string]string{
		"sso/none":           "sso unit driver",
		"sso/saml_forwarded": "sso integration driver",
		"sso/saml_sealed":    "sso integration driver",
		"sso/oauth_gitlab":   "sso integration driver",
		"githubapp/none":     "githubapp driver",
	}
	requests := map[string]string{
		"sso/none":           "POST /api/v1/auth/oidc/00000000-0000-4000-8000-000000000001/authorize",
		"sso/saml_forwarded": "POST /api/v1/auth/saml/{provider}/initiate",
		"sso/saml_sealed":    "POST /api/v1/auth/saml/{provider}/initiate",
		"sso/oauth_gitlab":   "POST /api/v1/auth/oauth/{provider}/authorize",
		"githubapp/none":     "POST /api/v1/admin/integrations/github/install-callback",
	}
	loneSurrogate := regexp.MustCompile(`\\u[dD][89a-fA-F][0-9a-fA-F]{2}`)
	status := regexp.MustCompile(`^[1-5][0-9][0-9]( .*)?$`)
	ids := map[string]bool{}
	rows := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		rows++
		f := strings.Split(line, "\t")
		if len(f) != 9 {
			t.Errorf("row %q has %d columns, want 9", f[0], len(f))
			continue
		}
		id, pkg, request, body, setup, goAPI, authService, kind, reason := f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7], f[8]
		key := pkg + "/" + setup
		if id == "" || ids[id] {
			t.Errorf("row id %q is empty or repeated", id)
		}
		ids[id] = true
		if _, owned := owners[key]; !owned {
			t.Errorf("%s: package %q with setup %q is owned by no driver", id, pkg, setup)
			continue
		}
		if request != requests[key] {
			t.Errorf("%s: request %q, the %s drives %q", id, request, owners[key], requests[key])
		}
		decoded := tableBody(t, body)
		var object map[string]any
		if err := json.Unmarshal([]byte(decoded), &object); err != nil {
			t.Errorf("%s: body %q is not a JSON object", id, body)
		}
		if strings.HasPrefix(strings.Fields(goAPI + " x")[0], "400") {
			if _, err := pyjson.DecodeBody([]byte(decoded)); err == nil {
				t.Errorf("%s: go_api answers 400 but the api plane reads the body %q", id, body)
			}
		} else if !pyjson.HasLoneSurrogateEscape([]byte(decoded)) || (!strings.HasPrefix(body, "hex:") && !loneSurrogate.MatchString(body)) {
			t.Errorf("%s: body %q carries no lone surrogate", id, body)
		}
		if !status.MatchString(goAPI) {
			t.Errorf("%s: go_api cell %q is not \"<status>\" or \"<status> <type> <loc>\"", id, goAPI)
			continue
		}
		if authService != "-" && !status.MatchString(authService) {
			t.Errorf("%s: auth_service cell %q is neither \"-\" nor a status cell", id, authService)
			continue
		}
		if strings.TrimSpace(reason) == "" {
			t.Errorf("%s: empty reason", id)
		}
		goStatus := strings.Fields(goAPI)[0]
		switch kind {
		case "same":
			if authService == "-" || strings.Fields(authService)[0] != goStatus {
				t.Errorf("%s: kind same but go_api %q and auth_service %q answer different statuses", id, goAPI, authService)
			}
		case "stricter":
			if goStatus != "422" || len(strings.Fields(goAPI)) != 3 || strings.Fields(goAPI)[1] != "string_unicode" {
				t.Errorf("%s: kind stricter needs go_api \"422 string_unicode <loc>\", got %q", id, goAPI)
			}
		case "divergent":
			if authService == "-" || authService == goAPI {
				t.Errorf("%s: kind divergent needs an auth_service cell that differs from go_api %q, got %q", id, goAPI, authService)
			}
		default:
			t.Errorf("%s: kind %q is not same, stricter or divergent", id, kind)
		}
	}
	if rows == 0 {
		t.Fatal("the table has no rows")
	}
}
