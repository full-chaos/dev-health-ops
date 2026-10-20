package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// TestTransportErrorGoldensAreWhatTheTransportWrites: each accept fixture of
// transport-error.v1 is the body WriteError renders for its code (request id
// aside), every code has one, and every rendered body validates.
func TestTransportErrorGoldensAreWhatTheTransportWrites(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(contracts.ContractsDir(root), "examples", "transport-error")
	const id = "0b7e9a4e-5d1c-4f6a-9b2e-1c3d4e5f6a7b"
	covered := map[Code]bool{}
	for code := range message {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request = request.WithContext(context.WithValue(request.Context(), requestIDKey{}, id))
		WriteError(response, request, code)
		var rendered any
		if err := json.Unmarshal(response.Body.Bytes(), &rendered); err != nil {
			t.Fatalf("%s: %v", code, err)
		}
		if err := contracts.Validate(root, "transport-error.v1", rendered); err != nil {
			t.Errorf("%s: the transport's answer does not validate: %v", code, err)
		}
		raw, err := os.ReadFile(filepath.Join(dir, "valid-"+strings.ReplaceAll(string(code), "_", "-")+".json"))
		if err != nil {
			t.Errorf("%s: no golden fixture: %v", code, err)
			continue
		}
		var golden any
		if err := json.Unmarshal(raw, &golden); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(rendered, golden) {
			t.Errorf("%s: the transport writes %s, the golden says %s", code, response.Body, raw)
		}
		covered[code] = true
	}
	if len(covered) != len(status) {
		t.Fatalf("%d of %d codes covered", len(covered), len(status))
	}
}
