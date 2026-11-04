package authapi

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/repokey/cells"
)

func TestDecodeAuthorizationRequestHoldsTheRepositoryKeyCells(t *testing.T) {
	root := repoRoot(t)
	for _, c := range append(cells.Table(), cells.Cell{Name: "organization-wide", Key: "*", Valid: true}) {
		keyJSON, err := json.Marshal(c.Key)
		if err != nil {
			t.Fatal(err)
		}
		body := strings.Replace(authorizeValidBody, `"full-chaos/dev-health-ops"`, string(keyJSON), 1)
		if _, err := DecodeAuthorizationRequest([]byte(body)); (err == nil) != c.Valid {
			t.Errorf("%s: decoder error = %v, want valid=%v", c.Name, err, c.Valid)
		}
		var document any
		if err := json.Unmarshal([]byte(body), &document); err != nil {
			t.Fatal(err)
		}
		if err := contracts.Validate(root, "authorization_request.v1", document); (err == nil) != c.Valid {
			t.Errorf("%s: schema error = %v, want valid=%v", c.Name, err, c.Valid)
		}
	}
}

func TestGrantCreateKeyBoundIsTheKeyBound(t *testing.T) {
	if maxGrantKeyBytes != 255 {
		t.Errorf("maxGrantKeyBytes = %d, want 255", maxGrantKeyBytes)
	}
}
