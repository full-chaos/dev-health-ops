package edgetokenmint

import (
	"testing"
	"time"
)

type oracleRow struct {
	IsActive     bool `json:"is_active"`
	IsSuperuser  bool `json:"is_superuser"`
	TokenVersion int  `json:"token_version"`
}

type oracleMint struct {
	Key            string `json:"key"`
	Issuer         string `json:"issuer,omitempty"`
	Audience       string `json:"audience,omitempty"`
	ExpiresMinutes int    `json:"expires_minutes"`
}

// oraclePrincipal is the principal the Python side mints for and the
// validator's row answers as. A case without one uses the proof principal.
type oraclePrincipal struct {
	UserID       string `json:"user_id"`
	Email        string `json:"email"`
	OrgID        string `json:"org_id"`
	Role         string `json:"role"`
	TokenVersion int    `json:"token_version"`
}

type oracleCase struct {
	Name       string           `json:"name"`
	GoToken    string           `json:"go_token"`
	PythonMint oracleMint       `json:"python_mint"`
	DBRow      *oracleRow       `json:"db_row"`
	Principal  *oraclePrincipal `json:"principal,omitempty"`
	accepted   bool
}

type oracleJudgement struct {
	Accepted     bool           `json:"accepted"`
	User         map[string]any `json:"user"`
	BoundUserIDs []string       `json:"bound_user_ids"`
}

// edgeOracleSuite is the cases of the edge oracle with the keys and the principal they were built with.
type edgeOracleSuite struct {
	key, otherKey string
	principal     Principal
	cases         []oracleCase
}

// edgeOracleCases builds the cases of the edge oracle: a Go-minted token for each, the Python mint of the same
// inputs, and the users row the validator answers with.
func edgeOracleCases(t *testing.T) edgeOracleSuite {
	t.Helper()
	const (
		key      = "live-edge-oracle-signing-key-0123456789abcdef"
		otherKey = "live-edge-oracle-a-different-key-0123456789ab"
	)
	principal := Principal{
		UserID:       ProvePrincipalID,
		Email:        "go-api-prove@service.dev-health.invalid",
		OrgID:        "11111111-2222-4333-8444-555555555555",
		Role:         "viewer",
		TokenVersion: 2,
	}
	mint := func(signingKey string, opts Options) string {
		t.Helper()
		token, err := Mint([]byte(signingKey), principal, opts)
		if err != nil {
			t.Fatalf("Go mint: %v", err)
		}
		return token
	}
	// The org-admin proof principal: its own users row and token, minted by
	// Mint with the admin role (CHAOS-6570).
	adminPrincipal := Principal{
		UserID:       AdminProofPrincipalID,
		Email:        "go-api-admin-prove@service.dev-health.invalid",
		OrgID:        principal.OrgID,
		Role:         "admin",
		TokenVersion: 2,
	}
	adminToken, err := Mint([]byte(key), adminPrincipal, Options{})
	if err != nil {
		t.Fatalf("Go mint (admin principal): %v", err)
	}
	liveRow := &oracleRow{IsActive: true, TokenVersion: 2}
	past := func() time.Time { return time.Now().Add(-20 * time.Minute) }

	cases := []oracleCase{
		{Name: "valid", GoToken: mint(key, Options{}), PythonMint: oracleMint{Key: "key", ExpiresMinutes: 10}, DBRow: liveRow, accepted: true},
		{Name: "wrong key", GoToken: mint(otherKey, Options{}), PythonMint: oracleMint{Key: "other_key", ExpiresMinutes: 10}, DBRow: liveRow},
		{Name: "expired", GoToken: mint(key, Options{TTL: 5 * time.Minute, Now: past}), PythonMint: oracleMint{Key: "key", ExpiresMinutes: -15}, DBRow: liveRow},
		{Name: "wrong audience", GoToken: mint(key, Options{Audience: "query-api"}), PythonMint: oracleMint{Key: "key", Audience: "query-api", ExpiresMinutes: 10}, DBRow: liveRow},
		{Name: "wrong issuer", GoToken: mint(key, Options{Issuer: "dev-health-ops-edge"}), PythonMint: oracleMint{Key: "key", Issuer: "dev-health-ops-edge", ExpiresMinutes: 10}, DBRow: liveRow},
		{Name: "inactive principal", GoToken: mint(key, Options{}), PythonMint: oracleMint{Key: "key", ExpiresMinutes: 10}, DBRow: &oracleRow{IsActive: false, TokenVersion: 2}},
		{Name: "token_version mismatch", GoToken: mint(key, Options{}), PythonMint: oracleMint{Key: "key", ExpiresMinutes: 10}, DBRow: &oracleRow{IsActive: true, TokenVersion: 3}},
		{
			Name: "admin principal valid", GoToken: adminToken, PythonMint: oracleMint{Key: "key", ExpiresMinutes: 10}, DBRow: liveRow, accepted: true,
			Principal: &oraclePrincipal{UserID: adminPrincipal.UserID, Email: adminPrincipal.Email, OrgID: adminPrincipal.OrgID, Role: adminPrincipal.Role, TokenVersion: adminPrincipal.TokenVersion},
		},
		{Name: "principal row missing", GoToken: mint(key, Options{}), PythonMint: oracleMint{Key: "key", ExpiresMinutes: 10}, DBRow: nil},
	}
	return edgeOracleSuite{key: key, otherKey: otherKey, principal: principal, cases: cases}
}
