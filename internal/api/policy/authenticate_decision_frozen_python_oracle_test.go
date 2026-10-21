package policy_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// authenticateRow is the users row of a case as the Python validator reads it.
// A null token_version is what a row written before the column had a default
// holds; the Go store reads it as 0.
type authenticateRow struct {
	IsActive     bool   `json:"is_active"`
	IsSuperuser  bool   `json:"is_superuser"`
	TokenVersion *int64 `json:"token_version"`
}

// authenticateCase is one case: the changes to the base claims and the users
// row (nil = the user does not exist).
type authenticateCase struct {
	Name   string           `json:"name"`
	Claims map[string]any   `json:"claims"`
	Remove []string         `json:"remove"`
	Row    *authenticateRow `json:"row"`
}

func authenticateCases() []authenticateCase {
	version := func(value int64) *int64 { return &value }
	row := func(active, superuser bool, tokenVersion *int64) *authenticateRow {
		return &authenticateRow{IsActive: active, IsSuperuser: superuser, TokenVersion: tokenVersion}
	}
	none := map[string]any{}
	return []authenticateCase{
		{"active, the same version", map[string]any{"tv": 2}, nil, row(true, false, version(2))},
		{"active superuser row", none, nil, row(true, true, version(0))},
		{"the token claims superuser, the row does not", map[string]any{"is_superuser": true}, nil, row(true, false, version(0))},
		{"no users row", none, nil, nil},
		{"inactive user", none, nil, row(false, false, version(0))},
		{"inactive superuser", none, nil, row(false, true, version(0))},
		{"the row's version is ahead", map[string]any{"tv": 2}, nil, row(true, false, version(3))},
		{"the token's version is ahead", map[string]any{"tv": 3}, nil, row(true, false, version(2))},
		{"tv absent, row at 0", none, []string{"tv"}, row(true, false, version(0))},
		{"tv absent, row at 1", none, []string{"tv"}, row(true, false, version(1))},
		{"tv null, row at 0", map[string]any{"tv": nil}, nil, row(true, false, version(0))},
		{"tv null, row at 1", map[string]any{"tv": nil}, nil, row(true, false, version(1))},
		{"the row's version is null", none, nil, row(true, false, nil)},
		{"tv a numeric string that is the row's version", map[string]any{"tv": " 7 "}, nil, row(true, false, version(7))},
		{"tv a numeric string that is not the row's version", map[string]any{"tv": " 7 "}, nil, row(true, false, version(8))},
		{"tv unreadable", map[string]any{"tv": "seven"}, nil, row(true, false, version(0))},
		// int() of a string: one sign, then digits with single underscores
		// between them. A sign before an underscore, a leading, a trailing or
		// a double underscore is not a number.
		{"tv with a plus sign", map[string]any{"tv": "+7"}, nil, row(true, false, version(7))},
		{"tv with a plus sign and then an underscore", map[string]any{"tv": "+_7"}, nil, row(true, false, version(7))},
		{"tv with a minus sign and then an underscore", map[string]any{"tv": "-_7"}, nil, row(true, false, version(-7))},
		{"tv with a leading underscore", map[string]any{"tv": "_7"}, nil, row(true, false, version(7))},
		{"tv with a trailing underscore", map[string]any{"tv": "7_"}, nil, row(true, false, version(7))},
		{"tv with one underscore between digits", map[string]any{"tv": "1_0"}, nil, row(true, false, version(10))},
		{"tv with two underscores between digits", map[string]any{"tv": "1__0"}, nil, row(true, false, version(10))},
		{"tv a float that truncates to the row's version", map[string]any{"tv": 2.9}, nil, row(true, false, version(2))},
		{"tv true, row at 1", map[string]any{"tv": true}, nil, row(true, false, version(1))},
		{"sub is not a UUID", map[string]any{"sub": "not-a-uuid"}, nil, row(true, false, version(0))},
	}
}

// authenticateStore answers the one users row of a case.
type authenticateStore struct{ row *authenticateRow }

func (s authenticateStore) UserState(context.Context, uuid.UUID) (policy.UserState, bool, error) {
	if s.row == nil {
		return policy.UserState{}, false, nil
	}
	state := policy.UserState{IsActive: s.row.IsActive, IsSuperuser: s.row.IsSuperuser}
	if s.row.TokenVersion != nil {
		state.TokenVersion = *s.row.TokenVersion
	}
	return state, true, nil
}

func (authenticateStore) Membership(context.Context, uuid.UUID, uuid.UUID) (string, bool, error) {
	return "", false, nil
}

func (authenticateStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}

// TestAuthenticateDecisionsMatchFrozenPython holds Authenticator.Authenticate
// to the decisions Python's authenticate_access_token gave for the same users
// row and a token of the same shape: accepted or refused, and for an accepted
// user the superuser flag (the row's, never the token's) and the token
// version. The golden holds no token.
func TestAuthenticateDecisionsMatchFrozenPython(t *testing.T) {
	cases := authenticateCases()
	input, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	output := decisionGoldens.Outputs(t, "users-row-decisions.golden.json", programoracle.Program{
		Name: "authenticate decisions", Stdin: input,
	})[0]
	var want []struct {
		Name         string `json:"name"`
		Accepted     bool   `json:"accepted"`
		IsSuperuser  bool   `json:"is_superuser"`
		TokenVersion int    `json:"token_version"`
	}
	if err := json.Unmarshal([]byte(output), &want); err != nil || len(want) != len(cases) {
		t.Fatalf("decode the frozen decisions: %v (%d of %d)", err, len(want), len(cases))
	}
	verifier, err := edgetoken.New(testKey, principalDecisionIssuer, principalDecisionAudience)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	accepted, refused := 0, 0
	for index, c := range cases {
		if want[index].Name != c.Name {
			t.Fatalf("answer %d is for %q, want %q", index, want[index].Name, c.Name)
		}
		claims := jwt.MapClaims{
			"sub": "11111111-1111-4111-8111-111111111111", "email": "u@example.com", "org_id": "org-1", "role": "member",
			"is_superuser": false, "type": "access", "iss": principalDecisionIssuer, "aud": principalDecisionAudience,
			"exp": now.Add(5 * time.Minute).Unix(), "iat": now.Unix(), "jti": "j", "tv": 0,
		}
		for name, value := range c.Claims {
			claims[name] = value
		}
		for _, name := range c.Remove {
			delete(claims, name)
		}
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(testKey))
		if err != nil {
			t.Fatal(err)
		}
		authenticator, err := policy.NewAuthenticator(verifier, authenticateStore{row: c.Row}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
		user, authErr := authenticator.Authenticate(context.Background(), token)
		python := want[index]
		if (authErr == nil) != python.Accepted {
			t.Errorf("%s: Go accepted=%v (%v), Python accepted=%v", c.Name, authErr == nil, authErr, python.Accepted)
			continue
		}
		if authErr != nil {
			refused++
			continue
		}
		accepted++
		if user.IsSuperuser != python.IsSuperuser || user.TokenVersion != python.TokenVersion {
			t.Errorf("%s: Go superuser=%v version=%d, Python superuser=%v version=%d", c.Name, user.IsSuperuser, user.TokenVersion, python.IsSuperuser, python.TokenVersion)
		}
	}
	if accepted == 0 || refused == 0 {
		t.Fatalf("one-sided comparison: %d accepted, %d refused", accepted, refused)
	}
	t.Logf("%d cases: %d accepted, %d refused", len(cases), accepted, refused)
}
