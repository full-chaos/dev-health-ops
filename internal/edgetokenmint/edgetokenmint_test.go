package edgetokenmint

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	testOrg = "11111111-2222-4333-8444-555555555555"
	testKey = "edge-token-unit-test-signing-key-0123456789"
)

var testNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func testPrincipal() Principal {
	return Principal{
		UserID:       ProvePrincipalID,
		Email:        "go-api-prove@service.dev-health.invalid",
		OrgID:        testOrg,
		Role:         "viewer",
		TokenVersion: 3,
	}
}

func clearIssuerAudienceEnv(t *testing.T) {
	t.Helper()
	t.Setenv(IssuerEnvVar, "")
	t.Setenv(AudienceEnvVar, "")
}

func decodeSegment(t *testing.T, token string, index int) map[string]any {
	t.Helper()
	segments := strings.Split(token, ".")
	if len(segments) != 3 {
		t.Fatalf("token has %d segments, want 3", len(segments))
	}
	raw, err := base64.RawURLEncoding.DecodeString(segments[index])
	if err != nil {
		t.Fatalf("decode segment %d: %v", index, err)
	}
	decoded := map[string]any{}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal segment %d: %v", index, err)
	}
	return decoded
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// The header and the claim set are exactly what auth.py's
// create_access_token writes for a principal with no username or
// full_name. There is no kid: the edge verifies with its one active
// JWT_SECRET_KEY and never selects a key by id.
func TestMintWritesExactlyTheClaimsTheEdgeReads(t *testing.T) {
	clearIssuerAudienceEnv(t)
	token, err := Mint([]byte(testKey), testPrincipal(), Options{TTL: 7 * time.Minute, Now: func() time.Time { return testNow }})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	header := decodeSegment(t, token, 0)
	if !reflect.DeepEqual(header, map[string]any{"alg": "HS256", "typ": "JWT"}) {
		t.Fatalf("header = %v, want exactly alg=HS256 typ=JWT and no kid", header)
	}

	payload := decodeSegment(t, token, 1)
	wantKeys := []string{"aud", "email", "exp", "iat", "is_superuser", "iss", "jti", "org_id", "role", "sub", "tv", "type"}
	if got := sortedKeys(payload); !reflect.DeepEqual(got, wantKeys) {
		t.Fatalf("claim keys = %v, want %v", got, wantKeys)
	}
	checks := map[string]any{
		"sub":          ProvePrincipalID,
		"email":        "go-api-prove@service.dev-health.invalid",
		"org_id":       testOrg,
		"role":         "viewer",
		"is_superuser": false,
		"type":         "access",
		"iss":          DefaultIssuer,
		"aud":          DefaultAudience, // a STRING, as PyJWT writes it, not a one-element list
		"tv":           float64(3),
		"iat":          float64(testNow.Unix()),
		"exp":          float64(testNow.Add(7 * time.Minute).Unix()),
	}
	for key, want := range checks {
		if !reflect.DeepEqual(payload[key], want) {
			t.Errorf("claim %s = %#v, want %#v", key, payload[key], want)
		}
	}
	if _, err := uuid.Parse(payload["jti"].(string)); err != nil {
		t.Errorf("jti is not a UUID: %v", err)
	}
}

func TestMintDefaultsTTLAndReadsIssuerAndAudienceFromTheEdgeEnvNames(t *testing.T) {
	t.Setenv(IssuerEnvVar, "issuer-from-env")
	t.Setenv(AudienceEnvVar, "audience-from-env")
	token, err := Mint([]byte(testKey), testPrincipal(), Options{Now: func() time.Time { return testNow }})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	payload := decodeSegment(t, token, 1)
	if payload["iss"] != "issuer-from-env" || payload["aud"] != "audience-from-env" {
		t.Fatalf("iss/aud = %v/%v, want the env values", payload["iss"], payload["aud"])
	}
	if got := int64(payload["exp"].(float64)) - int64(payload["iat"].(float64)); got != int64(DefaultTTL/time.Second) {
		t.Fatalf("exp-iat = %ds, want the default TTL %s", got, DefaultTTL)
	}
}

func parseForTest(token string, key []byte, at time.Time, issuer, audience string) error {
	_, err := jwt.ParseWithClaims(token, &Claims{}, func(*jwt.Token) (any, error) { return key, nil },
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithIssuer(issuer),
		jwt.WithAudience(audience),
		jwt.WithExpirationRequired(),
		jwt.WithTimeFunc(func() time.Time { return at }),
	)
	return err
}

// A minted token verifies under the right key, issuer, audience and clock,
// and under nothing else. The edge-side refusal of the same failures is the
// live Python oracle's job; this pins the Go side's half.
func TestTheMintedTokenVerifiesOnlyUnderTheRightKeyIssuerAudienceAndClock(t *testing.T) {
	clearIssuerAudienceEnv(t)
	token, err := Mint([]byte(testKey), testPrincipal(), Options{TTL: 5 * time.Minute, Now: func() time.Time { return testNow }})
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	within := testNow.Add(time.Minute)
	if err := parseForTest(token, []byte(testKey), within, DefaultIssuer, DefaultAudience); err != nil {
		t.Fatalf("the token did not verify under its own key: %v", err)
	}
	for name, tc := range map[string]struct {
		key      string
		at       time.Time
		issuer   string
		audience string
		want     error
	}{
		"wrong key":      {key: testKey + "-other", at: within, issuer: DefaultIssuer, audience: DefaultAudience, want: jwt.ErrTokenSignatureInvalid},
		"expired":        {key: testKey, at: testNow.Add(6 * time.Minute), issuer: DefaultIssuer, audience: DefaultAudience, want: jwt.ErrTokenExpired},
		"wrong audience": {key: testKey, at: within, issuer: DefaultIssuer, audience: "query-api", want: jwt.ErrTokenInvalidAudience},
		"wrong issuer":   {key: testKey, at: within, issuer: "dev-health-ops-edge", audience: DefaultAudience, want: jwt.ErrTokenInvalidIssuer},
	} {
		t.Run(name, func(t *testing.T) {
			if err := parseForTest(token, []byte(tc.key), tc.at, tc.issuer, tc.audience); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestMintRefusesWhatTheEdgeOrTheScopeMustNotSee(t *testing.T) {
	clearIssuerAudienceEnv(t)
	for name, tc := range map[string]struct {
		key    string
		mutate func(*Principal)
		opts   Options
	}{
		"key one character short": {key: testKey[:MinSigningKeyLength-1]},
		"non-UUID user id":        {key: testKey, mutate: func(p *Principal) { p.UserID = "admin@test.com" }},
		"non-UUID org id":         {key: testKey, mutate: func(p *Principal) { p.OrgID = "70d529e0" }},
		"empty email":             {key: testKey, mutate: func(p *Principal) { p.Email = " " }},
		"owner role":              {key: testKey, mutate: func(p *Principal) { p.Role = "owner" }},
		"admin role":              {key: testKey, mutate: func(p *Principal) { p.Role = "admin" }},
		"empty role":              {key: testKey, mutate: func(p *Principal) { p.Role = "" }},
		"TTL above the cap":       {key: testKey, opts: Options{TTL: MaxTTL + time.Second}},
		"negative TTL":            {key: testKey, opts: Options{TTL: -time.Second}},
	} {
		t.Run(name, func(t *testing.T) {
			principal := testPrincipal()
			if tc.mutate != nil {
				tc.mutate(&principal)
			}
			token, err := Mint([]byte(tc.key), principal, tc.opts)
			if err == nil {
				t.Fatalf("minted %d bytes; want a refusal", len(token))
			}
			if strings.Contains(err.Error(), tc.key) {
				t.Fatalf("the refusal carried the signing key: %v", err)
			}
		})
	}
	if _, err := Mint([]byte(testKey), testPrincipal(), Options{TTL: MaxTTL}); err != nil {
		t.Fatalf("a TTL at the cap was refused: %v", err)
	}
	member := testPrincipal()
	member.Role = "member"
	if _, err := Mint([]byte(testKey), member, Options{}); err != nil {
		t.Fatalf("the member role was refused: %v", err)
	}
}

// Python's len() counts characters, not bytes. A 32-character key of
// two-byte characters is accepted there, and a 31-character one is refused
// even though it is 62 bytes long.
func TestLoadSigningKeyCountsCharactersLikeTheEdge(t *testing.T) {
	if _, err := LoadSigningKey(""); !errors.Is(err, ErrSigningKeyMissing) {
		t.Fatalf("empty: err = %v", err)
	}
	short := strings.Repeat("k", MinSigningKeyLength-1)
	if _, err := LoadSigningKey(short); !errors.Is(err, ErrSigningKeyTooShort) {
		t.Fatalf("31 chars: err = %v", err)
	} else if strings.Contains(err.Error(), short) {
		t.Fatalf("the refusal carried the key: %v", err)
	}
	if _, err := LoadSigningKey(strings.Repeat("é", MinSigningKeyLength-1)); !errors.Is(err, ErrSigningKeyTooShort) {
		t.Fatalf("31 two-byte chars: err = %v", err)
	}
	if key, err := LoadSigningKey(strings.Repeat("é", MinSigningKeyLength)); err != nil || len(key) != 2*MinSigningKeyLength {
		t.Fatalf("32 two-byte chars: len=%d err=%v", len(key), err)
	}
	t.Setenv(SigningKeyEnvVar, testKey)
	if key, err := LoadSigningKeyFromEnv(); err != nil || string(key) != testKey {
		t.Fatalf("LoadSigningKeyFromEnv: err=%v", err)
	}
}

// fakeRow scans fixed values into the destinations, in order.
type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return errors.New("fakeRow: column count mismatch")
	}
	for i, d := range dest {
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(r.values[i]))
	}
	return nil
}

type fakeQuerier struct {
	row   fakeRow
	calls int
	sql   string
	args  []any
}

func (q *fakeQuerier) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	q.calls++
	q.sql, q.args = sql, args
	return q.row
}

func strPtr(s string) *string { return &s }

// principalRow builds the eight columns lookupSQL selects, for a row the
// minter accepts, with mutate applied.
func principalRow(mutate func(values []any)) fakeRow {
	values := []any{
		ProvePrincipalID,
		"go-api-prove@service.dev-health.invalid",
		true,  // is_active
		false, // is_superuser
		4,     // token_version
		ServiceAuthProvider,
		false, // has a password hash
		strPtr("viewer"),
	}
	if mutate != nil {
		mutate(values)
	}
	return fakeRow{values: values}
}

func TestLookupPrincipalRefusesEveryRowItMustNotMintFor(t *testing.T) {
	for name, tc := range map[string]struct {
		row  fakeRow
		want error
	}{
		"no users row":            {row: fakeRow{err: pgx.ErrNoRows}, want: ErrPrincipalNotFound},
		"a human row":             {row: principalRow(func(v []any) { v[5] = "local" }), want: ErrPrincipalNotService},
		"no auth provider":        {row: principalRow(func(v []any) { v[5] = "" }), want: ErrPrincipalNotService},
		"service with a password": {row: principalRow(func(v []any) { v[6] = true }), want: ErrPrincipalNotService},
		"inactive":                {row: principalRow(func(v []any) { v[2] = false }), want: ErrPrincipalInactive},
		"superuser":               {row: principalRow(func(v []any) { v[3] = true }), want: ErrPrincipalSuperuser},
		"no membership in org":    {row: principalRow(func(v []any) { v[7] = (*string)(nil) }), want: ErrNoMembership},
		"owner in org":            {row: principalRow(func(v []any) { v[7] = strPtr("owner") }), want: ErrRoleNotAllowed},
		"admin in org":            {row: principalRow(func(v []any) { v[7] = strPtr("admin") }), want: ErrRoleNotAllowed},
	} {
		t.Run(name, func(t *testing.T) {
			q := &fakeQuerier{row: tc.row}
			if _, err := LookupPrincipal(context.Background(), q, testOrg); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}

	readErr := errors.New("connection reset")
	if _, err := LookupPrincipal(context.Background(), &fakeQuerier{row: fakeRow{err: readErr}}, testOrg); !errors.Is(err, readErr) || errors.Is(err, ErrPrincipalNotFound) {
		t.Fatalf("a read failure must surface as itself, not as a missing row: %v", err)
	}
}

func TestLookupPrincipalReadsTheFixedPrincipalInTheRequestedOrg(t *testing.T) {
	for _, role := range []string{"viewer", "member"} {
		q := &fakeQuerier{row: principalRow(func(v []any) { v[7] = strPtr(role) })}
		principal, err := LookupPrincipal(context.Background(), q, testOrg)
		if err != nil {
			t.Fatalf("role %s: %v", role, err)
		}
		want := Principal{UserID: ProvePrincipalID, Email: "go-api-prove@service.dev-health.invalid", OrgID: testOrg, Role: role, TokenVersion: 4}
		if principal != want {
			t.Fatalf("principal = %+v, want %+v", principal, want)
		}
		if !reflect.DeepEqual(q.args, []any{ProvePrincipalID, testOrg}) {
			t.Fatalf("bound args = %v, want the fixed principal id then the org", q.args)
		}
		for _, cast := range []string{"u.id = $1::uuid", "m.org_id = $2::uuid"} {
			if !strings.Contains(q.sql, cast) {
				t.Fatalf("lookup SQL lost %q:\n%s", cast, q.sql)
			}
		}
	}
}

func TestLookupPrincipalRefusesANonUUIDOrgWithoutQuerying(t *testing.T) {
	q := &fakeQuerier{row: principalRow(nil)}
	if _, err := LookupPrincipal(context.Background(), q, "70d529e0"); err == nil {
		t.Fatal("a non-UUID org was accepted")
	}
	if q.calls != 0 {
		t.Fatalf("the database was queried %d time(s) for a malformed org", q.calls)
	}
}

// The token carries the row's CURRENT token_version, so a bumped version
// revokes outstanding tokens and the next mint follows it.
func TestMintForProveCarriesTheRowTokenVersion(t *testing.T) {
	clearIssuerAudienceEnv(t)
	q := &fakeQuerier{row: principalRow(func(v []any) { v[4] = 9 })}
	token, err := MintForProve(context.Background(), q, []byte(testKey), testOrg, Options{})
	if err != nil {
		t.Fatalf("MintForProve: %v", err)
	}
	if got := decodeSegment(t, token, 1)["tv"]; got != float64(9) {
		t.Fatalf("tv = %v, want 9", got)
	}

	inactive := &fakeQuerier{row: principalRow(func(v []any) { v[2] = false })}
	if token, err := MintForProve(context.Background(), inactive, []byte(testKey), testOrg, Options{}); err == nil || token != "" {
		t.Fatalf("an inactive principal was minted for (token bytes=%d, err=%v)", len(token), err)
	}
}
