package edgetoken_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func TestMain(m *testing.M) { os.Exit(venueoracle.RunTests(m)) }

// signerGoldens is the set of this package's frozen Python answers. The
// producer is the AuthService of the pinned build and the token library under
// it, so Identity names that distribution. A golden recorded by another
// producer is refused.
var signerGoldens = programoracle.Set{
	Build:    "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity: "python 3.14.7\nunicodedata 16.0.0\npyjwt 2.15.0",
	Pins: map[string]string{
		"signer.golden.json": "1eb5e3577e4dc0fb5e5bcf00c38c17e936a9c720a698cea731ea1c33dafcff38",
	},
}

// tokenDigest is how the golden holds a token: the SHA-256 of its text and
// the length of the text in bytes. No golden holds a token.
type tokenDigest struct {
	SHA256 string `json:"sha256"`
	Length int    `json:"length"`
}

func digestOf(token string) tokenDigest {
	sum := sha256.Sum256([]byte(token))
	return tokenDigest{SHA256: hex.EncodeToString(sum[:]), Length: len(token)}
}

// signerInstant is the instant every token of the oracle is minted and judged
// at. It is fixed: it is in the program's input, and an input is the same in
// every run.
var signerInstant = time.Unix(1_790_000_000, 0).UTC()

type signerCase struct {
	Kind     string         `json:"kind"`
	Args     map[string]any `json:"args"`
	JTI      string         `json:"jti"`
	VerifyAs string         `json:"verify_as"`
}

// TestSignerMatchesFrozenAuthService pins every refresh token the Signer mints
// to the token the Python AuthService mints from the same arguments, clock and
// jti (the same SHA-256 and length), and to the frozen verdict of the real
// validate_token for that token: accepted. The Go token is those same bytes,
// so the verdict is the verdict for the Go token. The program's input still
// holds the six recorded access-token cases, so the frozen golden is read as
// recorded; no Go token is compared to them: the HS256 access-token class is
// retired and nothing mints one (CHAOS-13197).
func TestSignerMatchesFrozenAuthService(t *testing.T) {
	const key = "signer-oracle-key-0123456789abcdef-0123"
	signer, err := edgetoken.NewSigner(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	now := signerInstant
	retiredAccessArgs := []map[string]any{
		{"user_id": "u-1", "email": "a@b.com"},
		{"user_id": "u-2", "email": "Ünï@bücher.de", "org_id": "o-1", "role": "owner", "is_superuser": true,
			"username": "ü-name", "full_name": "Zoë \"Q\" <x> &   \U0001f600", "token_version": 7},
		{"user_id": "u-3", "email": "c@d.org", "org_id": "o-2", "role": "admin", "username": "", "full_name": "", "token_version": 2},
		{"user_id": "u-4", "email": "tab\t@x.com", "org_id": "o-3", "role": "viewer", "username": "\x7f\x00", "token_version": -1},
		{"user_id": "u-5", "email": "e@f.io", "org_id": "o-4", "role": "admin", "full_name": "F", "impersonating_user_id": "super-1"},
		{"user_id": "u-6", "email": "g@h.io", "role": "member", "impersonating_user_id": ""},
	}
	var cases []signerCase
	var want []string
	for index, args := range retiredAccessArgs {
		jti := []string{"11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222",
			"33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444",
			"88888888-8888-4888-8888-888888888888", "99999999-9999-4999-8999-999999999999"}[index]
		cases = append(cases, signerCase{Kind: "access", Args: args, JTI: jti, VerifyAs: "access"})
		want = append(want, "")
	}
	refreshJTI, familyJTI := "55555555-5555-4555-8555-555555555555", "66666666-6666-4666-8666-666666666666"
	refresh, err := signer.Refresh(edgetoken.RefreshClaims{UserID: "u-1", OrgID: "o-1", FamilyID: "fam-1"}, now, refreshJTI)
	if err != nil {
		t.Fatal(err)
	}
	cases = append(cases, signerCase{Kind: "refresh", Args: map[string]any{"user_id": "u-1", "org_id": "o-1", "family_id": "fam-1"},
		JTI: refreshJTI, VerifyAs: "refresh"})
	want = append(want, refresh)
	expiresAt := now.Add(5 * 24 * time.Hour)
	reissued, err := signer.RefreshUntil(edgetoken.RefreshClaims{UserID: "u-9", OrgID: "", FamilyID: "fam-9"}, now, expiresAt, familyJTI)
	if err != nil {
		t.Fatal(err)
	}
	cases = append(cases, signerCase{Kind: "with_jti", Args: map[string]any{"jti": familyJTI, "user_id": "u-9", "org_id": "",
		"family_id": "fam-9", "expires_at": expiresAt.Unix()}, JTI: "77777777-7777-4777-8777-777777777777", VerifyAs: "refresh"})
	want = append(want, reissued)

	payload, err := json.Marshal(map[string]any{"key": key, "now": now.Unix(), "cases": cases})
	if err != nil {
		t.Fatal(err)
	}
	output := signerGoldens.Outputs(t, "signer.golden.json", programoracle.Program{Name: "auth service signer", Stdin: payload})[0]
	var got []struct {
		Token    tokenDigest `json:"token"`
		Accepted bool        `json:"accepted"`
	}
	if err := json.Unmarshal([]byte(output), &got); err != nil || len(got) != len(cases) {
		t.Fatalf("decode: %v (%d of %d answers)", err, len(got), len(cases))
	}
	for index, c := range cases {
		if c.Kind == "access" {
			continue
		}
		if digestOf(want[index]) != got[index].Token {
			t.Errorf("case %d (%s): the Go token is not the Python token: go %+v, python %+v", index, c.Kind, digestOf(want[index]), got[index].Token)
		}
		if !got[index].Accepted {
			t.Errorf("case %d (%s): python validate_token refused the token", index, c.Kind)
		}
	}
}
