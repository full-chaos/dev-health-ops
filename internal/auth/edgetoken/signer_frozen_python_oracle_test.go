package edgetoken_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
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
	Package:       "./internal/auth/edgetoken/",
	Build:         "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity:      "python 3.14.7\nunicodedata 16.0.0\npyjwt 2.15.0",
	Distributions: []string{"pyjwt"},
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"signer.golden.json": "5c802c712e983f5089a109a9add9fa73ff34c8bbfedab00750761588fd7933f9",
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

// signerProgram mints with the REAL AuthService (api/services/auth.py), with
// only its clock and uuid4 fixed so the output is the same in every run, and
// reports what the real validate_token says about each token it minted, at
// the same instant (the token library reads the clock itself, so its clock is
// fixed too). A token is answered as its digest. The key arrives on stdin,
// never argv.
const signerProgram = `
import hashlib, json, sys, uuid
from datetime import datetime, timezone
import jwt.api_jwt
import dev_health_ops.api.services.auth as auth

spec = json.load(sys.stdin)

class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return now

# Every datetime the service and the token library make is a FixedDatetime, so
# the library's own isinstance checks see a datetime.
now = FixedDatetime.fromtimestamp(spec["now"], tz=timezone.utc)
auth.datetime = FixedDatetime
jwt.api_jwt.datetime = FixedDatetime
service = auth.AuthService(secret_key=spec["key"])
out = []
for case in spec["cases"]:
    auth.uuid.uuid4 = (lambda value: (lambda: uuid.UUID(value)))(case["jti"])
    if case["kind"] == "access":
        token = service.create_access_token(**case["args"])
    elif case["kind"] == "refresh":
        token = service.create_refresh_token(**case["args"])
    else:
        args = dict(case["args"])
        args["expires_at"] = FixedDatetime.fromtimestamp(args["expires_at"], tz=timezone.utc)
        token = service.create_refresh_token_with_jti(**args)
    verdict = service.validate_token(token, token_type=case["verify_as"])
    raw = token.encode("utf-8")
    out.append({"token": {"sha256": hashlib.sha256(raw).hexdigest(), "length": len(raw)}, "accepted": verdict is not None})
print(json.dumps(out))
`

type signerCase struct {
	Kind     string         `json:"kind"`
	Args     map[string]any `json:"args"`
	JTI      string         `json:"jti"`
	VerifyAs string         `json:"verify_as"`
}

// TestSignerMatchesFrozenAuthService pins every token the Signer mints to the
// token the Python AuthService mints from the same arguments, clock and jti
// (the same SHA-256 and length), and to the frozen verdict of the real
// validate_token for that token: accepted. The Go token is those same bytes,
// so the verdict is the verdict for the Go token.
func TestSignerMatchesFrozenAuthService(t *testing.T) {
	const key = "signer-oracle-key-0123456789abcdef-0123"
	signer, err := edgetoken.NewSigner(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	now := signerInstant
	strPtr := func(s string) *string { return &s }
	type accessCase struct {
		claims edgetoken.AccessClaims
		args   map[string]any
	}
	accessCases := []accessCase{
		{edgetoken.AccessClaims{UserID: "u-1", Email: "a@b.com", OrgID: "", Role: "member", TokenVersion: 0},
			map[string]any{"user_id": "u-1", "email": "a@b.com"}},
		{edgetoken.AccessClaims{UserID: "u-2", Email: "Ünï@bücher.de", OrgID: "o-1", Role: "owner", IsSuperuser: true,
			Username: strPtr("ü-name"), FullName: strPtr("Zoë \"Q\" <x> &   \U0001f600"), TokenVersion: 7},
			map[string]any{"user_id": "u-2", "email": "Ünï@bücher.de", "org_id": "o-1", "role": "owner", "is_superuser": true,
				"username": "ü-name", "full_name": "Zoë \"Q\" <x> &   \U0001f600", "token_version": 7}},
		{edgetoken.AccessClaims{UserID: "u-3", Email: "c@d.org", OrgID: "o-2", Role: "admin", Username: strPtr(""), FullName: strPtr(""), TokenVersion: 2},
			map[string]any{"user_id": "u-3", "email": "c@d.org", "org_id": "o-2", "role": "admin", "username": "", "full_name": "", "token_version": 2}},
		{edgetoken.AccessClaims{UserID: "u-4", Email: "tab\t@x.com", OrgID: "o-3", Role: "viewer", Username: strPtr("\x7f\x00"), TokenVersion: -1},
			map[string]any{"user_id": "u-4", "email": "tab\t@x.com", "org_id": "o-3", "role": "viewer", "username": "\x7f\x00", "token_version": -1}},
		{edgetoken.AccessClaims{UserID: "u-5", Email: "e@f.io", OrgID: "o-4", Role: "admin", FullName: strPtr("F"), ImpersonatingUserID: strPtr("super-1")},
			map[string]any{"user_id": "u-5", "email": "e@f.io", "org_id": "o-4", "role": "admin", "full_name": "F", "impersonating_user_id": "super-1"}},
		{edgetoken.AccessClaims{UserID: "u-6", Email: "g@h.io", Role: "member", ImpersonatingUserID: strPtr("")},
			map[string]any{"user_id": "u-6", "email": "g@h.io", "role": "member", "impersonating_user_id": ""}},
	}
	var cases []signerCase
	var want []string
	for index, c := range accessCases {
		jti := []string{"11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222",
			"33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444",
			"88888888-8888-4888-8888-888888888888", "99999999-9999-4999-8999-999999999999"}[index]
		token, err := signer.Access(c.claims, now, jti)
		if err != nil {
			t.Fatal(err)
		}
		cases = append(cases, signerCase{Kind: "access", Args: c.args, JTI: jti, VerifyAs: "access"})
		want = append(want, token)
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
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	output := signerGoldens.Outputs(t, root, "signer.golden.json", programoracle.Program{Name: "auth service signer", Text: signerProgram, Stdin: payload})[0]
	var got []struct {
		Token    tokenDigest `json:"token"`
		Accepted bool        `json:"accepted"`
	}
	if err := json.Unmarshal([]byte(output), &got); err != nil || len(got) != len(cases) {
		t.Fatalf("decode: %v (%d of %d answers)", err, len(got), len(cases))
	}
	for index, c := range cases {
		if digestOf(want[index]) != got[index].Token {
			t.Errorf("case %d (%s): the Go token is not the Python token: go %+v, python %+v", index, c.Kind, digestOf(want[index]), got[index].Token)
		}
		if !got[index].Accepted {
			t.Errorf("case %d (%s): python validate_token refused the token", index, c.Kind)
		}
	}
}
