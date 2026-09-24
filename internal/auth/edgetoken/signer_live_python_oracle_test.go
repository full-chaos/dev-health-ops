package edgetoken

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// signerProgram mints with the REAL AuthService (api/services/auth.py),
// with only its clock and uuid4 pinned so the output is reproducible, and
// also reports what validate_token says about each Go-minted token. The key
// arrives on stdin, never argv.
const signerProgram = `
import json, sys, uuid
from datetime import datetime, timezone
import dev_health_ops.api.services.auth as auth

spec = json.load(sys.stdin)
now = datetime.fromtimestamp(spec["now"], tz=timezone.utc)

class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return now

auth.datetime = FixedDatetime
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
        args["expires_at"] = datetime.fromtimestamp(args["expires_at"], tz=timezone.utc)
        token = service.create_refresh_token_with_jti(**args)
    verdict = service.validate_token(case["go_token"], token_type=case["verify_as"])
    out.append({"token": token, "go_accepted": verdict is not None})
print(json.dumps(out))
`

type signerCase struct {
	Kind     string         `json:"kind"`
	Args     map[string]any `json:"args"`
	JTI      string         `json:"jti"`
	GoToken  string         `json:"go_token"`
	VerifyAs string         `json:"verify_as"`
}

// TestSignerMatchesLiveAuthService pins every token the Signer mints to the
// byte-exact token the Python AuthService mints from the same arguments,
// clock and jti, and has the live validate_token accept each Go token.
func TestSignerMatchesLiveAuthService(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	const key = "signer-oracle-key-0123456789abcdef-0123"
	signer, err := NewSigner(key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second).Add(-time.Minute)
	strPtr := func(s string) *string { return &s }
	type accessCase struct {
		claims AccessClaims
		args   map[string]any
	}
	accessCases := []accessCase{
		{AccessClaims{UserID: "u-1", Email: "a@b.com", OrgID: "", Role: "member", TokenVersion: 0},
			map[string]any{"user_id": "u-1", "email": "a@b.com"}},
		{AccessClaims{UserID: "u-2", Email: "Ünï@bücher.de", OrgID: "o-1", Role: "owner", IsSuperuser: true,
			Username: strPtr("ü-name"), FullName: strPtr("Zoë \"Q\" <x> &   \U0001f600"), TokenVersion: 7},
			map[string]any{"user_id": "u-2", "email": "Ünï@bücher.de", "org_id": "o-1", "role": "owner", "is_superuser": true,
				"username": "ü-name", "full_name": "Zoë \"Q\" <x> &   \U0001f600", "token_version": 7}},
		{AccessClaims{UserID: "u-3", Email: "c@d.org", OrgID: "o-2", Role: "admin", Username: strPtr(""), FullName: strPtr(""), TokenVersion: 2},
			map[string]any{"user_id": "u-3", "email": "c@d.org", "org_id": "o-2", "role": "admin", "username": "", "full_name": "", "token_version": 2}},
		{AccessClaims{UserID: "u-4", Email: "tab\t@x.com", OrgID: "o-3", Role: "viewer", Username: strPtr("\x7f\x00"), TokenVersion: -1},
			map[string]any{"user_id": "u-4", "email": "tab\t@x.com", "org_id": "o-3", "role": "viewer", "username": "\x7f\x00", "token_version": -1}},
	}
	var cases []signerCase
	var want []string
	for index, c := range accessCases {
		jti := []string{"11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222",
			"33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444"}[index]
		token, err := signer.Access(c.claims, now, jti)
		if err != nil {
			t.Fatal(err)
		}
		cases = append(cases, signerCase{Kind: "access", Args: c.args, JTI: jti, GoToken: token, VerifyAs: "access"})
		want = append(want, token)
	}
	refreshJTI, familyJTI := "55555555-5555-4555-8555-555555555555", "66666666-6666-4666-8666-666666666666"
	refresh, err := signer.Refresh(RefreshClaims{UserID: "u-1", OrgID: "o-1", FamilyID: "fam-1"}, now, refreshJTI)
	if err != nil {
		t.Fatal(err)
	}
	cases = append(cases, signerCase{Kind: "refresh", Args: map[string]any{"user_id": "u-1", "org_id": "o-1", "family_id": "fam-1"},
		JTI: refreshJTI, GoToken: refresh, VerifyAs: "refresh"})
	want = append(want, refresh)
	expiresAt := now.Add(5 * 24 * time.Hour)
	reissued, err := signer.RefreshUntil(RefreshClaims{UserID: "u-9", OrgID: "", FamilyID: "fam-9"}, now, expiresAt, familyJTI)
	if err != nil {
		t.Fatal(err)
	}
	cases = append(cases, signerCase{Kind: "with_jti", Args: map[string]any{"jti": familyJTI, "user_id": "u-9", "org_id": "",
		"family_id": "fam-9", "expires_at": expiresAt.Unix()}, JTI: "77777777-7777-4777-8777-777777777777", GoToken: reissued, VerifyAs: "refresh"})
	want = append(want, reissued)

	payload, _ := json.Marshal(map[string]any{"key": key, "now": now.Unix(), "cases": cases})
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, "-c", signerProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = bytes.NewReader(payload)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, stderr.Bytes()))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var got []struct {
		Token      string `json:"token"`
		GoAccepted bool   `json:"go_accepted"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &got); err != nil {
		t.Fatal(err)
	}
	for index, c := range cases {
		if got[index].Token != want[index] {
			t.Errorf("case %d (%s): python token differs from Go (lengths %d vs %d)", index, c.Kind, len(got[index].Token), len(want[index]))
		}
		if !got[index].GoAccepted {
			t.Errorf("case %d (%s): python validate_token refused the Go token", index, c.Kind)
		}
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "edgetoken-signer"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}
