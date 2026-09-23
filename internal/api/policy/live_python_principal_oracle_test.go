package policy

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonPrincipalProgram builds every token with the REAL Python minter
// (AuthService.create_access_token / create_refresh_token) or with pyjwt the
// way the minter calls it, then records the REAL Python decision:
// validate_token(token, "access") for the JWT, and authenticate_access_token's
// own tv conversion (`0 if tv is None else int(tv)`, TypeError/ValueError
// refused) for the stale-session check. The Go side must decide every case
// the same way.
const pythonPrincipalProgram = `
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import jwt

from dev_health_ops.api.services.auth import AuthService, JWT_ALGORITHM

key = os.environ["ORACLE_SIGNING_KEY"]
svc = AuthService(secret_key=key)
now = datetime.now(timezone.utc)
uid = "11111111-1111-4111-8111-111111111111"

def base(**over):
    payload = {
        "sub": uid, "email": "u@example.com", "org_id": "org-1", "role": "member",
        "is_superuser": False, "type": "access", "iss": svc.issuer, "aud": svc.audience,
        "exp": now + timedelta(minutes=5), "iat": now, "jti": "j", "tv": 0,
    }
    for k, v in over.items():
        if v is KeyError:
            payload.pop(k, None)
        else:
            payload[k] = v
    return jwt.encode(payload, key, algorithm=JWT_ALGORITHM)

cases = {
    "minted": svc.create_access_token(uid, "u@example.com", org_id="org-1", token_version=3),
    "minted superuser": svc.create_access_token(uid, "u@example.com", is_superuser=True, username="u", full_name="U"),
    "minted impersonating": svc.create_access_token(uid, "u@example.com", impersonating_user_id="x"),
    "refresh": svc.create_refresh_token(uid, org_id="org-1"),
    "minted expired": svc.create_access_token(uid, "u@example.com", expires_delta=timedelta(seconds=-5)),
    "other key": jwt.encode({"sub": uid, "type": "access", "exp": now + timedelta(minutes=5)}, key + "x", algorithm=JWT_ALGORITHM),
    "hs512": jwt.encode({"sub": uid, "type": "access", "exp": now + timedelta(minutes=5)}, key, algorithm="HS512"),
    "alg none": jwt.encode({"sub": uid, "type": "access", "exp": now + timedelta(minutes=5)}, None, algorithm="none"),
    "tampered": base()[:-2] + ("AA" if not base().endswith("AA") else "BB"),
    "garbage": "not.a.token",
    "no aud no iss": base(aud=KeyError, iss=KeyError),
    "wrong aud": base(aud="other"),
    "wrong iss": base(iss="other"),
    "aud list with match": base(aud=["x", svc.audience]),
    "missing sub": base(sub=KeyError),
    "missing type": base(type=KeyError),
    "missing exp": base(exp=KeyError),
    "wrong type": base(type="refresh"),
    "type not a string": base(type=1),
    "iat future": base(iat=now + timedelta(minutes=10)),
    "nbf future": base(nbf=now + timedelta(minutes=10)),
    "nbf past": base(nbf=now - timedelta(minutes=10)),
    "sub empty": base(sub=""),
    "tv absent": base(tv=KeyError),
    "tv null": base(tv=None),
    "tv int": base(tv=4),
    "tv true": base(tv=True),
    "tv float": base(tv=2.9),
    "tv negative float": base(tv=-2.9),
    "tv numeric string": base(tv=" 7 "),
    "tv underscore string": base(tv="1_0"),
    "tv signed string": base(tv="-3"),
    "tv bad string": base(tv="seven"),
    "tv float string": base(tv="1.0"),
    "tv list": base(tv=[1]),
    "tv object": base(tv={"a": 1}),
}

out = []
for name, token in cases.items():
    try:
        payload = svc.validate_token(token, token_type="access")
        decision = "accepted" if payload is not None else "refused"
    except Exception as exc:
        payload, decision = None, "raised:" + type(exc).__name__
    tv = None
    if payload is not None:
        raw = payload.get("tv")
        try:
            tv = {"value": 0 if raw is None else int(raw)}
        except (TypeError, ValueError):
            tv = {"unreadable": True}
    out.append({"name": name, "token": token, "decision": decision, "tv": tv})
print(json.dumps(out))
`

// TestPrincipalMatchesLivePythonAuthService is the differential oracle for
// the access-token decision: every token above goes through Python's
// validate_token and authenticate_access_token's tv conversion, and through
// edgetoken.Verifier.Verify and tokenVersion here; the two must agree case
// by case.
func TestPrincipalMatchesLivePythonAuthService(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve package path")
	}
	// .../internal/api/policy -> repository root.
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, "-c", pythonPrincipalProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "ORACLE_SIGNING_KEY="+testKey)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live Python principal oracle: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var cases []struct {
		Name     string `json:"name"`
		Token    string `json:"token"`
		Decision string `json:"decision"`
		TV       *struct {
			Value      *int64 `json:"value"`
			Unreadable bool   `json:"unreadable"`
		} `json:"tv"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &cases); err != nil {
		t.Fatalf("decode oracle output: %v: %s", err, output)
	}
	if len(cases) < 30 {
		t.Fatalf("oracle produced %d cases", len(cases))
	}
	verifier, err := edgetoken.New(testKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		claims, verifyErr := verifier.Verify(c.Token)
		goDecision := "accepted"
		if verifyErr != nil {
			goDecision = "refused"
		}
		if goDecision != c.Decision {
			t.Errorf("%s: Go %s (%v), Python %s", c.Name, goDecision, verifyErr, c.Decision)
			continue
		}
		if verifyErr != nil {
			continue
		}
		version, readable := tokenVersion(claims)
		switch {
		case c.TV == nil:
			t.Errorf("%s: Python accepted without a tv decision", c.Name)
		case c.TV.Unreadable == readable:
			t.Errorf("%s: Go readable=%v, Python unreadable=%v", c.Name, readable, c.TV.Unreadable)
		case readable && (c.TV.Value == nil || *c.TV.Value != version):
			t.Errorf("%s: Go tv %d, Python %v", c.Name, version, c.TV.Value)
		}
	}

	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proofDir, "api-policy-principal"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}
