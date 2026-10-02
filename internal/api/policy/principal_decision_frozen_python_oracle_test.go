package policy_test

import (
	"encoding/json"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// testKey is the signing key of the package's token fixtures.
var testKey = policy.OracleTestKey()

// decisionGoldens is the set of this package's frozen Python decisions. The
// producer is the AuthService of the pinned build and the token library under
// it, so Identity names that distribution. A golden recorded by another
// producer is refused.
var decisionGoldens = programoracle.Set{
	Package:       "./internal/api/policy/",
	Build:         "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity:      "python 3.14.7\nunicodedata 16.0.0\npyjwt 2.15.0",
	Distributions: []string{"pyjwt"},
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"users-row-decisions.golden.json": "faf04be9a3e2ebbef4f520ac9a8fec3d7ca931aa382891d5f7ef4edd8b2682e8",
		"principal-decisions.golden.json": "d86d5409f3d0460d24984ad5e310221fd3a9f93dd79786e7c458736237f8aad0",
	},
}

// pythonPrincipalDecisionProgram is pythonPrincipalProgram with the token left
// out of every answer: the golden holds the case name, the decision and the tv
// reading, and no token. Each token is minted against the clock of the run
// that recorded it, so only its decision can be kept.
const pythonPrincipalDecisionProgram = `
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
    out.append({"name": name, "decision": decision, "tv": tv})
print(json.dumps(out))
`

// principalDecisionIssuer and principalDecisionAudience are the AuthService
// defaults (JWT_ISSUER, JWT_AUDIENCE) the Python cases were minted under.
const (
	principalDecisionIssuer   = "dev-health-ops"
	principalDecisionAudience = "dev-health-api"
)

// principalDecisionTokens builds, for every case of the Python program, a
// token of the same shape: the same algorithm and key, the same claims, and
// the same times relative to now. The decision on such a token is the
// decision Python recorded for its own token of that case.
func principalDecisionTokens(t *testing.T, now time.Time) map[string]string {
	t.Helper()
	const uid = "11111111-1111-4111-8111-111111111111"
	at := func(offset time.Duration) int64 { return now.Add(offset).Unix() }
	sign := func(method jwt.SigningMethod, key any, claims jwt.MapClaims) string {
		t.Helper()
		token, err := jwt.NewWithClaims(method, claims).SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	hs256 := func(claims jwt.MapClaims) string { return sign(jwt.SigningMethodHS256, []byte(testKey), claims) }
	// remove marks a claim the case leaves out.
	type removeClaim struct{}
	remove := removeClaim{}
	base := func(overrides map[string]any) string {
		claims := jwt.MapClaims{
			"sub": uid, "email": "u@example.com", "org_id": "org-1", "role": "member",
			"is_superuser": false, "type": "access", "iss": principalDecisionIssuer, "aud": principalDecisionAudience,
			"exp": at(5 * time.Minute), "iat": at(0), "jti": "j", "tv": 0,
		}
		for name, value := range overrides {
			if _, removed := value.(removeClaim); removed {
				delete(claims, name)
				continue
			}
			claims[name] = value
		}
		return hs256(claims)
	}
	minted := func(extra map[string]any, expires time.Duration) string {
		claims := jwt.MapClaims{
			"sub": uid, "email": "u@example.com", "org_id": "", "role": "member", "is_superuser": false,
			"type": "access", "iss": principalDecisionIssuer, "aud": principalDecisionAudience,
			"exp": at(expires), "iat": at(0), "jti": "minted", "tv": 0,
		}
		for name, value := range extra {
			claims[name] = value
		}
		return hs256(claims)
	}
	plain := jwt.MapClaims{"sub": uid, "type": "access", "exp": at(5 * time.Minute)}
	tampered := base(nil)
	suffix := "AA"
	if tampered[len(tampered)-2:] == "AA" {
		suffix = "BB"
	}
	tampered = tampered[:len(tampered)-2] + suffix
	return map[string]string{
		"minted":               minted(map[string]any{"org_id": "org-1", "tv": 3}, time.Hour),
		"minted superuser":     minted(map[string]any{"is_superuser": true, "username": "u", "full_name": "U"}, time.Hour),
		"minted impersonating": minted(map[string]any{"impersonating_user_id": "x"}, time.Hour),
		"refresh": hs256(jwt.MapClaims{
			"sub": uid, "org_id": "org-1", "family_id": "family", "type": "refresh", "iss": principalDecisionIssuer,
			"aud": principalDecisionAudience, "exp": at(7 * 24 * time.Hour), "iat": at(0), "jti": "refresh",
		}),
		"minted expired":       minted(nil, -5*time.Second),
		"other key":            sign(jwt.SigningMethodHS256, []byte(testKey+"x"), plain),
		"hs512":                sign(jwt.SigningMethodHS512, []byte(testKey), plain),
		"alg none":             sign(jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, plain),
		"tampered":             tampered,
		"garbage":              "not.a.token",
		"no aud no iss":        base(map[string]any{"aud": remove, "iss": remove}),
		"wrong aud":            base(map[string]any{"aud": "other"}),
		"wrong iss":            base(map[string]any{"iss": "other"}),
		"aud list with match":  base(map[string]any{"aud": []any{"x", principalDecisionAudience}}),
		"missing sub":          base(map[string]any{"sub": remove}),
		"missing type":         base(map[string]any{"type": remove}),
		"missing exp":          base(map[string]any{"exp": remove}),
		"wrong type":           base(map[string]any{"type": "refresh"}),
		"type not a string":    base(map[string]any{"type": 1}),
		"iat future":           base(map[string]any{"iat": at(10 * time.Minute)}),
		"nbf future":           base(map[string]any{"nbf": at(10 * time.Minute)}),
		"nbf past":             base(map[string]any{"nbf": at(-10 * time.Minute)}),
		"sub empty":            base(map[string]any{"sub": ""}),
		"tv absent":            base(map[string]any{"tv": remove}),
		"tv null":              base(map[string]any{"tv": nil}),
		"tv int":               base(map[string]any{"tv": 4}),
		"tv true":              base(map[string]any{"tv": true}),
		"tv float":             base(map[string]any{"tv": 2.9}),
		"tv negative float":    base(map[string]any{"tv": -2.9}),
		"tv numeric string":    base(map[string]any{"tv": " 7 "}),
		"tv underscore string": base(map[string]any{"tv": "1_0"}),
		"tv signed string":     base(map[string]any{"tv": "-3"}),
		"tv bad string":        base(map[string]any{"tv": "seven"}),
		"tv float string":      base(map[string]any{"tv": "1.0"}),
		"tv list":              base(map[string]any{"tv": []any{1}}),
		"tv object":            base(map[string]any{"tv": map[string]any{"a": 1}}),
	}
}

// TestPrincipalDecisionsMatchFrozenPython holds edgetoken.Verifier and
// tokenVersion to the decisions Python's validate_token and
// authenticate_access_token's tv conversion gave, case by case, when the
// golden was recorded. The golden holds no token: the test builds a token of
// the same shape for each case against the clock of this run (Python minted
// its own against the clock of the recording), so the times relative to now
// are the same and the decision must be too.
func TestPrincipalDecisionsMatchFrozenPython(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	output := decisionGoldens.Outputs(t, root, "principal-decisions.golden.json", programoracle.Program{
		Name: "principal decisions", Text: pythonPrincipalDecisionProgram, Env: map[string]string{"ORACLE_SIGNING_KEY": testKey},
	})[0]
	var cases []struct {
		Name     string `json:"name"`
		Decision string `json:"decision"`
		TV       *struct {
			Value      *int64 `json:"value"`
			Unreadable bool   `json:"unreadable"`
		} `json:"tv"`
	}
	if err := json.Unmarshal([]byte(output), &cases); err != nil {
		t.Fatalf("decode the frozen decisions: %v", err)
	}
	tokens := principalDecisionTokens(t, time.Now())
	names := make([]string, 0, len(cases))
	for _, c := range cases {
		names = append(names, c.Name)
	}
	built := make([]string, 0, len(tokens))
	for name := range tokens {
		built = append(built, name)
	}
	slices.Sort(built)
	sorted := slices.Sorted(slices.Values(names))
	if !slices.Equal(sorted, built) {
		t.Fatalf("the frozen cases are %v, the test builds %v", sorted, built)
	}
	verifier, err := edgetoken.New(testKey, principalDecisionIssuer, principalDecisionAudience)
	if err != nil {
		t.Fatal(err)
	}
	decisions := map[string]int{}
	for _, c := range cases {
		claims, verifyErr := verifier.Verify(tokens[c.Name])
		goDecision := "accepted"
		if verifyErr != nil {
			goDecision = "refused"
		}
		decisions[c.Decision]++
		if goDecision != c.Decision {
			t.Errorf("%s: Go %s (%v), Python %s", c.Name, goDecision, verifyErr, c.Decision)
			continue
		}
		if verifyErr != nil {
			continue
		}
		version, readable := policy.OracleTokenVersion(claims)
		switch {
		case c.TV == nil:
			t.Errorf("%s: Python accepted without a tv decision", c.Name)
		case c.TV.Unreadable == readable:
			t.Errorf("%s: Go readable=%v, Python unreadable=%v", c.Name, readable, c.TV.Unreadable)
		case readable && c.TV.Value == nil:
			t.Errorf("%s: Go tv %d, Python recorded no value", c.Name, version)
		case readable && *c.TV.Value != version:
			t.Errorf("%s: Go tv %d, Python %d", c.Name, version, *c.TV.Value)
		}
	}
	if decisions["accepted"] == 0 || decisions["refused"] == 0 {
		t.Fatalf("one-sided comparison: %v", decisions)
	}
	t.Logf("%d cases, decisions %v", len(cases), decisions)
}
