package edgetokenmint

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// edgeMintGoldens is the set of this package's frozen Python answers: the verdicts of the real edge validator on
// tokens the Python AuthService mints, with the structure of those tokens. The producer is
// dev_health_ops.api.services.auth over PyJWT. A golden recorded by another producer is refused.
var edgeMintGoldens = programoracle.Set{
	Package:  "./internal/edgetokenmint/",
	Build:    "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity: "python 3.14.7\nunicodedata 16.0.0",
	// The goldenrecord verb writes each digest when it promotes a recording; a new golden starts as
	// "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"edge-mint-decisions.golden.json": "0bd3295b40a648e844947f8ea471a7a53f5936e3e56b6e565f876027166cde8e",
	},
}

// frozenEdgeProgram drives the REAL edge validator, AuthService.authenticate_access_token, over one token per case
// that the Python AuthService mints from the case's inputs (the Go-minted token cannot be an input of a recording:
// it carries a random id), and answers, per case, the validator's verdict (accepted, the authenticated user, the
// user id the statement bound) and the structure of the Python token: its header, its claims without the three
// per-run values (iat, exp, jti), its lifetime, whether it was expired when the program ran, and whether its id is a
// UUID. No token and no signature is answered. The database session is fake, as in the live oracle. The program's
// own stdout lines are sent to stderr, so the answer is the only stdout.
const frozenEdgeProgram = `
import asyncio, dataclasses, json, sys, time, uuid
from datetime import timedelta
from types import SimpleNamespace

answer_stream = sys.stdout
sys.stdout = sys.stderr

import jwt
from dev_health_ops.api.services.auth import AuthService

spec = json.load(sys.stdin)
default_principal = spec["principal"]


class Result:
    def __init__(self, row):
        self.row = row

    def one_or_none(self):
        return self.row


class Session:
    def __init__(self, row, principal):
        self.row = row
        self.principal = principal
        self.bound = []

    async def execute(self, statement):
        self.bound = [str(v) for v in statement.compile().params.values() if isinstance(v, uuid.UUID)]
        if self.row is None:
            return Result(None)
        return Result(SimpleNamespace(
            id=uuid.UUID(self.principal["user_id"]),
            is_active=self.row["is_active"],
            is_superuser=self.row["is_superuser"],
            token_version=self.row["token_version"],
        ))


def python_token(mint, principal):
    service = AuthService(secret_key=spec[mint["key"]])
    if mint.get("issuer"):
        service.issuer = mint["issuer"]
    if mint.get("audience"):
        service.audience = mint["audience"]
    return service.create_access_token(
        user_id=principal["user_id"], email=principal["email"], org_id=principal["org_id"],
        role=principal["role"], is_superuser=False, token_version=principal["token_version"],
        expires_delta=timedelta(minutes=mint["expires_minutes"]),
    )


async def judge(token, row, principal):
    session = Session(row, principal)
    user = await AuthService(secret_key=spec["key"]).authenticate_access_token(token, session)
    return {
        "accepted": user is not None,
        "user": dataclasses.asdict(user) if user is not None else None,
        "bound_user_ids": session.bound,
    }


def structure(token):
    header = jwt.get_unverified_header(token)
    payload = jwt.decode(token, options={"verify_signature": False, "verify_aud": False, "verify_exp": False})
    return {
        "header": header,
        "claims": {name: value for name, value in payload.items() if name not in ("iat", "exp", "jti")},
        "ttl_seconds": payload["exp"] - payload["iat"],
        "expired_when_judged": payload["exp"] < time.time(),
        "jti_is_uuid": str(uuid.UUID(payload["jti"])) == payload["jti"],
    }


async def main():
    verdicts = {}
    for case in spec["cases"]:
        principal = case.get("principal") or default_principal
        token = python_token(case["python_mint"], principal)
        verdicts[case["name"]] = {"judgement": await judge(token, case["db_row"], principal), "token": structure(token)}
    answer_stream.write(json.dumps(verdicts, sort_keys=True) + "\n")


asyncio.run(main())
`

// frozenEdgeCase is the input of one recorded case: the Python mint, the users row, the principal. The Go token is
// not part of it.
type frozenEdgeCase struct {
	Name       string           `json:"name"`
	PythonMint oracleMint       `json:"python_mint"`
	DBRow      *oracleRow       `json:"db_row"`
	Principal  *oraclePrincipal `json:"principal,omitempty"`
}

type frozenEdgeAnswer struct {
	Judgement oracleJudgement `json:"judgement"`
	Token     struct {
		Header           map[string]any `json:"header"`
		Claims           map[string]any `json:"claims"`
		TTLSeconds       int            `json:"ttl_seconds"`
		ExpiredWhenJudge bool           `json:"expired_when_judged"`
		JTIIsUUID        bool           `json:"jti_is_uuid"`
	} `json:"token"`
}

// TestGoMintedEdgeTokenMatchesTheFrozenEdgeDecisions replaces the live comparison with a decision golden: the real
// Python edge validator's verdict on a token the Python AuthService mints for each case, frozen, and the structure
// of that token. For every case the Go mint must produce a token of the same structure (header, every claim but the
// three per-run ones, the signing key it claims, whether it is expired when judged, a UUID id) and the recorded
// verdict must be the one the case expects. Equal structure and equal signing state is what makes the Python
// validator judge the Go token as it judged the Python one. The live test stays until the Python delete
// (CHAOS-7308) for the judgement of the Go token itself.
func TestGoMintedEdgeTokenMatchesTheFrozenEdgeDecisions(t *testing.T) {
	_, currentFile, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("resolve edgetokenmint package path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	clearIssuerAudienceEnv(t)
	suite := edgeOracleCases(t)

	recorded := make([]frozenEdgeCase, len(suite.cases))
	for i, c := range suite.cases {
		recorded[i] = frozenEdgeCase{Name: c.Name, PythonMint: c.PythonMint, DBRow: c.DBRow, Principal: c.Principal}
	}
	input, err := json.Marshal(map[string]any{
		"key":       suite.key,
		"other_key": suite.otherKey,
		"principal": map[string]any{
			"user_id": suite.principal.UserID, "email": suite.principal.Email, "org_id": suite.principal.OrgID,
			"role": suite.principal.Role, "token_version": suite.principal.TokenVersion,
		},
		"cases": recorded,
	})
	if err != nil {
		t.Fatal(err)
	}
	output := []byte(edgeMintGoldens.Outputs(t, root, "edge-mint-decisions.golden.json",
		programoracle.Program{Name: "edge mint decisions", Text: frozenEdgeProgram, Stdin: input})[0])
	var answers map[string]frozenEdgeAnswer
	if err := json.Unmarshal(output, &answers); err != nil {
		t.Fatalf("decode the frozen edge decisions: %v", err)
	}
	if len(answers) != len(suite.cases) {
		t.Fatalf("the golden holds %d cases, the oracle has %d", len(answers), len(suite.cases))
	}

	keys := map[string]string{"key": suite.key, "other_key": suite.otherKey}
	now := time.Now()
	for _, tc := range suite.cases {
		want, found := answers[tc.Name]
		if !found {
			t.Errorf("%s: no recorded decision", tc.Name)
			continue
		}
		if want.Judgement.Accepted != tc.accepted {
			t.Errorf("%s: the recorded edge verdict accepted=%v, the case expects %v", tc.Name, want.Judgement.Accepted, tc.accepted)
		}
		expected := oraclePrincipal{UserID: suite.principal.UserID, Email: suite.principal.Email, OrgID: suite.principal.OrgID, Role: suite.principal.Role}
		if tc.Principal != nil {
			expected = *tc.Principal
		}
		if tc.accepted {
			wantUser := map[string]any{
				"user_id": expected.UserID, "email": expected.Email, "org_id": expected.OrgID,
				"role": expected.Role, "is_superuser": false, "is_superuser_verified": true,
				"token_version": float64(2), "username": nil, "full_name": nil, "impersonated_by": nil,
			}
			if !reflect.DeepEqual(want.Judgement.User, wantUser) {
				t.Errorf("%s: the recorded authenticated user = %v, want %v", tc.Name, want.Judgement.User, wantUser)
			}
			if !reflect.DeepEqual(want.Judgement.BoundUserIDs, []string{expected.UserID}) {
				t.Errorf("%s: the recorded edge looked up %v, want %s", tc.Name, want.Judgement.BoundUserIDs, expected.UserID)
			}
		}

		// The Go token: same structure as the Python token of the case, signed by the key the case names.
		signingKey := keys[tc.PythonMint.Key]
		parsed, claims := parseUnverifiedToken(t, tc.Name, tc.GoToken)
		if !reflect.DeepEqual(normalizeJSON(t, parsed.Header), normalizeJSON(t, want.Token.Header)) {
			t.Errorf("%s: Go token header = %v, Python token header = %v", tc.Name, parsed.Header, want.Token.Header)
		}
		stable := map[string]any{}
		for name, value := range claims {
			if name != "iat" && name != "exp" && name != "jti" {
				stable[name] = value
			}
		}
		if !reflect.DeepEqual(normalizeJSON(t, stable), normalizeJSON(t, want.Token.Claims)) {
			t.Errorf("%s: Go token claims = %v, Python token claims = %v", tc.Name, stable, want.Token.Claims)
		}
		if id, _ := claims["jti"].(string); want.Token.JTIIsUUID != (uuid.Validate(id) == nil) {
			t.Errorf("%s: Go token id %q, Python token id is a UUID = %v", tc.Name, id, want.Token.JTIIsUUID)
		}
		exp, _ := claims["exp"].(float64)
		iat, _ := claims["iat"].(float64)
		if expired := time.Unix(int64(exp), 0).Before(now); expired != want.Token.ExpiredWhenJudge {
			t.Errorf("%s: Go token expired = %v, Python token expired when judged = %v", tc.Name, expired, want.Token.ExpiredWhenJudge)
		}
		// The lifetime: a token that is not expired is minted with the default lifetime on both sides. The
		// expired case is built differently on each side (Python with a negative lifetime, Go with a past issue
		// time), so only its expired state is compared.
		if !want.Token.ExpiredWhenJudge && int(exp-iat) != want.Token.TTLSeconds {
			t.Errorf("%s: Go token lifetime = %d seconds, Python token lifetime = %d", tc.Name, int(exp-iat), want.Token.TTLSeconds)
		}
		// The signature: valid under the key the case names, and (the wrong-key case) not under the edge's key.
		if _, err := jwt.Parse(tc.GoToken, func(*jwt.Token) (any, error) { return []byte(signingKey), nil },
			jwt.WithValidMethods([]string{"HS256"}), jwt.WithoutClaimsValidation()); err != nil {
			t.Errorf("%s: Go token does not verify under the %s it names: %v", tc.Name, tc.PythonMint.Key, err)
		}
		if tc.PythonMint.Key == "other_key" {
			if _, err := jwt.Parse(tc.GoToken, func(*jwt.Token) (any, error) { return []byte(suite.key), nil },
				jwt.WithValidMethods([]string{"HS256"}), jwt.WithoutClaimsValidation()); err == nil {
				t.Errorf("%s: the wrong-key Go token verifies under the edge's key", tc.Name)
			}
		}
	}
}

func parseUnverifiedToken(t *testing.T, name, token string) (*jwt.Token, jwt.MapClaims) {
	t.Helper()
	claims := jwt.MapClaims{}
	parsed, _, err := jwt.NewParser().ParseUnverified(token, claims)
	if err != nil {
		t.Fatalf("%s: decode Go token: %v", name, err)
	}
	return parsed, claims
}

// normalizeJSON is value as JSON round-tripped, so numbers compare as the golden's decoded numbers do.
func normalizeJSON(t *testing.T, value any) any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.NewDecoder(strings.NewReader(string(raw))).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}
