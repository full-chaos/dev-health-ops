package edgetokenmint

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// edgeOracleProgram drives the REAL edge validator,
// AuthService.authenticate_access_token in
// src/dev_health_ops/api/services/auth.py, over two tokens per case: the
// one Go minted and one the Python AuthService mints from the same inputs.
// Only the database session is fake: it answers the validator's own users
// SELECT with the case's row and records which user id the statement bound.
// Inputs arrive on stdin, so no key reaches argv.
const edgeOracleProgram = `
import asyncio, dataclasses, json, sys, uuid
from datetime import timedelta
from types import SimpleNamespace
from dev_health_ops.api.services.auth import AuthService

spec = json.load(sys.stdin)
principal = spec["principal"]


class Result:
    def __init__(self, row):
        self.row = row

    def one_or_none(self):
        return self.row


class Session:
    def __init__(self, row):
        self.row = row
        self.bound = []

    async def execute(self, statement):
        self.bound = [str(v) for v in statement.compile().params.values() if isinstance(v, uuid.UUID)]
        if self.row is None:
            return Result(None)
        return Result(SimpleNamespace(
            id=uuid.UUID(principal["user_id"]),
            is_active=self.row["is_active"],
            is_superuser=self.row["is_superuser"],
            token_version=self.row["token_version"],
        ))


def python_token(mint):
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


async def judge(token, row):
    session = Session(row)
    user = await AuthService(secret_key=spec["key"]).authenticate_access_token(token, session)
    return {
        "accepted": user is not None,
        "user": dataclasses.asdict(user) if user is not None else None,
        "bound_user_ids": session.bound,
    }


async def main():
    verdicts = {}
    for case in spec["cases"]:
        verdicts[case["name"]] = {
            "go": await judge(case["go_token"], case["db_row"]),
            "python": await judge(python_token(case["python_mint"]), case["db_row"]),
        }
    json.dump(verdicts, sys.stdout, sort_keys=True)


asyncio.run(main())
`

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

type oracleCase struct {
	Name       string     `json:"name"`
	GoToken    string     `json:"go_token"`
	PythonMint oracleMint `json:"python_mint"`
	DBRow      *oracleRow `json:"db_row"`
	accepted   bool
}

type oracleJudgement struct {
	Accepted     bool           `json:"accepted"`
	User         map[string]any `json:"user"`
	BoundUserIDs []string       `json:"bound_user_ids"`
}

func oraclePython(t *testing.T, repositoryRoot string) string {
	t.Helper()
	if python := os.Getenv("PYTHON"); python != "" {
		if resolved, err := exec.LookPath(python); err == nil {
			return resolved
		}
	}
	if venv := filepath.Join(repositoryRoot, ".venv", "bin", "python"); fileExists(venv) {
		return venv
	}
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatalf("live Python oracle interpreter: %v", err)
	}
	return python
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// TestGoMintedEdgeTokenIsJudgedByTheLiveEdgeExactlyLikeAPythonMintedOne is
// the cross-runtime proof: for every case, the live Python validator gives
// the Go token and the Python token the SAME verdict and, when it accepts,
// the SAME authenticated user. Each case also pins the expected verdict, so
// the two cannot agree by both refusing everything.
func TestGoMintedEdgeTokenIsJudgedByTheLiveEdgeExactlyLikeAPythonMintedOne(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve edgetokenmint package path")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	clearIssuerAudienceEnv(t)

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
		{Name: "principal row missing", GoToken: mint(key, Options{}), PythonMint: oracleMint{Key: "key", ExpiresMinutes: 10}, DBRow: nil},
	}

	input, err := json.Marshal(map[string]any{
		"key":       key,
		"other_key": otherKey,
		"principal": map[string]any{
			"user_id": principal.UserID, "email": principal.Email, "org_id": principal.OrgID,
			"role": principal.Role, "token_version": principal.TokenVersion,
		},
		"cases": cases,
	})
	if err != nil {
		t.Fatal(err)
	}

	command := exec.Command(oraclePython(t, repositoryRoot), "-c", edgeOracleProgram)
	command.Env = []string{"PYTHONPATH=" + filepath.Join(repositoryRoot, "src")}
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		switch name {
		case "PYTHONPATH", IssuerEnvVar, AudienceEnvVar, SigningKeyEnvVar:
			// The validator must see the defaults both sides were minted
			// under, and nothing from the caller's shell.
		default:
			command.Env = append(command.Env, entry)
		}
	}
	command.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("live Python edge oracle: %v\n%s", err, stderr.String())
	}

	verdicts := map[string]map[string]oracleJudgement{}
	if err := json.Unmarshal(output, &verdicts); err != nil {
		t.Fatalf("decode oracle output: %v", err)
	}
	names := make([]string, 0, len(verdicts))
	for name := range verdicts {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) != len(cases) {
		t.Fatalf("the oracle judged %v; want every one of the %d cases", names, len(cases))
	}

	for _, tc := range cases {
		verdict, ok := verdicts[tc.Name]
		if !ok {
			t.Fatalf("case %q was not judged", tc.Name)
		}
		goSide, pySide := verdict["go"], verdict["python"]
		if goSide.Accepted != tc.accepted || pySide.Accepted != tc.accepted {
			t.Errorf("%s: edge accepted go=%v python=%v, want %v for both", tc.Name, goSide.Accepted, pySide.Accepted, tc.accepted)
			continue
		}
		if !reflect.DeepEqual(goSide.User, pySide.User) {
			t.Errorf("%s: the edge authenticated different users:\n go:     %v\n python: %v", tc.Name, goSide.User, pySide.User)
		}
		if tc.DBRow != nil && tc.accepted && (!reflect.DeepEqual(goSide.BoundUserIDs, []string{ProvePrincipalID}) || !reflect.DeepEqual(pySide.BoundUserIDs, goSide.BoundUserIDs)) {
			t.Errorf("%s: the edge looked up go=%v python=%v, want the proof principal", tc.Name, goSide.BoundUserIDs, pySide.BoundUserIDs)
		}
		if tc.accepted {
			want := map[string]any{
				"user_id": ProvePrincipalID, "email": principal.Email, "org_id": principal.OrgID,
				"role": "viewer", "is_superuser": false, "is_superuser_verified": true,
				"token_version": float64(2), "username": nil, "full_name": nil, "impersonated_by": nil,
			}
			if !reflect.DeepEqual(goSide.User, want) {
				t.Errorf("%s: authenticated user = %v, want %v", tc.Name, goSide.User, want)
			}
		}
	}
	if t.Failed() {
		return
	}
	if err := os.WriteFile(filepath.Join(proofDir, "edgetokenmint-edge-oracle"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}
