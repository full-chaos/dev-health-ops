package venueoracle

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

// pythonCiphertext is encrypt_value's output for pythonPlaintext under
// SETTINGS_ENCRYPTION_KEY=frozen-venue-fixture-key (default salt), EXECUTED
// once on ops a4847c5e93607451a0c987b314d37e02fc43ce85:
//
//	SETTINGS_ENCRYPTION_KEY=frozen-venue-fixture-key PYTHONPATH=src .venv/bin/python -c \
//	  "from dev_health_ops.core.encryption import encrypt_value; import json; print(json.dumps(encrypt_value(<plaintext>)))"
//
// and pythonDecrypted is json.dumps(decrypt_value(pythonCiphertext)) on the
// same build. A frozen venue opens every stored secret with the Go port, so
// this pins the one property a Go-to-Go round trip cannot: Go reads what
// Python wrote, and writes the JSON Python's json.dumps writes.
const (
	pythonCiphertext = "v1:gAAAAABqvXHd_VjOEretz5sM70XF9R0ega2C7sUYl3vDmZQFoKpfJyn32mYj0c_TzD37ThikDY_cWrqHGHEAkbic6OifPTCYHxZxZSDLSFT9V1mdhFUbZvs="
	pythonPlaintext  = "p\u00e4ss \"w\\ord\" \U0001f600"
	pythonDecrypted  = `"p\u00e4ss \"w\\ord\" \ud83d\ude00"`
)

func frozenVenue(env ...string) *Venue {
	return &Venue{frozen: true, pythonEnv: env}
}

func TestFrozenDecryptOpensWhatPythonEncryptedAsJSONDumpsWritesIt(t *testing.T) {
	v := frozenVenue("SETTINGS_ENCRYPTION_KEY=frozen-venue-fixture-key")
	out, err := v.callGoErr([]PythonCall{{Target: "dev_health_ops.core.encryption:decrypt_value", Args: []any{pythonCiphertext}}})
	if err != nil {
		t.Fatal(err)
	}
	if string(out[0]) != pythonDecrypted {
		t.Fatalf("decrypt_value answered %s, Python wrote %s", out[0], pythonDecrypted)
	}
	wrongKey := frozenVenue("SETTINGS_ENCRYPTION_KEY=another-key")
	if _, err := wrongKey.callGoErr([]PythonCall{{Target: "dev_health_ops.core.encryption:decrypt_value", Args: []any{pythonCiphertext}}}); err == nil {
		t.Fatal("a ciphertext opened under the wrong key")
	}
	// The salt is part of the key, as SETTINGS_ENCRYPTION_SALT is in Python.
	salted := frozenVenue("SETTINGS_ENCRYPTION_KEY=frozen-venue-fixture-key", "SETTINGS_ENCRYPTION_SALT=other-salt")
	if _, err := salted.callGoErr([]PythonCall{{Target: "dev_health_ops.core.encryption:decrypt_value", Args: []any{pythonCiphertext}}}); err == nil {
		t.Fatal("a ciphertext opened under another salt")
	}
}

func TestFrozenEncryptWritesAVersionedCiphertextTheDecryptOpens(t *testing.T) {
	v := frozenVenue("SETTINGS_ENCRYPTION_KEY=first", "SETTINGS_ENCRYPTION_KEY=frozen-venue-fixture-key")
	out, err := v.callGoErr([]PythonCall{{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{pythonPlaintext}}})
	if err != nil {
		t.Fatal(err)
	}
	var ciphertext string
	if err := json.Unmarshal(out[0], &ciphertext); err != nil || !strings.HasPrefix(ciphertext, "v1:gAAAAA") {
		t.Fatalf("encrypt_value answered %s (%v)", out[0], err)
	}
	// The later SETTINGS_ENCRYPTION_KEY entry won: the ciphertext opens under it.
	opened, err := frozenVenue("SETTINGS_ENCRYPTION_KEY=frozen-venue-fixture-key").callGoErr([]PythonCall{{Target: "dev_health_ops.core.encryption:decrypt_value", Args: []any{ciphertext}}})
	if err != nil {
		t.Fatal(err)
	}
	if string(opened[0]) != pythonDecrypted {
		t.Fatalf("round trip answered %s, want %s", opened[0], pythonDecrypted)
	}
}

func TestFrozenCallsRefuseWhatPythonWouldRefuseOrCannotAnswer(t *testing.T) {
	cases := []struct {
		name  string
		venue *Venue
		call  PythonCall
		want  string
	}{
		{"no key", frozenVenue(), PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{"x"}}, "SETTINGS_ENCRYPTION_KEY is not set"},
		{"empty key", frozenVenue("SETTINGS_ENCRYPTION_KEY="), PythonCall{Target: "dev_health_ops.core.encryption:decrypt_value", Args: []any{"x"}}, "SETTINGS_ENCRYPTION_KEY is not set"},
		{"unported target", frozenVenue(), PythonCall{Target: "dev_health_ops.api.services.auth:AuthService"}, "has no Go port"},
		{"two arguments", frozenVenue("SETTINGS_ENCRYPTION_KEY=k"), PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{"a", "b"}}, "one positional argument"},
		{"keyword argument", frozenVenue("SETTINGS_ENCRYPTION_KEY=k"), PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Kwargs: map[string]any{"plaintext": "a"}}, "one positional argument"},
		{"not a string", frozenVenue("SETTINGS_ENCRYPTION_KEY=k"), PythonCall{Target: "dev_health_ops.core.encryption:encrypt_value", Args: []any{7}}, "want a string"},
		{"password over 72 bytes", frozenVenue(), PythonCall{Target: "dev_health_ops.api.services.users:_hash_password", Args: []any{strings.Repeat("p", 73)}}, "_hash_password"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.venue.callGoErr([]PythonCall{c.call})
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want one naming %q", err, c.want)
			}
		})
	}
}

func TestFrozenHashPasswordWritesTheBcryptFormPythonWrites(t *testing.T) {
	out, err := frozenVenue().callGoErr([]PythonCall{{Target: "dev_health_ops.api.services.users:_hash_password", Args: []any{"hunter2"}}})
	if err != nil {
		t.Fatal(err)
	}
	var hash string
	if err := json.Unmarshal(out[0], &hash); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(hash, "$2b$12$") {
		t.Fatalf("hash %q is not bcrypt.gensalt()'s $2b$12$ form", hash)
	}
	if err := bcrypt.CompareHashAndPassword([]byte("$2a$"+strings.TrimPrefix(hash, "$2b$")), []byte("hunter2")); err != nil {
		t.Fatalf("hash does not verify: %v", err)
	}
}

func TestAccessClaimsReadCreateAccessTokenArgumentsWithItsDefaults(t *testing.T) {
	claims, err := accessClaims(map[string]any{"user_id": "u", "email": "e@x"})
	if err != nil {
		t.Fatal(err)
	}
	if claims.Role != "member" || claims.OrgID != "" || claims.IsSuperuser || claims.TokenVersion != 0 ||
		claims.Username != nil || claims.FullName != nil || claims.ImpersonatingUserID != nil {
		t.Fatalf("defaults: %+v", claims)
	}
	claims, err = accessClaims(map[string]any{"user_id": "u", "email": "e@x", "org_id": "o", "role": "admin", "is_superuser": true,
		"username": "n", "full_name": "F", "token_version": 3, "impersonating_user_id": "s"})
	if err != nil {
		t.Fatal(err)
	}
	if claims.UserID != "u" || claims.Email != "e@x" || claims.OrgID != "o" || claims.Role != "admin" || !claims.IsSuperuser ||
		*claims.Username != "n" || *claims.FullName != "F" || claims.TokenVersion != 3 || *claims.ImpersonatingUserID != "s" {
		t.Fatalf("every argument: %+v", claims)
	}
	// None is the default for the optional strings, as in Python.
	claims, err = accessClaims(map[string]any{"user_id": "u", "email": "e@x", "username": nil, "full_name": nil})
	if err != nil || claims.Username != nil || claims.FullName != nil {
		t.Fatalf("None arguments: %+v %v", claims, err)
	}
	for _, c := range []struct {
		spec map[string]any
		want string
	}{
		{map[string]any{"user_id": "u", "email": "e", "expires_delta": 5}, `"expires_delta" has no Go counterpart`},
		{map[string]any{"email": "e"}, "user_id is required"},
		{map[string]any{"user_id": "u"}, "email is required"},
		{map[string]any{"user_id": 5, "email": "e"}, "user_id is int"},
		{map[string]any{"user_id": "u", "email": "e", "is_superuser": "yes"}, "is_superuser is string"},
		{map[string]any{"user_id": "u", "email": "e", "token_version": "1"}, "token_version is string"},
	} {
		if _, err := accessClaims(c.spec); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%v: error %v, want one naming %q", c.spec, err, c.want)
		}
	}
}

// The request key of a frozen golden holds a bearer token by its claims
// (bearerIdentity), so a Go-minted token must key the caller the recording's
// Python-minted token named. The claims below are create_access_token's
// payload for the same arguments (auth.py), minus the volatile ones; the token
// itself is a user access token of the venue's issuer.
func TestGoMintedTokensKeyTheCallerThePythonTokenKeyed(t *testing.T) {
	const u1, u2, s1, o1 = "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-0000000000a1", "00000000-0000-4000-8000-0000000000b1"
	v := frozenVenue()
	tokens := v.mintGo(t, map[string]map[string]any{
		"plain": {"user_id": u1, "email": "a@b.c"},
		"full": {"user_id": u2, "email": "d@e.f", "org_id": o1, "role": "admin", "is_superuser": true,
			"username": "n", "full_name": "N", "token_version": 4, "impersonating_user_id": s1},
	})
	want := map[string]string{
		"plain": `{"aud":"dev-health-api","email":"a@b.c","is_superuser":false,"iss":"dev-health-ops","org_id":"","role":"member","sub":"` + u1 + `","tv":0,"type":"access"}`,
		"full":  `{"aud":"dev-health-api","email":"d@e.f","full_name":"N","impersonating_user_id":"` + s1 + `","is_superuser":true,"iss":"dev-health-ops","org_id":"` + o1 + `","role":"admin","sub":"` + u2 + `","tv":4,"type":"access","username":"n"}`,
	}
	verifier := v.TokenIssuer.Verifier(t)
	for name, claims := range want {
		identity := bearerIdentity("Bearer " + tokens[name])
		if identity != `Bearer header:{"alg":"HS256","typ":"JWT"} claims:`+claims {
			t.Errorf("%s: %s", name, identity)
		}
		got, err := verifier.Verify(tokens[name], time.Now())
		if err != nil {
			t.Fatalf("%s: the minted token is not a user access token of the venue's issuer: %v", name, err)
		}
		if got.Subject != edtokens.PrincipalID(uuid.MustParse(map[string]string{"plain": u1, "full": u2}[name])) {
			t.Errorf("%s: sub %s", name, got.Subject)
		}
	}
	full, err := verifier.Verify(tokens["full"], time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if full.OrgID == nil || *full.OrgID != o1 || full.ActorSubject == nil || *full.ActorSubject != edtokens.PrincipalID(uuid.MustParse(s1)) {
		t.Errorf("full: org %v actor %v, want the seed's organization and impersonating user", full.OrgID, full.ActorSubject)
	}
	// The session pins the seed's token_version, as create_access_token's tv claim does.
	sid, err := sessionid.Parse(full.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if row, found, _ := v.TokenIssuer.Store(policy.PGStore{}).SessionState(context.Background(), sid); !found || row.LegacyTokenVersion == nil || *row.LegacyTokenVersion != 4 {
		t.Errorf("full: session %+v found=%v, want token_version 4 pinned", row, found)
	}
	// JWT_ISSUER and JWT_AUDIENCE on the Python plane are the recorded token's.
	custom := frozenVenue("JWT_ISSUER=iss-x", "JWT_AUDIENCE=aud-y")
	token := custom.mintGo(t, map[string]map[string]any{"t": {"user_id": u1, "email": "e"}})["t"]
	if identity := bearerIdentity("Bearer " + token); !strings.Contains(identity, `"aud":"aud-y"`) || !strings.Contains(identity, `"iss":"iss-x"`) {
		t.Errorf("the recorded caller does not carry JWT_ISSUER/JWT_AUDIENCE: %s", identity)
	}
}

// A venue built with LegacyHS256Tokens mints the recording's class: an HS256 token under the venue's key whose
// identity is the recorded caller's, and no user access token.
func TestLegacyVenueMintsTheRecordedHS256Token(t *testing.T) {
	const u1 = "00000000-0000-4000-8000-000000000001"
	v := frozenVenue()
	v.legacyTokenKey = strings.Repeat("legacy-venue-key-", 3)
	token := v.mintGo(t, map[string]map[string]any{"t": {"user_id": u1, "email": "a@b.c", "token_version": 2}})["t"]
	want := `Bearer header:{"alg":"HS256","typ":"JWT"} claims:{"aud":"dev-health-api","email":"a@b.c","is_superuser":false,"iss":"dev-health-ops","org_id":"","role":"member","sub":"` + u1 + `","tv":2,"type":"access"}`
	if identity := bearerIdentity("Bearer " + token); identity != want {
		t.Errorf("identity %s, want %s", identity, want)
	}
	verifier, err := edgetoken.New(v.legacyTokenKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Verify(token); err != nil {
		t.Errorf("the legacy token does not verify under the venue key: %v", err)
	}
	if v.TokenIssuer != nil {
		t.Error("a legacy venue built a user access token issuer")
	}
}

// An org-less superuser seed is minted without its org claim (as auth-service login mints a superuser with no
// membership), keyed as the recorded caller with the claim; any other seed keeps its claim.
func TestOrglessSuperuserSeedsDropOnlyTheirOrgClaim(t *testing.T) {
	const u1, u2, o1 = "00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-0000000000b1"
	v := frozenVenue()
	v.orglessSuperusers = map[string]bool{"super": true}
	tokens := v.mintGo(t, map[string]map[string]any{
		"super":  {"user_id": u1, "email": "s@x", "org_id": o1, "is_superuser": true},
		"member": {"user_id": u2, "email": "m@x", "org_id": o1},
	})
	verifier := v.TokenIssuer.Verifier(t)
	super, err := verifier.Verify(tokens["super"], time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if super.OrgID != nil {
		t.Errorf("the org-less superuser carries org claim %q", *super.OrgID)
	}
	if identity := bearerIdentity("Bearer " + tokens["super"]); !strings.Contains(identity, `"org_id":"`+o1+`"`) || !strings.Contains(identity, `"is_superuser":true`) {
		t.Errorf("the org-less superuser is not keyed as the recorded caller: %s", identity)
	}
	member, err := verifier.Verify(tokens["member"], time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if member.OrgID == nil || *member.OrgID != o1 {
		t.Errorf("a member seed lost its org claim: %v", member.OrgID)
	}
}

// A seed whose user is not a UUID cannot be a user access token's principal: the venue refuses it by name.
func TestGoMintRefusesANonUUIDUser(t *testing.T) {
	_, err := (legacyClaims{UserID: "u-1"}).userToken()
	if err == nil || !strings.Contains(err.Error(), "not a UUID") {
		t.Fatalf("err %v, want a refusal naming the user id", err)
	}
}

// childRun runs this test again in a child process with env set to 1 and returns the child's output and error;
// inChild reports that the caller is the child.
func childRun(t *testing.T, env string) (output string, err error, inChild bool) {
	t.Helper()
	if os.Getenv(env) == "1" {
		return "", nil, true
	}
	command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	command.Env = append(os.Environ(), env+"=1")
	raw, err := command.CombinedOutput()
	return string(raw), err, false
}

// A golden answers only for the venue built for it (Options.Golden): a venue
// built without it is refused before an answer is read.
func TestAFrozenGoldenRefusesAVenueBuiltWithoutIt(t *testing.T) {
	output, err, inChild := childRun(t, "VENUEORACLE_UNFROZEN_VENUE_CHILD")
	if inChild {
		requests := sampleRequests()
		file := sampleGolden(requests)
		file.Header.Test = t.Name()
		path, digest := writeGoldenFile(t, t.TempDir(), file)
		golden := OpenGolden(t, GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest})
		answers := golden.Python(t, &Venue{}, requests)
		t.Logf("ANSWER SERVED %d", answers[0].Status)
		return
	}
	if err == nil || !strings.Contains(output, "Options.Golden") || strings.Contains(output, "ANSWER SERVED") {
		t.Fatalf("a venue built without the golden was served its answers (err %v):\n%s", err, output)
	}
}

// Start builds a venue only for a golden (Options.Golden) or a Go-only test
// (Options.GoOnly): with neither there is no Python plane to serve, and Start
// refuses before it builds anything.
func TestStartRefusesAVenueWithNeitherAGoldenNorGoOnly(t *testing.T) {
	output, err, inChild := childRun(t, "VENUEORACLE_NO_GOLDEN_START_CHILD")
	if inChild {
		t.Setenv("DEV_HEALTH_VENUE_ORACLES", "1")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		Start(t, ctx, Options{Root: t.TempDir(), JWTKey: "k"})
		t.Log("VENUE STARTED")
		return
	}
	if err == nil || !strings.Contains(output, "Options.Golden) or Options.GoOnly") || strings.Contains(output, "VENUE STARTED") {
		t.Fatalf("Start built a venue with neither a golden nor GoOnly (err %v):\n%s", err, output)
	}
}

// OrglessSuperusers names superuser seeds only: a named member seed, or a name that is no seed, is refused.
func TestOrglessSuperusersRefusesANonSuperuser(t *testing.T) {
	orglessRefusal(t, "VENUEORACLE_ORGLESS_MEMBER_CHILD", "member")
}

func TestOrglessSuperusersRefusesAnUnknownSeed(t *testing.T) {
	orglessRefusal(t, "VENUEORACLE_ORGLESS_UNKNOWN_CHILD", "nobody")
}

func orglessRefusal(t *testing.T, env, seed string) {
	t.Helper()
	output, err, inChild := childRun(t, env)
	if inChild {
		v := frozenVenue()
		v.orglessSuperusers = map[string]bool{seed: true}
		v.mintGo(t, map[string]map[string]any{"member": {"user_id": "00000000-0000-4000-8000-000000000002", "email": "m@x", "org_id": "o"}})
		t.Log("MINTED")
		return
	}
	if err == nil || strings.Contains(output, "MINTED") || !strings.Contains(output, `"`+seed+`"`) {
		t.Errorf("OrglessSuperusers %q was not refused (err %v):\n%s", seed, err, output)
	}
}
