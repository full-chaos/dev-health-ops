package venueoracle

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
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
// (bearerIdentity), so a Go-minted token must name the same caller the
// recording's Python-minted token named. The claims below are
// create_access_token's payload for the same arguments (auth.py), minus the
// volatile ones.
func TestGoMintedTokensKeyTheCallerThePythonTokenKeyed(t *testing.T) {
	v := frozenVenue()
	tokens := v.mintGo(t, "frozen-venue-mint-key-0123456789abcdef", map[string]map[string]any{
		"plain": {"user_id": "u-1", "email": "a@b.c"},
		"full": {"user_id": "u-2", "email": "d@e.f", "org_id": "o-1", "role": "admin", "is_superuser": true,
			"username": "n", "full_name": "N", "token_version": 4, "impersonating_user_id": "s-1"},
	})
	want := map[string]string{
		"plain": `{"aud":"dev-health-api","email":"a@b.c","is_superuser":false,"iss":"dev-health-ops","org_id":"","role":"member","sub":"u-1","tv":0,"type":"access"}`,
		"full":  `{"aud":"dev-health-api","email":"d@e.f","full_name":"N","impersonating_user_id":"s-1","is_superuser":true,"iss":"dev-health-ops","org_id":"o-1","role":"admin","sub":"u-2","tv":4,"type":"access","username":"n"}`,
	}
	for name, claims := range want {
		identity := bearerIdentity("Bearer " + tokens[name])
		if identity != `Bearer header:{"alg":"HS256","typ":"JWT"} claims:`+claims {
			t.Errorf("%s: %s", name, identity)
		}
	}
	// JWT_ISSUER and JWT_AUDIENCE on the Python plane are the token's.
	custom := frozenVenue("JWT_ISSUER=iss-x", "JWT_AUDIENCE=aud-y")
	token := custom.mintGo(t, "frozen-venue-mint-key-0123456789abcdef", map[string]map[string]any{"t": {"user_id": "u", "email": "e"}})["t"]
	verifier, err := edgetoken.New("frozen-venue-mint-key-0123456789abcdef", "iss-x", "aud-y")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Verify(token); err != nil {
		t.Fatalf("a token minted under JWT_ISSUER/JWT_AUDIENCE does not verify under them: %v", err)
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
