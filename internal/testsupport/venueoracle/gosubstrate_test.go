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

func TestAFrozenGoldenRefusesAVenueBuiltWithPython(t *testing.T) {
	golden := &Golden{spec: GoldenSpec{Path: "g.json"}}
	if err := golden.frozenVenueErr(t, &Venue{}); err == nil || !strings.Contains(err.Error(), "Options.Golden") {
		t.Fatalf("a Python-built venue was accepted: %v", err)
	}
	if err := golden.frozenVenueErr(t, frozenVenue()); err != nil {
		t.Fatalf("a frozen venue was refused: %v", err)
	}
	if err := golden.frozenVenueErr(t, nil); err != nil {
		t.Fatalf("no venue was refused: %v", err)
	}
	// No venue named, but the test's tree started a live one: refused, in the
	// test itself and in its subtests (the reviewer's bypass: a subtest is
	// another *testing.T).
	liveVenues.Store(rootTestName(t), struct{}{})
	t.Cleanup(func() { liveVenues.Delete(rootTestName(t)) })
	if err := golden.frozenVenueErr(t, nil); err == nil || !strings.Contains(err.Error(), "built with Python") {
		t.Fatalf("frozen answers in a test with a live venue were accepted: %v", err)
	}
	t.Run("child", func(child *testing.T) {
		if err := golden.frozenVenueErr(child, nil); err == nil || !strings.Contains(err.Error(), "built with Python") {
			child.Fatalf("frozen answers in a subtest of a test with a live venue were accepted: %v", err)
		}
		child.Run("grandchild", func(grandchild *testing.T) {
			if err := liveVenueErr(grandchild, "x"); err == nil {
				grandchild.Fatal("a nested subtest is outside the guard")
			}
		})
	})
}

// A live venue started by a subtest marks the whole tree: its parent and its
// sibling subtests are refused too.
func TestALiveVenueInASubtestMarksTheWholeTestTree(t *testing.T) {
	t.Cleanup(func() { liveVenues.Delete(rootTestName(t)) })
	if err := liveVenueErr(t, "x"); err != nil {
		t.Fatalf("refused before any live venue: %v", err)
	}
	t.Run("starts a live venue", func(child *testing.T) {
		liveVenues.Store(rootTestName(child), struct{}{}) // what Start does
	})
	if err := liveVenueErr(t, "x"); err == nil {
		t.Fatal("the parent of a subtest with a live venue is outside the guard")
	}
	t.Run("sibling", func(sibling *testing.T) {
		if err := liveVenueErr(sibling, "x"); err == nil {
			sibling.Fatal("a sibling of a subtest with a live venue is outside the guard")
		}
	})
}

// liveTreeChild runs body in a child process, as a subtest of a test whose
// tree started a live venue, and returns the child's output and error.
func liveTreeChild(t *testing.T, env string, body func(child *testing.T)) (string, error, bool) {
	t.Helper()
	if os.Getenv(env) == "1" {
		liveVenues.Store(rootTestName(t), struct{}{})
		t.Run("child", body)
		return "", nil, true
	}
	command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	command.Env = append(os.Environ(), env+"=1")
	output, err := command.CombinedOutput()
	return string(output), err, false
}

// A Go-only proof in the tree of a test that started a live venue
// (Python-built, no GoOnly, no golden) fails the test before the proof is
// written, also from a subtest.
func TestAGoOnlyProofIsRefusedInATestWithALiveVenue(t *testing.T) {
	output, err, inChild := liveTreeChild(t, "VENUEORACLE_GO_ONLY_LIVE_CHILD", func(child *testing.T) {
		child.Setenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR", child.TempDir())
		WriteGoOnlyProof(child, "measures nothing")
		child.Log("PROOF WRITTEN")
	})
	if inChild {
		return
	}
	if err == nil || !strings.Contains(output, "built with Python") || strings.Contains(output, "PROOF WRITTEN") {
		t.Fatalf("a Go-only proof was written in a test with a live venue (err %v):\n%s", err, output)
	}
}

// A frozen Produce in the tree of a test that started a live venue is refused
// before an answer is read, also from a subtest; so no golden proof follows.
func TestFrozenProduceIsRefusedInATestWithALiveVenue(t *testing.T) {
	output, err, inChild := liveTreeChild(t, "VENUEORACLE_PRODUCE_LIVE_CHILD", func(child *testing.T) {
		child.Setenv(goldenUpdateEnv, "")
		child.Setenv(goldenCandidateEnv, "")
		child.Setenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR", child.TempDir())
		request := ProgramRequest("corpus", sampleProgram, []byte("abc"), nil)
		path, digest := programGolden(child, []Request{request}, "ABC\n")
		golden := OpenGolden(child, GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"})
		answers := golden.Produce(child, "/no/python/here", []Request{request}, func(*Producer, []Request) []Response { return nil })
		// Reached only if Produce served the answer: the proof guard in
		// Finish would still fail the test, so the answer is the marker.
		child.Logf("ANSWER SERVED %q", answers[0].Body)
	})
	if inChild {
		return
	}
	if err == nil || !strings.Contains(output, "built with Python") || strings.Contains(output, "ANSWER SERVED") {
		t.Fatalf("a frozen answer was served in a test with a live venue (err %v):\n%s", err, output)
	}
}

// The mark works in both orders: a live venue started after a Python-free
// claim in the same tree (a frozen answer served, a Go-only proof written) is
// refused, from the parent, a sibling or a nested subtest.
func TestALiveVenueIsRefusedAfterAPythonFreeClaimInItsTree(t *testing.T) {
	t.Cleanup(func() { frozenTrees.Delete(rootTestName(t)) })
	if err := frozenTreeErr(t); err != nil {
		t.Fatalf("refused before any claim: %v", err)
	}
	t.Run("claims", func(child *testing.T) { markFrozenTree(child, "a Go-only proof") })
	if err := frozenTreeErr(t); err == nil || !strings.Contains(err.Error(), "already has a Go-only proof") {
		t.Fatalf("the parent of a subtest that made a claim may start a live venue: %v", err)
	}
	t.Run("sibling", func(sibling *testing.T) {
		if err := frozenTreeErr(sibling); err == nil {
			sibling.Fatal("a sibling of a subtest that made a claim may start a live venue")
		}
		sibling.Run("nested", func(nested *testing.T) {
			if err := frozenTreeErr(nested); err == nil {
				nested.Fatal("a nested subtest is outside the guard")
			}
		})
	})
}

// laterLiveStart runs first (a Python-free claim) and then the real Start with
// no GoOnly and no golden, as two sibling subtests in a child process, and
// returns the child's output. Start must refuse before it builds anything.
func laterLiveStart(t *testing.T, env string, first func(child *testing.T)) (string, error, bool) {
	t.Helper()
	if os.Getenv(env) == "1" {
		t.Setenv("DEV_HEALTH_VENUE_ORACLES", "1")
		t.Setenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR", t.TempDir())
		t.Run("first", first)
		t.Run("later", func(later *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			Start(later, ctx, Options{Root: later.TempDir(), JWTKey: "k"})
			later.Log("LIVE VENUE STARTED")
		})
		return "", nil, true
	}
	command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	command.Env = append(os.Environ(), env+"=1")
	output, err := command.CombinedOutput()
	return string(output), err, false
}

func TestStartRefusesALiveVenueAfterAFrozenAnswerInItsTree(t *testing.T) {
	output, err, inChild := laterLiveStart(t, "VENUEORACLE_LATER_LIVE_ANSWER_CHILD", func(child *testing.T) {
		child.Setenv(goldenUpdateEnv, "")
		child.Setenv(goldenCandidateEnv, "")
		request := ProgramRequest("corpus", sampleProgram, []byte("abc"), nil)
		path, digest := programGolden(child, []Request{request}, "ABC\n")
		golden := OpenGolden(child, GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"})
		answers := golden.Produce(child, "/no/python/here", []Request{request}, func(*Producer, []Request) []Response { return nil })
		golden.Consumed(child, answers...)
		golden.SkipDiff(child)
		golden.Finish(child)
	})
	if inChild {
		return
	}
	if err == nil || !strings.Contains(output, "already has the frozen answers of golden") || strings.Contains(output, "LIVE VENUE STARTED") {
		t.Fatalf("a live venue started after a frozen answer in its tree (err %v):\n%s", err, output)
	}
}

func TestStartRefusesALiveVenueAfterAGoOnlyProofInItsTree(t *testing.T) {
	output, err, inChild := laterLiveStart(t, "VENUEORACLE_LATER_LIVE_PROOF_CHILD", func(child *testing.T) {
		WriteGoOnlyProof(child, "measures something without Python")
	})
	if inChild {
		return
	}
	if err == nil || !strings.Contains(output, "already has a Go-only proof") || strings.Contains(output, "LIVE VENUE STARTED") {
		t.Fatalf("a live venue started after a Go-only proof in its tree (err %v):\n%s", err, output)
	}
}
