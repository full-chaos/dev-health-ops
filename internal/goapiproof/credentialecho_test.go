package goapiproof

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// echoTestToken is a JWT-shaped value assembled at run time (no committed
// literal a secret scanner would read as a key): three distinct parts, each
// longer than the guard's part and window lengths.
func echoTestToken() string {
	return strings.Join([]string{
		strings.Repeat("Hd", 15),              // 30 bytes
		strings.Repeat("Pl", 9),               // 18 bytes: a part shorter than the window, longer than the part minimum
		strings.Repeat("?>", 30) + "Zz" + "y", // 63 bytes; its base64 has '+'/'/' forms and needs padding
	}, ".")
}

func secretsFor(value string) SentSecrets {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+value)
	header.Set("Content-Type", "application/json")
	return SecretsOnRequest(header)
}

func TestSentSecretsRecognisesTheReflectionForms(t *testing.T) {
	token := echoTestToken()
	secrets := secretsFor(token)
	if secrets.Empty() {
		t.Fatal("a bearer must be a guarded secret")
	}
	half := len(token) / 2
	for name, body := range map[string]string{
		"raw":                  `{"seen":"` + token + `"}`,
		"raw with scheme":      `{"seen":"Bearer ` + token + `"}`,
		"base64 std":           base64.StdEncoding.EncodeToString([]byte(token)),
		"base64 raw std":       base64.RawStdEncoding.EncodeToString([]byte(token)),
		"base64 url":           base64.URLEncoding.EncodeToString([]byte(token)),
		"base64 raw url":       base64.RawURLEncoding.EncodeToString([]byte(token)),
		"payload part only":    `{"claims":"` + strings.Repeat("Pl", 9) + `"}`,
		"split in two fields":  `{"a":"` + token[:half] + `","b":"` + token[half:] + `"}`,
		"piece of a long run":  `{"x":"` + token[10:10+echoWindowLen] + `"}`,
		"padded by other text": strings.Repeat("noise ", 500) + token + strings.Repeat(" tail", 500),
	} {
		if !secrets.ReflectedIn([]byte(body)) {
			t.Errorf("%s: a reflected credential went unseen", name)
		}
	}
}

func TestSentSecretsLeavesOrdinaryTextAndTheStatedLimitsAlone(t *testing.T) {
	token := echoTestToken()
	secrets := secretsFor(token)
	for name, body := range map[string]string{
		"empty":                "",
		"ordinary json":        `{"items":[{"id":"abc","name":"x"}],"total":1}`,
		"content type":         `{"media":"application/json"}`,
		"short piece of value": `{"x":"` + token[10:10+echoWindowLen-1] + `"}`,
	} {
		if secrets.ReflectedIn([]byte(body)) {
			t.Errorf("%s: flagged, but it carries no recognised form of the credential", name)
		}
	}
	if !secretsFor("short").Empty() {
		t.Error("a value shorter than the minimum must not be guarded (it would match ordinary text)")
	}
	if !secretsFor("").Empty() {
		t.Error("an empty value must not be guarded")
	}
}

func TestSentSecretsHeaderReflects(t *testing.T) {
	token := echoTestToken()
	secrets := secretsFor(token)
	header := http.Header{}
	header.Set("x-dev-health-build", "abc123def456")
	header.Set("Server", "uvicorn")
	if secrets.HeaderReflects(header) {
		t.Fatal("ordinary headers flagged")
	}
	header.Set("x-dev-health-build", token)
	if !secrets.HeaderReflects(header) {
		t.Fatal("a header value that carries the credential went unseen")
	}
	// Header NAMES are not searched.
	named := http.Header{}
	named.Set("X-"+strings.Repeat("Pl", 9), "v")
	if secrets.HeaderReflects(named) {
		t.Fatal("a header name is not a value")
	}
}

// The window search must stay linear in the body: a multi-megabyte body with
// no credential in it is scanned well under a second.
func TestSentSecretsScansALargeBodyQuickly(t *testing.T) {
	secrets := secretsFor(echoTestToken())
	body := []byte(strings.Repeat(`{"id":"0123456789abcdef","v":1},`, 200000)) // ~6 MB
	start := time.Now()
	if secrets.ReflectedIn(body) {
		t.Fatal("no credential in this body")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("scanning %d bytes took %s", len(body), elapsed)
	}
}

// TestFetchBuildIdentityRefusesAResponseThatReflectsTheCredential (CHAOS-6612):
// the /buildinfo commit is printed and written into every receipt, so a route
// that reflects the bearer it was sent as `commit` (or in a header) must be
// refused before it can become a build identity -- and the refusal must not
// carry the value.
func TestFetchBuildIdentityRefusesAResponseThatReflectsTheCredential(t *testing.T) {
	token := echoTestToken()
	credential := StaticCredential("Authorization", "candidate bearer", token)
	serve := func(t *testing.T, handler http.HandlerFunc) string {
		t.Helper()
		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)
		return server.URL + "/buildinfo"
	}
	for name, handler := range map[string]http.HandlerFunc{
		"commit reflects the bearer": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"commit":"` + strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") + `","modified":false}`))
		},
		"commit is the bearer base64": func(w http.ResponseWriter, r *http.Request) {
			seen := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			_, _ = w.Write([]byte(`{"commit":"` + base64.RawURLEncoding.EncodeToString([]byte(seen)) + `","modified":false}`))
		},
		"a header reflects the bearer": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Debug", strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
			_, _ = w.Write([]byte(`{"commit":"abc123def456","modified":false}`))
		},
	} {
		commit, err := FetchBuildIdentity(t.Context(), NewLegClient(0), serve(t, handler), credential)
		if err == nil {
			t.Errorf("%s: accepted build identity %q, want a refusal", name, commit)
			continue
		}
		if strings.Contains(err.Error(), token) || commit != "" {
			t.Errorf("%s: the refusal or result carries the credential: %q / %v", name, commit, err)
		}
	}
	// An honest build identity still passes.
	ok := serve(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"commit":"abc123def456","modified":false}`))
	})
	if commit, err := FetchBuildIdentity(t.Context(), NewLegClient(0), ok, credential); err != nil || commit != "abc123def456" {
		t.Fatalf("an honest /buildinfo must still pass: %q, %v", commit, err)
	}
}

// jwtLike builds a JWT-shaped value at run time: the standard HS256 header
// (shared by every token of that algorithm) plus a payload and signature that
// are unique to the value.
func jwtLike(payload, signature string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	return strings.Join([]string{header, payload, signature}, ".")
}

// TestSentSecretsIgnoresAJWTHeaderShareButSeesItsPayloadAndSignature (r1 P1):
// two tokens of one algorithm share their header part, so a route that
// honestly returns ANOTHER token must not be refused for it; the payload and
// signature parts of the request's own token still are the credential.
func TestSentSecretsIgnoresAJWTHeaderShareButSeesItsPayloadAndSignature(t *testing.T) {
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"proof-principal","org":"org-one","exp":1900000000}`))
	signature := base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("sig-one-", 8)))
	own := jwtLike(payload, signature)
	secrets := secretsFor(own)
	otherPayload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"someone-else","org":"org-two","exp":1800000000}`))
	otherSignature := base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat("sig-two-", 8)))
	other := jwtLike(otherPayload, otherSignature)

	if secrets.ReflectedIn([]byte(`{"access_token":"` + other + `"}`)) {
		t.Fatal("an unrelated JWT that only shares the header was refused: an honest token-returning route could not be proved")
	}
	header := http.Header{}
	header.Set("X-Issued-Token", other)
	if secrets.HeaderReflects(header) {
		t.Fatal("an unrelated JWT in a header was refused")
	}
	for name, body := range map[string]string{
		"the whole token":      `{"t":"` + own + `"}`,
		"payload part only":    `{"t":"` + payload + `"}`,
		"signature part only":  `{"t":"` + signature + `"}`,
		"payload.signature":    `{"t":"` + payload + "." + signature + `"}`,
		"split in two fields":  `{"a":"` + own[len(own)/2:] + `","b":"` + own[:len(own)/2] + `"}`,
		"header alone is fine": "",
	} {
		reflected := secrets.ReflectedIn([]byte(body))
		if name == "header alone is fine" {
			if secrets.ReflectedIn([]byte(strings.Split(own, ".")[0])) {
				t.Error("the header part alone must not count as the credential")
			}
			continue
		}
		if !reflected {
			t.Errorf("%s: a reflected credential went unseen", name)
		}
	}
}

// TestGraphQLPostNeverStoresAReflectedCredential (r1 P1): the GraphQL prover's
// own request path stores each response as an artifact, so it carries the same
// guard as the REST prover's.
func TestGraphQLPostNeverStoresAReflectedCredential(t *testing.T) {
	token := echoTestToken()
	dir := t.TempDir()
	store, err := NewArtifactStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	runner := &Runner{Client: NewLegClient(0), Artifacts: store, Config: Config{Timeout: time.Second}}
	credential := StaticCredential("Authorization", "candidate bearer", token)
	reflecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"seen":"` + strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") + `"}}`))
	}))
	defer reflecting.Close()
	observation, err := runner.post(t.Context(), reflecting.URL, "query { x }", credential, nil)
	if err == nil || strings.Contains(err.Error(), token) || len(observation.Body) != 0 || observation.BodyRef != "" {
		t.Fatalf("a reflected credential must be refused without the value or a stored ref: %v %+v", err, observation)
	}
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		content, _ := os.ReadFile(filepath.Join(dir, entry.Name()))
		if strings.Contains(string(content), token) {
			t.Fatalf("artifact %s contains the credential", entry.Name())
		}
	}
	honest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"data":{}}`)) }))
	defer honest.Close()
	if observation, err := runner.post(t.Context(), honest.URL, "query { x }", credential, nil); err != nil || observation.StatusCode != 200 || observation.BodyRef == "" {
		t.Fatalf("an honest response must still be stored: %v %+v", err, observation)
	}
}

// TestSentSecretsNarrowingIsForRealJWTsOnly (r2 P1s): (a) a newly issued token
// for the SAME principal shares long payload runs with the request's token, so
// it must pass (a refresh route answers exactly that); (b) a value that merely
// LOOKS like a JWT (starts "eyJ", two dots) but whose first part is not a JSON
// header is opaque, so a 24-byte fragment of it is still a reflection.
func TestSentSecretsNarrowingIsForRealJWTsOnly(t *testing.T) {
	claims := func(iat, exp string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"proof-principal","org":"org-one","role":"admin","iat":` + iat + `,"exp":` + exp + `}`))
	}
	sign := func(seed string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(strings.Repeat(seed, 8)))
	}
	own := jwtLike(claims("1800000000", "1800003600"), sign("sig-a-"))
	renewed := jwtLike(claims("1800000900", "1800004500"), sign("sig-b-"))
	secrets := secretsFor(own)
	if secrets.ReflectedIn([]byte(`{"access_token":"` + renewed + `"}`)) {
		t.Fatal("a newly issued token for the same principal was refused: a refresh route could not be proved")
	}
	if !secrets.ReflectedIn([]byte(`{"access_token":"` + own + `"}`)) {
		t.Fatal("the request's own token must still be refused")
	}

	// A JWT whose signature part is shorter than the run length is still seen
	// by the part rule.
	shortSig := jwtLike(claims("1800000000", "1800003600"), strings.Repeat("Sq", 9))
	if !secretsFor(shortSig).ReflectedIn([]byte(`{"s":"` + strings.Repeat("Sq", 9) + `"}`)) {
		t.Fatal("a short signature part reflected alone must be refused")
	}

	opaque := "eyJ" + strings.Repeat("Ab1", 12) + "." + strings.Repeat("Cd2", 12) + "." + strings.Repeat("Ef3", 12) // not a JWT: first part is not a JSON header
	opaqueSecrets := secretsFor(opaque)
	fragment := opaque[:echoWindowLen]
	if !opaqueSecrets.ReflectedIn([]byte(`{"x":"` + fragment + `"}`)) {
		t.Fatal("a 24-byte fragment of an opaque bearer that merely starts with eyJ was not refused")
	}
}
