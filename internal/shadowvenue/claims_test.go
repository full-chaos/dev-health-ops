package shadowvenue

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	claimUser = "88888888-8888-4888-8888-888888888888"
	claimOrgE = "a3672b32-bbeb-49c1-ace5-8afb36e4e4ea"
)

// jwtOf builds a token of the access-token.v1 shape: header alg/typ, payload sub, exp and an optional org_id.
func jwtOf(t *testing.T, alg, typ, sub, org string, exp time.Time) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": alg, "typ": typ, "kid": "k1"})
	claims := map[string]any{"iss": "i", "sub": sub, "exp": exp.Unix(), "scope": "api:access"}
	if org != "" {
		claims["org_id"] = org
	}
	payload, _ := json.Marshal(claims)
	enc := base64.RawURLEncoding
	return enc.EncodeToString(header) + "." + enc.EncodeToString(payload) + "." + enc.EncodeToString([]byte("sig"))
}

func goodToken(t *testing.T, org string, exp time.Time) string {
	return jwtOf(t, "EdDSA", "at+jwt", WirePrincipal(claimUser), org, exp)
}

func TestWirePrincipalIsPrnAndTheBase64urlOfTheUUID(t *testing.T) {
	if got := WirePrincipal(claimUser); !strings.HasPrefix(got, "prn_") || len(got) != 4+22 {
		t.Fatalf("%q", got)
	}
	if id, ok := PrincipalUUID(WirePrincipal(claimUser)); !ok || id != claimUser {
		t.Fatalf("round trip: %q %v", id, ok)
	}
	for _, bad := range []string{"", "prn_", "usr_AAAAAAAAAAAAAAAAAAAAAA", "prn_!!!"} {
		if _, ok := PrincipalUUID(bad); ok {
			t.Errorf("%q decoded", bad)
		}
	}
}

func TestDecodeClaimsReadsSubOrgAndExpAndRefusesAnythingElse(t *testing.T) {
	exp := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	c, err := DecodeClaims(goodToken(t, claimOrgE, exp))
	if err != nil || c.Sub != WirePrincipal(claimUser) || c.Org != claimOrgE || !c.Exp.Equal(exp) {
		t.Fatalf("%+v %v", c, err)
	}
	if c, err = DecodeClaims(goodToken(t, "", exp)); err != nil || c.Org != "-" {
		t.Fatalf("a token with no org_id reads org %q: %v", c.Org, err)
	}
	secret := goodToken(t, claimOrgE, exp)
	for name, token := range map[string]string{
		"hs256":       jwtOf(t, "HS256", "at+jwt", WirePrincipal(claimUser), "", exp),
		"envelope":    jwtOf(t, "EdDSA", "JWT", WirePrincipal(claimUser), "", exp),
		"no sub":      jwtOf(t, "EdDSA", "at+jwt", "", "", exp),
		"two parts":   strings.Join(strings.Split(secret, ".")[:2], "."),
		"not base64":  "a.!!!.c",
		"empty":       "",
		"no exp":      strings.Join([]string{strings.Split(secret, ".")[0], base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"prn_x"}`)), "c"}, "."),
		"bad org_id":  jwtOf(t, "EdDSA", "at+jwt", WirePrincipal(claimUser), "not-a-uuid", exp),
		"exp as text": strings.Join([]string{strings.Split(secret, ".")[0], base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"prn_x","exp":"soon"}`)), "c"}, "."),
	} {
		_, err := DecodeClaims(token)
		if err == nil {
			t.Errorf("%s accepted", name)
		} else if token != "" && strings.Contains(err.Error(), token) {
			t.Errorf("%s: the token is in the error", name)
		}
	}
}

func TestReadTokenFileWantsAnOwnerOnlyRegularFileHoldingAUserAccessToken(t *testing.T) {
	dir := t.TempDir()
	token := goodToken(t, claimOrgE, time.Now().Add(time.Hour))
	path := filepath.Join(dir, "t.token")
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := ReadTokenFile(path); err != nil || got != token {
		t.Fatalf("%v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadTokenFile(path); err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("a world-readable file: %v", err)
	}
	bad := filepath.Join(dir, "b.token")
	if err := os.WriteFile(bad, []byte(jwtOf(t, "HS256", "JWT", "x", "", time.Now())), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadTokenFile(bad); err == nil {
		t.Fatal("an HS256 envelope accepted")
	}
	link := filepath.Join(dir, "l.token")
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadTokenFile(link); err == nil {
		t.Fatal("a symlink accepted")
	}
	extra := filepath.Join(dir, "extra.token")
	if err := os.WriteFile(extra, []byte(token+"\nsecond-line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadTokenFile(extra); err == nil {
		t.Fatal("a file with a second line after the token accepted")
	}
	fifo := filepath.Join(dir, "f.token")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := ReadTokenFile(fifo); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a FIFO accepted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadTokenFile blocked on a FIFO")
	}
	two := filepath.Join(dir, "two.token")
	if err := os.WriteFile(two, []byte(token+"\n"+token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadTokenFile(two); err == nil {
		t.Fatal("a file of two lines accepted")
	}
}
