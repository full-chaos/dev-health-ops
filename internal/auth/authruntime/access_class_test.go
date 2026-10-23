package authruntime

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

type stubSigner struct{ calls int }

func (s *stubSigner) Sign(context.Context, string, []byte) (string, string, error) {
	s.calls++
	return "", "", errors.New("stub")
}

func classConfig(t *testing.T, class string) authconfig.Config {
	t.Helper()
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(map[string]string{
		authconfig.EnvDatabaseURI:         "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:      "/nonexistent",
		authconfig.EnvSigningKeyID:        "class-test",
		authconfig.EnvAccessTokenClass:    class,
		authconfig.EnvTokenIssuerURL:      "https://auth.example.test",
		authconfig.EnvAccessTokenSecret:   "class-test-access-secret-0123456789abcdef",
		authconfig.EnvSessionSuccessorKey: base64.RawURLEncoding.EncodeToString([]byte("class-test-successor-key-0032-ab")),
	})})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return cfg
}

// The prod constructor path: the class in the config picks the one issuer.
func TestTheConfiguredAccessTokenClassPicksTheIssuer(t *testing.T) {
	grant := authapi.Grant{PrincipalID: uuid.New(), SessionID: uuid.New()}

	stub := &stubSigner{}
	hs, err := newAccessIssuer(classConfig(t, "hs256_compat"), stub)
	if err != nil {
		t.Fatal(err)
	}
	if token, err := hs.AccessContext(context.Background(), grant); err != nil || token == "" || stub.calls != 0 {
		t.Fatalf("hs256_compat: err %v, %d key-set signer calls", err, stub.calls)
	}

	ed, err := newAccessIssuer(classConfig(t, "eddsa"), stub)
	if err != nil {
		t.Fatal(err)
	}
	if token, err := ed.AccessContext(context.Background(), grant); err == nil || token != "" || stub.calls != 1 {
		t.Fatalf("eddsa: minted %q, err %v, %d key-set signer calls; want the signer used once and its failure final", token, err, stub.calls)
	}

	if _, err := newAccessIssuer(classConfig(t, "eddsa"), nil); err == nil {
		t.Fatal("an eddsa issuer was built without a signer")
	}
	cfg := classConfig(t, "hs256_compat")
	cfg.Session.AccessClass = "both"
	if _, err := newAccessIssuer(cfg, stub); err == nil {
		t.Fatal("an unknown class built an issuer")
	}
}
