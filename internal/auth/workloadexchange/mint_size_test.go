package workloadexchange

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/keystore"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// The longest token the issuer can mint fits WorkloadTokenMaxBytes: an issuer
// URL at AUTH_TOKEN_ISSUER_URL's pattern limit (a 253-byte host, a 5-digit port,
// 8 path segments of 64 bytes), a kid at the keystore's 64-byte limit, the
// longest audience and the latest NumericDate the schema allows. The token
// comes from mint itself, so a claim added to the minter counts.
func TestTheLongestMintableWorkloadTokenFitsTheBound(t *testing.T) {
	kid := strings.Repeat("k", 64)
	if !keystore.KeyIDPattern.MatchString(kid) || keystore.KeyIDPattern.MatchString(kid+"k") {
		t.Fatal("the kid is not at the keystore pattern's limit")
	}
	issuer := "https://" + strings.Repeat("a", 253) + ":65535" + strings.Repeat("/"+strings.Repeat("a", 64), 8)
	if len(issuer) != 787 {
		t.Fatalf("issuer is %d bytes, want 787", len(issuer))
	}
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "signing.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	h := &handler{Deps: Deps{Signer: token.NewFileSigner(token.FileConfig{KeyPath: path, KeyID: kid}), IssuerURL: issuer}}
	now := time.Unix(253_402_300_799-300, 0)
	compact, _, reason := h.mint(context.Background(), Registration{PrincipalID: uuid.New()}, "dev-health-api", now, 300)
	if reason != "" {
		t.Fatalf("mint refused: %s", reason)
	}
	if len(compact) > contracts.WorkloadTokenMaxBytes {
		t.Fatalf("the longest mintable workload token is %d bytes, over WorkloadTokenMaxBytes %d", len(compact), contracts.WorkloadTokenMaxBytes)
	}
	t.Logf("longest mintable workload token: %d bytes (bound %d)", len(compact), contracts.WorkloadTokenMaxBytes)
}
