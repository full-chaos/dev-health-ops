package admin

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func sealedSecretOfSize(t *testing.T, size int) string {
	t.Helper()
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("binding-secret-size-test-key"), "binding-secret-size-test-salt")
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := decryptor.Encrypt([]byte(strings.Repeat("s", size)))
	if err != nil {
		t.Fatal(err)
	}
	return sealed.Reveal()
}

func TestStoredSigningSecretCapIs1024(t *testing.T) {
	if maxStoredSigningSecretBytes != 1024 {
		t.Fatalf("cap %d, want 1024 (the read side's limit)", maxStoredSigningSecretBytes)
	}
}

func TestStoredSigningSecretFitsAtTheExactBoundary(t *testing.T) {
	for size, want := range map[int]bool{1: true, 16: true, 687: true, 688: true, 703: true, 704: false, 719: false, 4096: false} {
		stored := sealedSecretOfSize(t, size)
		if got := storedSigningSecretFits(stored); got != want {
			t.Errorf("plaintext %d bytes, stored %d bytes: fits %v, want %v", size, len(stored), got, want)
		}
	}
	if got := len(sealedSecretOfSize(t, 703)); got != 1019 {
		t.Errorf("largest accepted stored size %d, want 1019", got)
	}
	if got := len(sealedSecretOfSize(t, 704)); got != 1039 {
		t.Errorf("smallest refused stored size %d, want 1039", got)
	}
}

func TestInsertCandidateRefusesAnOversizeSecretBeforeTouchingTheDatabase(t *testing.T) {
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("binding-secret-size-test-key"), "binding-secret-size-test-salt")
	if err != nil {
		t.Fatal(err)
	}
	h := &handlers{decryptor: decryptor}
	_, err = h.insertCandidate(context.Background(), nil, uuid.Nil, bindingRequest{SigningSecret: strings.Repeat("s", 704)}, time.Now())
	if !errors.Is(err, errBindingSecretTooLarge) {
		t.Fatalf("err %v, want errBindingSecretTooLarge", err)
	}
	if strings.Contains(err.Error(), "sss") {
		t.Fatal("the error text carries secret text")
	}
}
