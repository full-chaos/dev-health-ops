package passwordhash

import (
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestHashIsPythonsForm(t *testing.T) {
	hash, err := Hash("Correct-horse-1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(hash, "$2b$12$") || len(hash) != 60 {
		t.Fatalf("hash %q is not bcrypt 2b cost 12", hash)
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte("Correct-horse-1")) != nil {
		t.Fatal("the rewritten hash no longer verifies")
	}
	if _, err := Hash(strings.Repeat("a", 73)); err == nil {
		t.Fatal("a password over 72 bytes was hashed")
	}
}
