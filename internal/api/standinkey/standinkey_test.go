package standinkey

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestStandInIs32NonZeroRandomBytesAndNeverEmpty(t *testing.T) {
	if len(StandIn()) != 32 {
		t.Fatalf("length %d, want 32", len(StandIn()))
	}
	if strings.Trim(StandIn(), "\x00") == "" {
		t.Fatal("the stand-in key is all zero bytes")
	}
	if Secret("set") != "set" || Secret("") != StandIn() {
		t.Fatal("Secret must return the configured credential, else the stand-in")
	}
}

// Two constructions of the key give different keys: it is random, not derived from a constant.
func TestTwoConstructionsOfTheKeyDiffer(t *testing.T) {
	a, b := MustRandomKey(rand.Reader), MustRandomKey(rand.Reader)
	if a == b {
		t.Fatal("two random keys are equal")
	}
	if a == StandIn() || b == StandIn() {
		t.Fatal("a new key equals the process key")
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("no entropy") }

func TestAFailedRandomReadStopsStartup(t *testing.T) {
	for name, source := range map[string]io.Reader{"error": failingReader{}, "short": bytes.NewReader(make([]byte, 5))} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("a failed random read must panic, never return a key")
				}
			}()
			MustRandomKey(source)
		})
	}
}

// The derived HMAC key gives the same MAC as a direct hmac.New(secret) for every key length around the
// block size; an absent credential never gives the MAC of the empty key.
func TestDerivedHMACKeyMatchesADirectHMACForEveryLength(t *testing.T) {
	body := []byte("1790000000.{}")
	mac := func(key []byte) string {
		m := hmac.New(sha256.New, key)
		m.Write(body)
		return string(m.Sum(nil))
	}
	for _, length := range []int{1, 63, 64, 65, 128, 4096} {
		secret := strings.Repeat("k", length-1) + "z"
		if mac(New(secret).HMACKey) != mac([]byte(secret)) {
			t.Fatalf("length %d: the derived key gives another MAC", length)
		}
		if len(New(secret).HMACKey) != 64 {
			t.Fatalf("length %d: the derived key is %d bytes, want exactly one block", length, len(New(secret).HMACKey))
		}
		if New(secret).Digest != sha256.Sum256([]byte(secret)) {
			t.Fatalf("length %d: the digest is not the SHA-256 of the credential", length)
		}
	}
	if len(New("").HMACKey) != 64 {
		t.Fatal("the stand-in key is not one block")
	}
	if mac(New("").HMACKey) == mac(nil) {
		t.Fatal("an absent credential gives the MAC of the empty key")
	}
}

func TestDerivationsCountsNewCalls(t *testing.T) {
	before := Derivations()
	New("a")
	New("")
	if Derivations()-before != 2 {
		t.Fatalf("derivations moved by %d, want 2", Derivations()-before)
	}
}
