package fernet

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// withFastDerive replaces the derivation with a cheap, deterministic one and
// returns its call counter, so cache tests run in microseconds. A reset empties
// the cache before and after.
func withFastDerive(t *testing.T) *int {
	t.Helper()
	resetCache()
	previous := deriveKey
	calls := 0
	var mu sync.Mutex
	deriveKey = func(key, salt []byte) ([]byte, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		out := make([]byte, 32)
		copy(out, append(append([]byte{}, key...), salt...))
		return out, nil
	}
	t.Cleanup(func() { deriveKey = previous; resetCache() })
	return &calls
}

func resetCache() {
	cacheMu.Lock()
	cacheEntries = map[cacheID]*derivation{}
	cacheOrder = nil
	cacheMu.Unlock()
}

func cachedEntries() int {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	return len(cacheEntries)
}

func TestKeyIsDerivedOncePerKeyAndSalt(t *testing.T) {
	calls := withFastDerive(t)
	cipher, err := New("master-key", "salt")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		sealed, err := cipher.Seal([]byte("secret"), time.Now(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		plain, err := cipher.Open(sealed)
		if err != nil || string(plain) != "secret" {
			t.Fatalf("round trip %d: %q %v", i, plain, err)
		}
	}
	if *calls != 1 {
		t.Fatalf("%d derivations for one (key, salt), want 1", *calls)
	}
}

func TestRealDerivationRunsOnce(t *testing.T) {
	resetCache()
	t.Cleanup(resetCache)
	before := Derivations()
	cipher, err := New("real-master-key", "real-salt")
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := cipher.Seal([]byte("value"), time.Now(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if plain, err := cipher.Open(sealed); err != nil || string(plain) != "value" {
			t.Fatalf("open %d: %q %v", i, plain, err)
		}
	}
	if got := Derivations() - before; got != 1 {
		t.Fatalf("%d real derivations, want 1", got)
	}
}

func TestConcurrentFirstUseDerivesOnce(t *testing.T) {
	calls := withFastDerive(t)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := derivedKey([]byte("k"), []byte("s")); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if *calls != 1 {
		t.Fatalf("%d derivations under concurrent first use, want 1", *calls)
	}
}

func TestCacheHoldsAtMostEightKeysAndAnEvictedKeyDerivesAgain(t *testing.T) {
	calls := withFastDerive(t)
	for i := 0; i < 12; i++ {
		if _, err := derivedKey([]byte(fmt.Sprintf("key-%d", i)), []byte("s")); err != nil {
			t.Fatal(err)
		}
	}
	if got := cachedEntries(); got > cacheLimit {
		t.Fatalf("%d cached keys, want at most %d", got, cacheLimit)
	}
	before := *calls
	if _, err := derivedKey([]byte("key-0"), []byte("s")); err != nil {
		t.Fatal(err)
	}
	if *calls != before+1 {
		t.Fatal("an evicted key was not derived again")
	}
	before = *calls
	if _, err := derivedKey([]byte("key-11"), []byte("s")); err != nil {
		t.Fatal(err)
	}
	if *calls != before {
		t.Fatal("a held key was derived again")
	}
}

func TestKeyAndSaltBoundaryCannotCollide(t *testing.T) {
	calls := withFastDerive(t)
	if _, err := derivedKey([]byte("ab"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	if _, err := derivedKey([]byte("a"), []byte("bc")); err != nil {
		t.Fatal(err)
	}
	if *calls != 2 || cacheKey([]byte("ab"), []byte("c")) == cacheKey([]byte("a"), []byte("bc")) {
		t.Fatalf("(\"ab\",\"c\") and (\"a\",\"bc\") share a cache entry (%d derivations)", *calls)
	}
}

func TestADerivationErrorIsNeverCached(t *testing.T) {
	resetCache()
	t.Cleanup(resetCache)
	previous := deriveKey
	t.Cleanup(func() { deriveKey = previous })
	calls := 0
	deriveKey = func(key, salt []byte) ([]byte, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("derivation failed")
		}
		return make([]byte, 32), nil
	}
	if _, err := derivedKey([]byte("k"), []byte("s")); err == nil {
		t.Fatal("the first derivation did not fail")
	}
	if cachedEntries() != 0 {
		t.Fatal("a failed derivation stayed in the cache")
	}
	if _, err := derivedKey([]byte("k"), []byte("s")); err != nil || calls != 2 {
		t.Fatalf("the retry after a failure: err %v after %d derivations", err, calls)
	}
}

func TestWrongKeyAndWrongSaltStillRefuse(t *testing.T) {
	resetCache()
	t.Cleanup(resetCache)
	right, _ := New("right-key", "salt")
	sealed, err := right.Seal([]byte("x"), time.Now(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]func() (Cipher, error){
		"wrong key":  func() (Cipher, error) { return New("other-key", "salt") },
		"wrong salt": func() (Cipher, error) { return New("right-key", "other") },
	} {
		cipher, err := c()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cipher.Open(sealed); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s opened a value: %v", name, err)
		}
	}
	if _, err := New("", "salt"); !errors.Is(err, ErrInvalid) {
		t.Fatal("an empty key was accepted")
	}
}

func TestNothingPrintsAKeySaltOrDigest(t *testing.T) {
	calls := withFastDerive(t)
	_ = calls
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	key, salt := "distinctive-master-key-text", "distinctive-salt-text"
	cipher, _ := New(key, salt)
	sealed, err := cipher.Seal([]byte("v"), time.Now(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = cipher.Open(sealed)
	_, _ = cipher.Open("v1:garbage")
	digest := cacheKey([]byte(key), []byte(salt))
	cacheMu.Lock()
	printed := fmt.Sprintf("%v|%+v|%#v|%s|%x|%d", cacheEntries, cacheEntries, cacheEntries, cacheOrder, cacheOrder, cacheOrder)
	cacheMu.Unlock()
	derived, _ := derivedKey([]byte(key), []byte(salt))
	hexDigest := fmt.Sprintf("%x", [32]byte(digest))
	for _, text := range []string{logs.String(), printed} {
		for _, secret := range []string{key, salt, hexDigest, fmt.Sprintf("%x", derived)} {
			if strings.Contains(text, secret) {
				t.Errorf("output carries %q", secret[:8])
			}
		}
	}
}

func TestNewDerivesTheKeyBeforeAnyOpenOrSeal(t *testing.T) {
	calls := withFastDerive(t)
	cipher, err := New("construction-key", "salt")
	if err != nil {
		t.Fatal(err)
	}
	if *calls != 1 {
		t.Fatalf("%d derivations after New, want 1", *calls)
	}
	sealed, err := cipher.Seal([]byte("x"), time.Now(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cipher.Open(sealed); err != nil || *calls != 1 {
		t.Fatalf("open after New: %v with %d derivations, want none more", err, *calls)
	}
}

func TestNoValueHoldingKeyMaterialPrintsInAnyEncoding(t *testing.T) {
	withFastDerive(t)
	key, salt := "encoding-master-key-text", "encoding-salt-text"
	cipher, err := New(key, salt)
	if err != nil {
		t.Fatal(err)
	}
	derived, _ := derivedKey([]byte(key), []byte(salt))
	cacheMu.Lock()
	entry := cacheEntries[cacheKey([]byte(key), []byte(salt))]
	index := cacheKey([]byte(key), []byte(salt))
	cacheMu.Unlock()
	values := map[string]any{"cipher": cipher, "cipher pointer": &cipher, "entry": entry, "index": index, "map": cacheEntries, "order": cacheOrder}
	var out strings.Builder
	var jsonBuf, textBuf bytes.Buffer
	jsonLogger := slog.New(slog.NewJSONHandler(&jsonBuf, nil))
	textLogger := slog.New(slog.NewTextHandler(&textBuf, nil))
	for name, value := range values {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d"} {
			cacheMu.Lock()
			fmt.Fprintf(&out, "%s "+verb+"|", name, value)
			cacheMu.Unlock()
		}
		cacheMu.Lock()
		if raw, err := json.Marshal(value); err == nil {
			out.Write(raw)
		}
		cacheMu.Unlock()
		jsonLogger.Info("value", name, value)
		textLogger.Info("value", name, value)
	}
	out.Write(jsonBuf.Bytes())
	out.Write(textBuf.Bytes())
	digest := cacheKey([]byte(key), []byte(salt))
	digestBytes := [32]byte(digest)
	for _, secret := range []string{key, salt, fmt.Sprintf("%x", digestBytes), fmt.Sprintf("%x", derived), fmt.Sprint(digestBytes[:8]), strings.Trim(strings.Join(strings.Fields(fmt.Sprint(digestBytes[:8])), ","), "[]")} {
		if strings.Contains(out.String(), secret) {
			t.Errorf("a formatted or encoded value carries %q", secret[:6])
		}
	}
}
