package webhookintake

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// decryptWork counts the stored-secret decrypt steps this package runs: one per
// delivery that reaches the body stage, whether its binding is usable or not.
var decryptWork atomic.Uint64

// decryptStored is the one decrypt step of a delivery.
func (d Deps) decryptStored(stored string) ([]byte, error) {
	decryptWork.Add(1)
	return d.Decryptor.Decrypt(secrets.NewValue(stored))
}

const (
	mirrorPlaintext = "pagerduty-intake-work-equivalence"
	mirrorTokenKeep = 4
)

// mirrorTokens holds one valid sealed token per decryptor (per key and salt),
// so a refused delivery decrypts a real token and runs the same steps a stored
// secret does. A decryptor with a new key gets its own token.
var mirrorTokens = struct {
	mu    sync.Mutex
	byKey map[mirrorKey]string
	order []mirrorKey
}{byKey: map[mirrorKey]string{}}

// mirrorKey names a decryptor in the token map. It never prints: a decryptor's
// own fields hold the key and salt.
type mirrorKey struct {
	decryptor providerfoundation.FernetDecryptor
}

func (mirrorKey) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("webhookintake.mirrorKey"))
}

func (d Deps) mirrorToken() string {
	mirrorTokens.mu.Lock()
	defer mirrorTokens.mu.Unlock()
	if token, ok := mirrorTokens.byKey[mirrorKey{d.Decryptor}]; ok {
		return token
	}
	sealed, err := d.Decryptor.Encrypt([]byte(mirrorPlaintext))
	if err != nil {
		return ""
	}
	token := sealed.Reveal()
	mirrorTokens.byKey[mirrorKey{d.Decryptor}] = token
	mirrorTokens.order = append(mirrorTokens.order, mirrorKey{d.Decryptor})
	for len(mirrorTokens.order) > mirrorTokenKeep {
		delete(mirrorTokens.byKey, mirrorTokens.order[0])
		mirrorTokens.order = mirrorTokens.order[1:]
	}
	return token
}

func forgetMirrorToken(decryptor providerfoundation.FernetDecryptor) {
	mirrorTokens.mu.Lock()
	defer mirrorTokens.mu.Unlock()
	key := mirrorKey{decryptor}
	delete(mirrorTokens.byKey, key)
	for index, held := range mirrorTokens.order {
		if held == key {
			mirrorTokens.order = append(mirrorTokens.order[:index], mirrorTokens.order[index+1:]...)
			break
		}
	}
}

// mirrorDecrypt is the decrypt step of a refused delivery: a real token, the
// result discarded. A token the decryptor cannot open is dropped, so the next
// delivery seals a fresh one.
func (d Deps) mirrorDecrypt() {
	if _, err := d.decryptStored(d.mirrorToken()); err != nil {
		forgetMirrorToken(d.Decryptor)
	}
}
