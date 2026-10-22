package keystore

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// writeManifest writes a manifest at mode 0600 by an atomic rename, as an
// operator rotating the set would.
func writeManifest(t testing.TB, dir string, body string) string {
	t.Helper()
	path := filepath.Join(dir, "keyset.json")
	staged, err := os.CreateTemp(dir, ".keyset-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := staged.WriteString(body); err != nil {
		t.Fatal(err)
	}
	if err := staged.Chmod(0o600); err != nil {
		t.Fatal(err)
	}
	if err := staged.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(staged.Name(), path); err != nil {
		t.Fatal(err)
	}
	return path
}

func entries(pairs ...string) string {
	var keys []map[string]string
	for i := 0; i+1 < len(pairs); i += 2 {
		keys = append(keys, map[string]string{"kid": pairs[i], "state": pairs[i+1], "file": pairs[i] + ".pem"})
	}
	out, _ := json.Marshal(map[string]any{"keys": keys})
	return string(out)
}

func reasonOf(t *testing.T, err error) Reason {
	t.Helper()
	var custody *Error
	if !errors.As(err, &custody) {
		t.Fatalf("error %v is not a custody error", err)
	}
	return custody.Reason
}

func TestManifestIsClosedAndValidated(t *testing.T) {
	cases := map[string]string{
		"unknown top-level member": `{"keys":[{"kid":"a","state":"active","file":"a.pem"}],"extra":1}`,
		"unknown entry member":     `{"keys":[{"kid":"a","state":"active","file":"a.pem","private":"x"}]}`,
		"trailing data":            `{"keys":[{"kid":"a","state":"active","file":"a.pem"}]} {}`,
		"not json":                 `keys: [a]`,
		"empty set":                `{"keys":[]}`,
		"no keys member":           `{}`,
		"unknown state":            `{"keys":[{"kid":"a","state":"enabled","file":"a.pem"}]}`,
		"bad kid":                  `{"keys":[{"kid":"a b","state":"active","file":"a.pem"}]}`,
		"long kid":                 `{"keys":[{"kid":"` + strings.Repeat("k", 65) + `","state":"active","file":"a.pem"}]}`,
		"parent file":              `{"keys":[{"kid":"a","state":"active","file":"../a.pem"}]}`,
		"nested file":              `{"keys":[{"kid":"a","state":"active","file":"d/a.pem"}]}`,
		"dot file":                 `{"keys":[{"kid":"a","state":"active","file":".."}]}`,
		"duplicate kid":            `{"keys":[{"kid":"a","state":"active","file":"a.pem"},{"kid":"a","state":"retiring","file":"b.pem"}]}`,
		"duplicate file":           `{"keys":[{"kid":"a","state":"active","file":"a.pem"},{"kid":"b","state":"retiring","file":"a.pem"}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parseManifest([]byte(body))
			if err == nil {
				t.Fatal("manifest accepted")
			}
			if reason := reasonOf(t, err); reason != ReasonManifestUnparseable && reason != ReasonManifestInvalid {
				t.Fatalf("reason %s", reason)
			}
		})
	}
	if _, err := parseManifest([]byte(entries("a", "active", "b", "pending"))); err != nil {
		t.Fatalf("valid manifest refused: %v", err)
	}
}

func TestFileCustodySignsOnlyWithTheActiveKey(t *testing.T) {
	dir := t.TempDir()
	publics := map[string]ed25519.PublicKey{}
	for _, kid := range []string{"p", "a", "r", "x", "c"} {
		_, publics[kid] = writeKeyFile(t, dir, kid+".pem")
	}
	custody := NewFileCustody(writeManifest(t, dir, entries("p", "pending", "a", "active", "r", "retiring", "x", "retired", "c", "compromised")))
	ctx := context.Background()
	keys, err := custody.Keys(ctx)
	if err != nil || len(keys) != 5 {
		t.Fatalf("Keys = %v, %v", keys, err)
	}
	for _, key := range keys {
		if !key.PublicKey.Equal(publics[key.KeyID]) || key.Algorithm != Algorithm {
			t.Errorf("%s: wrong public key or algorithm", key.KeyID)
		}
		signature, err := custody.Sign(ctx, key.KeyID, []byte("message"))
		if key.State == StateActive {
			if err != nil || !ed25519.Verify(publics[key.KeyID], []byte("message"), signature) {
				t.Errorf("active key did not sign a verifiable signature: %v", err)
			}
			continue
		}
		if err == nil || reasonOf(t, err) != ReasonKeyNotActive {
			t.Errorf("%s key %s signed (%v)", key.State, key.KeyID, err)
		}
	}
	if _, err := custody.Sign(ctx, "missing", []byte("m")); err == nil || reasonOf(t, err) != ReasonKeyUnknown {
		t.Errorf("unknown kid: %v", err)
	}
}

func TestFileCustodyAppliesTheFileContractToTheManifestAndKeys(t *testing.T) {
	ctx := context.Background()
	t.Run("manifest mode", func(t *testing.T) {
		dir := t.TempDir()
		writeKeyFile(t, dir, "a.pem")
		path := writeManifest(t, dir, entries("a", "active"))
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := NewFileCustody(path).Keys(ctx); err == nil || reasonOf(t, err) != ReasonPermissiveMode {
			t.Fatalf("world-readable manifest: %v", err)
		}
	})
	t.Run("manifest symlink", func(t *testing.T) {
		dir := t.TempDir()
		writeKeyFile(t, dir, "a.pem")
		real := writeManifest(t, dir, entries("a", "active"))
		link := filepath.Join(dir, "link.json")
		if err := os.Symlink(real, link); err != nil {
			t.Skip(err)
		}
		if _, err := NewFileCustody(link).Keys(ctx); err == nil || reasonOf(t, err) != ReasonUnreadable {
			t.Fatalf("symlinked manifest: %v", err)
		}
	})
	t.Run("key symlink", func(t *testing.T) {
		dir := t.TempDir()
		other := t.TempDir()
		target, _ := writeKeyFile(t, other, "elsewhere.pem")
		if err := os.Symlink(target, filepath.Join(dir, "a.pem")); err != nil {
			t.Skip(err)
		}
		custody := NewFileCustody(writeManifest(t, dir, entries("a", "active")))
		if _, err := custody.Sign(ctx, "a", []byte("m")); err == nil || reasonOf(t, err) != ReasonUnreadable {
			t.Fatalf("symlinked key: %v", err)
		}
	})
	t.Run("manifest too large", func(t *testing.T) {
		dir := t.TempDir()
		path := writeManifest(t, dir, `{"keys":[]}`+strings.Repeat(" ", int(MaxManifestBytes)))
		if _, err := NewFileCustody(path).Keys(ctx); err == nil || reasonOf(t, err) != ReasonTooLarge {
			t.Fatalf("large manifest: %v", err)
		}
	})
	t.Run("missing key file", func(t *testing.T) {
		dir := t.TempDir()
		path := writeManifest(t, dir, entries("a", "active"))
		if _, err := NewFileCustody(path).Keys(ctx); err == nil || reasonOf(t, err) != ReasonUnreadable {
			t.Fatalf("missing key: %v", err)
		}
	})
}

func TestFileSourceIsAnActiveOneKeySet(t *testing.T) {
	dir := t.TempDir()
	path, public := writeKeyFile(t, dir, "single.pem")
	source := FileSource{Path: path, KeyID: "single-1"}
	ctx := context.Background()
	keys, err := source.Keys(ctx)
	if err != nil || len(keys) != 1 || keys[0].KeyID != "single-1" || keys[0].State != StateActive || !keys[0].PublicKey.Equal(public) {
		t.Fatalf("Keys = %+v, %v", keys, err)
	}
	signature, err := source.Sign(ctx, "single-1", []byte("m"))
	if err != nil || !ed25519.Verify(public, []byte("m"), signature) {
		t.Fatalf("Sign: %v", err)
	}
	if _, err := source.Sign(ctx, "other", []byte("m")); err == nil || reasonOf(t, err) != ReasonKeyUnknown {
		t.Fatalf("other kid: %v", err)
	}
}

// TestLiveKeyRotation rotates a running custody by manifest renames only.
func TestLiveKeyRotation(t *testing.T) {
	dir := t.TempDir()
	_, k1 := writeKeyFile(t, dir, "k1.pem")
	_, k2 := writeKeyFile(t, dir, "k2.pem")
	ctx := context.Background()
	path := writeManifest(t, dir, entries("k1", "active", "k2", "pending"))
	custody := NewFileCustody(path)
	signs := func(kid string, public ed25519.PublicKey) bool {
		signature, err := custody.Sign(ctx, kid, []byte("m"))
		return err == nil && ed25519.Verify(public, []byte("m"), signature)
	}
	if !signs("k1", k1) || signs("k2", k2) {
		t.Fatal("k1 active, k2 pending: only k1 signs")
	}
	writeManifest(t, dir, entries("k1", "retiring", "k2", "active"))
	if signs("k1", k1) || !signs("k2", k2) {
		t.Fatal("after the rename k2 signs and k1 does not")
	}
	writeManifest(t, dir, entries("k1", "retired", "k2", "active"))
	keys, err := custody.Keys(ctx)
	if err != nil || keys[0].State != StateRetired {
		t.Fatalf("k1 retired: %+v %v", keys, err)
	}
	writeManifest(t, dir, entries("k1", "retired", "k2", "compromised"))
	if signs("k2", k2) {
		t.Fatal("a compromised key signed")
	}
	writeManifest(t, dir, entries("k1", "retired", "k2", "active"))
	if signs("k2", k2) {
		t.Fatal("a kid seen compromised signed again after a later manifest named it active")
	}
	keys, err = custody.Keys(ctx)
	if err != nil || keys[1].State != StateCompromised {
		t.Fatalf("the latched kid reads %+v, %v; want compromised", keys, err)
	}
}

// TestSigningDuringRotationIsRaceFree signs from many goroutines while a writer
// renames manifests. Every signature verifies under the key of the kid it was
// asked for, and a refusal is only ever "not active".
func TestSigningDuringRotationIsRaceFree(t *testing.T) {
	dir := t.TempDir()
	_, k1 := writeKeyFile(t, dir, "k1.pem")
	_, k2 := writeKeyFile(t, dir, "k2.pem")
	publics := map[string]ed25519.PublicKey{"k1": k1, "k2": k2}
	custody := NewFileCustody(writeManifest(t, dir, entries("k1", "active", "k2", "pending")))
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	stop := make(chan struct{})
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				kid := []string{"k1", "k2"}[(g+i)%2]
				message := []byte(fmt.Sprintf("%d/%d", g, i))
				signature, err := custody.Sign(ctx, kid, message)
				if err != nil {
					var custodyErr *Error
					if !errors.As(err, &custodyErr) || custodyErr.Reason != ReasonKeyNotActive {
						errs <- err
						return
					}
					continue
				}
				if !ed25519.Verify(publics[kid], message, signature) {
					errs <- fmt.Errorf("signature for %s does not verify", kid)
					return
				}
			}
		}(g)
	}
	for i := 0; i < 50; i++ {
		if i%2 == 0 {
			writeManifest(t, dir, entries("k1", "retiring", "k2", "active"))
		} else {
			writeManifest(t, dir, entries("k1", "active", "k2", "retiring"))
		}
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestNoExportedAPIReturnsPrivateMaterial: the package's exported functions and
// methods return no private key and no crypto.Signer (ACP-ADR-02 §1).
func TestNoExportedAPIReturnsPrivateMaterial(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(set, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !fn.Name.IsExported() || fn.Type.Results == nil {
				continue
			}
			checked++
			for _, result := range fn.Type.Results.List {
				text := types(result.Type)
				if strings.Contains(text, "PrivateKey") || strings.Contains(text, "crypto.Signer") {
					t.Errorf("%s: exported %s returns %s", name, fn.Name.Name, text)
				}
			}
		}
	}
	if checked == 0 {
		t.Fatal("checked no exported function")
	}
}

func types(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return types(e.X) + "." + e.Sel.Name
	case *ast.StarExpr:
		return "*" + types(e.X)
	case *ast.ArrayType:
		return "[]" + types(e.Elt)
	case *ast.MapType:
		return "map[" + types(e.Key) + "]" + types(e.Value)
	}
	return fmt.Sprintf("%T", expr)
}

// TestARotationDuringSignDiscardsTheSignature: the manifest is replaced after
// the key has signed and before Sign returns; the signature is discarded.
func TestARotationDuringSignDiscardsTheSignature(t *testing.T) {
	for _, next := range []string{"compromised", "retiring"} {
		t.Run(next, func(t *testing.T) {
			dir := t.TempDir()
			writeKeyFile(t, dir, "a.pem")
			writeKeyFile(t, dir, "b.pem")
			custody := NewFileCustody(writeManifest(t, dir, entries("a", "active", "b", "pending")))
			afterSign = func() { writeManifest(t, dir, entries("a", next, "b", "active")) }
			t.Cleanup(func() { afterSign = nil })
			signature, err := custody.Sign(context.Background(), "a", []byte("m"))
			if err == nil || reasonOf(t, err) != ReasonKeyNotActive || signature != nil {
				t.Fatalf("Sign across a rotation to a=%s returned %x, %v; want no signature and %s", next, signature, err, ReasonKeyNotActive)
			}
			afterSign = nil
			if _, err := custody.Sign(context.Background(), "b", []byte("m")); err != nil {
				t.Fatalf("the new active key does not sign: %v", err)
			}
		})
	}
}

// TestFileCustodySignsOnlyFromASetWithOneActiveKey: a manifest with two
// active keys signs with neither, read before or after the signature.
func TestFileCustodySignsOnlyFromASetWithOneActiveKey(t *testing.T) {
	dir := t.TempDir()
	writeKeyFile(t, dir, "a.pem")
	writeKeyFile(t, dir, "b.pem")
	ctx := context.Background()
	custody := NewFileCustody(writeManifest(t, dir, entries("a", "active", "b", "active")))
	for _, kid := range []string{"a", "b"} {
		if signature, err := custody.Sign(ctx, kid, []byte("m")); err == nil {
			t.Fatalf("%s signed (%x) from a set with two active keys", kid, signature)
		}
	}
	writeManifest(t, dir, entries("a", "active", "b", "pending"))
	afterSign = func() { writeManifest(t, dir, entries("a", "active", "b", "active")) }
	t.Cleanup(func() { afterSign = nil })
	if signature, err := custody.Sign(ctx, "a", []byte("m")); err == nil {
		t.Fatalf("a's signature (%x) was returned after the set gained a second active key", signature)
	}
}

// TestFileCustodySignsOnlyFromASetWhoseEveryKeyIsCustodied: a manifest entry
// in any state whose key file fails the file contract makes the set invalid,
// so the active key does not sign from it, read before or after the signature.
func TestFileCustodySignsOnlyFromASetWhoseEveryKeyIsCustodied(t *testing.T) {
	ctx := context.Background()
	for _, state := range []string{"pending", "retiring", "retired", "compromised"} {
		t.Run(state, func(t *testing.T) {
			dir := t.TempDir()
			writeKeyFile(t, dir, "a.pem")
			custody := NewFileCustody(writeManifest(t, dir, entries("a", "active", "missing", state)))
			if _, err := custody.Keys(ctx); err == nil {
				t.Fatal("Keys accepted a set with a missing key file")
			}
			if signature, err := custody.Sign(ctx, "a", []byte("m")); err == nil {
				t.Fatalf("a signed (%x) from a set whose %s key file is missing", signature, state)
			}
		})
	}
	dir := t.TempDir()
	writeKeyFile(t, dir, "a.pem")
	custody := NewFileCustody(writeManifest(t, dir, entries("a", "active")))
	afterSign = func() { writeManifest(t, dir, entries("a", "active", "missing", "pending")) }
	t.Cleanup(func() { afterSign = nil })
	if signature, err := custody.Sign(ctx, "a", []byte("m")); err == nil {
		t.Fatalf("a's signature (%x) was returned after the set gained an uncustodied key", signature)
	}
}

// replaceKeyFile atomically renames a fresh key onto name.
func replaceKeyFile(t *testing.T, dir, name string) ed25519.PublicKey {
	t.Helper()
	staged, public := writeKeyFile(t, dir, ".staged-"+name)
	if err := os.Rename(staged, filepath.Join(dir, name)); err != nil {
		t.Fatal(err)
	}
	return public
}

// TestAKeyFileReplacedInPlaceNeverSigns: the key under a kid is replaced by
// another key under the same file name between the first read and the
// signature, or between the signature and the second read; the signature is
// refused.
func TestAKeyFileReplacedInPlaceNeverSigns(t *testing.T) {
	ctx := context.Background()
	for name, hook := range map[string]*func(){"before the signature": &beforeSign, "after the signature": &afterSign} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			writeKeyFile(t, dir, "a.pem")
			custody := NewFileCustody(writeManifest(t, dir, entries("a", "active")))
			*hook = func() { replaceKeyFile(t, dir, "a.pem") }
			t.Cleanup(func() { *hook = nil })
			if signature, err := custody.Sign(ctx, "a", []byte("m")); err == nil {
				t.Fatalf("a signed (%x) across an in-place replacement of a.pem", signature)
			}
		})
	}
}

// TestACompromisedKeyNeverSignsUnderAnotherKid: the latch follows the key, and
// a set that lists one key under two kids is refused.
func TestACompromisedKeyNeverSignsUnderAnotherKid(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	writeKeyFile(t, dir, "a.pem")
	custody := NewFileCustody(writeManifest(t, dir, entries("a", "compromised", "c", "active")))
	writeKeyFile(t, dir, "c.pem")
	if _, err := custody.Keys(ctx); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "a.pem"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.pem"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	writeManifest(t, dir, entries("b", "active", "c", "retiring"))
	if signature, err := custody.Sign(ctx, "b", []byte("m")); err == nil {
		t.Fatalf("the compromised key signed (%x) under kid b", signature)
	}
	fresh := NewFileCustody(writeManifest(t, dir, entries("a", "retiring", "b", "active")))
	if _, err := fresh.Keys(ctx); err == nil {
		t.Fatal("Keys accepted one key listed under two kids")
	}
	if signature, err := fresh.Sign(ctx, "b", []byte("m")); err == nil {
		t.Fatalf("b signed (%x) from a set that lists its key under two kids", signature)
	}
}

func TestManifestMemberNamesAreExactAndUnique(t *testing.T) {
	for name, body := range map[string]string{
		"case-folded state over state": `{"keys":[{"kid":"a","state":"compromised","State":"active","file":"a.pem"}]}`,
		"upper-case members":           `{"KEYS":[{"KID":"a","STATE":"active","FILE":"a.pem"}]}`,
		"repeated state":               `{"keys":[{"kid":"a","state":"compromised","state":"active","file":"a.pem"}]}`,
		"repeated kid":                 `{"keys":[{"kid":"x","kid":"a","state":"active","file":"a.pem"}]}`,
		"repeated keys":                `{"keys":[{"kid":"x","state":"compromised","file":"x.pem"}],"keys":[{"kid":"a","state":"active","file":"a.pem"}]}`,
		"mixed-case file":              `{"keys":[{"kid":"a","state":"active","File":"a.pem"}]}`,
	} {
		if _, err := parseManifest([]byte(body)); err == nil {
			t.Errorf("%s: manifest accepted", name)
		}
	}
}

// overrideTo returns a StateOverride that sets each named kid's state.
func overrideTo(states map[string]State, err error) StateOverride {
	return func(_ context.Context, keys []Key) ([]Key, error) {
		if err != nil {
			return nil, err
		}
		out := append([]Key(nil), keys...)
		for i := range out {
			if state, ok := states[out[i].KeyID]; ok {
				out[i].State = state
			}
		}
		return out, nil
	}
}

// With a StateOverride the durable states decide which key signs, whatever the
// manifest says.
func TestStateOverrideDecidesWhichKeySigns(t *testing.T) {
	dir := t.TempDir()
	writeKeyFile(t, dir, "a.pem")
	_, publicB := writeKeyFile(t, dir, "b.pem")
	manifest := writeManifest(t, dir, entries("a", "active", "b", "pending"))
	custody := NewFileCustodyWithStates(manifest, overrideTo(map[string]State{"a": StateRetiring, "b": StateActive}, nil))
	ctx := context.Background()
	keys, err := custody.Keys(ctx)
	if err != nil || keys[0].State != StateRetiring || keys[1].State != StateActive {
		t.Fatalf("Keys = %+v, %v", keys, err)
	}
	signature, err := custody.Sign(ctx, "b", []byte("m"))
	if err != nil || !ed25519.Verify(publicB, []byte("m"), signature) {
		t.Fatalf("Sign(b) = %v", err)
	}
	if _, err := custody.Sign(ctx, "a", []byte("m")); err == nil || reasonOf(t, err) != ReasonKeyNotActive {
		t.Fatalf("Sign(a) = %v, want %s", err, ReasonKeyNotActive)
	}

	failing := NewFileCustodyWithStates(manifest, overrideTo(nil, errors.New("states unavailable")))
	if _, err := failing.Keys(ctx); err == nil {
		t.Fatal("Keys succeeded with the state source failing")
	}
	if _, err := failing.Sign(ctx, "a", []byte("m")); err == nil {
		t.Fatal("Sign succeeded with the state source failing")
	}
}

// A compromise the override reports latches: a later override that names the
// kid active again does not re-enable it.
func TestAnOverrideCompromiseLatches(t *testing.T) {
	dir := t.TempDir()
	writeKeyFile(t, dir, "a.pem")
	manifest := writeManifest(t, dir, entries("a", "active"))
	states := map[string]State{"a": StateCompromised}
	var mu sync.Mutex
	custody := NewFileCustodyWithStates(manifest, func(ctx context.Context, keys []Key) ([]Key, error) {
		mu.Lock()
		defer mu.Unlock()
		return overrideTo(states, nil)(ctx, keys)
	})
	ctx := context.Background()
	if _, err := custody.Sign(ctx, "a", []byte("m")); err == nil {
		t.Fatal("a compromised key signed")
	}
	mu.Lock()
	states = map[string]State{"a": StateActive}
	mu.Unlock()
	keys, err := custody.Keys(ctx)
	if err != nil || keys[0].State != StateCompromised {
		t.Fatalf("Keys after the override un-compromised a = %+v, %v", keys, err)
	}
	if _, err := custody.Sign(ctx, "a", []byte("m")); err == nil {
		t.Fatal("a key seen compromised signed again")
	}
}

// A single key file signs only while its durable state is active.
func TestFileSourceFollowsTheStateOverride(t *testing.T) {
	dir := t.TempDir()
	path, public := writeKeyFile(t, dir, "one.pem")
	ctx := context.Background()
	active := FileSource{Path: path, KeyID: "one", States: overrideTo(map[string]State{"one": StateActive}, nil)}
	signature, err := active.Sign(ctx, "one", []byte("m"))
	if err != nil || !ed25519.Verify(public, []byte("m"), signature) {
		t.Fatalf("Sign while active = %v", err)
	}
	for _, state := range []State{StatePending, StateRetiring, StateRetired, StateCompromised} {
		source := FileSource{Path: path, KeyID: "one", States: overrideTo(map[string]State{"one": state}, nil)}
		keys, err := source.Keys(ctx)
		if err != nil || keys[0].State != state {
			t.Fatalf("Keys under %s = %+v, %v", state, keys, err)
		}
		if _, err := source.Sign(ctx, "one", []byte("m")); err == nil || reasonOf(t, err) != ReasonKeyNotActive {
			t.Errorf("Sign under %s = %v, want %s", state, err, ReasonKeyNotActive)
		}
	}
	failing := FileSource{Path: path, KeyID: "one", States: overrideTo(nil, errors.New("states unavailable"))}
	if _, err := failing.Sign(ctx, "one", []byte("m")); err == nil {
		t.Fatal("Sign succeeded with the state source failing")
	}
}
