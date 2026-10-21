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
