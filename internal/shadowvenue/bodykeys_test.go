package shadowvenue

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const keysBody = `{"a":[{"b":1},{"b":"SECRETVALUE-123"}],"c":{"d":true,"e":null},"id":"x"}`

func keyOf(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "k")
	if err := NewBodyKey(path); err != nil {
		t.Fatal(err)
	}
	key, err := ReadBodyKey(path)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func leafMap(leaves []Leaf) map[string][]string {
	out := map[string][]string{}
	for _, l := range leaves {
		out[l.Path] = append(out[l.Path], l.HMAC12)
	}
	return out
}

func TestLeavesNameEachLeafByItsNormalizedPath(t *testing.T) {
	got := leafMap(BodyLeaves(keyOf(t), []byte(keysBody)))
	for path, n := range map[string]int{"$.a[].b": 2, "$.c.d": 1, "$.c.e": 1, "$.id": 1} {
		if len(got[path]) != n {
			t.Errorf("path %s has %d leaves, want %d (%v)", path, len(got[path]), n, got)
		}
	}
	if len(got) != 4 {
		t.Errorf("paths %v", got)
	}
	for path := range got {
		if strings.Contains(path, "[0]") || strings.Contains(path, "[1]") {
			t.Errorf("array index not normalized: %s", path)
		}
	}
}

func TestLeafHashIsAnHMACOfTheLeafUnderTheKeyNotAPlainSHA(t *testing.T) {
	key := keyOf(t)
	got := leafMap(BodyLeaves(key, []byte(`{"v":"abc"}`)))["$.v"]
	if len(got) != 1 {
		t.Fatalf("%v", got)
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(`"abc"`))
	if want := hex.EncodeToString(mac.Sum(nil))[:12]; got[0] != want {
		t.Fatalf("leaf %s, want the HMAC-SHA256 under the key %s", got[0], want)
	}
	plain := sha256.Sum256([]byte(`"abc"`))
	if got[0] == hex.EncodeToString(plain[:])[:12] {
		t.Fatal("the leaf hash is a plain sha256 of the value")
	}
	if other := leafMap(BodyLeaves(keyOf(t), []byte(`{"v":"abc"}`)))["$.v"]; other[0] == got[0] {
		t.Fatal("two keys gave the same leaf hash")
	}
}

func TestANonJSONBodyIsOneRawLeaf(t *testing.T) {
	key := keyOf(t)
	leaves := BodyLeaves(key, []byte("not json"))
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("not json"))
	if len(leaves) != 1 || leaves[0].Path != "$raw" || leaves[0].HMAC12 != hex.EncodeToString(mac.Sum(nil))[:12] {
		t.Fatalf("%v", leaves)
	}
}

func TestBodyKeyIsRandomOwnerOnlyAndNeverOverwritten(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "k")
	if err := NewBodyKey(path); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(path)
	raw, _ := os.ReadFile(path)
	if info.Mode().Perm() != 0o600 || len(strings.TrimSpace(string(raw))) != 64 {
		t.Fatalf("mode %v, %d hex chars", info.Mode().Perm(), len(strings.TrimSpace(string(raw))))
	}
	if err := NewBodyKey(path); err == nil {
		t.Fatal("an existing key file was overwritten")
	}
	if after, _ := os.ReadFile(path); !bytes.Equal(raw, after) {
		t.Fatal("the existing key file changed")
	}
	other := filepath.Join(dir, "k2")
	if err := NewBodyKey(other); err != nil {
		t.Fatal(err)
	}
	if rawOther, _ := os.ReadFile(other); bytes.Equal(raw, rawOther) {
		t.Fatal("two keys are equal")
	}
}

func TestBodyKeyFileMustBeOwnerOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "k")
	if err := os.WriteFile(path, []byte(strings.Repeat("ab", 32)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBodyKey(path); err == nil {
		t.Fatal("a 0644 key file was read")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBodyKey(path); err != nil {
		t.Fatalf("a 0600 key file was refused: %v", err)
	}
	if err := os.WriteFile(path, []byte("short\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadBodyKey(path); err == nil {
		t.Fatal("a key that is not 64 hex chars was read")
	}
}

func TestCreateKeysFileIsOwnerOnlyAndNeverOverwritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "side")
	f, err := CreateKeysFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if info, _ := os.Stat(path); info.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", info.Mode().Perm())
	}
	if _, err := CreateKeysFile(path); err == nil {
		t.Fatal("an existing side file was overwritten")
	}
}

func TestDriveWritesTheSideFileWithPathsAndHMACsAndNoValue(t *testing.T) {
	api := newBodyAPI(t, keysBody)
	plan, err := planOf(t, "58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	if err != nil {
		t.Fatal(err)
	}
	venue, _ := testPrincipals(t)
	var side bytes.Buffer
	d := &Driver{Base: api.srv.URL, Client: api.srv.Client(), Gap: MinGap, Sleep: func(context.Context, time.Duration) error { return nil },
		BodyKey: keyOf(t), KeysOut: &side}
	var out bytes.Buffer
	if err := d.Drive(context.Background(), plan, healthyTokens(t, venue, time.Now().Add(time.Hour)), &out); err != nil {
		t.Fatal(err)
	}
	text := side.String()
	if !strings.HasPrefix(text, "seq\tpath\thmac12\n") {
		t.Fatalf("header: %q", text)
	}
	for _, leak := range []string{"SECRETVALUE-123", "true", "null", keysBody} {
		if strings.Contains(strings.TrimPrefix(text, "seq\tpath\thmac12\n"), leak) {
			t.Fatalf("the side file holds the value %q: %s", leak, text)
		}
	}
	lines := strings.Split(strings.TrimSpace(text), "\n")
	sent := 0
	for _, s := range plan.Slots {
		if !s.Gap() {
			sent++
		}
	}
	if len(lines) != 1+5*sent {
		t.Fatalf("%d lines, want a header and 5 leaves for each of %d sent slots", len(lines), sent)
	}
	for _, l := range lines[1:] {
		f := strings.Split(l, "\t")
		if len(f) != 3 || len(f[2]) != 12 || !strings.HasPrefix(f[1], "$") {
			t.Fatalf("line %q", l)
		}
	}
}

func TestSideFileIsOffWithoutAKey(t *testing.T) {
	api := newBodyAPI(t, keysBody)
	plan, _ := planOf(t, "58\tGET\t/api/v1/admin/teams\tA\tno\t-\t-\tmatch\t/api/v1/admin/teams\t-\n")
	venue, _ := testPrincipals(t)
	var side bytes.Buffer
	d := &Driver{Base: api.srv.URL, Client: api.srv.Client(), Gap: MinGap, Sleep: func(context.Context, time.Duration) error { return nil }, KeysOut: &side}
	if err := d.Drive(context.Background(), plan, healthyTokens(t, venue, time.Now().Add(time.Hour)), io.Discard); err == nil {
		t.Fatal("a side file without a key was driven")
	}
}

func TestKeyDiffListsOnlyPathsThatDifferOrAreInOneSideOnly(t *testing.T) {
	key := keyOf(t)
	mk := func(body string) string {
		var b strings.Builder
		b.WriteString("seq\tpath\thmac12\n")
		for _, l := range BodyLeaves(key, []byte(body)) {
			b.WriteString("1\t" + l.Path + "\t" + l.HMAC12 + "\n")
		}
		return b.String()
	}
	a, err := ParseKeysFile([]byte(mk(`{"a":1,"b":"same","t":"2026-01-01"}`)))
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseKeysFile([]byte(mk(`{"a":1,"b":"same","t":"2026-01-02","extra":"SECRETVALUE-9"}`)))
	if err != nil {
		t.Fatal(err)
	}
	diff := KeyDiff(a[1], b[1])
	if strings.Join(diff, ",") != "$.extra,$.t" {
		t.Fatalf("%v", diff)
	}
	if same := KeyDiff(a[1], a[1]); len(same) != 0 {
		t.Fatalf("equal sides differ: %v", same)
	}
	for _, p := range diff {
		if strings.Contains(p, "SECRETVALUE") {
			t.Fatal("a value in a diff path")
		}
	}
}

func TestParseKeysFileRefusesAWrongHeaderAndAShortLine(t *testing.T) {
	for _, bad := range []string{"", "seq\tpath\n", "seq\tpath\thmac12\n1\t$.a\n", "seq\tpath\thmac12\nx\t$.a\tabcdefabcdef\n"} {
		if _, err := ParseKeysFile([]byte(bad)); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestEmptyContainersAreLeavesToo(t *testing.T) {
	got := leafMap(BodyLeaves(keyOf(t), []byte(`{"a":{},"b":[],"c":[{}]}`)))
	for _, path := range []string{"$.a", "$.b", "$.c[]"} {
		if len(got[path]) != 1 {
			t.Errorf("path %s has %d leaves: %v", path, len(got[path]), got)
		}
	}
}
