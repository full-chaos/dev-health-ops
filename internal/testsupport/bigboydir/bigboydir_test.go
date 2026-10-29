package bigboydir

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirPrefersEnv(t *testing.T) {
	d := t.TempDir()
	if err := os.WriteFile(filepath.Join(d, marker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(Env, d)
	if got := Dir(t, "/nonexistent"); got != d {
		t.Fatalf("Dir = %q, want %q", got, d)
	}
}

func TestDirDefaultsToInRepo(t *testing.T) {
	root := t.TempDir()
	d := filepath.Join(root, "ci", "bigboy")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, marker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(Env, "")
	if got := Path(t, root, "x"); got != filepath.Join(d, "x") {
		t.Fatalf("Path = %q", got)
	}
}

func TestDirSkipsWhenAbsent(t *testing.T) {
	t.Setenv(Env, t.TempDir())
	t.Run("inner", func(t *testing.T) {
		Dir(t, t.TempDir())
		t.Fatal("Dir returned for an empty tree: it must skip")
	})
}

func TestRelativeEnvIsReadFromTheRepoRoot(t *testing.T) {
	root := t.TempDir()
	d := filepath.Join(root, "tree")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, marker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(Env, "tree")
	if got, ok := Lookup(root); !ok || got != d {
		t.Fatalf("Lookup = %q, %v, want %q, true", got, ok, d)
	}
}
