package restorefence

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The push projection reader is an outbox consumer that starts no audit.Publisher:
// a file that builds one (pushprojection.NewReader) must register a fence floor
// input in the same file, so the census cannot be satisfied by another file of
// the package.
func TestEveryPushProjectionReaderHasAFenceFloorInputInTheSameFile(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	if got := unpairedReaderFiles(t, filepath.Join(root, "internal")); len(got) > 0 {
		t.Fatalf("these files build a push projection reader without restorefence.RegisterFloorInput in the same file: %v", got)
	}
}

func TestTheReaderCensusSeesAReaderWithoutAnInput(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		path := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	const imp = "import (\n\t\"github.com/full-chaos/dev-health-ops/internal/auth/pushprojection\"\n\t\"github.com/full-chaos/dev-health-ops/internal/auth/restorefence\"\n)\n"
	write("lone/lone.go", "package lone\n\nimport \"github.com/full-chaos/dev-health-ops/internal/auth/pushprojection\"\n\nfunc start() { _, _ = pushprojection.NewReader(pushprojection.Config{}) }\n")
	write("paired/paired.go", "package paired\n\n"+imp+"\nfunc start() {\n\t_, _ = pushprojection.NewReader(pushprojection.Config{})\n\trestorefence.RegisterFloorInput(nil)\n}\n")
	write("split/a.go", "package split\n\nimport \"github.com/full-chaos/dev-health-ops/internal/auth/pushprojection\"\n\nfunc start() { _, _ = pushprojection.NewReader(pushprojection.Config{}) }\n")
	write("split/b.go", "package split\n\nimport \"github.com/full-chaos/dev-health-ops/internal/auth/restorefence\"\n\nfunc reg() { restorefence.RegisterFloorInput(nil) }\n")
	write("mention/m.go", "package mention\n\n// pushprojection.NewReader is only named here.\nvar _ = \"NewReader\"\n")
	got := unpairedReaderFiles(t, root)
	for i := range got {
		got[i] = filepath.ToSlash(got[i])
	}
	if strings.Join(got, ",") != "lone/lone.go,split/a.go" {
		t.Fatalf("unpaired reader files = %v, want lone/lone.go and split/a.go", got)
	}
}
