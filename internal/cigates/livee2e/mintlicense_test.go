package livee2e

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing/processlicense"
)

func TestMintLicenseCLI(t *testing.T) {
	dir := t.TempDir()
	public, licence := filepath.Join(dir, "public.b64"), filepath.Join(dir, "licence.txt")
	var out, logs bytes.Buffer
	if rc := Run([]string{"mint-license", public, licence}, os.Getenv, &out, &logs); rc != 0 || out.Len() != 0 || logs.Len() != 0 {
		t.Fatal("mint-license must succeed silently")
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 2 {
		t.Fatal("mint-license must create exactly two files")
	}
	for _, path := range []string{public, licence} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatal("licence output must have mode 0600")
		}
	}
	publicRaw, err := os.ReadFile(public)
	if err != nil {
		t.Fatal(err)
	}
	licenceRaw, err := os.ReadFile(licence)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := processlicense.NewVerifier(string(publicRaw))
	if err != nil {
		t.Fatal("invalid public key")
	}
	now := time.Now().Unix()
	result := verifier.Validate(string(licenceRaw), now)
	if result.License == nil || result.Reason != "" || result.License.Tier != "enterprise" || !result.License.Features["sso_saml"] || result.License.InGracePeriod {
		t.Fatal("licence must enable enterprise SAML in force")
	}
	payload, err := base64.StdEncoding.DecodeString(strings.Split(string(licenceRaw), ".")[0])
	if err != nil {
		t.Fatal("invalid payload encoding")
	}
	var claims struct {
		Subject string `json:"sub"`
		ID      string `json:"license_id"`
		Issued  int64  `json:"iat"`
		Expiry  int64  `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Subject != "live-e2e" || claims.ID != "live-e2e" || claims.Expiry-claims.Issued != 2*86400 || claims.Issued > now || claims.Issued < now-10 {
		t.Fatal("licence must have the job identity and a two-day duration")
	}
	other, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal("cannot generate unrelated public key")
	}
	wrong, err := processlicense.NewVerifier(base64.StdEncoding.EncodeToString(other))
	if err != nil {
		t.Fatal("cannot construct unrelated verifier")
	}
	if result := wrong.Validate(string(licenceRaw), now); result.License != nil || result.Reason != processlicense.ReasonSignature {
		t.Fatal("licence accepted by unrelated key")
	}
	if _, err := processlicense.NewVerifier(""); err == nil {
		t.Fatal("empty verification key accepted")
	}
	out.Reset()
	logs.Reset()
	if rc := Run([]string{"mint-license", public, licence}, os.Getenv, &out, &logs); rc != 1 || out.Len() != 0 {
		t.Fatal("second mint-license must refuse existing outputs")
	}
	for path, want := range map[string][]byte{public: publicRaw, licence: licenceRaw} {
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatal("second call changed an output")
		}
	}
	files, err = os.ReadDir(dir)
	if err != nil || len(files) != 2 {
		t.Fatal("second call created another file")
	}
}

func TestMintLicenseRefusesEitherExistingOutputBeforeWriting(t *testing.T) {
	for _, name := range []string{"public.b64", "licence.txt"} {
		for _, kind := range []string{"file", "symlink"} {
			t.Run(name+"/"+kind, func(t *testing.T) {
				dir := t.TempDir()
				existing := filepath.Join(dir, name)
				var err error
				if kind == "symlink" {
					err = os.Symlink("absent-target", existing)
				} else {
					err = os.WriteFile(existing, []byte("existing"), 0o600)
				}
				if err != nil {
					t.Fatal(err)
				}
				var out, logs bytes.Buffer
				if rc := Run([]string{"mint-license", filepath.Join(dir, "public.b64"), filepath.Join(dir, "licence.txt")}, os.Getenv, &out, &logs); rc != 1 || out.Len() != 0 {
					t.Fatal("existing output accepted")
				}
				files, err := os.ReadDir(dir)
				if err != nil || len(files) != 1 || files[0].Name() != name {
					t.Fatal("refused call created a file")
				}
				if kind == "symlink" {
					if target, err := os.Readlink(existing); err != nil || target != "absent-target" {
						t.Fatal("refused call changed the symlink")
					}
				} else if raw, err := os.ReadFile(existing); err != nil || string(raw) != "existing" {
					t.Fatal("refused call changed the existing file")
				}
			})
		}
	}
}

func TestMintLicenseOnlyLinkedByCigate(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	const pkg = "github.com/full-chaos/dev-health-ops/internal/cigates/livee2e"
	for _, cell := range []struct {
		commands []string
		want     bool
	}{
		{[]string{"./cmd/dho", "./cmd/auth-service", "./cmd/auth-migrate", "./cmd/dev-health-worker-migrate", "./cmd/dev-health-migration-matrix", "./cmd/credrefgen"}, false},
		{[]string{"./cmd/cigate"}, true},
	} {
		cmd := exec.Command("go", append([]string{"list", "-deps"}, cell.commands...)...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GOFLAGS=-mod=readonly", "GOPROXY=off", "GOSUMDB=off")
		raw, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("go list -deps: %v\n%s", err, raw)
		}
		found := false
		for _, line := range strings.Split(string(raw), "\n") {
			found = found || line == pkg
		}
		if found != cell.want {
			t.Fatalf("livee2e dependency in %v: got %v, want %v", cell.commands, found, cell.want)
		}
	}
}

// The built binary, not Run with captured writers: a print to the process's own stdout or stderr must show.
func TestMintLicenseBinaryIsSilentAndMakesTwoFiles(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "cigate")
	build := exec.Command("go", "build", "-o", bin, "./cmd/cigate")
	build.Dir = root
	build.Env = append(os.Environ(), "GOFLAGS=-mod=readonly", "GOPROXY=off", "GOSUMDB=off")
	if raw, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build cigate: %v\n%s", err, raw)
	}
	out := filepath.Join(dir, "out")
	if err := os.Mkdir(out, 0o700); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(bin, "live-e2e", "mint-license", filepath.Join(out, "public.b64"), filepath.Join(out, "licence.txt"))
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatal("the cigate binary must mint silently")
	}
	if files, err := os.ReadDir(out); err != nil || len(files) != 2 {
		t.Fatal("the cigate binary must create exactly two files")
	}
}
