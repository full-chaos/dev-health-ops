package providersync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os/exec"
	"strings"
	"testing"
)

// DO NOT MERGE. An operator card mints a GitHub repository id on a Mac shell (no database read) and passes it to a verb that
// takes the full id. This cell runs that shell pipeline byte for byte (the sha256 below is the sha256 of the card's text) and
// an openssl form of it, with every tool by absolute path, under three PATH values (GNU first, the system order, no PATH),
// and compares the result with repositoryIdentity for two synthetic names. The card writes the name lower case: the mutant
// that feeds a mixed-case name without lowering it must differ from repositoryIdentity.
const repoIdentityMintLineSHA256 = "df258190e8963109d225625f87d156a8083d1914be58e090d44f8bb5483fd2dc"

const repoIdentityMintLine = ``

const repoIdentityMintLineOpenSSL = `/usr/bin/openssl dgst -sha256 -r | /usr/bin/cut -c1-32 | /usr/bin/sed -E 's/^(.{8})(.{4})(.{4})(.{4})(.{12})$/\1-\2-\3-\4-\5/'`

func repoIdentityMintRun(t *testing.T, line, path, name string) string {
	t.Helper()
	command := exec.Command("/bin/bash", "-c", `printf '%s' "$1" | `+line, "mint", name)
	command.Env = []string{"PATH=" + path, "LC_ALL=C"}
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("mint line (PATH=%s): %v: %s", path, err, stderr.String())
	}
	return stdout.String()
}

func TestRepoIdentityMintLineMatchesRepositoryIdentity(t *testing.T) {
	t.Setenv("REPO_UUID", "")
	sum := sha256.Sum256([]byte(repoIdentityMintLine))
	if got := hex.EncodeToString(sum[:]); got != repoIdentityMintLineSHA256 {
		t.Fatalf("sha256 of the embedded line = %s, want %s (the card's text)", got, repoIdentityMintLineSHA256)
	}
	for _, name := range []string{"fx/rewalk-mint-b", "FX/Rewalk-Mint-A"} {
		want, err := repositoryIdentity(name)
		if err != nil {
			t.Fatalf("repositoryIdentity(%q): %v", name, err)
		}
		lower := strings.ToLower(name)
		for _, path := range []string{"/opt/homebrew/opt/coreutils/libexec/gnubin:/opt/homebrew/opt/gnu-sed/libexec/gnubin:/usr/bin:/bin", "/usr/bin:/bin", "/nonexistent"} {
			for label, line := range map[string]string{"shasum": repoIdentityMintLine, "openssl": repoIdentityMintLineOpenSSL} {
				got := repoIdentityMintRun(t, line, path, lower)
				if got != want+"\n" {
					t.Errorf("%s line, PATH=%s, name %q: %q, want %q + newline (repositoryIdentity)", label, path, lower, got, want)
				}
				t.Logf("MINT %s PATH=%s %s -> %s", label, path, lower, strings.TrimSpace(got)[:8])
			}
		}
		if name != lower {
			if got := repoIdentityMintRun(t, repoIdentityMintLine, "/usr/bin:/bin", name); got == want+"\n" {
				t.Errorf("mutant (no lower) on %q = repositoryIdentity: the lower case step is not proven", name)
			} else {
				t.Logf("MINT mutant no-lower %q -> %s != %s: killed", name, strings.TrimSpace(got)[:8], want[:8])
			}
		}
	}
}
