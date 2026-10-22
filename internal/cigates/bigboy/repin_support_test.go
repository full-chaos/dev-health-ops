package bigboy_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

const (
	repinOld8   = "aaaaaaaa"
	repinWebSHA = "cccccccccccccccccccccccccccccccccccccccc"
	repinGHCR   = "ghcr.io/full-chaos/"
)

var (
	repinNew    = strings.Repeat("b", 40)
	repinImages = []string{"dev-health-go-dho", "dev-health-go-api-tools", "dev-health-go-operator"}
	repinVar    = map[string]string{
		"dev-health-go-dho":       "BIGBOY_DHO_IMAGE",
		"dev-health-go-api-tools": "BIGBOY_API_TOOLS_IMAGE",
		"dev-health-go-operator":  "BIGBOY_OPERATOR_IMAGE",
		"dev-health-web":          "BIGBOY_WEB_IMAGE",
	}
	repinAssign = regexp.MustCompile(`^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$`)
)

func repinH(parts ...string) string {
	s := sha256.Sum256([]byte(strings.Join(parts, "-")))
	return "sha256:" + hex.EncodeToString(s[:])
}

func repinIndexDigest(image, sha7 string) string { return repinH("index", image, sha7) }
func repinArm64Digest(image, sha7 string) string { return repinH("arm64", image, sha7) }

func repinDigestOf(c byte) string { return "sha256:" + strings.Repeat(string(c), 64) }

func repinRawIndex(image, sha7 string) string {
	return `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[` +
		`{"digest":"` + repinH("amd64", image, sha7) + `","platform":{"architecture":"amd64","os":"linux"}},` +
		`{"digest":"` + repinArm64Digest(image, sha7) + `","platform":{"architecture":"arm64","os":"linux"}},` +
		`{"digest":"` + repinH("att", image, sha7) + `","platform":{"architecture":"unknown","os":"unknown"}}]}`
}

type repinRef struct{ image, sha7 string }

func repinIndexDocker(refs []repinRef) string {
	var b strings.Builder
	b.WriteString("args=\"$*\"\n[ -z \"${REPIN_DOCKER_LOG:-}\" ] || printf '%s\\n' \"$args\" >> \"$REPIN_DOCKER_LOG\"\n")
	for _, r := range refs {
		fmt.Fprintf(&b, "if [[ \"$args\" == *\"%s:sha-%s\"* ]]; then\n", r.image, r.sha7)
		fmt.Fprintf(&b, "  if [[ \"$args\" == *\"--raw\"* ]]; then\n    cat <<'EOF_RAW'\n%s\nEOF_RAW\n", repinRawIndex(r.image, r.sha7))
		fmt.Fprintf(&b, "  else\n    echo '{\"digest\":\"%s\"}'\n  fi; exit 0\nfi\n", repinIndexDigest(r.image, r.sha7))
	}
	b.WriteString("exit 1\n")
	return b.String()
}

func repinRepinRefs() []repinRef {
	var refs []repinRef
	for _, i := range repinImages {
		refs = append(refs, repinRef{i, repinNew[:7]})
	}
	return append(refs, repinRef{"dev-health-go-dho", repinOld8[:7]})
}

var scratchSeq atomic.Int64

func repinDir(g *guard, pattern string) string {
	d := filepath.Join(g.tmp, fmt.Sprintf("%s%d", strings.TrimSuffix(pattern, "*"), scratchSeq.Add(1)))
	if err := os.MkdirAll(d, 0o755); err != nil {
		g.failf("scratch dir: %v", err)
	}
	return d
}

func repinWrite(g *guard, p, content string, mode os.FileMode) {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		g.failf("mkdir %s: %v", p, err)
		return
	}
	// ForkLock keeps a parallel test's fork from inheriting this write fd and failing the exec with ETXTBSY (golang/go#22315).
	syscall.ForkLock.RLock()
	err := os.WriteFile(p, []byte(content), mode)
	syscall.ForkLock.RUnlock()
	if err != nil {
		g.failf("write %s: %v", p, err)
	}
}

func repinReadFile(g *guard, p string) string {
	b, err := os.ReadFile(p)
	if err != nil {
		g.failf("cannot read %s: %v", p, err)
	}
	return string(b)
}

func repinBin(g *guard, stubs map[string]string) (dir, path string) {
	dir = repinDir(g, "bin")
	for name, body := range stubs {
		repinWrite(g, filepath.Join(dir, name), "#!/usr/bin/env bash\n"+body, 0o755)
	}
	return dir, dir + ":/usr/bin:/bin"
}

// Nothing inherited: the host's own docker and the caller's environment must not reach the script.
func repinRun(g *guard, env map[string]string, args ...string) scriptRun {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", args...)
	cmd.Dir = g.tmp
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + g.tmp, "LANG=C", "TMPDIR=" + g.tmp}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	code := 0
	if err := cmd.Run(); err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ctx.Err() != nil {
			g.failf("%v: %v (timeout or start failure)", args, err)
			return scriptRun{code: -1}
		}
		code = ee.ExitCode()
	}
	return scriptRun{stdout: so.String(), stderr: se.String(), code: code}
}

func (r scriptRun) repinOut() string { return r.stdout + r.stderr }

func repinEnv(text string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if m := repinAssign.FindStringSubmatch(line); m != nil {
			out[m[1]] = strings.Trim(m[2], `"'`)
		}
	}
	return out
}

func repinFixture(g *guard, pinnedDhoOld string) string {
	root := repinDir(g, "root")
	rec := filepath.Join(root, "_records")
	repinWrite(g, filepath.Join(rec, "bigboy-"+repinOld8, "bigboy-rest-commands.sh"),
		"#!/usr/bin/env bash\nSTEP=x\nB="+strings.Repeat("0", 40)+"\n", 0o644)
	repinWrite(g, filepath.Join(rec, "images.env"),
		"BIGBOY_DHO_IMAGE="+repinGHCR+"dev-health-go-dho@"+pinnedDhoOld+"\n"+
			"BIGBOY_API_TOOLS_IMAGE="+repinGHCR+"dev-health-go-api-tools@"+repinDigestOf('1')+"\n"+
			"BIGBOY_OPERATOR_IMAGE="+repinGHCR+"dev-health-go-operator@"+repinDigestOf('2')+"\n"+
			"BIGBOY_WEB_IMAGE="+repinGHCR+"dev-health-web@"+repinDigestOf('3')+"\n", 0o644)
	return root
}

func repinTree(root string) []string {
	var out []string
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err == nil && p != root {
			r, _ := filepath.Rel(root, p)
			out = append(out, r)
		}
		return nil
	})
	sort.Strings(out)
	return out
}

func repinSentinel() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return "sentinel-value-" + hex.EncodeToString(b)
}

// copyPath writes 0644, and the cut runs its sibling scripts directly.
func repinExecutable(t *testing.T, root string) {
	t.Helper()
	matches, _ := filepath.Glob(filepath.Join(root, "ci", "bigboy", "*.sh"))
	for _, m := range matches {
		if err := os.Chmod(m, 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

func repinMut(rel, old, repl string) func(*testing.T, string) {
	return all(mut(rel, old, repl), repinExecutable)
}
