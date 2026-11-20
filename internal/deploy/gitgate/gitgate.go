// Package gitgate is the deploy-tree gates of `dho deploy` (the kit
// gate_snippet and p0_snippet): the clone checks, the fetch of main from the
// pinned URL, the first-parent rule for every code-selecting sha, and the tree
// pin. Every git call is a fixed argv through run.Runner; any miss is a STOP.
package gitgate

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/deploy/run"
)

// The prod constants (kit prodroll.sh DEPLOY_URL, DEPLOY_TREE, ROLLBACK_TREE).
const (
	DeployURL    = "https://github.com/full-chaos/dev-health-deploy.git"
	DeployTree   = "/home/ubuntu/lane-prod-k8s/deploy-roll-24d"
	RollbackTree = "/home/ubuntu/lane-prod-k8s/deploy-rollback-24d"
)

var (
	sha40   = regexp.MustCompile(`^[0-9a-f]{40}$`)
	sha256x = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

func git(ctx context.Context, r run.Runner, tree string, args ...string) (run.Output, error) {
	return r.Run(ctx, run.Cmd{Argv: append([]string{"git", "-C", tree}, args...)})
}

func lines(b []byte) []string {
	var out []string
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) != "" {
			out = append(out, sc.Text())
		}
	}
	return out
}

// Clone is a passed clone check.
type Clone struct{ Tree string }

func (c Clone) Record() string {
	return fmt.Sprintf("CLONE %s origin=pinned url_keys=0 porcelain=0", c.Tree)
}

// CheckClone: tree is the top of its own work tree, remote.origin.url == url,
// no url.* key in any git config scope, and `git status --porcelain` is empty.
func CheckClone(ctx context.Context, r run.Runner, url, tree string) (Clone, error) {
	o, err := git(ctx, r, tree, "rev-parse", "--show-toplevel")
	if err != nil || o.RC != 0 || strings.TrimSuffix(string(o.Stdout), "\n") != tree {
		return Clone{}, run.Stop("%s: not the top of a git work tree", tree)
	}
	o, err = git(ctx, r, tree, "config", "--get", "remote.origin.url")
	if err != nil || o.RC != 0 {
		return Clone{}, run.Stop("%s: no remote.origin.url", tree)
	}
	if strings.TrimSuffix(string(o.Stdout), "\n") != url {
		return Clone{}, run.Stop("%s: remote.origin.url is not the pinned URL", tree)
	}
	o, err = git(ctx, r, tree, "config", "--get-regexp", `^url\.`)
	if err != nil || (o.RC != 0 && o.RC != 1) {
		return Clone{}, run.Stop("%s: git config --get-regexp failed", tree)
	}
	if n := len(lines(o.Stdout)); n != 0 {
		return Clone{}, run.Stop("%s: %d url.* key(s) in its git config (insteadOf)", tree, n)
	}
	o, err = git(ctx, r, tree, "status", "--porcelain")
	if err != nil || o.RC != 0 {
		return Clone{}, run.Stop("%s: git status failed", tree)
	}
	if n := len(lines(o.Stdout)); n != 0 {
		return Clone{}, run.Stop("%s: git status --porcelain shows %d line(s)", tree, n)
	}
	return Clone{Tree: tree}, nil
}

// Main is the fetched main sha, equal to ls-remote refs/heads/main.
type Main struct{ SHA string }

func (m Main) Record() string { return fmt.Sprintf("MAIN %s = ls-remote refs/heads/main", m.SHA) }

// FetchMain fetches refs/heads/main from url with no tags; FETCH_HEAD must
// equal the one refs/heads/main row of ls-remote.
func FetchMain(ctx context.Context, r run.Runner, url, tree string) (Main, error) {
	o, err := git(ctx, r, tree, "fetch", "-q", "--no-tags", url, "refs/heads/main")
	if err != nil || o.RC != 0 {
		return Main{}, run.Stop("git fetch --no-tags refs/heads/main failed")
	}
	o, err = git(ctx, r, tree, "rev-parse", "--verify", "FETCH_HEAD^{commit}")
	f := strings.TrimSuffix(string(o.Stdout), "\n")
	if err != nil || o.RC != 0 || !sha40.MatchString(f) {
		return Main{}, run.Stop("FETCH_HEAD is not a commit")
	}
	o, err = git(ctx, r, tree, "ls-remote", url, "refs/heads/main")
	if err != nil || o.RC != 0 {
		return Main{}, run.Stop("git ls-remote refs/heads/main failed")
	}
	rows := lines(o.Stdout)
	l := ""
	if len(rows) == 1 {
		if sha, ref, ok := strings.Cut(rows[0], "\t"); ok && ref == "refs/heads/main" && sha40.MatchString(sha) {
			l = sha
		}
	}
	if l == "" || f != l {
		return Main{}, run.Stop("fetched main %s != ls-remote refs/heads/main %s", f, l)
	}
	return Main{SHA: f}, nil
}

// Ancestry is a passed first-parent check.
type Ancestry struct{ SHAs []string }

func (a Ancestry) Record() string {
	var b strings.Builder
	for i, s := range a.SHAs {
		if i > 0 {
			b.WriteByte('\n')
		}
		fmt.Fprintf(&b, "SHA %s ancestor=1 first_parent=1", s)
	}
	return b.String()
}

// OnFirstParent: every sha is an ancestor of main AND on its first-parent chain.
func OnFirstParent(ctx context.Context, r run.Runner, tree, main string, shas ...string) (Ancestry, error) {
	if !sha40.MatchString(main) {
		return Ancestry{}, run.Stop("main is not a 40-hex sha")
	}
	if len(shas) == 0 {
		return Ancestry{}, run.Stop("no sha to check")
	}
	o, err := git(ctx, r, tree, "rev-list", "--first-parent", main)
	if err != nil || o.RC != 0 {
		return Ancestry{}, run.Stop("rev-list --first-parent failed")
	}
	chain := map[string]bool{}
	for _, l := range lines(o.Stdout) {
		chain[l] = true
	}
	for _, s := range shas {
		if !sha40.MatchString(s) {
			return Ancestry{}, run.Stop("a code-selecting sha is not 40 hex")
		}
		o, err := git(ctx, r, tree, "merge-base", "--is-ancestor", s, main)
		if err != nil || o.RC != 0 {
			return Ancestry{}, run.Stop("%s is not an ancestor of main %s", s, main)
		}
		if !chain[s] {
			return Ancestry{}, run.Stop("%s is not on the first-parent chain of main %s", s, main)
		}
	}
	return Ancestry{SHAs: shas}, nil
}

// Pin is the tree pin of one roll: HEAD, the values blob and the chart tgz.
type Pin struct {
	Tree       string
	TreeSHA    string
	ValuesPath string
	ValuesBlob string
	TgzPath    string
	TgzSHA256  string
}

// Pinned is a passed tree pin.
type Pinned struct {
	Head, ValuesBlob, Disk, Tgz string
	Dirty                       int
}

func (p Pinned) Record() string {
	return fmt.Sprintf("P0u HEAD %s dirty=%d values_blob=%s disk=%s tgz=%s", p.Head, p.Dirty, p.ValuesBlob, p.Disk, p.Tgz)
}

// TreePin: HEAD == TreeSHA, 0 dirty tracked files, the committed values blob
// == the disk blob == ValuesBlob, sha256(tgz) == TgzSHA256.
func TreePin(ctx context.Context, r run.Runner, p Pin) (Pinned, error) {
	if !sha40.MatchString(p.TreeSHA) || !sha40.MatchString(p.ValuesBlob) || !sha256x.MatchString(p.TgzSHA256) || !inTree(p.ValuesPath) {
		return Pinned{}, run.Stop("tree pin: a bad pin value")
	}
	var got Pinned
	o, err := git(ctx, r, p.Tree, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil || o.RC != 0 {
		return Pinned{}, run.Stop("tree pin: rev-parse HEAD failed")
	}
	got.Head = strings.TrimSuffix(string(o.Stdout), "\n")
	o, err = git(ctx, r, p.Tree, "status", "--porcelain", "--untracked-files=no")
	if err != nil || o.RC != 0 {
		return Pinned{}, run.Stop("tree pin: git status failed")
	}
	got.Dirty = len(lines(o.Stdout))
	o, err = git(ctx, r, p.Tree, "rev-parse", "--verify", "HEAD:"+p.ValuesPath)
	if err != nil || o.RC != 0 {
		return Pinned{}, run.Stop("tree pin: the values file is not in HEAD")
	}
	got.ValuesBlob = strings.TrimSuffix(string(o.Stdout), "\n")
	o, err = git(ctx, r, p.Tree, "hash-object", "--", p.ValuesPath)
	if err != nil || o.RC != 0 {
		return Pinned{}, run.Stop("tree pin: hash-object of the values file failed")
	}
	got.Disk = strings.TrimSuffix(string(o.Stdout), "\n")
	if got.Tgz, err = fileSHA256(p.TgzPath); err != nil {
		return Pinned{}, run.Stop("tree pin: the chart tgz is unreadable")
	}
	var miss []string
	if got.Head != p.TreeSHA {
		miss = append(miss, "HEAD")
	}
	if got.Dirty != 0 {
		miss = append(miss, "dirty")
	}
	if got.ValuesBlob != p.ValuesBlob {
		miss = append(miss, "values_blob")
	}
	if got.Disk != p.ValuesBlob {
		miss = append(miss, "disk")
	}
	if got.Tgz != p.TgzSHA256 {
		miss = append(miss, "tgz")
	}
	if len(miss) != 0 {
		return Pinned{}, run.Stop("%s; tree pin: %s differ from the inputs and the pins", got.Record(), strings.Join(miss, ","))
	}
	return got, nil
}

var pathPart = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]*$`)

// inTree: a relative path of plain parts (no ".", "..", or a leading dot part).
func inTree(p string) bool {
	for _, part := range strings.Split(p, "/") {
		if !pathPart.MatchString(part) {
			return false
		}
	}
	return true
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
