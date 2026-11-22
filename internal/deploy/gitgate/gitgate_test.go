package gitgate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/deploy/run"
)

// world is a REAL git fixture: a bare origin served by file://, a work repo
// that makes commits and pushes, and the deploy clone under test.
type world struct {
	t                    *testing.T
	dir, url, work, tree string
	r                    run.Exec
}

func newWorld(t *testing.T) *world {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(dir, "home")
	if err := os.Mkdir(home, 0o700); err != nil {
		t.Fatal(err)
	}
	w := &world{t: t, dir: dir, url: "file://" + filepath.Join(dir, "origin.git"), work: filepath.Join(dir, "work"), tree: filepath.Join(dir, "deploy")}
	// The fixed PATH of the real Runner; HOME and the system scope are the
	// fixture's own so no host git config reaches a cell.
	w.r = run.Exec{Env: []string{
		"PATH=/usr/sbin:/usr/bin:/sbin:/bin", "HOME=" + home, "LC_ALL=C", "GIT_CONFIG_NOSYSTEM=1",
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.invalid", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.invalid",
	}}
	w.git("", "init", "-q", "--bare", "-b", "main", filepath.Join(dir, "origin.git"))
	w.git("", "init", "-q", "-b", "main", w.work)
	w.git(w.work, "remote", "add", "origin", w.url)
	w.commit("prod/values.yaml", "image: a\n")
	w.git(w.work, "push", "-q", "origin", "refs/heads/main:refs/heads/main")
	w.git("", "clone", "-q", w.url, w.tree)
	return w
}

func (w *world) git(dir string, args ...string) string {
	w.t.Helper()
	if dir != "" {
		args = append([]string{"-C", dir}, args...)
	}
	o, err := w.r.Run(context.Background(), run.Cmd{Argv: append([]string{"git"}, args...)})
	if err != nil || o.RC != 0 {
		w.t.Fatalf("git %q: %v rc %d %s", args, err, o.RC, o.Stderr)
	}
	return strings.TrimSuffix(string(o.Stdout), "\n")
}

func (w *world) write(dir, path, body string) {
	w.t.Helper()
	p := filepath.Join(dir, path)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		w.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		w.t.Fatal(err)
	}
}

// commit commits one file in the work repo and returns the sha.
func (w *world) commit(path, body string) string {
	w.t.Helper()
	w.write(w.work, path, body)
	w.git(w.work, "add", "--", path)
	w.git(w.work, "commit", "-q", "-m", path+" "+body)
	return w.git(w.work, "rev-parse", "HEAD")
}

// newRepo makes a repo at dir with one commit and returns its sha.
func (w *world) newRepo(dir, file string) string {
	w.t.Helper()
	w.git("", "init", "-q", "-b", "main", dir)
	w.write(dir, file, file+"\n")
	w.git(dir, "add", "--", file)
	w.git(dir, "commit", "-q", "-m", file)
	return w.git(dir, "rev-parse", "HEAD")
}

// gitlink records a gitlink at path in repo (with its .gitmodules row) and commits.
func (w *world) gitlink(repo, path, url, sha string) {
	w.t.Helper()
	w.appendConfig(filepath.Join(repo, ".gitmodules"), "[submodule \""+path+"\"]\n\tpath = "+path+"\n\turl = "+url+"\n")
	w.git(repo, "update-index", "--add", "--cacheinfo", "160000,"+sha+","+path)
	w.git(repo, "add", ".gitmodules")
	w.git(repo, "commit", "-q", "-m", "gitlink "+path)
}

// addSubmodule records the gitlink "module" on main and pulls it into the
// deploy clone, uninitialized (an empty dir, as a plain clone leaves it).
// It returns the module repo, which itself has the gitlink "inner".
func (w *world) addSubmodule() string {
	w.t.Helper()
	inner := filepath.Join(w.dir, "inner")
	innerSHA := w.newRepo(inner, "i.txt")
	sub := filepath.Join(w.dir, "sub")
	w.newRepo(sub, "s.txt")
	w.gitlink(sub, "inner", "file://"+inner, innerSHA)
	w.gitlink(w.work, "module", "file://"+sub, w.git(sub, "rev-parse", "HEAD"))
	w.git(w.work, "push", "-q", "origin", "refs/heads/main:refs/heads/main")
	w.git(w.tree, "pull", "-q", "--ff-only")
	return sub
}

// checkout checks the module out in the deploy clone (and, nested, its inner).
func (w *world) checkout(sub string, nested bool) {
	w.t.Helper()
	m := filepath.Join(w.tree, "module")
	w.git("", "clone", "-q", "file://"+sub, m)
	if nested {
		w.git("", "clone", "-q", "file://"+filepath.Join(w.dir, "inner"), filepath.Join(m, "inner"))
	}
}

func (w *world) appendConfig(path, text string) {
	w.t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		w.t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(text); err != nil {
		w.t.Fatal(err)
	}
}

func stop(t *testing.T, err error, what string) {
	t.Helper()
	if run.Code(err) != run.CodeStop {
		t.Fatalf("%s: want STOP, got %v", what, err)
	}
}

func TestTheProdConstantsAreTheKitPins(t *testing.T) {
	if DeployURL != "https://github.com/full-chaos/dev-health-deploy.git" ||
		DeployTree != "/home/ubuntu/lane-prod-k8s/deploy-roll-24d" ||
		RollbackTree != "/home/ubuntu/lane-prod-k8s/deploy-rollback-24d" {
		t.Fatal("the prod constants differ from the kit prodroll.sh DEPLOY_URL / DEPLOY_TREE / ROLLBACK_TREE")
	}
}

// G8 on REAL git: the clean clone passes; each listed defect is a STOP.
func TestCloneChecks(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	c, err := CheckClone(ctx, w.r, w.url, w.tree)
	if err != nil || c.Record() != "CLONE "+w.tree+" origin=pinned url_keys=0 porcelain=0" {
		t.Fatalf("clean clone: %v %q", err, c.Record())
	}
	_, err = CheckClone(ctx, w.r, "https://github.com/full-chaos/dev-health-deploy.git", w.tree)
	stop(t, err, "origin is not the pinned URL")

	for name, defect := range map[string]func(w *world) (url, tree string){
		"origin changed": func(w *world) (string, string) {
			w.git(w.tree, "remote", "set-url", "origin", w.url+"x")
			return w.url, w.tree
		},
		"url insteadOf in the repo config": func(w *world) (string, string) {
			w.appendConfig(filepath.Join(w.tree, ".git", "config"), "[url \"file:///elsewhere\"]\n\tinsteadOf = "+w.url+"\n")
			return w.url, w.tree
		},
		"url insteadOf in a GIT_CONFIG_GLOBAL file": func(w *world) (string, string) {
			g := filepath.Join(w.dir, "global.gitconfig")
			w.appendConfig(g, "[url \"file:///elsewhere\"]\n\tpushInsteadOf = x\n")
			w.r.Env = append(w.r.Env, "GIT_CONFIG_GLOBAL="+g)
			return w.url, w.tree
		},
		"an untracked file": func(w *world) (string, string) {
			w.write(w.tree, "stray.txt", "x\n")
			return w.url, w.tree
		},
		"a modified tracked file": func(w *world) (string, string) {
			w.write(w.tree, "prod/values.yaml", "image: b\n")
			return w.url, w.tree
		},
		"a subdir of the clone": func(w *world) (string, string) {
			return w.url, filepath.Join(w.tree, "prod")
		},
		"not a repo": func(w *world) (string, string) {
			return w.url, w.dir
		},
		"another fetch URL first, the pinned URL second": func(w *world) (string, string) {
			w.git(w.tree, "remote", "set-url", "origin", w.url+"x")
			w.git(w.tree, "remote", "set-url", "--add", "origin", w.url)
			return w.url, w.tree
		},
		"an untracked file under status.showUntrackedFiles=no": func(w *world) (string, string) {
			w.appendConfig(filepath.Join(w.tree, ".git", "config"), "[status]\n\tshowUntrackedFiles = no\n")
			w.write(w.tree, "stray.txt", "x\n")
			return w.url, w.tree
		},
		"a url.* key longer than 64 KiB in the global config": func(w *world) (string, string) {
			g := filepath.Join(w.dir, "global.gitconfig")
			w.appendConfig(g, "[url \"file:///"+strings.Repeat("e", 70000)+"\"]\n\tinsteadOf = x\n")
			w.r.Env = append(w.r.Env, "GIT_CONFIG_GLOBAL="+g)
			return w.url, w.tree
		},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			url, tree := defect(w)
			_, err := CheckClone(ctx, w.r, url, tree)
			stop(t, err, name)
		})
	}

	// git fetch uses the first url: the pinned one first passes; a second url or a push url after it does not matter.
	t.Run("the pinned fetch URL first", func(t *testing.T) {
		w := newWorld(t)
		w.git(w.tree, "remote", "set-url", "--add", "origin", w.url+"x")
		w.git(w.tree, "remote", "set-url", "--push", "origin", w.url+"y")
		if _, err := CheckClone(ctx, w.r, w.url, w.tree); err != nil {
			t.Fatal(err)
		}
	})
}

// moveAfterFetch pushes a new main commit right after the fetch, before ls-remote.
type moveAfterFetch struct {
	w     *world
	moved bool
}

func (m *moveAfterFetch) Run(ctx context.Context, c run.Cmd) (run.Output, error) {
	o, err := m.w.r.Run(ctx, c)
	if !m.moved && slices.Contains(c.Argv, "fetch") {
		m.moved = true
		m.w.commit("moved.txt", "x\n")
		m.w.git(m.w.work, "push", "-q", "origin", "refs/heads/main:refs/heads/main")
	}
	return o, err
}

// G9 on REAL git: fetch main with no tags; FETCH_HEAD == ls-remote refs/heads/main.
func TestFetchMain(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	old := w.git(w.work, "rev-parse", "HEAD")
	tip := w.commit("prod/values.yaml", "image: c\n")
	w.git(w.work, "tag", "main", old)
	w.git(w.work, "push", "-q", "origin", "refs/heads/main:refs/heads/main", "refs/tags/main:refs/tags/main")
	m, err := FetchMain(ctx, w.r, w.url, w.tree)
	if err != nil || m.SHA != tip {
		t.Fatalf("a tag named main must not count: got %q %v, want the branch %s", m.SHA, err, tip)
	}
	if got := w.git(w.tree, "tag", "--list"); got != "" {
		t.Fatalf("--no-tags fetched tags: %q", got)
	}

	_, err = FetchMain(ctx, &moveAfterFetch{w: w}, w.url, w.tree)
	stop(t, err, "main moves between fetch and ls-remote")
	w.git(w.work, "push", "-q", "origin", "refs/heads/main:refs/heads/x/refs/heads/main", "refs/heads/main:refs/tags/refs/heads/main")
	if m, err := FetchMain(ctx, w.r, w.url, w.tree); err != nil || m.SHA != w.git(w.work, "rev-parse", "HEAD") {
		t.Fatalf("refs that end in /refs/heads/main must not count: %q %v", m.SHA, err)
	}
	_, err = FetchMain(ctx, w.r, w.url+"-none", w.tree)
	stop(t, err, "no such origin")
}

// G10 on REAL git: an ancestor on the first-parent chain passes; a side
// commit of a merge and a commit not on main are a STOP.
func TestOnFirstParent(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	a := w.git(w.work, "rev-parse", "HEAD")
	w.git(w.work, "checkout", "-q", "-b", "side")
	side := w.commit("side.txt", "s\n")
	w.git(w.work, "checkout", "-q", "main")
	b := w.commit("prod/values.yaml", "image: b\n")
	w.git(w.work, "merge", "-q", "--no-ff", "-m", "merge side", "side")
	c := w.commit("prod/values.yaml", "image: c\n")
	w.git(w.work, "checkout", "-q", "-b", "other", a)
	other := w.commit("other.txt", "o\n")
	w.git(w.work, "push", "-q", "origin", "main", "other", "side")
	m, err := FetchMain(ctx, w.r, w.url, w.tree)
	if err != nil || m.SHA != c {
		t.Fatalf("main tip: %q %v, want %s", m.SHA, err, c)
	}
	w.git(w.tree, "fetch", "-q", "--no-tags", w.url, "refs/heads/other", "refs/heads/side")

	got, err := OnFirstParent(ctx, w.r, w.tree, m.SHA, a, b, c)
	if err != nil || got.Record() != "SHA "+a+" ancestor=1 first_parent=1\nSHA "+b+" ancestor=1 first_parent=1\nSHA "+c+" ancestor=1 first_parent=1" {
		t.Fatalf("main commits: %v %q", err, got.Record())
	}
	_, err = OnFirstParent(ctx, w.r, w.tree, m.SHA, a, side)
	stop(t, err, "the side commit of a merge")
	if !strings.HasPrefix(err.Error(), side+" is not on the first-parent chain of main") {
		t.Fatalf("the side commit is an ancestor; the first-parent rule must name it: %v", err)
	}
	_, err = OnFirstParent(ctx, w.r, w.tree, m.SHA, other)
	stop(t, err, "a commit not on main")
	if !strings.HasPrefix(err.Error(), other+" is not an ancestor of main") {
		t.Fatalf("the ancestor rule (kit order: ancestor, then first parent) must name it: %v", err)
	}
	_, err = OnFirstParent(ctx, w.r, w.tree, m.SHA, a[:12])
	stop(t, err, "a short sha")
	_, err = OnFirstParent(ctx, w.r, w.tree, m.SHA)
	stop(t, err, "no sha")
}

// G11 on REAL git: HEAD, 0 dirty, values blob == disk blob == pin, tgz sha256 == pin.
func TestTreePin(t *testing.T) {
	ctx := context.Background()
	newPin := func(w *world) Pin {
		tgz := filepath.Join(w.dir, "chart.tgz")
		if err := os.WriteFile(tgz, []byte("chart bytes"), 0o600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte("chart bytes"))
		return Pin{
			Tree: w.tree, TreeSHA: w.git(w.tree, "rev-parse", "HEAD"),
			ValuesPath: "prod/values.yaml", ValuesBlob: w.git(w.tree, "rev-parse", "HEAD:prod/values.yaml"),
			TgzPath: tgz, TgzSHA256: hex.EncodeToString(sum[:]),
		}
	}
	w := newWorld(t)
	p := newPin(w)
	w.write(w.tree, "untracked.txt", "u\n")
	got, err := TreePin(ctx, w.r, p)
	if err != nil || got.Record() != "P0u HEAD "+p.TreeSHA+" dirty=0 values_blob="+p.ValuesBlob+" disk="+p.ValuesBlob+" tgz="+p.TgzSHA256 {
		t.Fatalf("the pinned tree: %v %q", err, got.Record())
	}

	for name, c := range map[string]struct {
		defect func(w *world, p *Pin)
		field  string
	}{
		"HEAD off by one": {func(w *world, p *Pin) {
			w.commit("prod/values.yaml", "image: a\n# 2\n")
			w.git(w.work, "push", "-q", "origin", "refs/heads/main:refs/heads/main")
			w.git(w.tree, "pull", "-q", "--ff-only")
			p.ValuesBlob = w.git(w.tree, "rev-parse", "HEAD:prod/values.yaml")
		}, "HEAD"},
		"a dirty tracked file": {func(w *world, p *Pin) {
			w.commit("other.yaml", "o\n")
			w.git(w.work, "push", "-q", "origin", "refs/heads/main:refs/heads/main")
			w.git(w.tree, "pull", "-q", "--ff-only")
			*p = newPin(w)
			w.write(w.tree, "other.yaml", "changed\n")
		}, "dirty"},
		"a values edit on disk": {func(w *world, p *Pin) {
			w.write(w.tree, "prod/values.yaml", "image: z\n")
		}, "dirty,disk"},
		"a values blob pin off": {func(w *world, p *Pin) {
			p.ValuesBlob = strings.Repeat("1", 40)
		}, "values_blob,disk"},
		"a CRLF checkout under eol=crlf (status clean)": {func(w *world, p *Pin) {
			w.write(w.tree, ".git/info/attributes", "prod/values.yaml text eol=crlf\n")
			if err := os.Remove(filepath.Join(w.tree, "prod/values.yaml")); err != nil {
				t.Fatal(err)
			}
			w.git(w.tree, "checkout", "--", "prod/values.yaml")
		}, "disk"},
		"a CRLF checkout under core.autocrlf=true (status clean)": {func(w *world, p *Pin) {
			w.appendConfig(filepath.Join(w.tree, ".git", "config"), "[core]\n\tautocrlf = true\n")
			if err := os.Remove(filepath.Join(w.tree, "prod/values.yaml")); err != nil {
				t.Fatal(err)
			}
			w.git(w.tree, "checkout", "--", "prod/values.yaml")
		}, "disk"},
		"a tgz byte change": {func(w *world, p *Pin) {
			if err := os.WriteFile(p.TgzPath, []byte("chart bytez"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, "tgz"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			p := newPin(w)
			c.defect(w, &p)
			_, err := TreePin(ctx, w.r, p)
			stop(t, err, name)
			if !strings.HasSuffix(err.Error(), "tree pin: "+c.field+" differ from the inputs and the pins") {
				t.Fatalf("want the fields %q named, got %v", c.field, err)
			}
		})
	}
	w = newWorld(t)
	p = newPin(w)
	p.ValuesPath = "../etc/passwd"
	_, err = TreePin(ctx, w.r, p)
	stop(t, err, "a values path out of the tree")
}

func refused(t *testing.T, err error, tree, what string) {
	t.Helper()
	want := tree + ": an initialized submodule (" + what + "): dho deploy serves only a tree with uninitialized submodules"
	if run.Code(err) != run.CodeRefused || err.Error() != want {
		t.Fatalf("want exit 3 %q, got %v", want, err)
	}
}

// r2 P1 (acp (a)): both gates refuse a tree with an initialized submodule
// (exit 3, fixed text), since git status reads each submodule's own config;
// an uninitialized gitlink passes.
func TestInitializedSubmodulesAreRefused(t *testing.T) {
	ctx := context.Background()
	tgz := func(w *world) Pin {
		p := filepath.Join(w.dir, "chart.tgz")
		if err := os.WriteFile(p, []byte("chart bytes"), 0o600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte("chart bytes"))
		return Pin{Tree: w.tree, TreeSHA: w.git(w.tree, "rev-parse", "HEAD"), ValuesPath: "prod/values.yaml",
			ValuesBlob: w.git(w.tree, "rev-parse", "HEAD:prod/values.yaml"), TgzPath: p, TgzSHA256: hex.EncodeToString(sum[:])}
	}
	for name, c := range map[string]struct {
		defect func(w *world, sub string)
		what   string
	}{
		"a checked-out submodule": {func(w *world, sub string) { w.checkout(sub, false) }, "a checked-out gitlink"},
		"nested 2 levels, inner edit under .gitmodules ignore=dirty": {func(w *world, sub string) {
			w.checkout(sub, true)
			w.appendConfig(filepath.Join(w.tree, "module", ".gitmodules"), "\tignore = dirty\n")
			w.write(filepath.Join(w.tree, "module", "inner"), "i.txt", "edited\n")
		}, "a checked-out gitlink"},
		"its own status.showUntrackedFiles=no hides an untracked file": {func(w *world, sub string) {
			w.checkout(sub, false)
			w.appendConfig(filepath.Join(w.tree, "module", ".git", "config"), "[status]\n\tshowUntrackedFiles = no\n")
			w.write(filepath.Join(w.tree, "module"), "stray.txt", "x\n")
		}, "a checked-out gitlink"},
		"git submodule init (url in the local config), dir empty": {func(w *world, sub string) {
			w.git(w.tree, "submodule", "init")
		}, "a submodule.<name> config key"},
		"submodule.<name>.ignore in the local config, dir empty": {func(w *world, sub string) {
			w.appendConfig(filepath.Join(w.tree, ".git", "config"), "[submodule \"module\"]\n\tignore = all\n")
		}, "a submodule.<name> config key"},
		"a modules git dir, dir empty": {func(w *world, sub string) {
			if err := os.Mkdir(filepath.Join(w.tree, ".git", "modules"), 0o700); err != nil {
				t.Fatal(err)
			}
		}, "a modules git dir"},
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			sub := w.addSubmodule()
			p := tgz(w)
			c.defect(w, sub)
			_, err := CheckClone(ctx, w.r, w.url, w.tree)
			refused(t, err, w.tree, c.what)
			_, err = TreePin(ctx, w.r, p)
			refused(t, err, w.tree, c.what)
		})
	}
	t.Run("an uninitialized gitlink passes both gates", func(t *testing.T) {
		w := newWorld(t)
		w.addSubmodule()
		if _, err := CheckClone(ctx, w.r, w.url, w.tree); err != nil {
			t.Fatal(err)
		}
		if _, err := TreePin(ctx, w.r, tgz(w)); err != nil {
			t.Fatal(err)
		}
	})
}

// synthShape is a SYNTHETIC deploy-tree-like layout (made-up names, modelled
// on the shape of the real deploy tree, not its paths): 400 blobs (213 mode
// 100644, 187 mode 100755) in nested dirs, a .gitignore that ignores one
// tracked path, a .gitmodules with 3 entries (path + url keys, example.invalid
// URLs) and 3 vendor gitlinks (mode 160000) at made-up shas.
func synthShape() (blobs map[string]string, execs map[string]bool, links [][2]string) {
	blobs, execs = map[string]string{}, map[string]bool{}
	names := []string{"alpha", "bravo", "charlie"}
	var gm strings.Builder
	for _, n := range names {
		path := "vendor/example-" + n
		fmt.Fprintf(&gm, "[submodule %q]\n\tpath = %s\n\turl = https://git.example.invalid/example/%s.git\n", path, path, n)
		sum := sha256.Sum256([]byte(n))
		links = append(links, [2]string{hex.EncodeToString(sum[:20]), path})
	}
	blobs[".gitmodules"] = gm.String()
	blobs[".gitignore"] = "build/\n"
	blobs["build/keep.txt"] = "tracked under an ignored dir\n"
	blobs["prod/example.values.yaml"] = "image: example\n"
	for i := 0; len(blobs) < 400; i++ {
		p := fmt.Sprintf("area-%d/part-%d/file-%03d.yaml", i%7, i%3, i)
		blobs[p] = p + "\n"
		if len(execs) < 187 {
			execs[p] = true
		}
	}
	return blobs, execs, links
}

// acp control: a clone of a deploy-tree-like shape (synthShape) passes both
// gates: its 3 vendor gitlinks stay uninitialized (empty dirs).
func TestTheDeployTreeShapePassesBothGates(t *testing.T) {
	ctx := context.Background()
	blobs, execs, links := synthShape()
	if len(blobs) != 400 || len(execs) != 187 || len(links) != 3 {
		t.Fatalf("synthetic shape: %d blobs, %d executable, %d gitlinks", len(blobs), len(execs), len(links))
	}
	w := newWorld(t)
	shape := filepath.Join(w.dir, "shape")
	w.git("", "init", "-q", "-b", "main", shape)
	for path, body := range blobs {
		w.write(shape, path, body)
		if execs[path] {
			if err := os.Chmod(filepath.Join(shape, path), 0o755); err != nil {
				t.Fatal(err)
			}
		}
	}
	w.git(shape, "add", "-f", "--", ".")
	// gitlinks after `git add .`: it stages a gitlink with no dir on disk as removed
	for _, l := range links {
		w.git(shape, "update-index", "--add", "--cacheinfo", "160000,"+l[0]+","+l[1])
	}
	w.git(shape, "commit", "-q", "-m", "synthetic deploy tree shape")
	got := w.git(shape, "ls-tree", "-r", "HEAD")
	if strings.Count(got, "160000 commit") != 3 || strings.Count(got, "100755 blob") != 187 || strings.Count(got, "\n")+1 != 403 {
		t.Fatalf("the shape commit holds %d gitlinks, %d executables of %d entries, want 3, 187 of 403", strings.Count(got, "160000 commit"), strings.Count(got, "100755 blob"), strings.Count(got, "\n")+1)
	}
	tree := filepath.Join(w.dir, "deploy-shape")
	url := "file://" + shape
	w.git("", "clone", "-q", url, tree)
	if _, err := CheckClone(ctx, w.r, url, tree); err != nil {
		t.Fatalf("CheckClone: %v", err)
	}
	sum := sha256.Sum256([]byte("chart bytes"))
	p := filepath.Join(w.dir, "chart.tgz")
	if err := os.WriteFile(p, []byte("chart bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	pin := Pin{Tree: tree, TreeSHA: w.git(tree, "rev-parse", "HEAD"), ValuesPath: "prod/example.values.yaml",
		ValuesBlob: w.git(tree, "rev-parse", "HEAD:prod/example.values.yaml"), TgzPath: p, TgzSHA256: hex.EncodeToString(sum[:])}
	if _, err := TreePin(ctx, w.r, pin); err != nil {
		t.Fatalf("TreePin: %v", err)
	}
}
