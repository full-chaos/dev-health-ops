package run

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// The kit CHILD_ENV (prodroll.sh: PATH, HOME, LANG, LC_ALL, KUBECONFIG), as text.
var kitChildEnv = []string{
	"PATH=/usr/sbin:/usr/bin:/sbin:/bin",
	"HOME=/home/ubuntu",
	"LANG=C.UTF-8",
	"LC_ALL=C",
	"KUBECONFIG=/etc/rancher/k3s/k3s.yaml",
}

func TestTheFixedEnvIsTheKitChildEnv(t *testing.T) {
	if !reflect.DeepEqual(FixedEnv(), kitChildEnv) {
		t.Fatalf("FixedEnv = %q, want the kit CHILD_ENV %q", FixedEnv(), kitChildEnv)
	}
	if r := Real(); !reflect.DeepEqual(r.Env, kitChildEnv) || !reflect.DeepEqual(Tools, []string{"git", "helm"}) {
		t.Fatalf("Real() = %+v, Tools = %q", r, Tools)
	}
}

// fakeBin is the one copy of this test binary that every fake tool links to.
var fakeBin string

// TestMain has two roles.
//   - In a fake (a hard link of the test binary under a tool name, started by
//     a cell), the run never reaches the tests: a fake-* word answers, and
//     any other start exits 3 (a fake started by mistake can never run the
//     suite again). Every answer first reports the running binary's path
//     (EXE=, /proc/self/exe: the linked name) and the exact argv (ARGV=),
//     on stderr; fake-env then prints the env, fake-pwd the cwd, and fake-cat
//     copies stdin to stdout.
//   - In the test run, it copies the test binary ONCE into a temp dir (under
//     TMPDIR); fakeBins hard-links it per name.
//
// No system binary is used: on some hosts their behaviour depends on argv[0]
// (a multicall coreutils).
func TestMain(m *testing.M) {
	if fakeWord(os.Args) {
		os.Exit(fakeTool(os.Args))
	}
	if !strings.HasSuffix(filepath.Base(os.Args[0]), ".test") {
		os.Exit(3)
	}
	dir, err := os.MkdirTemp("", "run-fakes-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if fakeBin, err = copySelf(dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// fakeWord: a fake-* mode word is among the args.
func fakeWord(argv []string) bool {
	for _, a := range argv[1:] {
		if a == "fake-env" || a == "fake-pwd" || a == "fake-cat" {
			return true
		}
	}
	return false
}

func fakeTool(argv []string) int {
	exe, _ := os.Executable()
	fmt.Fprintf(os.Stderr, "EXE=%s\nARGV=%q\n", exe, argv)
	for _, a := range argv[1:] {
		switch a {
		case "fake-env":
			for _, kv := range os.Environ() {
				fmt.Println(kv)
			}
			return 0
		case "fake-pwd":
			wd, _ := os.Getwd()
			fmt.Println(wd)
			return 0
		case "fake-cat":
			if _, err := io.Copy(os.Stdout, os.Stdin); err != nil {
				return 2
			}
			return 0
		}
	}
	return 3
}

func copySelf(dir string) (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", err
	}
	b, err := os.ReadFile(self)
	if err != nil {
		return "", err
	}
	p := filepath.Join(dir, "fake")
	return p, os.WriteFile(p, b, 0o755)
}

// fakeBins hard-links the one fake binary under each name in a new temp dir
// (a hard link keeps its own path in /proc/self/exe; a symlink would not).
func fakeBins(t *testing.T, names ...string) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range names {
		if err := os.Link(fakeBin, filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// ran parses a fake's stderr: the path it ran from and its exact argv.
func ran(t *testing.T, stderr []byte) (exe, argv string) {
	t.Helper()
	lines := strings.SplitN(string(stderr), "\n", 3)
	if len(lines) < 2 || !strings.HasPrefix(lines[0], "EXE=") || !strings.HasPrefix(lines[1], "ARGV=") {
		t.Fatalf("the fake reported no EXE/ARGV lines: %q", stderr)
	}
	return strings.TrimPrefix(lines[0], "EXE="), strings.TrimPrefix(lines[1], "ARGV=")
}

// wantArgv is the exact argv a fake must get for name + words (git gets the
// maintenance pair first).
func wantArgv(name string, words ...string) string {
	if name == "git" {
		words = append([]string{"-c", "maintenance.auto=false"}, words...)
	}
	return fmt.Sprintf("%q", append([]string{name}, words...))
}

// withPATH is the fixed env with its PATH replaced.
func withPATH(path string) []string {
	env := FixedEnv()
	env[0] = "PATH=" + path
	return env
}

// G4: a planted var, GIT_DIR and planted binaries of the same names on the
// parent PATH never reach the child; the binary is the one on the PATH of
// Env (by its path), with the exact argv; the child env is exactly Env and
// its cwd is "/".
func TestTheChildGetsOnlyItsEnvAndRunsInRoot(t *testing.T) {
	onEnv := fakeBins(t, "git", "helm")
	onParent := fakeBins(t, "git", "helm")
	t.Setenv("EVILVAR", "planted")
	t.Setenv("PATH", onParent+":"+os.Getenv("PATH"))
	t.Setenv("GIT_DIR", "/planted")
	e := Exec{Env: withPATH(onEnv + ":/usr/sbin:/usr/bin:/sbin:/bin")}
	for _, name := range Tools {
		for _, mode := range []string{"fake-env", "fake-pwd"} {
			o, err := e.Run(context.Background(), Cmd{Argv: []string{name, mode}})
			if err != nil || o.RC != 0 {
				t.Fatalf("%s %s: %v rc %d", name, mode, err, o.RC)
			}
			exe, argv := ran(t, o.Stderr)
			if exe != filepath.Join(onEnv, name) || argv != wantArgv(name, mode) {
				t.Fatalf("%s %s ran %s with argv %s, want %s with %s", name, mode, exe, argv, filepath.Join(onEnv, name), wantArgv(name, mode))
			}
			got := strings.TrimSuffix(string(o.Stdout), "\n")
			if mode == "fake-env" && !reflect.DeepEqual(strings.Split(got, "\n"), e.Env) {
				t.Fatalf("%s child env = %q, want exactly %q", name, got, e.Env)
			}
			if mode == "fake-pwd" && got != "/" {
				t.Fatalf("%s child cwd = %q, want /", name, got)
			}
		}
	}
	if o, err := Real().Run(context.Background(), Cmd{Argv: []string{"git", "version"}}); err != nil || o.RC != 0 || !strings.HasPrefix(string(o.Stdout), "git version ") {
		t.Fatalf("the real Runner runs git from the fixed PATH: %v rc %d %q", err, o.RC, o.Stdout)
	}
}

// acp (Semgrep dangerous-exec-command; ACP-WORD-11788-12): only git and helm
// start, each by a literal name; any other argv[0] (kubectl included) is
// refused with fixed text and nothing runs, even when a binary of that name
// is on the PATH of Env.
func TestOnlyTheClosedBinarySetStarts(t *testing.T) {
	bins := fakeBins(t, "kubectl", "touch", "sh", "env", "curl")
	e := Exec{Env: withPATH(bins + ":/usr/sbin:/usr/bin:/sbin:/bin")}
	for name, argv := range map[string][]string{
		"kubectl on the PATH": {"kubectl", "fake-env"},
		"touch on the PATH":   {"touch", "fake-env"},
		"sh -c":               {"sh", "-c", "fake-env"},
		"env":                 {"env", "fake-env"},
		"curl":                {"curl", "fake-env"},
		"bash -c":             {"bash", "-c", "echo x"},
		"absolute git":        {"/usr/bin/git", "version"},
		"relative path":       {"./git", "version"},
		"a name with a NUL":   {"git\x00", "version"},
		"empty name":          {""},
	} {
		t.Run(name, func(t *testing.T) {
			o, err := e.Run(context.Background(), Cmd{Argv: argv})
			if Code(err) != CodeRefused || err.Error() != "exec: argv[0] is not one of git, helm" {
				t.Fatalf("want the fixed refusal, got %v", err)
			}
			if len(o.Stdout)+len(o.Stderr) != 0 {
				t.Fatalf("a refused binary ran: %q %q", o.Stdout, o.Stderr)
			}
		})
	}
	if _, err := e.Run(context.Background(), Cmd{}); Code(err) != CodeRefused {
		t.Fatalf("empty argv: %v", err)
	}
	if _, err := (Exec{Env: withPATH("relative/bin")}).Run(context.Background(), Cmd{Argv: []string{"git", "version"}}); Code(err) != CodeStop {
		t.Fatalf("a binary off the PATH of Env must stop, got %v", err)
	}
}

// Every git call of the Runner turns off git's detached auto-maintenance.
func TestGitCallsTurnOffAutoMaintenance(t *testing.T) {
	cmd, err := command(context.Background(), "git", []string{"fetch", "-q"})
	if err != nil || !reflect.DeepEqual(cmd.Args, []string{"git", "-c", "maintenance.auto=false", "fetch", "-q"}) {
		t.Fatalf("git argv = %q (%v)", cmd.Args, err)
	}
	cmd, err = command(context.Background(), "helm", []string{"history"})
	if err != nil || !reflect.DeepEqual(cmd.Args, []string{"helm", "history"}) {
		t.Fatalf("helm argv = %q (%v)", cmd.Args, err)
	}
}

func TestTheExitCodeAndStdinReachTheCaller(t *testing.T) {
	// an empty parent PATH: exec finds none of the names there, so only the
	// PATH of Env can supply the binary (checked by its path)
	t.Setenv("PATH", t.TempDir())
	bins := fakeBins(t, "helm")
	e := Exec{Env: withPATH(bins + ":/usr/sbin:/usr/bin:/sbin:/bin")}
	o, err := e.Run(context.Background(), Cmd{Argv: []string{"git", "no-such-subcommand-x"}})
	if err != nil || o.RC == 0 || len(o.Stderr) == 0 {
		t.Fatalf("want a non-zero rc and stderr, got %v rc %d", err, o.RC)
	}
	for _, in := range [][]byte{[]byte("in\n"), nil} {
		o, err = e.Run(context.Background(), Cmd{Argv: []string{"helm", "fake-cat"}, Stdin: in})
		if err != nil || o.RC != 0 || string(o.Stdout) != string(in) {
			t.Fatalf("stdin %q: %v rc %d out %q", in, err, o.RC, o.Stdout)
		}
		exe, argv := ran(t, o.Stderr)
		if exe != filepath.Join(bins, "helm") || argv != wantArgv("helm", "fake-cat") {
			t.Fatalf("ran %s with argv %s, want %s with %s", exe, argv, filepath.Join(bins, "helm"), wantArgv("helm", "fake-cat"))
		}
	}
	// a fake started with no fake-* word exits 3 and never runs the tests
	// (without the guard it would run this one harmless test and print PASS)
	o, err = e.Run(context.Background(), Cmd{Argv: []string{"helm", "-test.run=^TestCodesAreTheCLIContract$"}})
	if err != nil || o.RC != 3 || len(o.Stdout) != 0 {
		t.Fatalf("a fake with no fake-* word: %v rc %d out %q", err, o.RC, o.Stdout)
	}
}

// G4: umask 077 and cd / (the kit `umask 077`, `cd /`).
func TestHardenSetsUmaskAndRoot(t *testing.T) {
	t.Chdir(t.TempDir())
	old := syscall.Umask(0o022)
	defer syscall.Umask(old)
	if err := Harden(); err != nil {
		t.Fatal(err)
	}
	if got := syscall.Umask(0o022); got != 0o077 {
		t.Fatalf("umask = %#o, want 077", got)
	}
	if wd, _ := os.Getwd(); wd != "/" {
		t.Fatalf("cwd = %q, want /", wd)
	}
}

func TestCodesAreTheCLIContract(t *testing.T) {
	if CodeStop != cli.ExitFailure || CodeRefused != cli.ExitRefused {
		t.Fatal("run codes differ from the internal/cli exit contract")
	}
	if Code(nil) != 0 || Code(io.EOF) != CodeStop || Code(Refuse("x")) != CodeRefused || Code(Stop("x")) != CodeStop {
		t.Fatal("Code mapping")
	}
}
