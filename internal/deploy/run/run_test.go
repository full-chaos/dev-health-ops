package run

import (
	"context"
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
	if r := Real(); !reflect.DeepEqual(r.Env, kitChildEnv) || !reflect.DeepEqual(Tools, []string{"git", "helm", "kubectl"}) {
		t.Fatalf("Real() = %+v, Tools = %q", r, Tools)
	}
}

// fakeBins puts copies of real system binaries under the closed-set names in
// a temp dir (no shell script): name -> the binary to copy.
func fakeBins(t *testing.T, bins map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, src := range bins {
		b, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// withPATH is the fixed env with its PATH replaced.
func withPATH(path string) []string {
	env := FixedEnv()
	env[0] = "PATH=" + path
	return env
}

// G4: a planted var, GIT_DIR and a planted binary on the parent PATH never
// reach the child; the child env is exactly Env and its cwd is "/". The
// closed-set names run copies of env (as "git") and pwd (as "helm") from the
// PATH of Env.
func TestTheChildGetsOnlyItsEnvAndRunsInRoot(t *testing.T) {
	onEnv := fakeBins(t, map[string]string{"git": "/usr/bin/env", "helm": "/bin/pwd"})
	onParent := fakeBins(t, map[string]string{"git": "/bin/pwd", "helm": "/usr/bin/env"})
	t.Setenv("EVILVAR", "planted")
	t.Setenv("PATH", onParent+":"+os.Getenv("PATH"))
	t.Setenv("GIT_DIR", "/planted")
	e := Exec{Env: withPATH(onEnv + ":/usr/sbin:/usr/bin:/sbin:/bin")}
	o, err := e.Run(context.Background(), Cmd{Argv: []string{"git"}})
	if err != nil || o.RC != 0 {
		t.Fatalf("env as git: %v rc %d", err, o.RC)
	}
	if got := strings.Split(strings.TrimSuffix(string(o.Stdout), "\n"), "\n"); !reflect.DeepEqual(got, e.Env) {
		t.Fatalf("child env = %q, want exactly %q", got, e.Env)
	}
	o, err = e.Run(context.Background(), Cmd{Argv: []string{"helm"}})
	if err != nil || o.RC != 0 || string(o.Stdout) != "/\n" {
		t.Fatalf("child cwd = %q (%v rc %d), want /", o.Stdout, err, o.RC)
	}
	if o, err := Real().Run(context.Background(), Cmd{Argv: []string{"git", "version"}}); err != nil || o.RC != 0 || !strings.HasPrefix(string(o.Stdout), "git version ") {
		t.Fatalf("the real Runner runs git from the fixed PATH: %v rc %d %q", err, o.RC, o.Stdout)
	}
}

// acp (Semgrep dangerous-exec-command): only git, helm and kubectl start, each
// by a literal name; any other argv[0] is refused with fixed text and nothing
// runs, even when a binary of that name is on the PATH of Env.
func TestOnlyTheClosedBinarySetStarts(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	bins := fakeBins(t, map[string]string{"touch": "/usr/bin/touch", "sh": "/usr/bin/touch", "env": "/usr/bin/touch", "curl": "/usr/bin/touch"})
	e := Exec{Env: withPATH(bins + ":/usr/sbin:/usr/bin:/sbin:/bin")}
	for name, argv := range map[string][]string{
		"touch on the PATH": {"touch", marker},
		"sh -c":             {"sh", "-c", marker},
		"env":               {"env", marker},
		"curl":              {"curl", marker},
		"bash -c":           {"bash", "-c", "echo x"},
		"absolute git":      {"/usr/bin/git", "version"},
		"relative path":     {"./git", "version"},
		"a name with a NUL": {"git\x00", "version"},
		"empty name":        {""},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := e.Run(context.Background(), Cmd{Argv: argv})
			if Code(err) != CodeRefused || err.Error() != "exec: argv[0] is not one of git, helm, kubectl" {
				t.Fatalf("want the fixed refusal, got %v", err)
			}
			if _, err := os.Stat(marker); err == nil {
				t.Fatal("a refused binary ran")
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

func TestTheExitCodeAndStdinReachTheCaller(t *testing.T) {
	// an empty parent PATH: exec finds none of the names there, so only the
	// PATH of Env can supply the binary
	t.Setenv("PATH", t.TempDir())
	bins := fakeBins(t, map[string]string{"kubectl": "/bin/cat"})
	e := Exec{Env: withPATH(bins + ":/usr/sbin:/usr/bin:/sbin:/bin")}
	o, err := e.Run(context.Background(), Cmd{Argv: []string{"git", "no-such-subcommand-x"}})
	if err != nil || o.RC == 0 || len(o.Stderr) == 0 {
		t.Fatalf("want a non-zero rc and stderr, got %v rc %d", err, o.RC)
	}
	o, err = e.Run(context.Background(), Cmd{Argv: []string{"kubectl"}, Stdin: []byte("in\n")})
	if err != nil || o.RC != 0 || string(o.Stdout) != "in\n" {
		t.Fatalf("stdin: %v rc %d out %q", err, o.RC, o.Stdout)
	}
	o, err = e.Run(context.Background(), Cmd{Argv: []string{"kubectl"}})
	if err != nil || o.RC != 0 || len(o.Stdout) != 0 {
		t.Fatalf("nil stdin must be empty: %v rc %d out %q", err, o.RC, o.Stdout)
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
