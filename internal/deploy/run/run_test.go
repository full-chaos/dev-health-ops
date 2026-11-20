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
	r := Real()
	if !reflect.DeepEqual(r.Env, kitChildEnv) || !reflect.DeepEqual(r.Tools, []string{"git", "helm", "kubectl"}) {
		t.Fatalf("Real() = %+v", r)
	}
}

// G4: a planted var and a planted PATH in the parent never reach the child;
// the child env is exactly the fixed list and its cwd is "/".
func TestTheChildGetsOnlyTheFixedEnvAndRunsInRoot(t *testing.T) {
	plant := t.TempDir()
	pwd, err := os.ReadFile("/bin/pwd")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(plant, "env"), pwd, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EVILVAR", "planted")
	t.Setenv("PATH", plant+":"+os.Getenv("PATH"))
	t.Setenv("GIT_DIR", "/planted")
	e := Exec{Env: FixedEnv(), Tools: []string{"env", "pwd"}}
	o, err := e.Run(context.Background(), Cmd{Argv: []string{"env"}})
	if err != nil || o.RC != 0 {
		t.Fatalf("env: %v rc %d", err, o.RC)
	}
	if got := strings.Split(strings.TrimSuffix(string(o.Stdout), "\n"), "\n"); !reflect.DeepEqual(got, kitChildEnv) {
		t.Fatalf("child env = %q, want exactly %q", got, kitChildEnv)
	}
	o, err = e.Run(context.Background(), Cmd{Argv: []string{"pwd"}})
	if err != nil || o.RC != 0 || string(o.Stdout) != "/\n" {
		t.Fatalf("child cwd = %q (%v rc %d), want /", o.Stdout, err, o.RC)
	}
}

func TestOnlyAllowListedBareToolsStart(t *testing.T) {
	for name, argv := range map[string][]string{
		"sh -c":         {"sh", "-c", "echo x"},
		"bash -c":       {"bash", "-c", "echo x"},
		"env -i sh":     {"env", "-i", "sh"},
		"absolute git":  {"/usr/bin/git", "version"},
		"relative path": {"./git", "version"},
		"empty argv":    {},
		"empty name":    {""},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Real().Run(context.Background(), Cmd{Argv: argv})
			if Code(err) != CodeRefused {
				t.Fatalf("want refused, got %v", err)
			}
		})
	}
	_, err := Exec{Env: []string{"PATH=relative/bin"}, Tools: []string{"git"}}.Run(context.Background(), Cmd{Argv: []string{"git", "version"}})
	if Code(err) != CodeStop {
		t.Fatalf("a tool off the fixed PATH must stop, got %v", err)
	}
}

func TestTheExitCodeAndStdinReachTheCaller(t *testing.T) {
	e := Exec{Env: FixedEnv(), Tools: []string{"git", "cat"}}
	o, err := e.Run(context.Background(), Cmd{Argv: []string{"git", "no-such-subcommand-x"}})
	if err != nil || o.RC == 0 || len(o.Stderr) == 0 {
		t.Fatalf("want a non-zero rc and stderr, got %v rc %d", err, o.RC)
	}
	o, err = e.Run(context.Background(), Cmd{Argv: []string{"cat"}, Stdin: []byte("in\n")})
	if err != nil || o.RC != 0 || string(o.Stdout) != "in\n" {
		t.Fatalf("stdin: %v rc %d out %q", err, o.RC, o.Stdout)
	}
	o, err = e.Run(context.Background(), Cmd{Argv: []string{"cat"}})
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
