package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The production binary, built from this tree and run with an explicit environment
// and nothing injected, refuses to start a worker process that holds a database DSN
// variable outside its class. The invalid --http-addr stops a process that is not
// refused during configuration, before any listener or connection, so a pass here
// is the refusal line and nothing else. The build must succeed or the test fails.
func TestDhoWorkerProcessesRefuseDatabaseDSNsOutsideTheirClass(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "dho")
	build := exec.Command("go", "build", "-o", binary, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build dho: %v\n%s", err, out)
	}
	commands := map[string]struct {
		args  []string
		env   []string
		class string
	}{
		"worker":        {[]string{"worker", "--queues=metrics", "--http-addr=not-a-port"}, []string{"DEV_HEALTH_QUEUE_CONCURRENCY=metrics=1"}, "river"},
		"reconciler":    {[]string{"reconciler", "--http-addr=not-a-port"}, nil, "control"},
		"scheduler":     {[]string{"scheduler", "--http-addr=not-a-port"}, nil, "control"},
		"stream-runner": {[]string{"stream-runner", "--http-addr=not-a-port"}, nil, "stream"},
	}
	run := func(t *testing.T, command string, extra string) (int, string) {
		t.Helper()
		spec := commands[command]
		cmd := exec.Command(binary, spec.args...)
		cmd.Env = append([]string{"PATH=" + os.Getenv("PATH")}, spec.env...)
		if extra != "" {
			cmd.Env = append(cmd.Env, extra)
		}
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		code := 0
		if err := cmd.Run(); err != nil {
			exitErr, ok := err.(*exec.ExitError)
			if !ok {
				t.Fatal(err)
			}
			code = exitErr.ExitCode()
		}
		return code, stdout.String() + stderr.String()
	}
	foreign := []struct{ command, variable, value string }{
		{"worker", "ANALYTICS_DATABASE_URI", "postgresql://foreign:secret-x@invalid.example/db"},
		{"worker", "COORDINATOR_DATABASE_URI", "postgresql://foreign:secret-x@invalid.example/db"},
		{"worker", "DATABASE_URL", "secret-x"},
		{"worker", "POSTGRES_URL", "secret-x"},
		{"worker", "APP_DATABASE_DSN", "secret-x"},
		{"worker", "SOME_SETTING", "postgres://foreign:secret-x@invalid.example/db"},
		{"worker", "SOME_SETTING", "postgresql+asyncpg://foreign:secret-x@invalid.example/db"},
		{"worker", "MY-DSN", "postgres://foreign:secret-x@invalid.example/db"},
		{"worker", "MY_PG_DSN", "secret-x"},
		{"reconciler", "API_DATABASE_URI", "postgresql://foreign:secret-x@invalid.example/db"},
		{"scheduler", "API_DATABASE_URI", "postgresql://foreign:secret-x@invalid.example/db"},
		{"stream-runner", "WORKER_DATABASE_URI", "postgresql://foreign:secret-x@invalid.example/db"},
		{"stream-runner", "SOME_SETTING", "PostgreSQL://foreign:secret-x@invalid.example/db"},
	}
	for _, row := range foreign {
		t.Run(row.command+"/"+row.variable, func(t *testing.T) {
			code, output := run(t, row.command, row.variable+"="+row.value)
			shown := row.variable
			if strings.ContainsAny(shown, "- ") {
				shown = "<invalid>" // a name that is not a plain identifier is never echoed
			}
			if code != 1 || !strings.Contains(output, "start refused") || !strings.Contains(output, shown) ||
				!strings.Contains(output, "process class "+commands[row.command].class) {
				t.Fatalf("exit code %d, output %q: want exit 1 and a refusal naming %s and the class", code, output, shown)
			}
			if strings.Contains(output, "secret-x") {
				t.Fatalf("the refusal carries a value: %q", output)
			}
		})
	}
	passing := []struct{ command, variable, value string }{
		{"worker", "POSTGRES_URI", "postgres://u:secret-x@invalid.example/db"},
		{"worker", "WORKER_DATABASE_URI", "postgresql://u:secret-x@invalid.example/db"},
		{"worker", "COORDINATOR_DATABASE_URI", ""},
		{"reconciler", "COORDINATOR_DATABASE_URI", "postgresql://u:secret-x@invalid.example/db"},
		{"stream-runner", "DATABASE_URL", ""},
		{"worker", "UPGRADE_URL", "https://example.test"},
	}
	for _, row := range passing {
		t.Run("allowed/"+row.command+"/"+row.variable, func(t *testing.T) {
			code, output := run(t, row.command, row.variable+"="+row.value)
			if strings.Contains(output, "start refused") {
				t.Fatalf("a class-allowed or empty variable was refused: %q", output)
			}
			if code != 1 || !strings.Contains(output, "configuration_error") {
				t.Fatalf("exit code %d, output %q: want the configuration error that stops the run", code, output)
			}
		})
	}
}
