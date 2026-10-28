//go:build integration

package devhealth_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The transaction pooler RENDERED with the exporter on: pgbouncer on its pinned image and the exporter on its pinned image,
// with the rendered env, args and supervisor command, sharing the console socket directory (a volume owned by uid 70, as the
// pod's fsGroup makes the emptyDir). The exporter logs in with NO password (console user pgbouncer, same uid); 24 clients on
// the 16-connection pool must show cl_waiting > 0 for the pooler's own role, then 0; a killed exporter binary is restarted by
// the supervisor while the container keeps running.
func TestRenderedTransactionPoolerExporterReportsClWaiting(t *testing.T) {
	docs, out, err := renderAuth(t, authWith(exporterOn...)...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	deployment := authDoc(docs, "Deployment", transactionPooler)
	cm := authDoc(docs, "ConfigMap", usersConfigMap)
	if deployment == nil || cm == nil {
		t.Fatal("the transaction pooler and its users ConfigMap must render")
	}
	pod := at(deployment, "spec", "template", "spec")
	pgb, exp := containerNamed(pod, "pgbouncer"), containerNamed(pod, "exporter")
	if pgb == nil || exp == nil {
		t.Fatal("the transaction pod must carry pgbouncer and exporter")
	}
	strs := func(v any) []string {
		var s []string
		for _, x := range v.([]any) {
			s = append(s, x.(string))
		}
		return s
	}
	pgbImage, _ := at(pgb, "image").(string)
	pgbArgs := strs(at(pgb, "args"))
	expImage, _ := at(exp, "image").(string)
	expCmd, expArgs := strs(at(exp, "command")), strs(at(exp, "args"))
	usersIni, _ := at(cm, "data", "pgbouncer.ini").(string)

	const (
		own    = "devhealth_domain"
		keda   = "devhealth_keda_readonly"
		pw     = "test-only-pw-1"
		kedaPw = "test-only-pw-2"
		db     = "devhealth"
		sock   = "/var/run/pgbouncer"
	)
	env := map[string]string{}
	for _, e := range at(pgb, "env").([]any) {
		if v, ok := at(e, "value").(string); ok {
			env[at(e, "name").(string)] = v
		}
	}
	if env["UNIX_SOCKET_DIR"] != sock {
		t.Fatalf("rendered UNIX_SOCKET_DIR = %q", env["UNIX_SOCKET_DIR"])
	}
	env["DB_HOST"], env["DB_PORT"], env["DB_NAME"], env["DB_USER"], env["DB_PASSWORD"] = "pg", "5432", db, own, pw
	env["AUTH_FILE"] = "/etc/pgbouncer-userlist/userlist.txt"

	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })
	pg, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: containers.PostgresImage,
		Env: map[string]string{"POSTGRES_USER": own, "POSTGRES_PASSWORD": pw, "POSTGRES_DB": db,
			"POSTGRES_INITDB_ARGS": "--auth-host=scram-sha-256"},
		Networks: []string{nw.Name}, NetworkAliases: map[string][]string{nw.Name: {"pg"}}, ExposedPorts: []string{"5432/tcp"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90 * time.Second),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Terminate(context.Background()) })

	vol := fmt.Sprintf("pgb-console-%d", time.Now().UnixNano())
	mount := testcontainers.ContainerMounts{testcontainers.VolumeMount(vol, sock)}
	// The pod's fsGroup 70 owns the emptyDir; a fresh docker volume is root-owned, so one root step hands it to uid 70.
	own70, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: pgbImage, User: "0", Entrypoint: []string{"sh", "-c", "chown 70:70 " + sock + " && chmod 0770 " + sock},
		Mounts: mount, WaitingFor: wait.ForExit().WithExitTimeout(60 * time.Second),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := own70.Terminate(ctx); err != nil {
		t.Fatal(err)
	}

	bouncer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: pgbImage, Env: env, Cmd: pgbArgs, User: "70:70", Mounts: mount,
		Files: []testcontainers.ContainerFile{
			{Reader: strings.NewReader(fmt.Sprintf("\"%s\" \"%s\"\n\"%s\" \"%s\"\n", own, pw, keda, kedaPw)), ContainerFilePath: "/etc/pgbouncer-userlist/userlist.txt", FileMode: 0o644},
			{Reader: strings.NewReader(usersIni), ContainerFilePath: "/etc/pgbouncer-users/pgbouncer.ini", FileMode: 0o644},
		},
		ExposedPorts: []string{"6432/tcp"}, Networks: []string{nw.Name},
		WaitingFor: wait.ForListeningPort("6432/tcp").WithStartupTimeout(60 * time.Second),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(bouncer, testcontainers.RemoveVolumes(vol)) })

	exporter, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: expImage, Entrypoint: expCmd, Cmd: expArgs, User: "70:70", Mounts: mount,
		ExposedPorts: []string{"9127/tcp"},
		WaitingFor:   wait.ForHTTP("/metrics").WithPort("9127/tcp").WithStartupTimeout(60 * time.Second),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = exporter.Terminate(context.Background()) })

	host, _ := bouncer.Host(ctx)
	port, _ := bouncer.MappedPort(ctx, "6432/tcp")
	addr := fmt.Sprintf("%s:%s", host, port.Port())
	ehost, _ := exporter.Host(ctx)
	eport, _ := exporter.MappedPort(ctx, "9127/tcp")
	metricsURL := fmt.Sprintf("http://%s:%s/metrics", ehost, eport.Port())
	// sample reads one gauge: the value of the first line named `name` whose labels contain every `match`; ok=false if absent.
	sample := func(name string, match ...string) (float64, bool) {
		resp, err := http.Get(metricsURL)
		if err != nil {
			return 0, false
		}
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := sc.Text()
			if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
				continue
			}
			hit := true
			for _, m := range match {
				hit = hit && strings.Contains(line, m)
			}
			if hit {
				f := strings.Fields(line)
				v, err := strconv.ParseFloat(f[len(f)-1], 64)
				return v, err == nil
			}
		}
		return 0, false
	}
	waitFor := func(what string, d time.Duration, cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
			if cond() {
				return
			}
		}
		t.Fatalf("timed out: %s", what)
	}
	waitFor("pgbouncer_up 1 (console login over the socket, no password)", 20*time.Second, func() bool {
		v, ok := sample("pgbouncer_up")
		return ok && v == 1
	})
	waiting := func() float64 {
		v, _ := sample("pgbouncer_pools_client_waiting_connections", `user="`+own+`"`, `database="`+db+`"`)
		return v
	}
	if w := waiting(); w != 0 {
		t.Fatalf("idle pool: cl_waiting = %v, want 0", w)
	}

	hold, cancel := context.WithTimeout(ctx, 25*time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := pgx.Connect(hold, fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", own, pw, addr, db))
			if err != nil {
				return
			}
			defer conn.Close(context.Background())
			_, _ = conn.Exec(hold, "select pg_sleep(20)")
		}()
	}
	waitFor("cl_waiting > 0 with 24 clients on the 16-connection pool", 15*time.Second, func() bool { return waiting() > 0 })
	cancel()
	wg.Wait()
	waitFor("cl_waiting back to 0", 30*time.Second, func() bool { return waiting() == 0 })

	before, err := exporter.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	code, _, err := exporter.Exec(ctx, []string{"sh", "-c", `for p in $(pidof pgbouncer_exporter); do [ "$p" = 1 ] || kill -TERM "$p"; done`})
	if err != nil || code != 0 {
		t.Fatalf("kill the exporter binary: code=%d err=%v", code, err)
	}
	waitFor("the supervisor logs the exit", 10*time.Second, func() bool {
		logs, err := exporter.Logs(ctx)
		if err != nil {
			return false
		}
		raw, _ := io.ReadAll(logs)
		return strings.Contains(string(raw), "exporter exited rc=")
	})
	waitFor("the supervisor restarts the binary (pgbouncer_up 1 again)", 30*time.Second, func() bool {
		v, ok := sample("pgbouncer_up")
		return ok && v == 1
	})
	after, err := exporter.State(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !after.Running || after.Pid != before.Pid {
		t.Fatalf("the exporter container must keep running with the same PID 1 (supervisor): before pid=%d, after running=%v pid=%d", before.Pid, after.Running, after.Pid)
	}
}
