//go:build integration

package devhealth_test

import (
	"context"
	"fmt"
	"io"
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

// The transaction pooler RENDERED by helm is started on its pinned image with the
// rendered [users] file and args. The KEDA login holds ten clients; the pooler must
// open at most its own pool size (4) of server connections for it, while the
// pooler's own role still gets the full transaction pool (16), and the config the
// image generates is the same as without the users file.
func TestRenderedTransactionPoolerCapsTheKedaLoginAtItsPoolSize(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render: %v\n%.400s", err, out)
	}
	deployment := authDoc(docs, "Deployment", transactionPooler)
	cm := authDoc(docs, "ConfigMap", usersConfigMap)
	if deployment == nil || cm == nil {
		t.Fatal("the transaction pooler and its users ConfigMap must render")
	}
	container := at(deployment, "spec", "template", "spec", "containers", 0)
	image, _ := at(container, "image").(string)
	var args []string
	for _, a := range at(container, "args").([]any) {
		args = append(args, a.(string))
	}
	usersIni, _ := at(cm, "data", "pgbouncer.ini").(string)

	const (
		own    = "devhealth_domain"
		keda   = "devhealth_keda_readonly"
		pw     = "test-only-pw-1"
		kedaPw = "test-only-pw-2"
		db     = "devhealth"
	)
	renderedEnv := map[string]string{}
	for _, e := range at(container, "env").([]any) {
		if v, ok := at(e, "value").(string); ok {
			renderedEnv[at(e, "name").(string)] = v
		}
	}
	if renderedEnv["DEFAULT_POOL_SIZE"] != "16" {
		t.Fatalf("rendered DEFAULT_POOL_SIZE = %q, want 16", renderedEnv["DEFAULT_POOL_SIZE"])
	}

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
	if code, _, err := pg.Exec(ctx, []string{"psql", "-U", own, "-d", db, "-c",
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'; GRANT CONNECT ON DATABASE %s TO %s;", keda, kedaPw, db, keda)}); err != nil || code != 0 {
		t.Fatalf("create the KEDA role: code=%d err=%v", code, err)
	}

	type observed struct {
		ini      string
		keda, me int
	}
	run := func(wrapped bool) observed {
		env := map[string]string{}
		for k, v := range renderedEnv {
			env[k] = v
		}
		env["DB_HOST"], env["DB_PORT"], env["DB_NAME"], env["DB_USER"], env["DB_PASSWORD"] = "pg", "5432", db, own, pw
		env["AUTH_FILE"] = "/etc/pgbouncer-userlist/userlist.txt"
		env["ADMIN_USERS"] = own
		files := []testcontainers.ContainerFile{{
			Reader:            strings.NewReader(fmt.Sprintf("\"%s\" \"%s\"\n\"%s\" \"%s\"\n", own, pw, keda, kedaPw)),
			ContainerFilePath: "/etc/pgbouncer-userlist/userlist.txt", FileMode: 0o644}}
		var cmd []string
		if wrapped {
			cmd = args
			files = append(files, testcontainers.ContainerFile{Reader: strings.NewReader(usersIni), ContainerFilePath: "/etc/pgbouncer-users/pgbouncer.ini", FileMode: 0o644})
		}
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
			Image: image, Env: env, Cmd: cmd, Files: files, ExposedPorts: []string{"6432/tcp"}, Networks: []string{nw.Name},
			WaitingFor: wait.ForListeningPort("6432/tcp").WithStartupTimeout(60 * time.Second),
		}, Started: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Terminate(context.Background()) })
		host, _ := c.Host(ctx)
		port, _ := c.MappedPort(ctx, "6432/tcp")
		addr := fmt.Sprintf("%s:%s", host, port.Port())
		logs, _ := c.Logs(ctx)
		raw, _ := io.ReadAll(logs)
		ini, found := generatedPgbouncerIni(string(raw))
		if !found {
			t.Fatal("the image did not print its generated ini")
		}

		hold, cancel := context.WithTimeout(ctx, 25*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		clients := func(user, password string, n int) {
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					conn, err := pgx.Connect(hold, fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, password, addr, db))
					if err != nil {
						return
					}
					defer conn.Close(context.Background())
					_, _ = conn.Exec(hold, "select pg_sleep(20)")
				}()
			}
		}
		clients(keda, kedaPw, 10)
		clients(own, pw, 20)
		admin, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s/pgbouncer?sslmode=disable&default_query_exec_mode=simple_protocol", own, pw, addr))
		if err != nil {
			t.Fatalf("admin console: %v", err)
		}
		defer admin.Close(context.Background())
		var got observed
		got.ini = ini
		for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
			rows, err := admin.Query(ctx, "SHOW POOLS")
			if err != nil {
				t.Fatal(err)
			}
			fields := rows.FieldDescriptions()
			got.keda, got.me = 0, 0
			clientsSeen := map[string]int{}
			for rows.Next() {
				values, _ := rows.Values()
				var user string
				var active, waiting, server int64
				for i, f := range fields {
					switch f.Name {
					case "user":
						user = fmt.Sprint(values[i])
					case "cl_active":
						active = number(values[i])
					case "cl_waiting":
						waiting = number(values[i])
					case "sv_active":
						server = number(values[i])
					}
				}
				clientsSeen[user] = int(active + waiting)
				switch user {
				case keda:
					got.keda = int(server)
				case own:
					got.me = int(server)
				}
			}
			rows.Close()
			if clientsSeen[keda] == 10 && clientsSeen[own] == 20 && got.me == 16 && got.keda > 0 {
				break
			}
		}
		cancel()
		wg.Wait()
		return got
	}

	base := run(false)
	capped := run(true)
	if base.ini != capped.ini {
		t.Errorf("the users file changed the generated pgbouncer.ini:\n--- without\n%s\n--- with\n%s", base.ini, capped.ini)
	}
	if base.keda <= 4 {
		t.Errorf("control: without the users file the KEDA login reaches %d server connections, want more than 4", base.keda)
	}
	if capped.keda != 4 {
		t.Errorf("with the users file the KEDA login holds %d server connections, want exactly 4", capped.keda)
	}
	if capped.me != 16 {
		t.Errorf("the pooler's own role holds %d server connections, want 16", capped.me)
	}
}

// number reads a SHOW POOLS cell: the admin console answers in text.
func number(v any) int64 {
	var n int64
	_, _ = fmt.Sscan(fmt.Sprint(v), &n)
	return n
}
