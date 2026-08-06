// Package containers provides pinned, isolated service dependencies for Go
// integration tests. It never targets the developer's shared Compose project.
package containers

import (
	"context"
	"fmt"
	"net/url"
	"time"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	valkeygo "github.com/valkey-io/valkey-go"
)

const (
	PostgresImage   = "postgres:18-alpine@sha256:9a8afca54e7861fd90fab5fdf4c42477a6b1cb7d293595148e674e0a3181de15"
	ClickHouseImage = "clickhouse/clickhouse-server@sha256:1d1f6508eba2dccce2cee9913907c5f7766327debc57a6b1991f2c9e3176c163"
	ValkeyImage     = "valkey/valkey@sha256:c9b77919daeba2c02ad954d0c844cc4e7142069d177b89c5fd771f405daf9e02"
)

type Instance struct {
	Container testcontainers.Container
	URI       string
}

func (i *Instance) Close(ctx context.Context) error {
	if i == nil || i.Container == nil {
		return nil
	}
	return i.Container.Terminate(ctx)
}

func StartPostgres(ctx context.Context) (*Instance, error) {
	const (
		user     = "worker_test"
		password = "worker_test_password"
		database = "worker_test"
		port     = "5432/tcp"
	)
	container, host, mappedPort, err := start(ctx, testcontainers.ContainerRequest{
		Image:        PostgresImage,
		ExposedPorts: []string{port},
		Env: map[string]string{
			"POSTGRES_USER":     user,
			"POSTGRES_PASSWORD": password,
			"POSTGRES_DB":       database,
		},
		// The official image briefly accepts connections during initdb before
		// restarting PostgreSQL. Waiting for the second ready log prevents a
		// listening-port race against that restart.
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}, port)
	if err != nil {
		return nil, fmt.Errorf("start PostgreSQL test dependency: %w", err)
	}
	uri := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, password),
		Host:     host + ":" + mappedPort,
		Path:     database,
		RawQuery: "sslmode=disable",
	}
	return &Instance{Container: container, URI: uri.String()}, nil
}

// ClickHouse test credentials, exported so a helper that must address the
// container over a DIFFERENT protocol than Instance.URI can build its own DSN
// instead of re-declaring these and drifting from them.
const (
	ClickHouseUser     = "worker_test"
	ClickHousePassword = "worker_test_password"
	ClickHouseDatabase = "worker_test"
	// ClickHouseHTTPPort is exposed alongside the native port. Instance.URI
	// addresses the NATIVE protocol, which the Go driver speaks; the Python
	// migration runner is an HTTP client and needs this one.
	ClickHouseHTTPPort = "8123/tcp"
)

// ClickHouseHTTPDSN returns a clickhouse:// DSN addressed at the container's
// mapped HTTP port. The scheme must stay clickhouse:// -- the Python sink
// factory rejects http:// outright ("Only ClickHouse is supported", CHAOS-641)
// -- while the PORT must be the HTTP one, because that client speaks HTTP and
// not the native protocol Instance.URI points at.
func ClickHouseHTTPDSN(ctx context.Context, instance *Instance) (string, error) {
	if instance == nil || instance.Container == nil {
		return "", fmt.Errorf("clickhouse HTTP DSN: no container")
	}
	host, err := instance.Container.Host(ctx)
	if err != nil {
		return "", fmt.Errorf("clickhouse HTTP DSN host: %w", err)
	}
	mapped, err := instance.Container.MappedPort(ctx, ClickHouseHTTPPort)
	if err != nil {
		return "", fmt.Errorf("clickhouse HTTP DSN port: %w", err)
	}
	uri := url.URL{
		Scheme: "clickhouse",
		User:   url.UserPassword(ClickHouseUser, ClickHousePassword),
		Host:   host + ":" + mapped.Port(),
		Path:   ClickHouseDatabase,
	}
	return uri.String(), nil
}

func StartClickHouse(ctx context.Context) (*Instance, error) {
	const (
		user     = ClickHouseUser
		password = ClickHousePassword
		database = ClickHouseDatabase
		port     = "9000/tcp"
		httpPort = ClickHouseHTTPPort
	)
	container, host, mappedPort, err := start(ctx, testcontainers.ContainerRequest{
		Image:        ClickHouseImage,
		ExposedPorts: []string{port, httpPort},
		Env: map[string]string{
			"CLICKHOUSE_USER":                      user,
			"CLICKHOUSE_PASSWORD":                  password,
			"CLICKHOUSE_DB":                        database,
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
		},
		// The HTTP ping only proves port 8123 is serving; every caller of
		// this harness dials the native protocol on 9000. Under heavy host
		// contention the two ports' Docker NAT rules do not land atomically,
		// so 9000 can still be mid-setup for a few hundred ms after 8123
		// answers. Reproduced directly: running this harness's callers
		// concurrently with the rest of the integration gate (matching
		// ci/check_go.sh's package list) surfaced
		// `container.MappedPort(ctx, "9000/tcp")` failing with `port
		// "9000/tcp" not found` even though the container was otherwise
		// healthy and every prior/later run of the same test was green.
		// Waiting on both ports closes that gap.
		WaitingFor: wait.ForAll(
			wait.ForHTTP("/ping").WithPort(httpPort).WithStartupTimeout(90*time.Second),
			wait.ForListeningPort(port).WithStartupTimeout(90*time.Second),
		),
	}, port)
	if err != nil {
		return nil, fmt.Errorf("start ClickHouse test dependency: %w", err)
	}
	uri := url.URL{
		Scheme: "clickhouse",
		User:   url.UserPassword(user, password),
		Host:   host + ":" + mappedPort,
		Path:   database,
	}
	instance := &Instance{Container: container, URI: uri.String()}

	// wait.ForListeningPort(port) above proves only that 9000/tcp completed
	// a TCP handshake, not that the native-protocol handler behind it is
	// actually answering queries -- the same class of gap fixed for Valkey
	// below. The HTTP /ping check makes this a much smaller risk in
	// practice (ClickHouse's HTTP interface is very unlikely to answer
	// before its native handler is also live), but it is still an
	// indirect signal for a port every real caller's single-shot
	// clickhouse.Open->Ping (see internal/storage/clickhouse/factory.go)
	// depends on directly. Prove it directly instead of by inference, with
	// the same order-of-magnitude retry budget as the wait above.
	if err := waitForClickHouseCommandReady(ctx, instance.URI, 90*time.Second); err != nil {
		_ = instance.Close(ctx)
		return nil, fmt.Errorf("wait for ClickHouse command readiness: %w", err)
	}
	return instance, nil
}

// waitForClickHouseCommandReady polls uri with real Ping calls against the
// native protocol port until one succeeds or timeout elapses.
func waitForClickHouseCommandReady(ctx context.Context, uri string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		lastErr = pingClickHouseOnce(attemptCtx, uri)
		cancel()
		if lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("clickhouse did not answer Ping within %s: %w", timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// pingClickHouseOnce opens a short-lived native-protocol connection against
// uri and issues a single Ping. Like pingValkeyOnce, it intentionally does
// not reuse clickhousestore.Open/Config: production tuning there is meant
// for an already-healthy server, not a just-started test container.
func pingClickHouseOnce(ctx context.Context, uri string) error {
	options, err := clickhousego.ParseDSN(uri)
	if err != nil {
		return err
	}
	options.DialTimeout = 2 * time.Second
	conn, err := clickhousego.Open(options)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Ping(ctx)
}

func StartValkey(ctx context.Context) (*Instance, error) {
	const port = "6379/tcp"
	container, host, mappedPort, err := start(ctx, testcontainers.ContainerRequest{
		Image:        ValkeyImage,
		ExposedPorts: []string{port},
		WaitingFor:   wait.ForListeningPort(port).WithStartupTimeout(60 * time.Second),
	}, port)
	if err != nil {
		return nil, fmt.Errorf("start Valkey test dependency: %w", err)
	}
	uri := url.URL{
		Scheme: "redis",
		Host:   host + ":" + mappedPort,
		Path:   "1",
	}
	instance := &Instance{Container: container, URI: uri.String()}

	// wait.ForListeningPort only proves the kernel completed a TCP
	// handshake on the mapped port -- it says nothing about whether the
	// single-threaded Valkey event loop has actually been scheduled long
	// enough to answer a command. A listen() backlog can accept a
	// connection before (or between) the process getting CPU time,
	// especially once this harness's callers are competing with every
	// other integration package's own containers for a runner's CPU quota
	// (23 packages now discovered by ci/check_go.sh's integration job on a
	// 4-vCPU GitHub-hosted runner; see the ClickHouse dual-port wait above
	// for the same family of race, and CHAOS-3133 for pgxpool.MaxConns
	// defaulting to runtime.NumCPU() -- both "worked on every dev machine,
	// starved on a 4-vCPU runner"). The first application-level command
	// against a freshly-started Valkey is the PING inside
	// valkeystore.Open, which uses a 5s DialTimeout -- a *production*
	// tuning for an already-warm server a few milliseconds away, not for a
	// container whose TCP handshake just completed under load. Closing
	// that gap by loosening the production timeout would weaken a value
	// every real caller relies on; closing it here instead, with a real
	// PING loop bounded by the same order-of-magnitude startup budget the
	// Postgres (60s) and ClickHouse (90s) harnesses above already use,
	// keeps the assertion honest: it still must observe a genuine,
	// successful PING, so it cannot paper over a Valkey that is actually
	// broken.
	if err := waitForValkeyCommandReady(ctx, instance.URI, 60*time.Second); err != nil {
		_ = instance.Close(ctx)
		return nil, fmt.Errorf("wait for Valkey command readiness: %w", err)
	}
	return instance, nil
}

// waitForValkeyCommandReady polls uri with real PING commands until one
// succeeds or timeout elapses, retrying at a fixed short interval so the
// deadline is the caller's real budget rather than a single dial attempt's.
func waitForValkeyCommandReady(ctx context.Context, uri string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		lastErr = pingValkeyOnce(attemptCtx, uri)
		cancel()
		if lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("valkey did not answer PING within %s: %w", timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// pingValkeyOnce opens a short-lived client against uri and issues a single
// PING. It intentionally does not reuse valkeystore.Open/Config: that
// package's DefaultConfig is tuned for production traffic against an
// already-healthy server, and this harness needs its own, longer-lived
// readiness budget instead of inheriting that tuning.
func pingValkeyOnce(ctx context.Context, uri string) error {
	options, err := valkeygo.ParseURL(uri)
	if err != nil {
		return err
	}
	options.Dialer.Timeout = 2 * time.Second
	options.ClientSetInfo = valkeygo.DisableClientSetInfo
	client, err := valkeygo.NewClient(options)
	if err != nil {
		return err
	}
	defer client.Close()
	return client.Do(ctx, client.B().Ping().Build()).Error()
}

func start(
	ctx context.Context,
	request testcontainers.ContainerRequest,
	containerPort string,
) (testcontainers.Container, string, string, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: request,
		Started:          true,
	})
	if err != nil {
		return nil, "", "", err
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, "", "", err
	}
	mappedPort, err := container.MappedPort(ctx, containerPort)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, "", "", err
	}
	return container, host, mappedPort.Port(), nil
}
