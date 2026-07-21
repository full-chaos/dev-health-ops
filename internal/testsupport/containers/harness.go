// Package containers provides pinned, isolated service dependencies for Go
// integration tests. It never targets the developer's shared Compose project.
package containers

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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
		WaitingFor: wait.ForListeningPort(port).WithStartupTimeout(60 * time.Second),
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

func StartClickHouse(ctx context.Context) (*Instance, error) {
	const (
		user     = "worker_test"
		password = "worker_test_password"
		database = "worker_test"
		port     = "9000/tcp"
		httpPort = "8123/tcp"
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
		WaitingFor: wait.ForHTTP("/ping").WithPort(httpPort).WithStartupTimeout(90 * time.Second),
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
	return &Instance{Container: container, URI: uri.String()}, nil
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
	return &Instance{Container: container, URI: uri.String()}, nil
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
