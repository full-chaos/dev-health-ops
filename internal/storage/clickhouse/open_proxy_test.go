package clickhouse

import (
	"context"
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/proxycapture"
)

func runCaptureChild(args, env []string) ([]byte, error) {
	command := exec.Command(os.Args[0], args...)
	command.Env = env
	return command.CombinedOutput()
}

func TestOpenNeverDialsTheEnvironmentOrDSNProxy(t *testing.T) {
	proxycapture.Check(t, runCaptureChild, func(ctx context.Context) {
		dsn := "http://ch-user:" + proxycapture.Canary + "@ch.invalid:8123/default?http_proxy=" + url.QueryEscape(os.Getenv("HTTP_PROXY"))
		config := DefaultConfig(dsn)
		config.DialTimeout, config.ReadTimeout = time.Second, time.Second
		connection, err := Open(ctx, config)
		if err == nil {
			_ = connection.Close()
			panic("Open succeeded against an unresolvable host: the probe made no request")
		}
	}).AssertNothingReached(t)
}
