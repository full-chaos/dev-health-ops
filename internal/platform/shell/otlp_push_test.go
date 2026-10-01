package shell

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
)

type shellCollector struct {
	colmetricpb.UnimplementedMetricsServiceServer
	mu    sync.Mutex
	seen  map[string]string // metric name -> service.name of the resource
	count int
}

func (c *shellCollector) Export(_ context.Context, req *colmetricpb.ExportMetricsServiceRequest) (*colmetricpb.ExportMetricsServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count++
	for _, resource := range req.GetResourceMetrics() {
		service := ""
		for _, kv := range resource.GetResource().GetAttributes() {
			if kv.GetKey() == "service.name" {
				service = kv.GetValue().GetStringValue()
			}
		}
		for _, scope := range resource.GetScopeMetrics() {
			for _, metric := range scope.GetMetrics() {
				c.seen[metric.GetName()] = service
			}
		}
	}
	return &colmetricpb.ExportMetricsServiceResponse{}, nil
}

var _ = metricpb.Metric{}

// TestShellPushesItsRegisteredFragmentsOverOTLP runs a real shell process (a
// re-execution of this test binary: the MeterProvider is installed once per
// process, so the push cannot be proven in-process after other tests ran) with
// the OTLP env of the deployed pod, against a real collector. The runtime
// gauges the operator server writes as a hand-written fragment must arrive
// under the service name the env gives, and the pull endpoint must still serve
// them.
func TestShellPushesItsRegisteredFragmentsOverOTLP(t *testing.T) {
	if os.Getenv("SHELL_OTLP_HELPER") == "1" {
		runShellHelper()
		return
	}
	collector := &shellCollector{seen: map[string]string{}}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	colmetricpb.RegisterMetricsServiceServer(server, collector)
	go func() { _ = server.Serve(lis) }()
	defer server.Stop()
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpAddress := httpListener.Addr().String()
	_ = httpListener.Close()

	command := exec.Command(os.Args[0], "-test.run=^TestShellPushesItsRegisteredFragmentsOverOTLP$", "-test.v")
	command.Env = append(os.Environ(),
		"SHELL_OTLP_HELPER=1",
		"SHELL_OTLP_HTTP_ADDR="+httpAddress,
		"OTEL_ENABLED=true",
		"OTEL_EXPORTER_OTLP_ENDPOINT="+lis.Addr().String(),
		"OTEL_METRIC_EXPORT_INTERVAL=200",
		"OTEL_SERVICE_NAME=dev-health-go-shelltest",
	)
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = command.Process.Kill(); _ = command.Wait() }()

	client := &http.Client{Timeout: 300 * time.Millisecond}
	deadline := time.Now().Add(15 * time.Second)
	for {
		collector.mu.Lock()
		service, arrived := collector.seen["dev_health_runtime_live"]
		collector.mu.Unlock()
		if arrived {
			if service != "dev-health-go-shelltest" {
				t.Fatalf("dev_health_runtime_live arrived under service %q, want dev-health-go-shelltest", service)
			}
			break
		}
		if time.Now().After(deadline) {
			pulled := ""
			if response, err := client.Get("http://" + httpAddress + "/metrics"); err == nil {
				var body bytes.Buffer
				_, _ = body.ReadFrom(response.Body)
				_ = response.Body.Close()
				for _, line := range strings.Split(body.String(), "\n") {
					if strings.Contains(line, "otlp_metrics") && !strings.HasPrefix(line, "#") {
						pulled += line + "\n"
					}
				}
			}
			collector.mu.Lock()
			exports := collector.count
			collector.mu.Unlock()
			t.Fatalf("the fragment dev_health_runtime_live never reached the collector (exports received %d); push counters:\n%sprocess output:\n%s", exports, pulled, output.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	response, err := client.Get("http://" + httpAddress + "/metrics")
	if err != nil {
		t.Fatalf("the pull endpoint is gone: %v", err)
	}
	defer response.Body.Close()
	var body bytes.Buffer
	_, _ = body.ReadFrom(response.Body)
	if !strings.Contains(body.String(), "dev_health_runtime_live 1") || !strings.Contains(body.String(), "dev_health_otlp_metrics_exports_total") {
		t.Errorf("/metrics lost the fragment or the push counters:\n%s", body.String())
	}
}

func runShellHelper() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	lookup := testLookup(map[string]string{
		"DEV_HEALTH_HTTP_ADDR":        os.Getenv("SHELL_OTLP_HTTP_ADDR"),
		"DEV_HEALTH_SHUTDOWN_TIMEOUT": "1s",
		"OTEL_ENABLED":                os.Getenv("OTEL_ENABLED"),
		"OTEL_EXPORTER_OTLP_ENDPOINT": os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		"OTEL_METRIC_EXPORT_INTERVAL": os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"),
		"OTEL_SERVICE_NAME":           os.Getenv("OTEL_SERVICE_NAME"),
	})
	code := Execute(ctx, Spec{Service: "dev-health-worker"}, nil, lookup, IO{Stdout: &stdout, Stderr: &stderr})
	_, _ = os.Stderr.WriteString("helper exit=" + strconv.Itoa(code) + "\n" + stdout.String() + stderr.String())
	os.Exit(0)
}
