// Command loadtest drives a fixed-rate load against a dev stack and
// writes a JSON latency and error report. It refuses any host that is not
// local, a compose service name, or named with --allow-host.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/loadharness"
)

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error { *l = append(*l, v); return nil }

const (
	exitOK       = 0
	exitSLO      = 1
	exitUsageErr = 2
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		cfgPath     = fs.String("config", "", "target config JSON file (required)")
		profileName = fs.String("profile", "normal", "load profile: normal or peak")
		rate        = fs.Float64("rate", 0, "requests per second (overrides the profile)")
		duration    = fs.Duration("duration", 0, "run length (overrides the profile)")
		concurrency = fs.Int("concurrency", 0, "workers (overrides the profile)")
		baseURL     = fs.String("base-url", "", "base URL (overrides the config)")
		out         = fs.String("out", "", "report file (default: stdout)")
		allow       listFlag
		slos        listFlag
	)
	fs.Var(&allow, "allow-host", "extra allowed host name, exact match (repeatable)")
	fs.Var(&slos, "slo", "objective path:p95=25ms,p99=75ms,err=0.01 (repeatable)")
	if err := fs.Parse(args); err != nil {
		return exitUsageErr
	}
	fail := func(format string, a ...any) int {
		fmt.Fprintf(stderr, "loadtest: "+format+"\n", a...)
		return exitUsageErr
	}
	if *cfgPath == "" {
		return fail("--config is required")
	}
	data, err := readConfig(*cfgPath)
	if err != nil {
		return fail("read config: %v", err)
	}
	cfg, err := loadharness.ParseConfig(data, getenv)
	if err != nil {
		return fail("%v", err)
	}
	if *baseURL != "" {
		cfg.BaseURL = *baseURL
	}
	prof, err := loadharness.ProfileByName(*profileName)
	if err != nil {
		return fail("%v", err)
	}
	if *rate > 0 {
		prof.Rate = *rate
	}
	if *duration > 0 {
		prof.Duration = *duration
	}
	if *concurrency > 0 {
		prof.Concurrency = *concurrency
	}
	parsed, err := loadharness.ParseSLOs(slos)
	if err != nil {
		return fail("%v", err)
	}
	rep, err := loadharness.Run(ctx, cfg, loadharness.Options{Profile: prof, AllowHosts: allow, Getenv: getenv})
	if err != nil {
		return fail("%v", err)
	}
	rep = loadharness.ApplySLOs(rep, parsed)

	w := stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			return fail("create report: %v", err)
		}
		defer f.Close()
		w = f
	}
	if err := loadharness.WriteReport(w, rep); err != nil {
		return fail("write report: %v", err)
	}
	for _, p := range rep.Paths {
		fmt.Fprintf(stderr, "%s n=%d err=%.4f p50=%.1fms p95=%.1fms p99=%.1fms\n", p.Name, p.Count, p.ErrorRate, p.P50Ms, p.P95Ms, p.P99Ms)
	}
	for _, c := range rep.SLOChecks {
		if !c.OK {
			fmt.Fprintf(stderr, "SLO BREACH %s %s limit=%g actual=%g\n", c.Path, c.Metric, c.Limit, c.Actual)
		}
	}
	if !rep.SLOOK {
		return exitSLO
	}
	return exitOK
}

const maxConfigBytes = 1 << 20

func readConfig(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxConfigBytes {
		return nil, fmt.Errorf("config is larger than %d bytes", maxConfigBytes)
	}
	return data, nil
}
