// Command loadtest drives a fixed-rate load against a dev stack and
// writes a JSON latency and error report. It refuses any host that is not
// local, a compose service name, or named with --allow-host.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/loadharness"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
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
	logging.InstallDefault(logging.NewJSON(os.Stderr, slog.LevelInfo))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr, secrets.GetenvNamed))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	fs := flag.NewFlagSet("loadtest", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		cfgPath      = fs.String("config", "", "target config JSON file (required)")
		profileName  = fs.String("profile", "normal", "load profile: normal or peak")
		rate         = fs.Float64("rate", 0, "requests per second (overrides the profile)")
		duration     = fs.Duration("duration", 0, "run length (overrides the profile)")
		concurrency  = fs.Int("concurrency", 0, "workers (overrides the profile)")
		baseURL      = fs.String("base-url", "", "base URL (overrides the config)")
		faultName    = fs.String("fault-name", "", "label of the fault the operator toggles outside the harness")
		faultOn      = fs.Duration("fault-on", 0, "offset into the run when the fault is turned on")
		faultOff     = fs.Duration("fault-off", 0, "offset into the run when the fault is turned off")
		faultMaxErr  = fs.Float64("fault-max-err", 0.05, "error ratio per second counted as degraded")
		faultSustain = fs.Int("fault-sustain", 3, "clean seconds in a row that count as recovered")
		recoverIn    = fs.Duration("recover-within", 0, "fail when recovery takes longer than this (needs a fault window)")
		out          = fs.String("out", "", "report file (default: stdout)")
		allow        listFlag
		slos         listFlag
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
	opts := loadharness.Options{Profile: prof, AllowHosts: allow, Getenv: getenv}
	if *faultOff > 0 || *faultOn > 0 || *faultName != "" {
		if *faultOff <= *faultOn {
			return fail("--fault-off must be after --fault-on")
		}
		if math.IsNaN(*faultMaxErr) || math.IsInf(*faultMaxErr, 0) || *faultMaxErr < 0 || *faultMaxErr > 1 {
			return fail("--fault-max-err must be a ratio in [0,1]")
		}
		opts.Fault = &loadharness.FaultWindow{Name: *faultName, On: *faultOn, Off: *faultOff, MaxErr: *faultMaxErr, Sustain: *faultSustain}
	}
	rep, err := loadharness.Run(ctx, cfg, opts)
	if err != nil {
		return fail("%v", err)
	}
	rep = loadharness.ApplySLOs(rep, parsed)
	if *recoverIn > 0 {
		rep = loadharness.ApplyRecoveryBound(rep, *recoverIn)
	}

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
	for _, c := range rep.Assertions {
		if !c.OK {
			fmt.Fprintf(stderr, "ASSERTION FAILED %s %s: %s\n", c.Path, c.Name, c.Detail)
		}
	}
	if !rep.SLOOK || !rep.AssertOK {
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
