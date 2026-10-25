// Command faultproxy sits between a service and one dependency on a throwaway
// stack and injects network faults (delay, slow, drop, cut, blackhole), by a
// schedule or through a loopback control endpoint. It is a test tool and is
// never shipped.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/faultproxy"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	logging.InstallDefault(logging.NewJSON(os.Stderr, slog.LevelInfo))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) (code int) {
	fs := flag.NewFlagSet("faultproxy", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		listen   = fs.String("listen", "", "address to listen on, host:port (required)")
		upstream = fs.String("upstream", "", "address to forward to, host:port (required)")
		control  = fs.String("control", "", "loopback host:port for the control endpoint (off when empty)")
		schedule = fs.String("schedule", "", "fault schedule, e.g. 5s=blackhole,35s=pass,40s=delay:200ms,50s=slow:5000")
		events   = fs.String("events", "", "file that receives one JSON line per mode change")
		maxConns = fs.Int("max-conns", 256, "most live connections; more are refused")
		allow    listFlag
	)
	fs.Var(&allow, "allow-host", "extra allowed host name, exact match (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	fail := func(format string, a ...any) int {
		fmt.Fprintf(stderr, "faultproxy: "+format+"\n", a...)
		return 2
	}
	if *listen == "" || *upstream == "" {
		return fail("--listen and --upstream are required")
	}
	if *maxConns <= 0 {
		return fail("--max-conns must be positive")
	}
	for _, a := range []string{*listen, *upstream} {
		if err := faultproxy.CheckAddr(a, allow); err != nil {
			return fail("%v", err)
		}
	}
	var steps []faultproxy.Step
	if *schedule != "" {
		var err error
		if steps, err = faultproxy.ParseSchedule(*schedule); err != nil {
			return fail("%v", err)
		}
	}
	cfg := faultproxy.Config{Listen: *listen, Upstream: *upstream, MaxConns: *maxConns}
	var p *faultproxy.Proxy
	if *events != "" {
		f, err := os.OpenFile(*events, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return fail("open events file: %v", err)
		}
		// runs after the proxy has stopped, so no event is written after the close
		defer func() {
			var werr error
			if p != nil {
				werr = p.EventsErr()
			}
			if err := errors.Join(werr, f.Sync(), f.Close()); err != nil {
				fmt.Fprintf(stderr, "faultproxy: events file not written cleanly: %v\n", err)
				code = 1
			}
		}()
		cfg.Events = f
	}
	if *control != "" {
		if err := faultproxy.CheckControlAddr(*control); err != nil {
			return fail("control: %v", err)
		}
	}
	started, err := faultproxy.New(cfg)
	if err != nil {
		return fail("%v", err)
	}
	p = started
	defer p.Stop()
	var ctlAddr string
	if *control != "" {
		a, stop, err := faultproxy.StartControl(*control, p.ControlHandler())
		if err != nil {
			return fail("control: %v", err)
		}
		ctlAddr = a.String()
		defer stop()
	}
	if len(steps) > 0 {
		p.Schedule(steps)
	}
	fmt.Fprintf(stdout, "listening %s\n", p.Addr())
	if ctlAddr != "" {
		fmt.Fprintf(stdout, "control %s\n", ctlAddr)
	}
	<-ctx.Done()
	return 0
}
