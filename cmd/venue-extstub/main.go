// Command venue-extstub stands in, on the bigboy compose venue only, for the outside services go-api calls on the
// external GAP rows: Stripe, PagerDuty and GitHub over HTTPS (one listener, routed by host) and mail over plain SMTP.
// It logs one JSON line per request or message (t, service, method, host, path: no header, body, query or address)
// for the venue judge (internal/shadowvenue, -stub-log), answers from a frozen table, and never dials out.
//
//	venue-extstub -ca-cert <pem> -ca-key <pkcs8 pem, 0600> -log <new jsonl file> -status <new file> [-mode run|seed]
//	              [-https :8443] [-smtp :2525]
//
// -mode seed answers only GET api.pagerduty.com/services?limit=1 (the org E PagerDuty credential's validation, before
// the judged runs) and needs a log whose name holds ".seed."; run mode refuses such a log name, so the two logs never
// mix. -status names the mode, the answer-table digest and the start time for the venue's egress records.
//
// Exit: 2 usage or start failure; otherwise it serves until SIGINT or SIGTERM.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

const exitUsage = 2

func main() {
	logging.InstallDefault(logging.NewJSON(os.Stderr, slog.LevelInfo))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stderr))
}

func run(ctx context.Context, args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("venue-extstub", flag.ContinueOnError)
	fs.SetOutput(stderr)
	caCert := fs.String("ca-cert", "", "venue CA certificate (PEM)")
	caKey := fs.String("ca-key", "", "venue CA key (PKCS #8 PEM, mode 0600)")
	logPath := fs.String("log", "", "hit log to create (must not exist), mode 0600")
	httpsAddr := fs.String("https", ":8443", "HTTPS listen address")
	smtpAddr := fs.String("smtp", ":2525", "SMTP listen address")
	mode := fs.String("mode", modeRun, "run (the frozen answers of the judged runs) or seed (the org E credential probe only)")
	statusPath := fs.String("status", "", "status file to create (must not exist): mode, config digest, start time")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if *caCert == "" || *caKey == "" || *logPath == "" || *statusPath == "" {
		fmt.Fprintln(stderr, "venue-extstub: -ca-cert, -ca-key, -log and -status are required")
		return exitUsage
	}
	seedLog := strings.Contains(filepath.Base(*logPath), ".seed.")
	switch {
	case *mode != modeRun && *mode != modeSeed:
		fmt.Fprintf(stderr, "venue-extstub: -mode %q: want run or seed\n", *mode)
		return exitUsage
	case (*mode == modeSeed) != seedLog:
		fmt.Fprintln(stderr, "venue-extstub: a seed-mode log name holds \".seed.\" and a run-mode one does not, so the logs never mix")
		return exitUsage
	}
	certPEM, err := os.ReadFile(*caCert)
	if err != nil {
		fmt.Fprintf(stderr, "venue-extstub: -ca-cert: %v\n", errors.Unwrap(err))
		return exitUsage
	}
	leaf, err := leafCert(certPEM, *caKey, time.Now())
	if err != nil {
		fmt.Fprintf(stderr, "venue-extstub: %v\n", err)
		return exitUsage
	}
	file, err := openLog(*logPath)
	if err != nil {
		fmt.Fprintf(stderr, "venue-extstub: -log %s: %v\n", *logPath, errors.Unwrap(err))
		return exitUsage
	}
	defer file.Close()
	log := &hitLog{w: file, now: time.Now}
	httpsLn, err := net.Listen("tcp", *httpsAddr)
	if err != nil {
		fmt.Fprintf(stderr, "venue-extstub: -https: %v\n", err)
		return exitUsage
	}
	// Seed mode answers the one PagerDuty probe and nothing else: it opens no SMTP listener.
	smtpLn := net.Listener(nil)
	if *mode == modeRun {
		if smtpLn, err = net.Listen("tcp", *smtpAddr); err != nil {
			_ = httpsLn.Close()
			fmt.Fprintf(stderr, "venue-extstub: -smtp: %v\n", err)
			return exitUsage
		}
	}
	if err := writeStatus(*statusPath, *mode, time.Now()); err != nil {
		_ = httpsLn.Close()
		if smtpLn != nil {
			_ = smtpLn.Close()
		}
		fmt.Fprintf(stderr, "venue-extstub: -status %s: %v\n", *statusPath, errors.Unwrap(err))
		return exitUsage
	}
	srv := &http.Server{Handler: handler(log, *mode), ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{leaf}, MinVersion: tls.VersionTLS12}}
	go func() { _ = srv.ServeTLS(httpsLn, "", "") }()
	if smtpLn != nil {
		go func() { _ = serveSMTP(smtpLn, log) }()
	}
	fmt.Fprintf(stderr, "venue-extstub: mode %s config %s, serving https %s, log %s\n", *mode, configDigest(*mode), httpsLn.Addr(), *logPath)
	<-ctx.Done()
	if smtpLn != nil {
		_ = smtpLn.Close()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	return 0
}
