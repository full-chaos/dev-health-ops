// Command venue-extstub stands in, on the bigboy compose venue only, for the outside services go-api calls on the
// external GAP rows: Stripe, PagerDuty and GitHub over HTTPS (one listener, routed by host) and mail over plain SMTP.
// It logs one JSON line per request or message (t, service, method, host, path: no header, body, query or address)
// for the venue judge (internal/shadowvenue, -stub-log), answers from a frozen table, and never dials out.
//
//	venue-extstub -ca-cert <pem> -ca-key <pkcs8 pem, 0600> -log <new jsonl file> [-https :8443] [-smtp :2525]
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
	_ = fs.String("mode", modeRun, "skeleton")
	_ = fs.String("status", "", "skeleton")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if *caCert == "" || *caKey == "" || *logPath == "" {
		fmt.Fprintln(stderr, "venue-extstub: -ca-cert, -ca-key and -log are required")
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
	smtpLn, err := net.Listen("tcp", *smtpAddr)
	if err != nil {
		_ = httpsLn.Close()
		fmt.Fprintf(stderr, "venue-extstub: -smtp: %v\n", err)
		return exitUsage
	}
	srv := &http.Server{Handler: handler(log, modeRun), ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{leaf}, MinVersion: tls.VersionTLS12}}
	go func() { _ = srv.ServeTLS(httpsLn, "", "") }()
	go func() { _ = serveSMTP(smtpLn, log) }()
	fmt.Fprintf(stderr, "venue-extstub: serving https %s smtp %s, log %s\n", httpsLn.Addr(), smtpLn.Addr(), *logPath)
	<-ctx.Done()
	_ = smtpLn.Close()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	return 0
}
