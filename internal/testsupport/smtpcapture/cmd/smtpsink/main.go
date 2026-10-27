// Command smtpsink is the corpus mail sink (ci/auth_corpus_external.sh): an SMTP recorder on the project's networks
// and the read API the host-side test reads it through (see the smtpcapture package).
//
//	smtpsink [-listen :2525] [-admin :9090]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/smtpcapture"
)

func main() {
	listen := flag.String("listen", ":2525", "SMTP listen address")
	admin := flag.String("admin", ":9090", "read API listen address")
	flag.Parse()
	if err := run(*listen, *admin); err != nil {
		fmt.Fprintln(os.Stderr, "smtpsink:", err)
		os.Exit(1)
	}
}

func run(listen, admin string) error {
	sink, err := smtpcapture.NewSink(listen)
	if err != nil {
		return err
	}
	defer sink.Close()
	server := &http.Server{Addr: admin, Handler: sink.Handler(), ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	fmt.Fprintf(os.Stdout, "smtpsink: SMTP on %s, read API on %s\n", sink.Addr(), admin)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
