// Command providerstub runs the provider stub of the provider-stub venue
// (see the providerstub package): a TLS server for the provider hosts and a
// plain-HTTP admin listener for the request recorder.
//
//	providerstub certs -out DIR [-jira-host HOST ...]   write ca.pem, server.pem, server.key
//	providerstub serve -fixtures DIR -cert F -key F [-listen :443] [-admin :9090]
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/providerstub"
)

type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: providerstub certs|serve [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "certs":
		err = certs(os.Args[2:])
	case "serve":
		err = serve(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q (want certs or serve)", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "providerstub:", err)
		os.Exit(1)
	}
}

func certs(args []string) error {
	fs := flag.NewFlagSet("certs", flag.ContinueOnError)
	out := fs.String("out", "", "directory to write ca.pem, server.pem and server.key into (created 0700)")
	var jira stringList
	fs.Var(&jira, "jira-host", "a Jira tenant host to add to the certificate (repeatable), e.g. zz-venue.atlassian.net")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("-out is required")
	}
	if err := os.MkdirAll(*out, 0o700); err != nil {
		return err
	}
	ca, err := providerstub.NewCA(30 * 24 * time.Hour)
	if err != nil {
		return err
	}
	certPEM, keyPEM, err := ca.IssueServer(append(providerstub.Hosts(), jira...), 30*24*time.Hour)
	if err != nil {
		return err
	}
	for name, data := range map[string][]byte{"ca.pem": ca.CertPEM, "server.pem": certPEM, "server.key": keyPEM} {
		mode := os.FileMode(0o644)
		if name == "server.key" {
			mode = 0o600
		}
		path := filepath.Join(*out, name)
		if err := os.WriteFile(path, data, mode); err != nil {
			return err
		}
		// WriteFile leaves the mode of a file that already exists: set it explicitly
		if err := os.Chmod(path, mode); err != nil {
			return err
		}
	}
	fmt.Println("wrote ca.pem, server.pem, server.key to", *out)
	return nil
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fixtures := fs.String("fixtures", "", "directory of fixture *.json files")
	certFile := fs.String("cert", "", "server certificate PEM")
	keyFile := fs.String("key", "", "server key PEM")
	listen := fs.String("listen", ":443", "provider (TLS) listen address")
	admin := fs.String("admin", ":9090", "recorder (plain HTTP) listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *fixtures == "" || *certFile == "" || *keyFile == "" {
		return errors.New("-fixtures, -cert and -key are required")
	}
	stub, err := providerstub.LoadDir(*fixtures)
	if err != nil {
		return err
	}
	stub.Log = func(line string) { fmt.Println(line) } // one line per request, no path, no values
	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		return err
	}
	provider := &http.Server{Addr: *listen, Handler: stub, ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}}
	recorder := &http.Server{Addr: *admin, Handler: stub.AdminHandler(), ReadHeaderTimeout: 10 * time.Second}
	errs := make(chan error, 2)
	go func() { errs <- provider.ListenAndServeTLS("", "") }()
	go func() { errs <- recorder.ListenAndServe() }()
	fmt.Println("providerstub: provider TLS on", *listen, "recorder on", *admin)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errs:
		return err
	case <-stop:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = provider.Shutdown(ctx)
	_ = recorder.Shutdown(ctx)
	return nil
}
