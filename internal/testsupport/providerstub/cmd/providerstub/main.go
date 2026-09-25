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

// defaultAdminAddr keeps the recorder on loopback: it is reached with docker exec, never through a provider hostname.
const defaultAdminAddr = "127.0.0.1:9090"

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
		if err := writeFresh(filepath.Join(*out, name), data, mode); err != nil {
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
	admin := fs.String("admin", defaultAdminAddr, "recorder (plain HTTP) listen address (loopback by default)")
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
	provider := &http.Server{Addr: *listen, Handler: stub, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second,
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

// writeFresh writes data to a NEW regular file with the given mode. A file already there (a stale
// key, or a symlink planted in the directory) is removed first, never followed or reused.
func writeFresh(path string, data []byte, mode os.FileMode) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode) // #nosec G304 -- operator-chosen output directory
	if err != nil {
		return err
	}
	if err := f.Chmod(mode); err != nil { // the umask can only narrow a mode, but be explicit
		_ = f.Close()
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
