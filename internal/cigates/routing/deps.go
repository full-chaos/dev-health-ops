// Package routing holds bigboy's plane-split routing tools: the traefik router generator, the query-api env generator,
// the route coverage probe and the web env-name parity check. cmd/cigate runs them (plane-split-router,
// query-api-flags, route-coverage, web-env-parity); ci/bigboy/bigboy-cut.sh calls those subcommands.
//
// Each was a Python script; the ports keep their arguments, exit codes and output byte for byte (testdata/README.md
// records the Python runs the tests replay).
package routing

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

// Git runs `git -C repo args...` and returns its stdout; a non-zero exit is an error carrying the stderr text.
type Git interface {
	Run(repo string, args ...string) (stdout string, err error)
}

// GitError is a git run that exited non-zero.
type GitError struct{ Stderr string }

func (e *GitError) Error() string { return e.Stderr }

// Prober sends one request with method to http://host:port with the path as the request target, verbatim, and returns
// the status code.
type Prober interface {
	Probe(host string, port int, method, path string) (int, error)
}

// Files reads and writes the files the tools name.
type Files interface {
	ReadFile(name string) ([]byte, error)
	WriteFile(name string, data []byte) error
}

// Deps is what the tools reach outside the process through.
type Deps struct {
	Git    Git
	Prober Prober
	Files  Files
}

// OSDeps is the real git, HTTP and file system.
func OSDeps() Deps {
	return Deps{Git: ExecGit{}, Prober: HTTPProber{Timeout: 10 * time.Second}, Files: OSFiles{}}
}

// ExecGit runs the git binary.
type ExecGit struct{}

func (ExecGit) Run(repo string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return out.String(), &GitError{Stderr: errb.String()}
		}
		return out.String(), err
	}
	return out.String(), nil
}

// HTTPProber sends the request with net/http.
type HTTPProber struct{ Timeout time.Duration }

func (p HTTPProber) Probe(host string, port int, method, path string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.Timeout)
	defer cancel()
	u := &url.URL{Scheme: "http", Host: host + ":" + strconv.Itoa(port), Opaque: path}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return 0, err
	}
	req.URL = u
	req.Host = host + ":" + strconv.Itoa(port)
	client := &http.Client{Transport: httpguard.SharedTransport(), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if _, err := new(bytes.Buffer).ReadFrom(resp.Body); err != nil {
		return 0, err
	}
	return resp.StatusCode, nil
}

// OSFiles is the local file system.
type OSFiles struct{}

func (OSFiles) ReadFile(name string) ([]byte, error) { return os.ReadFile(name) }

func (OSFiles) WriteFile(name string, data []byte) error { return os.WriteFile(name, data, 0o666) }
