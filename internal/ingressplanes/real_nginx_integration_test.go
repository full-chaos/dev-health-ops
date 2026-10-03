//go:build integration

package ingressplanes

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// nginxBinaryEnv names a local nginx binary. Unset (CI), the test starts the
// CHECKED-IN file in a container of RouterImage, with two stub planes on the
// compose names. Set, it starts no container: it runs that binary on the
// locations the generator writes for the checked-in contract, with stub planes
// in this process. The second form is for a host where a lane may not start a
// container; it does not prove the checked-in bytes or the name lookup.
const nginxBinaryEnv = "DEV_HEALTH_ROUTER_NGINX_BIN"

// The stub planes answer every request with 200 and say in response headers
// which plane answered and what it received.
const (
	stubPlane         = "X-Stub-Plane"
	stubURI           = "X-Stub-Uri"
	stubMethod        = "X-Stub-Method"
	stubAuthorization = "X-Stub-Authorization"
	stubCookie        = "X-Stub-Cookie"
)

// stubRequestHeaders are the request headers a stub plane reports, each under
// the response header name beside it.
var stubRequestHeaders = [][2]string{
	{"Host", "X-Stub-Host"},
	{"X-Real-IP", "X-Stub-Real-Ip"},
	{"X-Forwarded-For", "X-Stub-Fwd-For"},
	{"X-Forwarded-Host", "X-Stub-Fwd-Host"},
	{"X-Forwarded-Port", "X-Stub-Fwd-Port"},
	{"X-Forwarded-Proto", "X-Stub-Fwd-Proto"},
	{"X-Forwarded-Scheme", "X-Stub-Fwd-Scheme"},
	{"X-Scheme", "X-Stub-Scheme"},
	{"X-Original-Forwarded-For", "X-Stub-Orig-Fwd-For"},
	{"X-Original-Forwarded-Host", "X-Stub-Orig-Fwd-Host"},
	{"Authorization", stubAuthorization},
	{"Cookie", stubCookie},
}

// stubHeader is the response header under which a stub plane reports the
// request header name.
func stubHeader(t *testing.T, name string) string {
	t.Helper()
	for _, pair := range stubRequestHeaders {
		if pair[0] == name {
			return pair[1]
		}
	}
	t.Fatalf("the stub planes do not report %s", name)
	return ""
}

// normalisedRoutingRows are the rows only a real nginx can answer: nginx
// chooses the location on the path AFTER it decoded percent-escapes, resolved
// dot segments and merged slashes, and without the query string. ingress-nginx
// is the same nginx, so prod chooses on the same text.
var normalisedRoutingRows = []routingRow{
	{"an escaped letter is the letter", "/%67raphql", PlaneQueryAPI},
	{"an escaped slash is a slash: it makes the rule's path", "/api/v1%2Fpeople", PlaneQueryAPI},
	{"an escaped slash is a slash: two segments for one token", "/api/v1/people/a%2Fb/metric", PlaneGoAPI},
	{"dot segments are resolved", "/x/../graphql", PlaneQueryAPI},
	{"dot segments are resolved: the path that is left has no rule", "/api/v1/people/../graphql", PlaneGoAPI},
	{"two slashes are one", "//graphql", PlaneQueryAPI},
	{"the query string is not part of the path", "/graphql?query=%7Bme%7D&next=a%2Fb", PlaneQueryAPI},
	{"the query string does not name a rule", "/health?next=/graphql", PlaneGoAPI},
}

func nginxStubLocation(plane string) string {
	var b strings.Builder
	b.WriteString("location / {\n")
	fmt.Fprintf(&b, "            add_header %s %s always;\n", stubPlane, plane)
	fmt.Fprintf(&b, "            add_header %s $request_uri always;\n", stubURI)
	fmt.Fprintf(&b, "            add_header %s $request_method always;\n", stubMethod)
	for _, pair := range stubRequestHeaders {
		variable := "$http_" + strings.ToLower(strings.ReplaceAll(pair[0], "-", "_"))
		fmt.Fprintf(&b, "            add_header %s %s always;\n", pair[1], variable)
	}
	b.WriteString("            return 200;\n        }")
	return b.String()
}

// nginxStubConfig is the two stub planes as one nginx: the Go api's port and
// the query-api's port of the compose stack.
func nginxStubConfig() string {
	return `pid /tmp/nginx.pid;
events {}
http {
    access_log off;
    client_body_temp_path /tmp/client_body_temp;
    proxy_temp_path /tmp/proxy_temp;
    fastcgi_temp_path /tmp/fastcgi_temp;
    uwsgi_temp_path /tmp/uwsgi_temp;
    scgi_temp_path /tmp/scgi_temp;
    server {
        listen 8000;
        ` + nginxStubLocation(PlaneGoAPI) + `
    }
    server {
        listen 8090;
        ` + nginxStubLocation(PlaneQueryAPI) + `
    }
}
`
}

// goStub is a stub plane in this process, with the headers of the nginx stub.
func goStub(plane string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		set := func(name, value string) {
			if value != "" {
				w.Header().Set(name, value)
			}
		}
		set(stubPlane, plane)
		set(stubURI, r.RequestURI)
		set(stubMethod, r.Method)
		for _, pair := range stubRequestHeaders {
			if pair[0] == "Host" {
				set(pair[1], r.Host)
				continue
			}
			set(pair[1], strings.Join(r.Header.Values(pair[0]), ", "))
		}
		w.WriteHeader(http.StatusOK)
	})
}

// router is a started router: where to send requests, and the port the router
// itself listens on (what it must report as X-Forwarded-Port).
type router struct {
	base string
	port string
}

// waitForPlanes sends GET / until a stub plane answers. The router was started
// BEFORE the planes, so this is the proof that it finds a plane that comes up
// later, with no restart.
func waitForPlanes(t *testing.T, base string, limit time.Duration, diagnose func() string) {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second}
	deadline := time.Now().Add(limit)
	last := "no answer yet"
	for {
		response, err := client.Get(base + "/")
		if err != nil {
			last = err.Error()
		} else {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK && response.Header.Get(stubPlane) == PlaneGoAPI {
				return
			}
			last = fmt.Sprintf("status %d, %s %q", response.StatusCode, stubPlane, response.Header.Get(stubPlane))
		}
		if time.Now().After(deadline) {
			t.Fatalf("no stub plane answered GET %s/ within %s after the planes were started (last: %s)\n%s", base, limit, last, diagnose())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// answersWithoutPlanes holds that a router whose planes are not there yet is
// up and answers 5xx itself (no stub plane header).
func answersWithoutPlanes(t *testing.T, base string, diagnose func() string) {
	t.Helper()
	client := &http.Client{Timeout: 90 * time.Second}
	response, err := client.Get(base + "/")
	if err != nil {
		t.Fatalf("the router with no plane behind it did not answer: %v\n%s", err, diagnose())
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 500 || response.Header.Get(stubPlane) != "" {
		t.Fatalf("the router with no plane behind it answered %d (%s %q), want its own 5xx\n%s",
			response.StatusCode, stubPlane, response.Header.Get(stubPlane), diagnose())
	}
}

// startRouterInContainers runs the checked-in file in RouterImage. The router
// starts FIRST, on a network where the names go-api and query-api do not
// resolve yet: it must start and answer 5xx. Then the stub planes start as one
// more container of the same image with the network aliases go-api and
// query-api, and the router must find them by name through Docker's DNS, as it
// finds the planes in the compose stack.
func startRouterInContainers(t *testing.T) router {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	planes, err := network.New(ctx)
	if err != nil {
		t.Fatalf("create the network of the router test: %v", err)
	}
	testcontainers.CleanupNetwork(t, planes)

	routerContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        RouterImage,
			ExposedPorts: []string{RouterPort + "/tcp"},
			Networks:     []string{planes.Name},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      repoFile(t, RouterConfigPath),
				ContainerFilePath: "/etc/nginx/nginx.conf",
				FileMode:          0o644,
			}},
			// No plane exists yet. A router that listens means: nginx took
			// the file and started with plane names that do not resolve.
			// (Not an HTTP wait: its requests give up after one second, and
			// the answer for a name that does not resolve can take longer.)
			WaitingFor: wait.ForListeningPort(RouterPort + "/tcp").WithStartupTimeout(3 * time.Minute),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, routerContainer)
	diagnose := func() (text string) {
		// The log is help for a failure, never a second failure: a container
		// that was not created has none.
		defer func() {
			if recover() != nil {
				text = "router log: not available"
			}
		}()
		if routerContainer == nil {
			return "no router container"
		}
		logs, logErr := routerContainer.Logs(context.Background())
		if logErr != nil {
			return "router log: " + logErr.Error()
		}
		defer func() { _ = logs.Close() }()
		read, _ := io.ReadAll(io.LimitReader(logs, 16<<10))
		return "router log:\n" + string(read)
	}
	if err != nil {
		t.Fatalf("start the router (%s) with %s before its planes: %v\n%s", RouterImage, RouterConfigPath, err, diagnose())
	}
	host, err := routerContainer.Host(ctx)
	if err != nil {
		t.Fatalf("router host: %v", err)
	}
	port, err := routerContainer.MappedPort(ctx, RouterPort+"/tcp")
	if err != nil {
		t.Fatalf("router port: %v", err)
	}
	base := "http://" + net.JoinHostPort(host, port.Port())
	answersWithoutPlanes(t, base, diagnose)

	stubs, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          RouterImage,
			ExposedPorts:   []string{"8000/tcp", "8090/tcp"},
			Networks:       []string{planes.Name},
			NetworkAliases: map[string][]string{planes.Name: {"go-api", "query-api"}},
			Files: []testcontainers.ContainerFile{{
				Reader:            strings.NewReader(nginxStubConfig()),
				ContainerFilePath: "/etc/nginx/nginx.conf",
				FileMode:          0o644,
			}},
			WaitingFor: wait.ForAll(
				wait.ForHTTP("/").WithPort("8000/tcp"),
				wait.ForHTTP("/").WithPort("8090/tcp"),
			).WithDeadline(2 * time.Minute),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, stubs)
	if err != nil {
		t.Fatalf("start the stub planes (%s): %v", RouterImage, err)
	}
	waitForPlanes(t, base, 90*time.Second, diagnose)
	return router{base: base, port: RouterPort}
}

// freeAddress is a loopback address nothing listens on now.
func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find a free port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

// startStubAt starts a stub plane of this process on address.
func startStubAt(t *testing.T, address, plane string) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("listen on %s for the %s stub: %v", address, plane, err)
	}
	server := httptest.NewUnstartedServer(goStub(plane))
	_ = server.Listener.Close()
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
}

// startRouterAsProcess runs a local nginx binary on the generator's output for
// the contract, with the stub planes in this process (addresses, so no name
// lookup) and every path nginx writes below the test's own directory. As in the
// container form, the router starts before its planes.
func startRouterAsProcess(t *testing.T, binary string, contract Contract) router {
	t.Helper()
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("%s names %q, which is not there: %v", nginxBinaryEnv, binary, err)
	}
	goAPI, queryAPI, address := freeAddress(t), freeAddress(t), freeAddress(t)
	directory := t.TempDir()
	config, err := Render(contract, Options{
		Listen:    address,
		Upstreams: map[string]string{PlaneGoAPI: goAPI, PlaneQueryAPI: queryAPI},
		RunDir:    directory,
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	configPath := filepath.Join(directory, "nginx.conf")
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	command := exec.Command(binary, "-p", directory, "-c", configPath, "-g", "daemon off; master_process off;")
	command.Stdout, command.Stderr = &log, &log
	if err := command.Start(); err != nil {
		t.Fatalf("start %s: %v", binary, err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	diagnose := func() string { return "nginx output:\n" + log.String() }
	base := "http://" + address
	deadline := time.Now().Add(15 * time.Second)
	for {
		connection, err := net.DialTimeout("tcp", address, time.Second)
		if err == nil {
			_ = connection.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("nginx did not listen on %s within 15 s: %v\n%s", address, err, diagnose())
		}
		time.Sleep(100 * time.Millisecond)
	}
	answersWithoutPlanes(t, base, diagnose)
	startStubAt(t, goAPI, PlaneGoAPI)
	startStubAt(t, queryAPI, PlaneQueryAPI)
	waitForPlanes(t, base, 15*time.Second, diagnose)
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	return router{base: base, port: port}
}

// TestRealNginxRoutesEveryPathToItsPlane sends every routing row through a
// real nginx and reads which stub plane answered. It also proves what the
// model cannot: nginx takes the file and starts before its planes, the path
// nginx chooses on is the normalised one, the plane receives the request
// target as the client sent it, no request can choose the plane, the router
// owns the forwarded headers, and it adds no credential.
func TestRealNginxRoutesEveryPathToItsPlane(t *testing.T) {
	contract := checkedInContract(t)
	var started router
	if binary := os.Getenv(nginxBinaryEnv); binary != "" {
		started = startRouterAsProcess(t, binary, contract)
	} else {
		started = startRouterInContainers(t)
	}
	client := &http.Client{
		Timeout:       15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	send := func(t *testing.T, method, target string, headers map[string]string) http.Header {
		t.Helper()
		request, err := http.NewRequest(method, started.base+target, nil)
		if err != nil {
			t.Fatalf("%s %s: %v", method, target, err)
		}
		for name, value := range headers {
			if name == "Host" {
				request.Host = value
				continue
			}
			request.Header.Set(name, value)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("%s %s: %v", method, target, err)
		}
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s %s: status %d, want 200 from a stub plane", method, target, response.StatusCode)
		}
		return response.Header
	}

	t.Run("every row reaches its plane", func(t *testing.T) {
		rows := append(routingRows(t, contract), normalisedRoutingRows...)
		planes := map[string]int{}
		for _, row := range rows {
			answer := send(t, http.MethodGet, row.path, nil)
			if got := answer.Get(stubPlane); got != row.plane {
				t.Errorf("%s: %s reached %q, want %s", row.name, row.path, got, row.plane)
			}
			if got := answer.Get(stubURI); got != row.path {
				t.Errorf("%s: the plane received %q, the client sent %q", row.name, got, row.path)
			}
			planes[answer.Get(stubPlane)]++
		}
		if planes[PlaneGoAPI] == 0 || planes[PlaneQueryAPI] == 0 || planes[PlaneGoAPI]+planes[PlaneQueryAPI] != len(rows) {
			t.Fatalf("%d rows, answers by plane %v: both stub planes must have answered, and nothing else", len(rows), planes)
		}
	})

	t.Run("the method does not choose the plane", func(t *testing.T) {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodOptions} {
			if answer := send(t, method, "/graphql", nil); answer.Get(stubPlane) != PlaneQueryAPI || answer.Get(stubMethod) != method {
				t.Errorf("%s /graphql: plane %q, method %q", method, answer.Get(stubPlane), answer.Get(stubMethod))
			}
			if answer := send(t, method, "/api/v1/auth/login", nil); answer.Get(stubPlane) != PlaneGoAPI || answer.Get(stubMethod) != method {
				t.Errorf("%s /api/v1/auth/login: plane %q, method %q", method, answer.Get(stubPlane), answer.Get(stubMethod))
			}
		}
	})

	// The plane is chosen by the path only. A request that names the other
	// plane in its Host, in a header or in its query string reaches the plane
	// of its path.
	t.Run("a request cannot choose the plane", func(t *testing.T) {
		for _, c := range []struct{ target, plane, other string }{
			{"/api/v1/auth/login", PlaneGoAPI, QueryAPIUpstream},
			{"/graphql", PlaneQueryAPI, GoAPIUpstream},
		} {
			for name, headers := range map[string]map[string]string{
				"Host":                {"Host": c.other},
				"X-Forwarded-Host":    {"X-Forwarded-Host": c.other},
				"plane headers":       {"Plane": c.other, "Plane-Go-Api": c.other, "Plane-Query-Api": c.other, "X-Plane": c.other, "X-Upstream": c.other},
				"proxy header":        {"Proxy": "http://" + c.other},
				"no header, by query": nil,
			} {
				target := c.target
				if headers == nil {
					target += "?plane_go_api=" + c.other + "&plane_query_api=" + c.other + "&plane=" + c.other
				}
				if got := send(t, http.MethodGet, target, headers).Get(stubPlane); got != c.plane {
					t.Errorf("%s with %s naming %s reached %q, want %s", c.target, name, c.other, got, c.plane)
				}
			}
		}
	})

	// The router owns the forwarded headers as ingress-nginx does with its
	// defaults: each comes from the router's own connection, whatever the
	// client sent. The client's X-Forwarded-For and X-Forwarded-Host are kept
	// only under the X-Original-Forwarded-* names.
	t.Run("the router owns the forwarded headers", func(t *testing.T) {
		const host = "api.router.test:8000"
		forged := map[string]string{
			"Host":                      host,
			"X-Real-IP":                 "203.0.113.9",
			"X-Forwarded-For":           "203.0.113.9, 198.51.100.7",
			"X-Forwarded-Host":          "forged.router.test",
			"X-Forwarded-Port":          "443",
			"X-Forwarded-Proto":         "https",
			"X-Forwarded-Scheme":        "https",
			"X-Scheme":                  "https",
			"X-Original-Forwarded-For":  "192.0.2.44",
			"X-Original-Forwarded-Host": "other.router.test",
		}
		for _, target := range []string{"/graphql", "/api/v1/auth/login", "/no/such/path"} {
			for name, sent := range map[string]map[string]string{"forged headers": forged, "no forwarded header": {"Host": host}} {
				answer := send(t, http.MethodGet, target, sent)
				at := func(header string) string { return answer.Get(stubHeader(t, header)) }
				peer := at("X-Real-IP")
				if net.ParseIP(peer) == nil || peer == "203.0.113.9" {
					t.Errorf("%s, %s: X-Real-IP at the plane is %q, want the router's peer address", target, name, peer)
				}
				want := map[string]string{
					"Host":               host,
					"X-Forwarded-For":    peer, // one address: what the client sent is replaced
					"X-Forwarded-Host":   host,
					"X-Forwarded-Port":   started.port,
					"X-Forwarded-Proto":  "http",
					"X-Forwarded-Scheme": "http",
					"X-Scheme":           "http",
					// ingress-nginx keeps what the client sent under these names, and nothing when it sent none.
					"X-Original-Forwarded-For":  sent["X-Forwarded-For"],
					"X-Original-Forwarded-Host": sent["X-Forwarded-Host"],
				}
				for header, value := range want {
					if got := at(header); got != value {
						t.Errorf("%s, %s: %s at the plane is %q, want %q", target, name, header, got, value)
					}
				}
			}
		}
	})

	// Web's server-side proxy is the only place that turns the session cookie
	// into an Authorization bearer. The router must pass both headers as they
	// came and make neither.
	t.Run("the router adds no credential", func(t *testing.T) {
		const cookie = "authjs.session-token=router-test-cookie"
		for _, target := range []string{"/graphql", "/api/v1/auth/me"} {
			onlyCookie := send(t, http.MethodGet, target, map[string]string{"Cookie": cookie})
			if got := onlyCookie.Get(stubAuthorization); got != "" {
				t.Errorf("%s with a cookie and no Authorization: the plane received Authorization %q", target, got)
			}
			if got := onlyCookie.Get(stubCookie); got != cookie {
				t.Errorf("%s: the plane received cookie %q, the client sent %q", target, got, cookie)
			}
			withBearer := send(t, http.MethodGet, target, map[string]string{"Authorization": "Bearer router-test-bearer", "Cookie": cookie})
			if got := withBearer.Get(stubAuthorization); got != "Bearer router-test-bearer" {
				t.Errorf("%s: the plane received Authorization %q, the client sent another", target, got)
			}
			none := send(t, http.MethodGet, target, nil)
			if none.Get(stubAuthorization) != "" || none.Get(stubCookie) != "" {
				t.Errorf("%s with no credential: the plane received Authorization %q, cookie %q", target, none.Get(stubAuthorization), none.Get(stubCookie))
			}
		}
	})
}
