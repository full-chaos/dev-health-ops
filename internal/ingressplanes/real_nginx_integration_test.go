//go:build integration

package ingressplanes

import (
	"bytes"
	"context"
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
	stubHost          = "X-Stub-Host"
	stubForwardedFor  = "X-Stub-Xff"
	stubRealIP        = "X-Stub-Real-Ip"
	stubAuthorization = "X-Stub-Authorization"
	stubCookie        = "X-Stub-Cookie"
)

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
	return `location / {
            add_header ` + stubPlane + ` ` + plane + ` always;
            add_header ` + stubURI + ` $request_uri always;
            add_header ` + stubMethod + ` $request_method always;
            add_header ` + stubHost + ` $http_host always;
            add_header ` + stubForwardedFor + ` $http_x_forwarded_for always;
            add_header ` + stubRealIP + ` $http_x_real_ip always;
            add_header ` + stubAuthorization + ` $http_authorization always;
            add_header ` + stubCookie + ` $http_cookie always;
            return 200;
        }`
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
		set(stubHost, r.Host)
		set(stubForwardedFor, strings.Join(r.Header.Values("X-Forwarded-For"), ", "))
		set(stubRealIP, r.Header.Get("X-Real-IP"))
		set(stubAuthorization, r.Header.Get("Authorization"))
		set(stubCookie, r.Header.Get("Cookie"))
		w.WriteHeader(http.StatusOK)
	})
}

// startRouterInContainers runs the checked-in file in RouterImage. The stub
// planes are one more container of the same image with the network aliases
// go-api and query-api, so the router finds them by name through Docker's DNS,
// as it finds the planes in the compose stack.
func startRouterInContainers(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	planes, err := network.New(ctx)
	if err != nil {
		t.Fatalf("create the network of the router test: %v", err)
	}
	testcontainers.CleanupNetwork(t, planes)

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

	router, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        RouterImage,
			ExposedPorts: []string{RouterPort + "/tcp"},
			Networks:     []string{planes.Name},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      repoFile(t, RouterConfigPath),
				ContainerFilePath: "/etc/nginx/nginx.conf",
				FileMode:          0o644,
			}},
			// 200 on "/" means: nginx took the file, resolved go-api by
			// name and the stub answered.
			WaitingFor: wait.ForHTTP("/").WithPort(RouterPort + "/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, router)
	if err != nil {
		t.Fatalf("start the router (%s) with %s: %v", RouterImage, RouterConfigPath, err)
	}
	host, err := router.Host(ctx)
	if err != nil {
		t.Fatalf("router host: %v", err)
	}
	port, err := router.MappedPort(ctx, RouterPort+"/tcp")
	if err != nil {
		t.Fatalf("router port: %v", err)
	}
	return "http://" + net.JoinHostPort(host, port.Port())
}

// startRouterAsProcess runs a local nginx binary on the generator's output for
// the contract, with the stub planes in this process (addresses, so no name
// lookup) and every path nginx writes below the test's own directory.
func startRouterAsProcess(t *testing.T, binary string, contract Contract) string {
	t.Helper()
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("%s names %q, which is not there: %v", nginxBinaryEnv, binary, err)
	}
	goAPI := httptest.NewServer(goStub(PlaneGoAPI))
	t.Cleanup(goAPI.Close)
	queryAPI := httptest.NewServer(goStub(PlaneQueryAPI))
	t.Cleanup(queryAPI.Close)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find a free port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	config, err := Render(contract, Options{
		Listen:    address,
		Upstreams: map[string]string{PlaneGoAPI: goAPI.Listener.Addr().String(), PlaneQueryAPI: queryAPI.Listener.Addr().String()},
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
	base := "http://" + address
	deadline := time.Now().Add(15 * time.Second)
	for {
		response, err := http.Get(base + "/")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return base
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("nginx did not answer 200 on %s within 15 s (last error: %v); its output:\n%s", base, err, log.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestRealNginxRoutesEveryPathToItsPlane sends every routing row through a
// real nginx and reads which stub plane answered. It also proves what the
// model cannot: nginx takes the file, the path nginx chooses on is the
// normalised one, the plane receives the request target as the client sent
// it, and the router adds no credential.
func TestRealNginxRoutesEveryPathToItsPlane(t *testing.T) {
	contract := checkedInContract(t)
	var base string
	if binary := os.Getenv(nginxBinaryEnv); binary != "" {
		base = startRouterAsProcess(t, binary, contract)
	} else {
		base = startRouterInContainers(t)
	}
	client := &http.Client{
		Timeout:       15 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	send := func(t *testing.T, method, target string, headers map[string]string) http.Header {
		t.Helper()
		request, err := http.NewRequest(method, base+target, nil)
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

	t.Run("the plane sees the client's Host and the router as one proxy hop", func(t *testing.T) {
		answer := send(t, http.MethodGet, "/graphql", map[string]string{
			"Host":            "api.router.test:8000",
			"X-Forwarded-For": "203.0.113.9",
			"X-Real-IP":       "203.0.113.9",
		})
		if got := answer.Get(stubHost); got != "api.router.test:8000" {
			t.Errorf("Host at the plane is %q, want the Host the client sent", got)
		}
		hops := strings.Split(answer.Get(stubForwardedFor), ", ")
		if len(hops) != 2 || hops[0] != "203.0.113.9" || net.ParseIP(hops[1]) == nil {
			t.Errorf("X-Forwarded-For at the plane is %q, want the client's value and then the router's peer", answer.Get(stubForwardedFor))
		}
		if got := answer.Get(stubRealIP); got == "203.0.113.9" || net.ParseIP(got) == nil {
			t.Errorf("X-Real-IP at the plane is %q, want the router's peer (a client must not choose it)", got)
		}
		if len(hops) == 2 && answer.Get(stubRealIP) != hops[1] {
			t.Errorf("X-Real-IP %q and the last X-Forwarded-For hop %q must both be the router's peer", answer.Get(stubRealIP), hops[1])
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
