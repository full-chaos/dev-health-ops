package ingressplanes

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// RouterImage is the nginx image the real-nginx routing test runs the
// generated configuration in. The compose router must use this same reference:
// a routing result is proven only for the nginx build that was tested. It is
// NGINX's own unprivileged image (user 101, nginx 1.30.5 at this digest), from
// ghcr.io so that CI does not pull it from Docker Hub.
const RouterImage = "ghcr.io/nginx/nginx-unprivileged:1.30-alpine@sha256:ed04ec1ff34502c339ee5c3ae3f855442398edc1d05591e2b98981dcbbd20b1e"

const (
	// RouterPort is the port the router listens on in its container; the
	// compose stack publishes it as host port 8000.
	RouterPort = "8000"
	// GoAPIUpstream and QueryAPIUpstream are the compose service names and the
	// public listener ports of the two planes (go-api --api-addr=:8000,
	// query-api --query-addr=:8090).
	GoAPIUpstream    = "go-api:8000"
	QueryAPIUpstream = "query-api:8090"
	// DockerResolver is Docker's embedded DNS server, which answers a compose
	// service name on every user-defined network.
	DockerResolver = "127.0.0.11"
)

// Options are the parts of the configuration that are not routing: where the
// router listens, where each plane is, how a plane's name is resolved, and the
// directory nginx may write to. DefaultOptions is what the checked-in file is
// generated with; a test may point the same locations at stub upstreams.
type Options struct {
	// Listen is the argument of the listen directive.
	Listen string
	// Upstreams gives each plane its host:port.
	Upstreams map[string]string
	// Resolver is the DNS server nginx asks for a plane's name at request
	// time. Empty writes no resolver directive (upstreams that are addresses
	// need none).
	Resolver string
	// RunDir holds the pid file and the temporary files of nginx.
	RunDir string
}

// DefaultOptions are the options of the checked-in compose router file.
func DefaultOptions() Options {
	return Options{
		Listen:    RouterPort,
		Upstreams: map[string]string{PlaneGoAPI: GoAPIUpstream, PlaneQueryAPI: QueryAPIUpstream},
		Resolver:  DockerResolver,
		RunDir:    "/tmp",
	}
}

// Header is one request header the router writes for the plane.
type Header struct {
	Name  string
	Value string // an nginx value: a variable of the connection or of the request
}

// ForwardedHeaders are the headers the router sets on every proxied request,
// with the values ingress-nginx sends for a direct client with its default
// configuration (use-forwarded-headers and compute-full-forwarded-for off;
// nginx.tmpl of controller-v1.14.5): the Host the client sent, and every
// forwarded header from the router's own connection. A client-sent
// X-Forwarded-For is replaced with the peer address, and is kept only under
// X-Original-Forwarded-For, as ingress-nginx keeps it.
var ForwardedHeaders = []Header{
	{"Host", "$http_host"},
	{"X-Real-IP", "$remote_addr"},
	{"X-Forwarded-For", "$remote_addr"},
	{"X-Forwarded-Host", "$http_host"},
	{"X-Forwarded-Port", "$server_port"},
	{"X-Forwarded-Proto", "$scheme"},
	{"X-Forwarded-Scheme", "$scheme"},
	{"X-Scheme", "$scheme"},
	{"X-Original-Forwarded-For", "$http_x_forwarded_for"},
	{"X-Original-Forwarded-Host", "$http_x_forwarded_host"},
}

// Location is one nginx location of the router: the text after the keyword
// `location` and the plane the location proxies to.
type Location struct {
	// Match is the location's modifier and pattern as ingress-nginx writes
	// them for a host in regex mode: `~* "^<path>"`.
	Match string
	Plane string
}

// Locations returns the locations of a valid contract in the order
// ingress-nginx writes them: one for each of Rules. A public host rule gets
// none: Validate holds that the default rule gives its paths the same plane.
//
// Form: ingress-nginx's buildLocation writes `~* "^<path>"` for EVERY path of
// a server that has one use-regex location, whatever its path type, with
// backslashes and double quotes of the path escaped. So an anchored path stays
// anchored, the match ignores case, and the default rule "/" becomes `^/`.
//
// Order: nginx tries regex locations in the order of the file and takes the
// first that matches. ingress-nginx sorts the locations of a server by path,
// descending, and then (stable) by path length, descending: the longest path
// is first and the default rule (one character) is last.
func Locations(c Contract) []Location {
	rules := append([]Rule(nil), c.Rules...)
	sort.SliceStable(rules, func(i, j int) bool { return rules[i].Path > rules[j].Path })
	sort.SliceStable(rules, func(i, j int) bool { return len(rules[i].Path) > len(rules[j].Path) })
	locations := make([]Location, 0, len(rules))
	for _, rule := range rules {
		locations = append(locations, Location{Match: `~* "^` + quoteLocationPath(rule.Path) + `"`, Plane: rule.Plane})
	}
	return locations
}

// quoteLocationPath escapes a path for a double-quoted nginx string the way
// ingress-nginx's sanitizeQuotedRegex does: a backslash before every backslash
// and double quote. nginx removes that backslash again when it reads the file.
func quoteLocationPath(path string) string {
	var out strings.Builder
	for _, r := range path {
		if r == '\\' || r == '"' {
			out.WriteByte('\\')
		}
		out.WriteRune(r)
	}
	return out.String()
}

// planeVariable is the nginx variable that holds a plane's upstream.
func planeVariable(plane string) string {
	return "$plane_" + strings.ReplaceAll(plane, "-", "_")
}

// Render writes the complete nginx configuration of the router for a contract.
// The same contract and options always give the same bytes.
func Render(c Contract, o Options) ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if o.Listen == "" || o.RunDir == "" {
		return nil, errors.New("render the router configuration: Listen and RunDir are required")
	}
	for _, plane := range Planes {
		if o.Upstreams[plane] == "" {
			return nil, fmt.Errorf("render the router configuration: no upstream for plane %s", plane)
		}
	}

	var b strings.Builder
	line := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }

	line("# GENERATED from %s. Do not edit this file.", ContractPath)
	line("# To change a route: change the contract, then run from the repository root")
	line("#   go run ./internal/ingressplanes/cmd/routerconf")
	line("# A Go test (internal/ingressplanes) fails when this file and the contract disagree.")
	line("#")
	line("# The router of the self-hosted compose stack. One location per rule of the contract, in the")
	line("# form and the order ingress-nginx writes them for a host in regex mode, so a path reaches the")
	line("# plane it reaches in prod. The paths that only the public host lists get the default plane there,")
	line("# so they need no location here. The router adds no credential: it never sets Authorization and")
	line("# never reads a cookie. See contracts/ingress/v1/README.md.")
	line("")
	line("worker_processes auto;")
	line("error_log /dev/stderr notice;")
	line("pid %s/nginx.pid;", o.RunDir)
	line("")
	line("events {")
	line("    worker_connections 1024;")
	line("}")
	line("")
	line("http {")
	line("    # The path without the query string: a query string can hold a one-time code.")
	line("    log_format plane '$remote_addr [$time_iso8601] \"$request_method $uri\" $status $body_bytes_sent upstream=$upstream_addr time=$request_time';")
	line("    access_log /dev/stdout plane;")
	line("    server_tokens off;")
	line("")
	line("    # nginx writes only below this directory, so the router runs as any user id.")
	for _, name := range []string{"client_body", "proxy", "fastcgi", "uwsgi", "scgi"} {
		line("    %s_temp_path %s/%s_temp;", name, o.RunDir, name)
	}
	if o.Resolver != "" {
		line("")
		line("    # A plane is named through a variable, so nginx asks this resolver for the name at request")
		line("    # time: the router starts before the planes, and finds a plane again after it was recreated.")
		line("    resolver %s valid=10s ipv6=off;", o.Resolver)
	}
	line("")
	line("    server {")
	line("        listen %s;", o.Listen)
	line("        server_name _;")
	line("")
	line("        client_max_body_size 50m;")
	line("        proxy_http_version 1.1;")
	line("        proxy_buffering off;")
	line("        proxy_read_timeout 60s;")
	line("        proxy_send_timeout 60s;")
	line("")
	line("        # The router owns the forwarded headers, as ingress-nginx does with its defaults: X-Real-IP")
	line("        # and each X-Forwarded-* are written from the connection the router sees, so a value the")
	line("        # client sent for them never reaches a plane. X-Forwarded-For is replaced, not added to;")
	line("        # what the client sent is kept only under X-Original-Forwarded-*.")
	for _, header := range ForwardedHeaders {
		line("        proxy_set_header %s %s;", header.Name, header.Value)
	}
	line("")
	line("        # The two planes. Each variable is one constant, set here and nowhere else: nothing a")
	line("        # request sends can change where a location proxies to. They are variables only so that")
	line("        # nginx resolves the name when a request comes, not when it starts.")
	for _, plane := range Planes {
		line("        set %s \"%s\";", planeVariable(plane), o.Upstreams[plane])
	}
	for _, location := range Locations(c) {
		line("")
		line("        location %s {", location.Match)
		line("            proxy_pass http://%s;", planeVariable(location.Plane))
		line("        }")
	}
	line("    }")
	line("}")
	return []byte(b.String()), nil
}
