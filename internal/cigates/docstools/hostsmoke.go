package docstools

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

// host-smoke: smoke-test the deployed documentation host and one legacy redirect. Was scripts/check_docs_cloudflare_host.py
// (CHAOS-10434). The retry loop stays in the workflow. Where Go is stricter than Python (origin and redirect-path
// grammar, argparse abbreviations, urllib vs net/http error text) the row is named in testdata/refusals/host-smoke.tsv.

func init() {
	register("host-smoke", hostSmokeVerb)
}

const hsUserAgent = "dev-health-docs-smoke/1"

// hsTimeout is urllib's timeout=20 (per socket operation, not total). hsRoots is nil (system roots, as Python's default
// SSL context) except in a unit test.
var (
	hsTimeout = 20 * time.Second
	hsRoots   *x509.CertPool
)

type hsResponse struct {
	status  int
	headers map[string]string
	body    []byte
}

type hsIdleConn struct{ net.Conn }

func (c hsIdleConn) Read(p []byte) (int, error) {
	_ = c.Conn.SetReadDeadline(time.Now().Add(hsTimeout))
	return c.Conn.Read(p)
}

func hsClient(follow bool) *http.Client {
	// Python urllib honours HTTPS_PROXY / NO_PROXY (checked on 3.14.7): the environment proxy is deliberate here. https
	// URLs go through hsDialTLS, which does the proxy CONNECT itself; the transport's proxy hook serves http URLs only.
	transport := httpguard.Transport()
	transport.Proxy = func(req *http.Request) (*url.URL, error) {
		if req.URL.Scheme == "https" {
			return nil, nil
		}
		proxy := hsProxyFor("http", req.URL.Host)
		if proxy != nil && proxy.Scheme == "invalid" {
			return nil, fmt.Errorf("proxy URL with no authority: %q", proxy.Host)
		}
		return proxy, nil
	}
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := hsDialPlain(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return hsAttach(ctx, conn), nil
	}
	transport.DialTLSContext = hsDialTLS
	transport.ResponseHeaderTimeout = hsTimeout
	transport.DisableKeepAlives = true
	transport.DisableCompression = true
	transport.ForceAttemptHTTP2 = false
	transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	client := &http.Client{Transport: transport}
	if !follow {
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		return client
	}
	// urllib.request.HTTPRedirectHandler: per request chain, at most 10 distinct URLs and at most 4 visits of one URL, http and https only.
	chains := map[*http.Request]map[string]int{}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if requote, err := hsLocationNeedsRequote(req.Context()); err != nil || requote {
			if err != nil {
				return err
			}
			return errors.New("location needs re-quote (urllib re-quotes a followed Location; this port does not)")
		}
		next := req.URL.String()
		if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
			return http.ErrUseLastResponse
		}
		visited := chains[via[0]]
		if visited == nil {
			visited = map[string]int{}
			chains[via[0]] = visited
		}
		if visited[next] >= 4 || len(visited) >= 10 {
			return http.ErrUseLastResponse
		}
		visited[next]++
		return nil
	}
	return client
}

// hsGet sends a GET; with follow false it uses the transport directly, so net/http never parses the Location header of a
// 3xx (urllib with NoRedirect does not either).
func hsGet(client *http.Client, follow bool, target string) (hsResponse, error) {
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		return hsResponse{}, err
	}
	trace := &hsTrace{}
	req = req.WithContext(context.WithValue(req.Context(), hsTraceKey{}, trace))
	req.Header.Set("User-Agent", hsUserAgent)
	req.Header.Set("Accept-Encoding", "identity")
	req.Close = true
	var resp *http.Response
	if follow {
		resp, err = client.Do(req)
	} else {
		resp, err = client.Transport.RoundTrip(req)
	}
	if err != nil {
		return hsResponse{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return hsResponse{}, err
	}
	headers := map[string]string{}
	for key, values := range resp.Header {
		headers[strings.ToLower(key)] = values[len(values)-1]
	}
	if raw := trace.last(); raw != nil {
		parsed, ok, err := hsRawHeaders(raw)
		if err != nil {
			return hsResponse{}, err
		}
		if ok {
			headers = hsLastWins(parsed)
		}
	}
	return hsResponse{status: resp.StatusCode, headers: headers, body: body}, nil
}

var (
	hsNetlocRe = regexp.MustCompile(`^[A-Za-z0-9._-]+(:[0-9]{1,5})?$`)
	hsPathRe   = regexp.MustCompile(`^[A-Za-z0-9._~!$&'()*+,;=@/-]*$`)
)

// hsOrigin is _origin: scheme "https" (any case) and a non-empty netloc, giving "https://<netloc>/". Stricter than
// urlparse: the netloc must be a plain host (letters, digits, ".", "_", "-") with an optional 1 to 5 digit port <= 65535.
func hsOrigin(value string) (string, error) {
	bad := fmt.Errorf("expected an HTTPS origin: %s", value)
	if len(value) < 8 || !strings.EqualFold(value[:8], "https://") {
		return "", bad
	}
	netloc := value[8:]
	if i := strings.IndexAny(netloc, "/?#"); i >= 0 {
		netloc = netloc[:i]
	}
	if !hsNetlocRe.MatchString(netloc) {
		return "", bad
	}
	if i := strings.LastIndex(netloc, ":"); i >= 0 {
		if port, _ := strconv.Atoi(netloc[i+1:]); port > 65535 {
			return "", bad
		}
	}
	return "https://" + netloc + "/", nil
}

// hsSafePath is the character set Go accepts in a redirect source or target path of the manifest (no scheme colon, query,
// fragment, "%" or space): stricter than urllib, which sends whatever urljoin gives it.
func hsSafePath(value string) bool { return hsPathRe.MatchString(strings.TrimLeft(value, "/")) }

// hsResolvePath is the path half of urllib.parse.urljoin("https://host/", ref) for a reference with no scheme, netloc,
// query or fragment (CPython 3.14.7): dot segments resolved, interior empty segments of a relative reference dropped.
func hsResolvePath(ref string) string {
	var segments []string
	if strings.HasPrefix(ref, "/") {
		segments = strings.Split(ref, "/")
	} else {
		parts := strings.Split(ref, "/")
		segments = []string{""}
		for i, part := range parts {
			if part != "" || i == len(parts)-1 {
				segments = append(segments, part)
			}
		}
	}
	var resolved []string
	for _, seg := range segments {
		switch seg {
		case "..":
			if len(resolved) > 0 {
				resolved = resolved[:len(resolved)-1]
			}
		case ".":
		default:
			resolved = append(resolved, seg)
		}
	}
	if last := segments[len(segments)-1]; last == "." || last == ".." {
		resolved = append(resolved, "")
	}
	out := strings.Join(resolved, "/")
	if out == "" {
		return "/"
	}
	if !strings.HasPrefix(out, "/") {
		out = "/" + out
	}
	return out
}

var hsSchemeRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*:`)

// hsJoin is urljoin(origin, ref) for an origin "https://<netloc>/". A reference it cannot answer exactly (a scheme other
// than https, brackets in a netloc, a query or fragment on a relative path) comes back as text that is not an origin URL,
// so a comparison with the expected URL fails: where Python would have refused as well, the same; where it would not, Go
// is stricter (named in the refusal table).
func hsJoin(origin, ref string) string {
	ref = strings.Map(func(r rune) rune {
		if r == '\t' || r == '\r' || r == '\n' {
			return -1
		}
		return r
	}, ref)
	ref = strings.TrimLeftFunc(ref, func(r rune) bool { return r <= ' ' })
	if ref == "" {
		return origin
	}
	if loc := hsSchemeRe.FindStringIndex(ref); loc != nil {
		if !strings.EqualFold(ref[:loc[1]-1], "https") {
			return ref
		}
		ref = ref[loc[1]:]
		if ref == "" {
			return origin
		}
	}
	if strings.HasPrefix(ref, "//") {
		rest := ref[2:]
		netloc := rest
		if i := strings.IndexAny(rest, "/?#"); i >= 0 {
			netloc = rest[:i]
		}
		if strings.ContainsAny(netloc, "[]") {
			return "\x00" + ref
		}
		return "https://" + rest
	}
	if strings.ContainsAny(ref, "?#") {
		return "\x00" + ref
	}
	return strings.TrimSuffix(origin, "/") + hsResolvePath(ref)
}

type hsArgs struct{ origin, redirects string }

func hsParseArgs(args []string) (hsArgs, string) {
	var out hsArgs
	seen := map[string]bool{}
	for i := 0; i < len(args); i++ {
		name, value, hasValue := strings.Cut(args[i], "=")
		if name != "--origin" && name != "--redirects" {
			return out, "unrecognized arguments: " + args[i]
		}
		if !hasValue {
			if i+1 >= len(args) || (strings.HasPrefix(args[i+1], "-") && args[i+1] != "-") {
				return out, "argument " + name + ": expected one argument"
			}
			i++
			value = args[i]
		}
		seen[name] = true
		if name == "--origin" {
			out.origin = value
		} else {
			out.redirects = value
		}
	}
	var missing []string
	for _, name := range []string{"--origin", "--redirects"} {
		if !seen[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return out, "the following arguments are required: " + strings.Join(missing, ", ")
	}
	return out, ""
}

// hsFirstRedirect is _first_redirect: the first DictReader row (tab delimited) whose source_path and target_path are
// both non-blank after strip().
func hsFirstRedirect(path string) (source, target string, err error) {
	name := a1PathStr(path)
	raw, rerr := os.ReadFile(name)
	if rerr != nil {
		_, msg := a1OSError(rerr)
		return "", "", errors.New(msg)
	}
	reader := &pyCSVReader{file: &pyTextFile{data: raw}}
	header, ok, rerr := reader.next()
	if rerr != nil {
		return "", "", rerr
	}
	if ok {
		for {
			row, more, rerr := reader.next()
			if rerr != nil {
				return "", "", rerr
			}
			if !more {
				break
			}
			if len(row) == 0 {
				continue
			}
			get := func(key string) string {
				value := ""
				for i, field := range header {
					if field == key && i < len(row) {
						value = row[i]
					} else if field == key {
						value = ""
					}
				}
				return a1Strip(value)
			}
			source, target = get("source_path"), get("target_path")
			if source != "" && target != "" {
				return source, target, nil
			}
		}
	}
	return "", "", fmt.Errorf("redirect manifest has no rows: %s", name)
}

func hostSmokeVerb(args []string, stdout, stderr io.Writer) int {
	parsed, problem := hsParseArgs(args)
	if problem != "" {
		fmt.Fprintln(stderr, "usage: cigate docs host-smoke --origin ORIGIN --redirects REDIRECTS")
		fmt.Fprintln(stderr, "cigate docs host-smoke: error: "+problem)
		return 2
	}
	fail := func(err error) int {
		fmt.Fprintf(stderr, "ERROR: %s\n", err)
		return 1
	}
	origin, err := hsOrigin(parsed.origin)
	if err != nil {
		return fail(err)
	}
	source, target, err := hsFirstRedirect(parsed.redirects)
	if err != nil {
		return fail(err)
	}
	if !hsSafePath(source) || !hsSafePath(target) {
		return fail(fmt.Errorf("unsupported redirect path: %s -> %s", source, target))
	}

	var errs []string
	checks := []struct {
		path   string
		status int
	}{
		{"/", 200},
		{"/use/", 200},
		{"/reference/metrics/weighting-and-aggregation/", 200},
		{"/phase-11-intentional-missing-page/", 404},
	}
	requestFailed := func(err error) {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		errs = append(errs, fmt.Sprintf("request failed: %s", err))
	}
	func() {
		var home *hsResponse
		follow := hsClient(true)
		for _, check := range checks {
			result, err := hsGet(follow, true, hsJoin(origin, strings.TrimLeft(check.path, "/")))
			if err != nil {
				requestFailed(err)
				return
			}
			if check.path == "/" {
				home = &result
			}
			if result.status != check.status {
				errs = append(errs, fmt.Sprintf("%s: expected HTTP %d, received %d", check.path, check.status, result.status))
			}
		}
		if home != nil {
			if strings.ToLower(home.headers["x-content-type-options"]) != "nosniff" {
				errs = append(errs, "/: X-Content-Type-Options is not nosniff")
			}
			if strings.ToLower(home.headers["referrer-policy"]) != "strict-origin-when-cross-origin" {
				errs = append(errs, "/: Referrer-Policy is not strict-origin-when-cross-origin")
			}
			if strings.Contains(strings.ToLower(home.headers["x-robots-tag"]), "noindex") {
				errs = append(errs, "/: production response unexpectedly has X-Robots-Tag noindex")
			}
			if !strings.Contains(string(home.body), "Dev Health") {
				errs = append(errs, "/: response body does not contain the Dev Health site identity")
			}
		}
		redirect, err := hsGet(hsClient(false), false, hsJoin(origin, strings.TrimLeft(source, "/")))
		if err != nil {
			requestFailed(err)
			return
		}
		if redirect.status != 301 && redirect.status != 308 {
			errs = append(errs, fmt.Sprintf("%s: expected a permanent redirect, received %d", source, redirect.status))
		}
		location := redirect.headers["location"]
		expected := hsJoin(origin, strings.TrimLeft(target, "/"))
		if strings.TrimRight(hsJoin(origin, location), "/") != strings.TrimRight(expected, "/") {
			errs = append(errs, fmt.Sprintf("%s: expected Location %s, found %s", source, expected, location))
		}
	}()

	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(stdout, "ERROR: %s\n", e)
		}
		return 1
	}
	fmt.Fprintln(stdout, "Cloudflare host smoke checks passed for canonical pages, 404 behavior, security headers, and a legacy redirect")
	return 0
}
