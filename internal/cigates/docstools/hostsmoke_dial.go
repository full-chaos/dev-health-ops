package docstools

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// The checked response headers are read from the bytes the connection delivered, with the rules of Python's
// http.client.parse_headers (email compat32 parser), not from net/http: net/http trims the trailing space and tab of a
// value, Python keeps them, and a check that passes where Python failed would break "Go never accepts what Python
// refused". The dialer below puts a recording conn under every connection the transport uses; it also performs the
// proxy CONNECT itself (urllib: HTTPS_PROXY / NO_PROXY, Basic Proxy-Authorization from the proxy URL), offers no ALPN
// (urllib offers none, so a server cannot move the client to h2) and keeps HTTP/1.1.

const (
	// hsRecCap holds any header block Python accepts (101 lines of at most 65536 bytes, status line included).
	hsRecCap  = 8 << 20
	hsMaxLine = 65536
	hsMaxHdrs = 100
)

type hsRec struct {
	mu  sync.Mutex
	buf []byte
}

type hsRecConn struct {
	net.Conn
	rec *hsRec
}

func (c *hsRecConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.rec.mu.Lock()
		if room := hsRecCap - len(c.rec.buf); room > 0 {
			c.rec.buf = append(c.rec.buf, p[:min(n, room)]...)
		}
		c.rec.mu.Unlock()
	}
	return n, err
}

type hsTrace struct {
	mu   sync.Mutex
	recs []*hsRec
}

type hsTraceKey struct{}

func hsAttach(ctx context.Context, conn net.Conn) net.Conn {
	trace, ok := ctx.Value(hsTraceKey{}).(*hsTrace)
	if !ok {
		return conn
	}
	rec := &hsRec{}
	trace.mu.Lock()
	trace.recs = append(trace.recs, rec)
	trace.mu.Unlock()
	return &hsRecConn{Conn: conn, rec: rec}
}

func (t *hsTrace) last() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.recs) == 0 {
		return nil
	}
	rec := t.recs[len(t.recs)-1]
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]byte(nil), rec.buf...)
}

func hsDialPlain(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: hsTimeout}
	conn, err := dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return hsIdleConn{conn}, nil
}

// hsDialTLS dials addr (through an http proxy with CONNECT when the environment names one), runs the TLS handshake with
// the system roots and returns the recording conn.
func hsDialTLS(ctx context.Context, network, addr string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	proxy := hsProxyFor("https", addr)
	var conn net.Conn
	if proxy == nil {
		conn, err = hsDialPlain(ctx, network, addr)
	} else {
		conn, err = hsConnect(ctx, proxy, addr)
	}
	if err != nil {
		return nil, err
	}
	tlsConn := tls.Client(conn, &tls.Config{ServerName: host, RootCAs: hsRoots})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	return hsAttach(ctx, tlsConn), nil
}

// hsProxyEnv is urllib.request.getproxies_environment: variables named <scheme>_proxy; a lower-case name beats an upper-case
// one (two passes), an empty lower-case value removes the entry, REQUEST_METHOD drops http.
func hsProxyEnv() map[string]string {
	type entry struct{ name, value, key string }
	proxies := map[string]string{}
	var all []entry
	environ := map[string]string{}
	for _, kv := range os.Environ() {
		name, value, _ := strings.Cut(kv, "=")
		environ[name] = value
		if len(name) > 5 && name[len(name)-6] == '_' && strings.EqualFold(name[len(name)-5:], "proxy") {
			all = append(all, entry{name, value, strings.ToLower(name[:len(name)-6])})
			if value != "" {
				proxies[strings.ToLower(name[:len(name)-6])] = value
			}
		}
	}
	if _, cgi := environ["REQUEST_METHOD"]; cgi {
		delete(proxies, "http")
	}
	sort.Slice(all, func(i, j int) bool { return all[i].name < all[j].name })
	for _, e := range all {
		if strings.HasSuffix(e.name, "_proxy") {
			if e.value != "" {
				proxies[e.key] = e.value
			} else {
				delete(proxies, e.key)
			}
		}
	}
	return proxies
}

// hsBypass is proxy_bypass_environment.
func hsBypass(proxies map[string]string, host string) bool {
	noProxy, ok := proxies["no"]
	if !ok {
		return false
	}
	if noProxy == "*" {
		return true
	}
	host = strings.ToLower(host)
	hostOnly := host
	if i := strings.LastIndex(host, ":"); i >= 0 {
		if _, err := strconv.Atoi(host[i+1:]); err == nil || host[i+1:] == "" {
			hostOnly = host[:i]
		}
	}
	for _, name := range strings.Split(noProxy, ",") {
		name = strings.ToLower(strings.TrimLeft(strings.TrimSpace(name), "."))
		if name == "" {
			continue
		}
		if hostOnly == name || host == name || strings.HasSuffix(hostOnly, "."+name) || strings.HasSuffix(host, "."+name) {
			return true
		}
	}
	return false
}

// hsProxyFor is the proxy urllib's ProxyHandler picks for a URL of the given scheme and host[:port] ("host:port" from a
// dial): nil for none or a bypass. The proxy URL is read as urllib reads it (_parse_proxy): a scheme-less value is an
// authority, any scheme is ignored for an https target (urllib tunnels with a plain CONNECT whatever it says), the
// credentials count only when both user and password are present. A value it cannot read is no proxy here and the dial
// then fails like a refused connection would not: it returns a proxy that cannot be dialled.
func hsProxyFor(scheme, host string) *url.URL {
	proxies := hsProxyEnv()
	value, ok := proxies[scheme]
	if !ok {
		return nil
	}
	if hsBypass(proxies, host) {
		return nil
	}
	if i := strings.LastIndex(host, ":"); i >= 0 {
		if _, err := strconv.Atoi(host[i+1:]); err == nil {
			defaults := map[string]string{"https": "443", "http": "80"}
			if host[i+1:] == defaults[scheme] && hsBypass(proxies, host[:i]) {
				return nil
			}
		}
	}
	authority := value
	if _, rest, found := strings.Cut(value, ":"); found && strings.HasPrefix(rest, "/") {
		if !strings.HasPrefix(rest, "//") {
			return &url.URL{Scheme: "invalid", Host: value}
		}
		rest = rest[2:]
		end := len(rest)
		if at := strings.Index(rest, "@"); at >= 0 {
			if slash := strings.Index(rest[at:], "/"); slash >= 0 {
				end = at + slash
			}
		} else if slash := strings.Index(rest, "/"); slash >= 0 {
			end = slash
		}
		authority = rest[:end]
	}
	out := &url.URL{Scheme: "http"}
	if at := strings.LastIndex(authority, "@"); at >= 0 {
		userinfo := authority[:at]
		authority = authority[at+1:]
		user, password, hasPassword := strings.Cut(userinfo, ":")
		if hasPassword && user != "" && password != "" {
			u, _ := url.PathUnescape(user)
			p, _ := url.PathUnescape(password)
			out.User = url.UserPassword(u, p)
		}
	}
	if unescaped, err := url.PathUnescape(authority); err == nil {
		authority = unescaped
	}
	out.Host = authority
	return out
}

// hsConnect opens a CONNECT tunnel to addr through the proxy (plain HTTP to the proxy, as urllib).
func hsConnect(ctx context.Context, proxy *url.URL, addr string) (net.Conn, error) {
	if proxy.Scheme == "invalid" {
		return nil, fmt.Errorf("proxy URL with no authority: %q", proxy.Host)
	}
	proxyAddr := proxy.Host
	if proxy.Port() == "" {
		proxyAddr = net.JoinHostPort(strings.Trim(proxy.Host, "[]"), "443")
	}
	conn, err := hsDialPlain(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, err
	}
	request := "CONNECT " + addr + " HTTP/1.1\r\nHost: " + addr + "\r\n"
	if proxy.User != nil {
		password, _ := proxy.User.Password()
		request += "Proxy-Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte(proxy.User.Username()+":"+password)) + "\r\n"
	}
	if _, err := conn.Write([]byte(request + "\r\n")); err != nil {
		conn.Close()
		return nil, err
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		conn.Close()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK || reader.Buffered() > 0 {
		conn.Close()
		return nil, fmt.Errorf("Tunnel connection failed: %d %s", resp.StatusCode, strings.TrimSpace(strings.TrimPrefix(resp.Status, fmt.Sprint(resp.StatusCode))))
	}
	return conn, nil
}

func hsLatin1(b []byte) string {
	runes := make([]rune, len(b))
	for i, c := range b {
		runes[i] = rune(c)
	}
	return string(runes)
}

// hsRawHeaders parses the header block of the last (non-100) response in raw the way http.client.parse_headers does:
// at most 100 lines (the blank line counts) of at most 65536 bytes, the block ends at a blank line or at the end of the data; the email
// parser (compat32) then takes "name: value" lines, a line starting with space or tab continues the value (kept with
// its line ends), the value loses only its leading space/tab and trailing CR/LF, and a line that is neither stops the
// headers. Later duplicates win (the script builds a dict). ok is false when raw holds no complete status line.
func hsRawHeaders(raw []byte) (headers []hsHeader, ok bool, err error) {
	pos := 0
	readLine := func() (string, bool, error) {
		if pos >= len(raw) {
			return "", false, nil
		}
		end := pos
		for end < len(raw) && raw[end] != '\n' {
			end++
		}
		if end < len(raw) {
			end++
		}
		if end-pos > hsMaxLine {
			return "", false, errors.New("got more than 65536 bytes when reading header line")
		}
		line := hsLatin1(raw[pos:end])
		pos = end
		return line, true, nil
	}
	for {
		status, more, err := readLine()
		if err != nil || !more {
			return nil, false, err
		}
		fields := strings.Fields(status)
		var lines []string
		count := 0
		for {
			line, more, err := readLine()
			if err != nil {
				return nil, false, err
			}
			count++
			if count > hsMaxHdrs {
				return nil, false, errors.New("got more than 100 headers")
			}
			if !more && len(raw) >= hsRecCap {
				return nil, false, errors.New("header block longer than the recording")
			}
			if !more || line == "\r\n" || line == "\n" {
				break
			}
			lines = append(lines, line)
		}
		if len(fields) >= 2 && fields[1] == "100" {
			continue
		}
		return hsParseEmailHeaders(lines), true, nil
	}
}

func hsHeaderLine(line string) bool {
	for i := 0; i < len(line); i++ {
		c := line[i]
		if c == ':' {
			return true
		}
		if c < 0x21 || c > 0x7e {
			return false
		}
	}
	return false
}

type hsHeader struct{ name, value string }

func hsParseEmailHeaders(lines []string) []hsHeader {
	type header struct{ name, first, rest string }
	var parsed []header
	for _, line := range lines {
		if line[0] == ' ' || line[0] == '\t' {
			if len(parsed) > 0 {
				parsed[len(parsed)-1].rest += line
			}
			continue
		}
		if strings.HasPrefix(line, "From ") {
			continue
		}
		if !hsHeaderLine(line) {
			break
		}
		name, value, _ := strings.Cut(line, ":")
		parsed = append(parsed, header{name: name, first: value})
	}
	out := make([]hsHeader, 0, len(parsed))
	for _, h := range parsed {
		value := strings.TrimLeft(h.first, " \t") + h.rest
		out = append(out, hsHeader{strings.ToLower(h.name), strings.TrimRight(value, "\r\n")})
	}
	return out
}

// hsLastWins is the script's dict {key.lower(): value}: a later duplicate wins.
func hsLastWins(pairs []hsHeader) map[string]string {
	out := map[string]string{}
	for _, h := range pairs {
		out[h.name] = h.value
	}
	return out
}

// hsFirst is Message["name"]: the first header of that name.
func hsFirst(pairs []hsHeader, name string) (string, bool) {
	for _, h := range pairs {
		if h.name == name {
			return h.value, true
		}
	}
	return "", false
}

// hsLocationNeedsRequote reports whether urllib would change the Location of the redirect just answered before following
// it: HTTPRedirectHandler.http_error_302 runs urlunparse(urlparse(Location)) and quote(..., safe=string.punctuation) over
// it. Go follows the Location as net/http parses it, so a Location that urllib would rewrite (a byte outside printable
// ASCII, brackets, an uppercase scheme, a netloc without a path, an empty "?", "#" or ";") is refused instead: Go never
// follows a URL Python could have followed differently. Only the redirects of schemes urllib follows are asked.
func hsLocationNeedsRequote(ctx context.Context) (bool, error) {
	trace, _ := ctx.Value(hsTraceKey{}).(*hsTrace)
	if trace == nil {
		return false, nil
	}
	raw := trace.last()
	if raw == nil {
		return false, nil
	}
	pairs, ok, err := hsRawHeaders(raw)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	loc, found := hsFirst(pairs, "location")
	if !found {
		return false, nil
	}
	for i := 0; i < len(loc); i++ {
		if loc[i] < 0x21 || loc[i] > 0x7e || loc[i] == '[' || loc[i] == ']' {
			return true, nil
		}
	}
	rest := loc
	if m := hsSchemeRe.FindStringIndex(rest); m != nil {
		if rest[:m[1]-1] != strings.ToLower(rest[:m[1]-1]) {
			return true, nil
		}
		rest = rest[m[1]:]
	}
	if strings.HasPrefix(rest, "//") {
		after := rest[2:]
		end := strings.IndexAny(after, "/?#")
		if end < 0 || strings.ContainsAny(after[end:end+1], "?#") {
			return true, nil
		}
		rest = after[end:]
	}
	if i := strings.Index(rest, "#"); i >= 0 {
		if i == len(rest)-1 {
			return true, nil
		}
		rest = rest[:i]
	}
	if i := strings.Index(rest, "?"); i >= 0 {
		if i == len(rest)-1 {
			return true, nil
		}
		rest = rest[:i]
	}
	last := rest[strings.LastIndex(rest, "/")+1:]
	return strings.HasSuffix(last, ";"), nil
}
