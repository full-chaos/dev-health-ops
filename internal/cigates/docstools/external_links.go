package docstools

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// check-external-links: every absolute http(s) link of a built docs site answers, unless an unexpired allowlist entry
// covers it; a link to the site itself (same origin as --site-url) must resolve to a built page and anchor instead;
// an expired allowlist entry fails even when the link answers. Was scripts/check_external_links.py. The fetcher is
// urllib's, as the script used it: HEAD then GET, redirects only to http(s), 3 tries, and the URLs urllib's
// http.client refuses (a control character or a non-ASCII byte in the path, a non-numeric port) stop the run as the
// script's traceback did. It never uses a proxy from the environment: elTransport sets none (cigate does not link
// internal/httpguard, whose fallback logs through slog, so the binary stays a silent one).

func init() { register("check-external-links", externalLinksVerb) }

var (
	elHref = regexp.MustCompile(`<a[^>` + puWord + `](?:[^>]*[^>` + puWord + `])?href="(https?://[^"]+)"`)
	elID   = regexp.MustCompile(`id="([^"]+)"`)
)

const (
	elUserAgent  = "Mozilla/5.0 (compatible; dev-health-docs-link-check/1.0; +https://github.com/full-chaos/dev-health-ops)"
	elDefaultURL = "https://docs.fullchaos.dev"
)

// elFetcher answers one URL: ok, the detail, or a fatal error (the script stopped with a traceback).
type elFetcher func(url string) (bool, string, error)

type elEntry struct {
	owner, reason string
	expires       time.Time
}

func elLoadAllowlist(path string) (map[string]elEntry, error) {
	entries := map[string]elEntry{}
	if !puIsFile(path) {
		return entries, nil
	}
	text, err := a1ReadText(path)
	if err != nil {
		return nil, err
	}
	raw, err := ylLoad(text)
	if err != nil {
		return nil, err
	}
	if raw.falsy() {
		return entries, nil
	}
	if raw.kind != ylList {
		return nil, puValueError{path + " must be a YAML list of entries"}
	}
	for _, item := range raw.items {
		if item.kind != ylMap {
			return nil, puValueError{fmt.Sprintf("%s entries must be mappings, got %s", path, item.repr())}
		}
		u, owner, reason, expires := item.get("url"), item.get("owner"), item.get("reason"), item.get("expires")
		if !ylIsStr(u) || !ylIsStr(owner) || !ylIsStr(reason) || !ylIsStr(expires) {
			return nil, puValueError{fmt.Sprintf("%s entry missing required string fields: %s", path, item.repr())}
		}
		day, err := pdFromISOFormat(expires.s)
		if err != nil {
			return nil, err
		}
		entries[u.s] = elEntry{owner: owner.s, reason: reason.s, expires: day}
	}
	return entries, nil
}

type elOrigin struct {
	scheme, host string
	port         int
}

// elOriginOf is the script's _origin: nil for no hostname or an invalid port.
func elOriginOf(raw string) (*elOrigin, error) {
	u, err := puURLSplit(raw)
	if err != nil {
		return nil, err
	}
	host, ok := puHostname(u.netloc)
	if !ok {
		return nil, nil
	}
	port, _, perr := puPort(u.netloc)
	if perr != nil {
		return nil, nil
	}
	if port == 0 {
		port = 80
		if u.scheme == "https" {
			port = 443
		}
	}
	return &elOrigin{scheme: u.scheme, host: host, port: port}, nil
}

// elSameSite is _same_site_error: handled=false when the link is not on the site; else the error ("" for none).
func elSameSite(siteDir, raw, siteURL string) (msg string, handled bool, err error) {
	parsed, err := puURLSplit(raw)
	if err != nil {
		return "", false, err
	}
	o, err := elOriginOf(raw)
	if err != nil {
		return "", false, err
	}
	so, err := elOriginOf(siteURL)
	if err != nil {
		return "", false, err
	}
	if o == nil || so == nil || *o != *so {
		return "", false, nil
	}
	dest, err := puRealpath(puJoin(siteDir, strings.TrimLeft(puUnquote(parsed.path), "/")))
	if err != nil {
		return "", false, err
	}
	root, err := puRealpath(siteDir)
	if err != nil {
		return "", false, err
	}
	if !puWithin(dest, root) {
		return "missing built page for " + raw, true, nil
	}
	if puIsDir(dest) {
		dest += "/index.html"
	}
	if !puIsFile(dest) {
		return "missing built page for " + raw, true, nil
	}
	anchor := puUnquote(parsed.fragment)
	if anchor == "" {
		return "", true, nil
	}
	text, err := a1ReadText(dest)
	if err != nil {
		return "", false, err
	}
	for _, id := range puFindAllAfterBoundary(elID, text) {
		if id == anchor {
			return "", true, nil
		}
	}
	return fmt.Sprintf("missing anchor '%s' for %s", anchor, raw), true, nil
}

// elGlobHTML is sorted(site_dir.rglob("*.html")).
func elGlobHTML(siteDir string) ([]string, error) {
	walkRoot := siteDir + "/"
	var files []string
	err := filepath.WalkDir(walkRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if path == walkRoot {
				return fs.SkipAll
			}
			return err
		}
		if path != walkRoot && strings.HasSuffix(d.Name(), ".html") {
			files = append(files, path)
		}
		return nil
	})
	sort.Slice(files, func(i, j int) bool { return a1PartsLess(files[i], files[j]) })
	return files, err
}

func elRel(siteDir, path string) string {
	rel, err := filepath.Rel(apPathStr(siteDir), apPathStr(path))
	if err != nil {
		return path
	}
	return rel
}

func elCheck(siteDir, allowlistPath string, fetch elFetcher, today time.Time, maxRetries int, siteURL string) ([]string, error) {
	allowlist, err := elLoadAllowlist(allowlistPath)
	if err != nil {
		return nil, err
	}
	files, err := elGlobHTML(siteDir)
	if err != nil {
		return nil, err
	}
	var errs []string
	seen := map[string]bool{}
	for _, file := range files {
		text, err := a1ReadText(file)
		if err != nil {
			return nil, err
		}
		source := elRel(siteDir, file)
		for _, m := range elHref.FindAllStringSubmatch(text, -1) {
			link := m[1]
			if seen[link] {
				continue
			}
			seen[link] = true
			entry, listed := allowlist[link]
			if listed && entry.expires.Before(today) {
				errs = append(errs, fmt.Sprintf("%s: allowlist entry for %s expired on %s (owner: %s, reason: %s)", source, link, pdISO(entry.expires), entry.owner, entry.reason))
				continue
			}
			msg, handled, err := elSameSite(siteDir, link, siteURL)
			if err != nil {
				return nil, err
			}
			if handled {
				if msg != "" {
					errs = append(errs, source+": "+msg)
				}
				continue
			}
			if listed {
				continue
			}
			ok, detail := false, "no attempts made"
			for i := 0; i < maxRetries && !ok; i++ {
				if ok, detail, err = fetch(link); err != nil {
					return nil, err
				}
			}
			if !ok {
				errs = append(errs, fmt.Sprintf("%s: broken external link %s: %s", source, link, detail))
			}
		}
	}
	return errs, nil
}

// elHTTPFetcher is the script's default_fetcher over rt.
func elHTTPFetcher(rt http.RoundTripper, timeout time.Duration) elFetcher {
	return func(raw string) (bool, string, error) {
		u, err := puURLSplit(raw)
		if err != nil {
			return false, "", err
		}
		if _, ok := puHostname(u.netloc); !ok || u.scheme != "http" && u.scheme != "https" {
			return false, "unsupported URL scheme", nil
		}
		detail := ""
		for _, method := range []string{"HEAD", "GET"} {
			status, reqErr, fatal := elOpen(rt, timeout, method, raw)
			if fatal != nil {
				return false, "", fatal
			}
			switch {
			case reqErr != nil:
				detail = fmt.Sprintf("<urlopen error %v> (%s)", reqErr, method)
			case status < 400:
				return true, fmt.Sprintf("status %d (%s)", status, method), nil
			default:
				detail = fmt.Sprintf("status %d (%s)", status, method)
			}
		}
		return false, detail, nil
	}
}

var elDisallowed = regexp.MustCompile(`[\x00-\x20\x7f]`)

// elRequest is how urllib.request.Request and http.client.HTTPConnection read a URL.
type elRequest struct {
	scheme, host, selector, hostHeader string
	port                               int
}

// elTarget reads raw as urllib does: noHost is its "no host given" URLError; fatal is the exception the script died of
// (InvalidURL for a non-numeric port or a control character, UnicodeEncodeError for a non-ASCII request target or a
// Host header outside Latin-1).
func elTarget(method, raw string) (req elRequest, noHost bool, fatal error) {
	full := a1Strip(raw)
	if strings.HasPrefix(full, "<") && strings.HasSuffix(full, ">") {
		full = a1Strip(full[1 : len(full)-1])
	}
	if strings.HasPrefix(full, "URL:") {
		full = a1Strip(full[4:])
	}
	if i := strings.LastIndex(full, "#"); i >= 0 {
		full = full[:i]
	}
	colon := strings.IndexByte(full, ':')
	if colon <= 0 || strings.Contains(full[:colon], "/") {
		return req, false, puValueError{"unknown url type: " + a1Repr(full)}
	}
	req.scheme = strings.ToLower(full[:colon])
	rest := full[colon+1:]
	if !strings.HasPrefix(rest, "//") {
		return req, true, nil
	}
	rest = rest[2:]
	end := len(rest)
	if k := strings.IndexAny(rest, "/#?"); k >= 0 {
		end = k
	}
	hostport, selector := puUnquote(rest[:end]), rest[end:]
	if selector != "" && selector[0] != '/' {
		selector = "/" + selector
	}
	if hostport == "" {
		return req, true, nil
	}
	req.hostHeader = hostport
	req.port = 80
	if req.scheme == "https" {
		req.port = 443
	}
	req.host = hostport
	if i, j := strings.LastIndex(hostport, ":"), strings.LastIndex(hostport, "]"); i > j {
		p := hostport[i+1:]
		if n, ok := elPyInt(p); ok {
			req.port = n
		} else if p != "" {
			return req, false, ylError{"http.client.InvalidURL", "nonnumeric port: '" + p + "'"}
		}
		req.host = hostport[:i]
	}
	if strings.HasPrefix(req.host, "[") && strings.HasSuffix(req.host, "]") {
		req.host = req.host[1 : len(req.host)-1]
	}
	if m := elDisallowed.FindString(req.host); m != "" {
		return req, false, ylError{"http.client.InvalidURL", fmt.Sprintf("URL can't contain control characters. %s (found at least %s)", a1Repr(req.host), a1Repr(m))}
	}
	if selector == "" {
		selector = "/"
	}
	req.selector = selector
	if m := elDisallowed.FindString(selector); m != "" {
		return req, false, ylError{"http.client.InvalidURL", fmt.Sprintf("URL can't contain control characters. %s (found at least %s)", a1Repr(selector), a1Repr(m))}
	}
	if err := elEncodeError("ascii", method+" "+selector+" HTTP/1.1", 0x7F, "128"); err != nil {
		return req, false, err
	}
	if err := elEncodeError("latin-1", hostport, 0xFF, "256"); err != nil {
		return req, false, err
	}
	return req, false, nil
}

// elEncodeError is the UnicodeEncodeError of s.encode(codec) for a codec whose last code point is max: the first run
// of characters above it, by character position.
func elEncodeError(codec, s string, max rune, limit string) error {
	rs := []rune(s)
	for i := 0; i < len(rs); i++ {
		if rs[i] <= max {
			continue
		}
		j := i
		for j+1 < len(rs) && rs[j+1] > max {
			j++
		}
		esc := fmt.Sprintf(`\x%02x`, rs[i])
		if rs[i] > 0xFFFF {
			esc = fmt.Sprintf(`\U%08x`, rs[i])
		} else if rs[i] > 0xFF {
			esc = fmt.Sprintf(`\u%04x`, rs[i])
		}
		what := fmt.Sprintf("character '%s' in position %d", esc, i)
		if j > i {
			what = fmt.Sprintf("characters in position %d-%d", i, j)
		}
		return ylError{"UnicodeEncodeError", fmt.Sprintf("'%s' codec can't encode %s: ordinal not in range(%s)", codec, what, limit)}
	}
	return nil
}

// elPyInt is int(s) for a port: surrounding white space, a sign, decimal digits of any script, single underscores
// between digits.
func elPyInt(s string) (int, bool) {
	t := []rune(a1Strip(s))
	neg := false
	if len(t) > 0 && (t[0] == '+' || t[0] == '-') {
		neg, t = t[0] == '-', t[1:]
	}
	if len(t) == 0 || t[0] == '_' || t[len(t)-1] == '_' {
		return 0, false
	}
	n := 0
	for i, r := range t {
		if r == '_' {
			if t[i-1] == '_' {
				return 0, false
			}
			continue
		}
		d, ok := elDigit(r)
		if !ok {
			return 0, false
		}
		if n = n*10 + d; n > 1<<20 {
			n = 1 << 20
		}
	}
	if neg {
		n = -n
	}
	return n, true
}

func elDigit(r rune) (int, bool) {
	if !unicode.Is(unicode.Nd, r) {
		return 0, false
	}
	// Decimal digits come in runs of ten from a zero; a block of runs starts at a zero too.
	start := r
	for unicode.Is(unicode.Nd, start-1) {
		start--
	}
	return int(r-start) % 10, true
}

const (
	elMaxRepeats   = 4
	elMaxRedirects = 10
)

// elOpen is urllib's opener.open for one method: the final status, a URLError (OSError) the script caught, or a
// fatal exception. Redirects (301, 302, 303, 307, 308 with Location or URI) are followed as HTTPRedirectHandler does,
// to http(s) only (the script's handler); a loop or a refused target ends as an HTTPError with the redirect code.
func elOpen(rt http.RoundTripper, timeout time.Duration, method, raw string) (int, error, error) {
	visited := map[string]int{}
	current := raw
	for {
		t, noHost, fatal := elTarget(method, current)
		if fatal != nil {
			return 0, nil, fatal
		}
		if noHost {
			return 0, errors.New("no host given"), nil
		}
		if t.port < 0 || t.port > 65535 {
			return 0, fmt.Errorf("port %d is out of range 0-65535", t.port), nil
		}
		req := &http.Request{
			Method: method,
			URL:    &url.URL{Scheme: t.scheme, Host: net.JoinHostPort(t.host, strconv.Itoa(t.port)), Opaque: t.selector},
			Header: http.Header{"User-Agent": {elUserAgent}},
			Host:   t.hostHeader,
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		resp, err := rt.RoundTrip(req.WithContext(ctx))
		if err != nil {
			cancel()
			return 0, err, nil
		}
		_ = resp.Body.Close()
		cancel()
		code := resp.StatusCode
		switch code {
		case 301, 302, 303, 307, 308:
		default:
			return code, nil, nil
		}
		location, has := resp.Header["Location"]
		if !has {
			if location, has = resp.Header["Uri"]; !has {
				return code, nil, nil
			}
		}
		next, allowed, err := elRedirectTarget(current, location[0])
		var ve puValueError
		switch {
		case errors.As(err, &ve):
			return 0, nil, err
		case err != nil:
			return 0, err, nil
		case !allowed:
			return code, nil, nil
		}
		target, err := puURLSplit(next)
		if err != nil {
			return 0, nil, err
		}
		if _, hasHost := puHostname(target.netloc); !hasHost || target.scheme != "http" && target.scheme != "https" {
			return 0, errors.New("unsupported redirect URL scheme"), nil
		}
		if visited[next] >= elMaxRepeats || len(visited) >= elMaxRedirects {
			return code, nil, nil
		}
		visited[next]++
		current = strings.ReplaceAll(next, " ", "%20")
	}
}

// elRedirectTarget is http_error_302's new URL: an empty path with a netloc becomes "/", the URL is percent-quoted as
// Latin-1 bytes with string.punctuation safe, then joined to the request URL. allowed=false when the target is not
// http, https, ftp or relative (urllib answers with an HTTPError carrying the redirect code); err when Go cannot read
// the URL (stricter: the link is reported, never accepted).
func elRedirectTarget(current, location string) (next string, allowed bool, err error) {
	loc := location
	if u, splitErr := puURLSplit(loc); splitErr != nil {
		return "", false, splitErr
	} else if u.path == "" && u.netloc != "" {
		loc = "//" + u.netloc + "/"
		if u.scheme != "" {
			loc = u.scheme + ":" + loc
		}
		if u.query != "" {
			loc += "?" + u.query
		}
		if u.fragment != "" {
			loc += "#" + u.fragment
		}
	}
	var b strings.Builder
	for i := 0; i < len(loc); i++ {
		if c := loc[i]; c > 0x20 && c < 0x7f {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	base, err := url.Parse(current)
	if err != nil {
		return "", false, fmt.Errorf("cannot join a redirect to %q: %v", current, err)
	}
	ref, err := url.Parse(b.String())
	if err != nil {
		return "", false, fmt.Errorf("cannot follow the redirect to %q: %v", location, err)
	}
	next = base.ResolveReference(ref).String()
	t, err := puURLSplit(next)
	if err != nil {
		return "", false, err
	}
	switch t.scheme {
	case "http", "https", "ftp", "":
		return next, true, nil
	}
	return next, false, nil
}

func externalLinksVerb(args []string, stdout, stderr io.Writer) int {
	return externalLinksRun(args, stdout, stderr, frToday(), elHTTPFetcher(elTransport(), 10*time.Second))
}

func externalLinksRun(args []string, stdout, stderr io.Writer, today time.Time, fetch elFetcher) int {
	p := apParser{
		prog:        "cigate docs check-external-links",
		description: "Check external links in a built docs site.",
		opts: []apOpt{
			{flag: "--built-site", metavar: "BUILT_SITE", required: true},
			{flag: "--allowlist", metavar: "ALLOWLIST"},
			{flag: "--site-url", metavar: "SITE_URL"},
		},
	}
	vals, rc, done := p.parse(args, stdout, stderr)
	if done {
		return rc
	}
	root, err := puRealpath(".")
	if err != nil {
		return puTracebackError(stderr, err)
	}
	site := apPathStr(vals["--built-site"])
	allowlist := root + "/docs-data/external-link-allowlist.yml"
	if v, ok := vals["--allowlist"]; ok {
		allowlist = apPathStr(v)
	}
	siteURL := elDefaultURL
	if v, ok := vals["--site-url"]; ok {
		siteURL = v
	}
	if !puIsDir(site) {
		fmt.Fprintf(stderr, "ERROR: built site directory not found: %s\n", site)
		return 1
	}
	errs, err := elCheck(site, allowlist, fetch, today, 3, siteURL)
	if err != nil {
		return puTracebackError(stderr, err)
	}
	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(stdout, "ERROR: %s\n", e)
		}
		return 1
	}
	fmt.Fprintln(stdout, "External link check passed")
	return 0
}

// elTransport is net/http's default transport settings with no proxy (Proxy nil): HTTP_PROXY and HTTPS_PROXY are not
// read, as internal/httpguard.Transport does.
func elTransport() *http.Transport {
	return &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}
