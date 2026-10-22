package reverseproxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"
)

var zero httputil.ReverseProxy

type Holder struct {
	proxy httputil.ReverseProxy
}

func Bare() *httputil.ReverseProxy { return &httputil.ReverseProxy{Director: func(*http.Request) {}} }

func NilTransport() *httputil.ReverseProxy {
	return &httputil.ReverseProxy{Transport: nil}
}

func Single(target *url.URL) *httputil.ReverseProxy {
	return httputil.NewSingleHostReverseProxy(target)
}

func Param(next http.RoundTripper) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{Transport: next}
}

func NewZero() *httputil.ReverseProxy { return new(httputil.ReverseProxy) }

func LocalVar() *httputil.ReverseProxy {
	var p httputil.ReverseProxy
	return &p
}

func NamedResult() (p httputil.ReverseProxy) { return }

func Slice() []httputil.ReverseProxy { return nil }

func NilAssign(p *httputil.ReverseProxy) { p.Transport = nil }

func Assign(p *httputil.ReverseProxy, next http.RoundTripper) { p.Transport = next }

func Address(p *httputil.ReverseProxy) *http.RoundTripper { return &p.Transport }

func Overwrite(p *httputil.ReverseProxy) { *p = httputil.ReverseProxy{} }

func Zero() *httputil.ReverseProxy { return &zero }

func Serve(p *httputil.ReverseProxy, w http.ResponseWriter, r *http.Request) { p.ServeHTTP(w, r) }

func ServeValue(p httputil.ReverseProxy) {}

type Handler struct{}

func (Handler) Method(p *httputil.ReverseProxy) {}

func Literal() {
	_ = func(p *httputil.ReverseProxy) {}
}

func Copy(p *httputil.ReverseProxy) httputil.ReverseProxy { return *p }

func CopyVar(p *httputil.ReverseProxy) {
	c := *p
	_ = c
}
