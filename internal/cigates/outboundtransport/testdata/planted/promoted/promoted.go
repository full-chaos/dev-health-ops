package promoted

import (
	"net/http"
	"net/url"
	"time"
)

type Holder struct{ *http.Client }

type ProxyHolder struct{ *http.Transport }

type NamedPointer *http.Client

func pair() (http.RoundTripper, error) { return nil, nil }

func PromotedNil(h Holder) { h.Transport = nil }

func PromotedUnproven(h Holder, rt http.RoundTripper) { h.Transport = rt }

func PromotedAddress(h Holder) {
	slot := &h.Transport
	*slot = nil
}

func PromotedTuple(h Holder) error {
	var err error
	h.Transport, err = pair()
	return err
}

func NamedPointerWrite(p NamedPointer) { p.Transport = nil }

func RangeWrite(h Holder, transports []http.RoundTripper) {
	for _, h.Transport = range transports {
	}
}

func PromotedProxy(t ProxyHolder) { t.Proxy = nil }

func PromotedProxySet(t ProxyHolder) { t.Proxy = proxyFunc }

func ProxyAddress(t ProxyHolder) {
	slot := &t.Proxy
	_ = slot
}

func OverwriteTransport(t, other *http.Transport) { *t = *other }

func OverwriteClient(h Holder) { *h.Client = http.Client{Transport: &http.Transport{}} }

func FieldOfDereference(h Holder) time.Duration { return (*h.Client).Timeout }

var proxyFunc = func(*http.Request) (*url.URL, error) { return nil, nil }
