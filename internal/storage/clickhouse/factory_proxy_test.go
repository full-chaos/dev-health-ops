package clickhouse

import (
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestHTTPDSNUsesNoProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://env-proxy.invalid:3128")
	t.Setenv("HTTPS_PROXY", "http://env-proxy.invalid:3128")
	config := DefaultConfig("http://ch.example.invalid:8123/default?http_proxy=http%3A%2F%2Fdsn-proxy.invalid%3A3128")
	config.DialTimeout, config.ReadTimeout = time.Second, time.Second

	options, err := clickHouseOptions(config)
	if err != nil {
		t.Fatal(err)
	}
	if options.HTTPProxyURL != nil {
		t.Fatalf("HTTPProxyURL = %v, want nil", options.HTTPProxyURL)
	}
	if options.TransportFunc == nil {
		t.Fatal("TransportFunc is nil: the driver would keep http.ProxyFromEnvironment")
	}
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment}
	if _, err := options.TransportFunc(transport); err != nil {
		t.Fatal(err)
	}
	if transport.Proxy != nil {
		request := &http.Request{URL: &url.URL{Scheme: "http", Host: "ch.example.invalid:8123"}}
		proxy, _ := transport.Proxy(request)
		t.Fatalf("transport proxy still set, resolves to %v", proxy)
	}
}
