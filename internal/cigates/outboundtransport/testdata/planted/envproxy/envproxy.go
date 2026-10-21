package envproxy

import (
	"net/http"
	"net/url"

	"golang.org/x/net/http/httpproxy"
)

func Env() func(*http.Request) (*url.URL, error) { return http.ProxyFromEnvironment }

func Fixed(u *url.URL) func(*http.Request) (*url.URL, error) { return http.ProxyURL(u) }

func Config() *httpproxy.Config { return httpproxy.FromEnvironment() }
