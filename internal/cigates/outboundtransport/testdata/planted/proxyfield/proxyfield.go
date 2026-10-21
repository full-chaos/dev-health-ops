package proxyfield

import (
	"net/http"
	"net/url"
)

func Literal(proxy func(*http.Request) (*url.URL, error)) *http.Transport {
	return &http.Transport{Proxy: proxy}
}
