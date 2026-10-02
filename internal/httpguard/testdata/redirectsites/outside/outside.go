// Package outside stands for a replaced module's package in the fixtures: a config type with an exported *http.Client
// field that defaults to a following client when left nil.
package outside

import nethttp "net/http"

type Config struct {
	HTTPClient *nethttp.Client
	Name       string
}
