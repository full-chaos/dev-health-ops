// Package outside stands for a replaced module's package in the fixtures: a config type with an exported *http.Client
// field that defaults to a following client when left nil.
package outside

import nethttp "net/http"

type Config struct {
	HTTPClient *nethttp.Client
	Name       string
}

// Hidden has only an unexported *http.Client field: not a carrier (its client cannot be set from outside, so a literal
// cannot "leave it unset").
type Hidden struct {
	client *nethttp.Client
	Name   string
}
