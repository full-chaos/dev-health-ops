// Package literal is a fixture of the site walker: every way to build a client by literal, through an alias or a dot import.
package literal

import (
	. "net/http"
	nethttp "net/http"
)

func Aliased() *nethttp.Client { return &nethttp.Client{} }

func Dot() *Client { return &Client{} }

func Elided() {
	_ = []nethttp.Client{{}}
	_ = map[string]nethttp.Client{"a": {}}
}
