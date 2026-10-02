// Package decl is a fixture of the site walker: a zero-value client declared as a var, a field, an embedded field, a
// parameter and a result; a pointer is not one.
package decl

import nethttp "net/http"

var Zero nethttp.Client

type Holder struct {
	Field nethttp.Client
	nethttp.Client
	Pointer *nethttp.Client
}

func Param(c nethttp.Client, p *nethttp.Client) {}

func Result() nethttp.Client { return nethttp.Client{} }
