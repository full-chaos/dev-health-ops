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

var Arr [1]nethttp.Client

var InHolder Holder

func Makes() {
	_ = make([]nethttp.Client, 1)
	_ = new([2]nethttp.Client)
	_ = make(map[string]*nethttp.Client)
}

var Mp map[string]nethttp.Client

var Ch chan nethttp.Client

var Deep [1][1][1][1][1]nethttp.Client

type Level1 struct {
	L2 struct {
		L3 struct{ L4 struct{ L5 nethttp.Client } }
	}
}
