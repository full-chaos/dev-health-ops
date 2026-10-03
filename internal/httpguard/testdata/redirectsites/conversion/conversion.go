// Package conversion is a fixture of the site walker: a defined type converted to and from net/http.Client.
package conversion

import nethttp "net/http"

type shaped nethttp.Client

func ToClient(x *shaped) *nethttp.Client { return (*nethttp.Client)(x) }

func FromClient(c *nethttp.Client) *shaped { return (*shaped)(c) }
