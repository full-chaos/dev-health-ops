// Package newclient is a fixture of the site walker: new(http.Client).
package newclient

import nethttp "net/http"

func Make() *nethttp.Client { return new(nethttp.Client) }
