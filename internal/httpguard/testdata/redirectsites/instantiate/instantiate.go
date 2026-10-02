// Package instantiate is a fixture of the site walker: a generic helper instantiated with net/http.Client.
package instantiate

import nethttp "net/http"

func zero[T any]() *T { return new(T) }

func Make() *nethttp.Client { return zero[nethttp.Client]() }

func Array() *[1]nethttp.Client { return zero[[1]nethttp.Client]() }

func Pointer() **nethttp.Client { return zero[*nethttp.Client]() }
