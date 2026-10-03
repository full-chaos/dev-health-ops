// Package typedecl is a fixture of the site walker: a type alias and a defined type of http.Client.
package typedecl

import nethttp "net/http"

type Alias = nethttp.Client

type Named nethttp.Client

type Other struct{ n int }
