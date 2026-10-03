// Package unloaded is imported by a fixture but is NOT one of the loaded fixture packages: its function is not in the walker's index.
package unloaded

import nethttp "net/http"

func Check(*nethttp.Request, []*nethttp.Request) error { return nethttp.ErrUseLastResponse }
