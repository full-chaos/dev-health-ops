//go:build neverbuilt

package tagged

import nethttp "net/http"

func Tagged() *nethttp.Client { return &nethttp.Client{} }
