package tagged

import nethttp "net/http"

func Windows() *nethttp.Client { return &nethttp.Client{} }
