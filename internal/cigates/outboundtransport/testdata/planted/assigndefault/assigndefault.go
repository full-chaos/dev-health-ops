package assigndefault

import "net/http"

func Replace(rt http.RoundTripper) { http.DefaultTransport = rt }
