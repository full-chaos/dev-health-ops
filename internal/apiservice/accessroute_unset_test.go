package apiservice

import "path/filepath"

// issuerUnsetGolden was recorded on origin/main f831161db0, before go-api
// routed access tokens by endpoint profile. It is frozen: its writer (the
// issuer-unset listener) is gone.
var issuerUnsetGolden = filepath.Join("testdata", "accessroute", "issuer_unset.golden.json")
