package authruntime

import (
	"net/http"
	"time"
)

// preFaultControlBody is the pre-fault control login of the stall test: the route refuses it at the schema (the address is
// not an e-mail address) with a 422, writes the refusal's audit row (a database write through the proxy) and runs no
// password hash. A wrong-password login runs one bcrypt-12 compare that takes no context and can take as long as the whole
// 4 s request timeout under -race on a loaded host.
const preFaultControlBody = `{"email":"not-an-email-address","password":"wrong"}`

// preFaultControlAddress is a client address the stall test's logins never use (they use 198.51.100.1 to .250).
const preFaultControlAddress = "198.51.100.251"

// firstLoginAnswer returns the first answer to the process's first login that
// is not the retryable 503. The first login pays one-time costs (pool connect,
// key-state read, the bcrypt dummy hash); on a loaded host under -race they can
// pass the request timeout, and the service then answers its retryable 503
// (Retry-After 1). A later request is warm. It waits pause between tries and
// stops at guard, returning the last 503. Any other status is returned at once.
func firstLoginAnswer(login func() int, guard, pause time.Duration, sleep func(time.Duration)) int {
	spent := time.Duration(0)
	for {
		status := login()
		if status != http.StatusServiceUnavailable || spent >= guard {
			return status
		}
		sleep(pause)
		spent += pause
	}
}
