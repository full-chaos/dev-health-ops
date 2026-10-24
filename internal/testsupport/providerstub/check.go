package providerstub

import "fmt"

// CheckRecorded is nil when requests holds at least min answered requests for provider and none is unmatched. A run that
// reached no stub, or reached it with a request no fixture names, fails here.
func CheckRecorded(requests []Recorded, provider string, min int) error {
	matched := 0
	for _, r := range requests {
		if !r.Matched {
			return fmt.Errorf("providerstub: an unmatched request reached the stub (provider %q, host %q, %s %s, status %d)", r.Provider, r.Host, r.Method, r.Path, r.Status)
		}
		if r.Provider == provider {
			matched++
		}
	}
	if matched < min {
		return fmt.Errorf("providerstub: %d matched %s requests were recorded, want at least %d: the request never reached the stub", matched, provider, min)
	}
	return nil
}
