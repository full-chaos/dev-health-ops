// Package syncvenue is the venue oracle of POST /integrations/{id}/sync: the
// real Python api, which plans the run in the request, against the Go api,
// which hands the run to the scheduler (synchandoff) while the real
// NativeMaterializer runs against its copy of the database. It lives in its
// own package so its containers and its process-wide state are its own.
package syncvenue
