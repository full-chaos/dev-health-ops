// Package deploymentstatus holds the one meaning of a deployment's own status
// for every delivery metric (CHAOS-9621): change failure rate, deployment
// failure rate, deployment frequency and the deploy metrics of a day.
//
// Providers spell the same outcome differently (GitHub "failure", GitLab
// "failed"), and a push-ingested row may carry any string. Classify maps every
// stored value to one Class, so no metric keeps a private status list.
package deploymentstatus

import "strings"

// Class is what a deployment's own status says about its outcome.
type Class int

const (
	// Other is any status that is not a final outcome: in progress, queued,
	// pending, inactive (GitHub: superseded by a newer deployment), blocked
	// (GitLab: waits for approval), an unknown string, NULL or empty. Such a
	// deployment is a deployment of the day, and it is not failed.
	Other Class = iota
	// Succeeded is a deployment that finished well.
	Succeeded
	// Failed is a deployment that finished badly.
	Failed
	// Canceled is a deployment that never reached its environment. It is
	// neither failed nor a change: metrics do not count it at all.
	Canceled
)

// String names the class for logs.
func (c Class) String() string {
	switch c {
	case Succeeded:
		return "succeeded"
	case Failed:
		return "failed"
	case Canceled:
		return "canceled"
	default:
		return "other"
	}
}

// Terminal reports whether the class is a final outcome that counts as
// evidence for a rate: succeeded or failed. Canceled is not an outcome of a
// change, and Other has no outcome yet.
func (c Class) Terminal() bool {
	return c == Succeeded || c == Failed
}

// Classify maps one stored deployment status to its Class. Case and
// surrounding space do not matter.
func Classify(status string) Class {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "success":
		return Succeeded
	case "failure", "failed", "error":
		return Failed
	case "canceled", "cancelled":
		return Canceled
	default:
		return Other
	}
}
