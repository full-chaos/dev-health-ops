package reconcilerservice

// transientDialError marks a readiness failure that a later attempt can outlast: the endpoint
// refused, reset or dropped the connection, or the pooler had no free slot. It satisfies
// health.TransientError, so the registry reads cause=unreachable. It has the same shape as
// workerservice's transientDialError.
type transientDialError struct{ cause error }

func (e transientDialError) Error() string { return e.cause.Error() }
func (e transientDialError) Unwrap() error { return e.cause }
func (transientDialError) Transient() bool { return true }
