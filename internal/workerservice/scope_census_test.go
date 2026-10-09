package workerservice

import "context"

// soleIntegrationCensus is the scope census of a test whose collector is the
// organization's only integration of its provider: no other active one.
type soleIntegrationCensus struct{}

func (soleIntegrationCensus) CountActiveSiblingIntegrations(context.Context, string, string, string) (int, error) {
	return 0, nil
}
