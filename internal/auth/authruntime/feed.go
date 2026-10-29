package authruntime

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/revisionfeed"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
)

// revisionFeed builds the outbox sink and the subscriber when AUTH_REVISION_FEED=on, and nothing when it is off (the
// default): the outbox loop then keeps the nil publisher it has always had, no client is built and nothing dials. When on,
// the Valkey client dials on first use, so an unreachable Valkey leaves the process running: the sink answers
// consumer_unavailable (events wait in the outbox) and the subscriber reconnects with backoff.
//
// No decision cache exists yet (5.8), so the consumer only keeps its watermarks and metrics (NopInvalidator).
func revisionFeed(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
) (*audit.Publisher, []lifecycle.Component, error) {
	if !cfg.RevisionFeed {
		return nil, nil, nil
	}
	config := valkey.DefaultConfig(cfg.ValkeyURI.Reveal())
	config.ClientName = valkeyClientName
	client := revisionfeed.NewLazyClient(func(ctx context.Context) (valkeygo.Client, error) { return valkey.Open(ctx, config) })
	sink, err := revisionfeed.NewSink(client)
	if err != nil {
		return nil, nil, err
	}
	publisher, err := audit.NewPublisher(pool, sink, audit.PublisherConfig{Schema: schema, Logger: logger})
	if err != nil {
		return nil, nil, err
	}
	consumer, err := revisionfeed.NewConsumer(revisionfeed.Config{Invalidator: revisionfeed.NopInvalidator{}, Logger: logger})
	if err != nil {
		return nil, nil, err
	}
	subscriber, err := revisionfeed.NewSubscriber(revisionfeed.SubscriberConfig{Client: client, Consumer: consumer, Logger: logger})
	if err != nil {
		return nil, nil, err
	}
	return publisher, []lifecycle.Component{revisionfeed.NewComponent(subscriber, client.Close)}, nil
}
