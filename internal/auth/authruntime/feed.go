package authruntime

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/auth/revisionfeed"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/storage/valkey"
)

// feedParts are what the revision feed builds: the outbox publisher, the subscriber component, and the decision cache the
// authorize routes ask. All are nil when the feed is off.
type feedParts struct {
	publisher  *audit.Publisher
	components []lifecycle.Component
	decisions  *decisionFeed
}

// decisionFeed is the decision cache, kept current by the consumer (its invalidator), with the subscriber's readiness.
type decisionFeed struct {
	cache    *decisioncache.Cache
	ready    func() bool
	consumer *revisionfeed.Consumer
}

// consumerMarks reads the consumer's watermarks, which exist only once the consumer (built with the cache as its
// invalidator) does.
type consumerMarks struct{ marks *revisionfeed.Watermarks }

func (c *consumerMarks) Latest(aggregateType, aggregateID string) (int64, bool) {
	return c.marks.Latest(aggregateType, aggregateID)
}

// revisionFeed builds the outbox sink, the subscriber and the decision cache when AUTH_REVISION_FEED=on, and nothing when
// it is off (the default): the outbox loop then keeps the nil publisher it has always had, no client is built, nothing
// dials and the authorize routes decide without a cache. When on, the Valkey client dials on first use, so an unreachable
// Valkey leaves the process running: the sink answers consumer_unavailable (events wait in the outbox), the subscriber
// reconnects with backoff, and the cache is not asked until the subscriber is ready.
func revisionFeed(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
) (feedParts, error) {
	return revisionFeedWith(cfg, logger, pool, schema, valkey.Open)
}

// revisionFeedWith is revisionFeed with the Valkey dialer given (tests count dials).
func revisionFeedWith(
	cfg authconfig.Config, logger *slog.Logger, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier,
	dial func(context.Context, valkey.Config) (valkeygo.Client, error),
) (feedParts, error) {
	if !cfg.RevisionFeed {
		return feedParts{}, nil
	}
	config := valkey.DefaultConfig(cfg.ValkeyURI.Reveal())
	config.ClientName = valkeyClientName
	client := revisionfeed.NewLazyClient(func(ctx context.Context) (valkeygo.Client, error) { return dial(ctx, config) })
	// The fence's census requires every outbox consumer to be a floor input. The registry is per process: this one is
	// declared here, and the restore-fence CLI registers its own with its own login (a later leaf).
	restorefence.RegisterFloorInput(revisionfeed.NewFloorInput(client))
	sink, err := revisionfeed.NewSink(client)
	if err != nil {
		return feedParts{}, err
	}
	publisher, err := audit.NewPublisher(pool, sink, audit.PublisherConfig{Schema: schema, Logger: logger})
	if err != nil {
		return feedParts{}, err
	}
	marks := &consumerMarks{}
	cache, err := decisioncache.New(decisioncache.Config{Catalog: policycatalog.Default(), Watermarks: marks})
	if err != nil {
		return feedParts{}, err
	}
	consumer, err := revisionfeed.NewConsumer(revisionfeed.Config{Invalidator: cache, Logger: logger})
	if err != nil {
		return feedParts{}, err
	}
	marks.marks = consumer.Watermarks()
	subscriber, err := revisionfeed.NewSubscriber(revisionfeed.SubscriberConfig{Client: client, Consumer: consumer, Logger: logger})
	if err != nil {
		return feedParts{}, err
	}
	return feedParts{
		publisher:  publisher,
		components: []lifecycle.Component{revisionfeed.NewComponent(subscriber, client.Close)},
		decisions:  &decisionFeed{cache: cache, ready: subscriber.Ready, consumer: consumer},
	}, nil
}
