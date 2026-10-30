package revisionfeed

import (
	"context"
	"errors"
	"sync"

	valkeygo "github.com/valkey-io/valkey-go"
)

// Channel is the pub/sub channel the sink publishes on and every replica subscribes to. Messages are
// "<aggregate_type>:<aggregate_id>:<revision>". STABLE: the name sits under the auth ACL user's channel pattern &auth_svc_*.
const Channel = "auth_svc_rev"

// HighWaterKey is the Valkey hash that holds, per "<aggregate_type>:<aggregate_id>", the highest revision the sink has
// delivered. It is written only by the sink's script (max, never lower) and has no TTL. STABLE: it sits under the auth ACL
// user's key pattern ~dho:rl:auth_svc_*, and the CHAOS-11271 restore fence reads it through PublishedFloors.
const HighWaterKey = "dho:rl:auth_svc_rev_hwm"

var (
	// ErrUnavailable: Valkey could not be reached or did not answer. The text never carries the cause: a connection
	// error can quote the URI, which holds the password.
	ErrUnavailable = errors.New("revisionfeed: valkey unavailable")
	// ErrRejected: Valkey answered with an error reply (an ACL refusal, a script error).
	ErrRejected = errors.New("revisionfeed: valkey refused the command")
)

// SubscribeHooks are what Subscribe calls: OnSubscribed when the server has confirmed the subscription (it is called again
// after every reconnect that Subscribe itself survives), OnMessage for every message body.
type SubscribeHooks struct {
	OnSubscribed func()
	OnMessage    func(string)
}

// Client is the narrow Valkey surface the feed uses. Every method returns nil, ErrUnavailable or ErrRejected (or the
// context's error mapped to ErrUnavailable) and never an error that carries the server's text.
type Client interface {
	// EvalInt runs a script (EVAL, never EVALSHA) and returns its integer result.
	EvalInt(ctx context.Context, script string, keys, args []string) (int64, error)
	// HGetAll reads a whole hash.
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	// ConfigGet reads one configuration parameter (CONFIG GET).
	ConfigGet(ctx context.Context, name string) (string, error)
	// Info reads INFO for the sections and returns its field:value lines.
	Info(ctx context.Context, sections ...string) (map[string]string, error)
	// TTL is the key's TTL in seconds: -1 no expiry, -2 no such key.
	TTL(ctx context.Context, key string) (int64, error)
	// Subscribe subscribes to channel and blocks until ctx ends or the connection breaks.
	Subscribe(ctx context.Context, channel string, hooks SubscribeHooks) error
}

func classify(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrUnavailable), errors.Is(err, ErrRejected):
		return err
	}
	if _, ok := valkeygo.IsValkeyErr(err); ok {
		return ErrRejected
	}
	return ErrUnavailable
}

type valkeyClient struct {
	client  valkeygo.Client
	mu      sync.Mutex
	scripts map[string]*valkeygo.Lua
}

// NewClient adapts a valkey-go client.
func NewClient(client valkeygo.Client) Client {
	return &valkeyClient{client: client, scripts: map[string]*valkeygo.Lua{}}
}

func (c *valkeyClient) script(text string) *valkeygo.Lua {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.scripts[text]; ok {
		return s
	}
	s := valkeygo.NewLuaScriptNoSha(text)
	c.scripts[text] = s
	return s
}

func (c *valkeyClient) EvalInt(ctx context.Context, script string, keys, args []string) (int64, error) {
	n, err := c.script(script).Exec(ctx, c.client, keys, args).AsInt64()
	return n, classify(err)
}

func (c *valkeyClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	m, err := c.client.Do(ctx, c.client.B().Hgetall().Key(key).Build()).AsStrMap()
	return m, classify(err)
}

func (c *valkeyClient) ConfigGet(ctx context.Context, name string) (string, error) {
	m, err := c.client.Do(ctx, c.client.B().ConfigGet().Parameter(name).Build()).AsStrMap()
	if err != nil {
		return "", classify(err)
	}
	return m[name], nil
}

func (c *valkeyClient) Info(context.Context, ...string) (map[string]string, error) {
	return nil, ErrUnavailable
}

func (c *valkeyClient) TTL(ctx context.Context, key string) (int64, error) {
	n, err := c.client.Do(ctx, c.client.B().Ttl().Key(key).Build()).AsInt64()
	return n, classify(err)
}

func (c *valkeyClient) Subscribe(ctx context.Context, channel string, hooks SubscribeHooks) error {
	dedicated, release := c.client.Dedicate()
	defer release()
	wait := dedicated.SetPubSubHooks(valkeygo.PubSubHooks{
		OnMessage: func(m valkeygo.PubSubMessage) {
			if m.Channel == channel && hooks.OnMessage != nil {
				hooks.OnMessage(m.Message)
			}
		},
		OnSubscription: func(s valkeygo.PubSubSubscription) {
			if s.Kind == "subscribe" && s.Channel == channel && hooks.OnSubscribed != nil {
				hooks.OnSubscribed()
			}
		},
	})
	if err := dedicated.Do(ctx, dedicated.B().Subscribe().Channel(channel).Build()).Error(); err != nil {
		return classify(err)
	}
	select {
	case err := <-wait:
		if err == nil {
			return ErrUnavailable
		}
		return classify(err)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LazyClient dials on first use, as the rate-limit store does: an unreachable Valkey leaves the process running, and every
// call answers ErrUnavailable until it answers.
type LazyClient struct {
	open func(context.Context) (valkeygo.Client, error)

	mu     sync.Mutex
	client Client
	raw    valkeygo.Client
}

// NewLazyClient returns a client that calls open on first use.
func NewLazyClient(open func(context.Context) (valkeygo.Client, error)) *LazyClient {
	return &LazyClient{open: open}
}

func (l *LazyClient) get(ctx context.Context) (Client, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.client != nil {
		return l.client, nil
	}
	raw, err := l.open(ctx)
	if err != nil {
		return nil, ErrUnavailable
	}
	l.raw, l.client = raw, NewClient(raw)
	return l.client, nil
}

func (l *LazyClient) EvalInt(ctx context.Context, script string, keys, args []string) (int64, error) {
	c, err := l.get(ctx)
	if err != nil {
		return 0, err
	}
	return c.EvalInt(ctx, script, keys, args)
}

func (l *LazyClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	c, err := l.get(ctx)
	if err != nil {
		return nil, err
	}
	return c.HGetAll(ctx, key)
}

func (l *LazyClient) ConfigGet(ctx context.Context, name string) (string, error) {
	c, err := l.get(ctx)
	if err != nil {
		return "", err
	}
	return c.ConfigGet(ctx, name)
}

func (l *LazyClient) Info(ctx context.Context, sections ...string) (map[string]string, error) {
	c, err := l.get(ctx)
	if err != nil {
		return nil, err
	}
	return c.Info(ctx, sections...)
}

func (l *LazyClient) TTL(ctx context.Context, key string) (int64, error) {
	c, err := l.get(ctx)
	if err != nil {
		return 0, err
	}
	return c.TTL(ctx, key)
}

func (l *LazyClient) Subscribe(ctx context.Context, channel string, hooks SubscribeHooks) error {
	c, err := l.get(ctx)
	if err != nil {
		return err
	}
	return c.Subscribe(ctx, channel, hooks)
}

// Close closes the connection if one was opened; later calls dial again.
func (l *LazyClient) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.raw != nil {
		l.raw.Close()
		l.raw, l.client = nil, nil
	}
}
