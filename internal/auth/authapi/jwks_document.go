package authapi

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/token"
)

// JWKS document bounds: a refresh starts every JWKSRefreshEvery, one read is
// cut at JWKSReadTimeout, and a document is served for at most JWKSMaxStale
// after the read that built it began. A rotation or a compromise therefore
// reaches the document within one refresh, and a hung key store can hold an
// old one for JWKSMaxStale at most.
const (
	JWKSRefreshEvery = time.Second
	JWKSReadTimeout  = 2 * time.Second
	JWKSMaxStale     = 30 * time.Second
)

// CompromiseLatch reports, from memory only, whether a key was ever seen
// compromised (*token.Signer).
type CompromiseLatch interface {
	Compromised(kid string, public ed25519.PublicKey) bool
}

// JWKSDocumentConfig configures a JWKSDocument. Zero durations take the
// defaults above; a nil Now is time.Now. When Keys is a CompromiseLatch, every
// request checks the document against it.
type JWKSDocumentConfig struct {
	Keys         KeySetSource
	RefreshEvery time.Duration
	ReadTimeout  time.Duration
	MaxStale     time.Duration
	Now          func() time.Time
	Logger       *slog.Logger
}

// jwksSnapshot is one rendered document and the keys it publishes.
type jwksSnapshot struct {
	raw  []byte
	etag string
	at   time.Time
	keys []token.Key
}

// JWKSDocument is the last-known JWKS document. A background refresher reads
// the key set and swaps an atomic pointer to the rendered document; a request
// only loads the pointer and checks it against the compromise latch in memory,
// so no request waits on the key store. It is a lifecycle component: Start
// reads once and starts the refresher, Shutdown stops it.
//
// Latch contract: no response whose latch check runs after the latch is set
// carries the pre-latch document. A request whose check ran before the latch is
// served as if a moment earlier; after the latch, exposure of a compromised key
// is bounded by the verifiers' cache lifetime (Cache-Control max-age =
// token.JWKSCacheTTL, 300 s), not by this handler.
type JWKSDocument struct {
	keys         KeySetSource
	latch        CompromiseLatch
	refreshEvery time.Duration
	readTimeout  time.Duration
	maxStale     time.Duration
	now          func() time.Time
	logger       *slog.Logger

	current atomic.Pointer[jwksSnapshot]
	kick    chan struct{}

	// reason is the last refresh outcome, logged on change only; refreshes
	// run one at a time under refreshMu.
	refreshMu sync.Mutex
	reason    string

	mu      sync.Mutex
	started bool
	closed  bool
	stop    context.CancelFunc
	done    chan struct{}
}

// NewJWKSDocument returns an empty document over config.Keys: it answers 503
// until a read succeeds.
func NewJWKSDocument(config JWKSDocumentConfig) *JWKSDocument {
	d := &JWKSDocument{
		keys: config.Keys, refreshEvery: config.RefreshEvery, readTimeout: config.ReadTimeout,
		maxStale: config.MaxStale, now: config.Now, logger: config.Logger, kick: make(chan struct{}, 1),
	}
	d.latch, _ = config.Keys.(CompromiseLatch)
	if d.refreshEvery <= 0 {
		d.refreshEvery = JWKSRefreshEvery
	}
	if d.readTimeout <= 0 {
		d.readTimeout = JWKSReadTimeout
	}
	if d.maxStale <= 0 {
		d.maxStale = JWKSMaxStale
	}
	if d.now == nil {
		d.now = time.Now
	}
	if d.logger == nil {
		d.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return d
}

func (d *JWKSDocument) Name() string { return "auth-jwks-document" }

// Start reads the key set once, so the API serves a document from its first
// request when the store answers, and starts the refresher. A failed first read
// does not stop the service: the route answers 503 until a refresh lands.
func (d *JWKSDocument) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.started || d.closed {
		d.mu.Unlock()
		return nil
	}
	d.started = true
	base, stop := context.WithCancel(context.WithoutCancel(ctx))
	d.stop, d.done = stop, make(chan struct{})
	d.mu.Unlock()
	d.refresh(base)
	go d.run(base, d.done)
	return nil
}

// Shutdown stops the refresher, ending a read in flight, and waits for it
// within ctx. It is safe without Start and idempotent.
func (d *JWKSDocument) Shutdown(ctx context.Context) error {
	d.mu.Lock()
	d.closed = true
	stop, done := d.stop, d.done
	d.mu.Unlock()
	if stop == nil {
		return nil
	}
	stop()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *JWKSDocument) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(d.refreshEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-d.kick:
		}
		d.refresh(ctx)
	}
}

// refresh reads the key set once, bounded by the read timeout, and swaps in
// the rendered document. A failed read keeps the document held.
func (d *JWKSDocument) refresh(ctx context.Context) {
	d.refreshMu.Lock()
	defer d.refreshMu.Unlock()
	at := d.now()
	read, cancel := context.WithTimeout(ctx, d.readTimeout)
	set, err := d.keys.KeySet(read)
	cancel()
	if err != nil {
		if ctx.Err() == nil {
			d.note(ctx, token.ReasonOf(err))
		}
		return
	}
	snapshot, err := renderJWKS(set, at)
	if err != nil {
		d.note(ctx, token.ReasonOf(err))
		return
	}
	d.current.Store(snapshot)
	d.note(ctx, "")
}

// note logs a refresh outcome when it differs from the last one.
func (d *JWKSDocument) note(ctx context.Context, reason string) {
	if reason == d.reason {
		return
	}
	d.reason = reason
	if reason == "" {
		d.logger.InfoContext(ctx, "auth well-known: key set refresh recovered", slog.String("route", "jwks"))
		return
	}
	d.logger.ErrorContext(ctx, "auth well-known: key set refresh failed; serving the last-known document within its bound",
		slog.String("route", "jwks"), slog.String("reason_code", reason))
}

// load is the request path: memory only. It returns nil when no document is
// held, when it is older than the bound, or when a key it publishes is latched
// compromised; that last case drops the document and starts a refresh.
func (d *JWKSDocument) load() *jwksSnapshot {
	if d == nil {
		return nil
	}
	snapshot := d.current.Load()
	if snapshot == nil {
		return nil
	}
	if d.latch != nil {
		for _, key := range snapshot.keys {
			if d.latch.Compromised(key.KeyID, key.PublicKey) {
				if d.current.CompareAndSwap(snapshot, nil) {
					d.logger.Error("auth well-known: a published key is latched compromised; the key set document is dropped",
						slog.String("route", "jwks"))
				}
				d.requestRefresh()
				return nil
			}
		}
	}
	if d.now().Sub(snapshot.at) > d.maxStale {
		return nil
	}
	return snapshot
}

func (d *JWKSDocument) requestRefresh() {
	select {
	case d.kick <- struct{}{}:
	default:
	}
}

func renderJWKS(set token.KeySet, at time.Time) (*jwksSnapshot, error) {
	published := set.Published()
	doc := jwksDocument{Keys: make([]jwk, 0, len(published))}
	for _, key := range published {
		doc.Keys = append(doc.Keys, jwk{
			Kty: "OKP", Crv: "Ed25519", Alg: key.Algorithm, Use: "sig", Kid: key.KeyID,
			X: base64.RawURLEncoding.EncodeToString(key.PublicKey),
		})
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return &jwksSnapshot{raw: raw, etag: etagOf(raw), at: at, keys: published}, nil
}
