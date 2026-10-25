package faultproxy

import (
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	chunkSize   = 32 << 10
	slowPiece   = 1024
	dialTimeout = 5 * time.Second
)

// Config configures a proxy. Events, when set, receives one JSON line per
// mode change: the mode, the offset from start and the wall clock.
type Config struct {
	Listen   string
	Upstream string
	MaxConns int
	Events   io.Writer
}

// Stats are counters only; no payload, address or header is kept.
type Stats struct {
	Accepted  int64 `json:"accepted"`
	Refused   int64 `json:"refused"`
	Live      int64 `json:"live"`
	BytesUp   int64 `json:"bytes_up"`
	BytesDown int64 `json:"bytes_down"`
}

// Proxy forwards TCP connections to one upstream and applies the current
// setting to new and live connections.
type Proxy struct {
	cfg      Config
	ln       net.Listener
	start    time.Time
	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	mu      sync.Mutex
	cur     Setting
	live    map[net.Conn]struct{}
	clients int
	stopped bool

	evMu sync.Mutex

	accepted, refused, bytesUp, bytesDown atomic.Int64
}

// New starts listening and accepting. Stop must be called.
func New(cfg Config) (*Proxy, error) {
	if cfg.MaxConns <= 0 {
		return nil, errors.New("max conns must be positive")
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, err
	}
	p := &Proxy{cfg: cfg, ln: ln, start: time.Now(), stop: make(chan struct{}),
		cur: Setting{Mode: Pass}, live: map[net.Conn]struct{}{}}
	p.event(p.cur)
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

// Addr is the listen address.
func (p *Proxy) Addr() net.Addr { return p.ln.Addr() }

// Current is the setting in force.
func (p *Proxy) Current() Setting {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cur
}

// Stats returns the counters.
func (p *Proxy) Stats() Stats {
	p.mu.Lock()
	live := int64(p.clients)
	p.mu.Unlock()
	return Stats{Accepted: p.accepted.Load(), Refused: p.refused.Load(), Live: live,
		BytesUp: p.bytesUp.Load(), BytesDown: p.bytesDown.Load()}
}

// Set switches the fault. Cut and Drop also close every live connection (Drop
// with a reset). An invalid setting is ignored; callers validate first.
func (p *Proxy) Set(s Setting) {
	if s.Validate() != nil {
		return
	}
	p.mu.Lock()
	p.cur = s
	var victims []net.Conn
	if s.Mode == Cut || s.Mode == Drop {
		for c := range p.live {
			victims = append(victims, c)
		}
	}
	p.mu.Unlock()
	for _, c := range victims {
		closeConn(c, s.Mode == Drop)
	}
	p.event(s)
}

// Schedule applies steps at their offsets from now. It ends with the proxy.
func (p *Proxy) Schedule(steps []Step) {
	begin := time.Now()
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for _, st := range steps {
			t := time.NewTimer(time.Until(begin.Add(st.At)))
			select {
			case <-t.C:
				p.Set(st.Set)
			case <-p.stop:
				t.Stop()
				return
			}
		}
	}()
}

// Stop closes the listener and every connection and waits for all goroutines.
func (p *Proxy) Stop() {
	p.stopOnce.Do(func() {
		close(p.stop)
		p.ln.Close()
		p.mu.Lock()
		p.stopped = true
		var all []net.Conn
		for c := range p.live {
			all = append(all, c)
		}
		p.mu.Unlock()
		for _, c := range all {
			c.Close()
		}
	})
	p.wg.Wait()
}

func (p *Proxy) event(s Setting) {
	if p.cfg.Events == nil {
		return
	}
	line, _ := json.Marshal(struct {
		Mode     Mode  `json:"mode"`
		OffsetMs int64 `json:"offset_ms"`
		UnixMs   int64 `json:"unix_ms"`
	}{s.Mode, time.Since(p.start).Milliseconds(), time.Now().UnixMilli()})
	p.evMu.Lock()
	defer p.evMu.Unlock()
	_, _ = p.cfg.Events.Write(append(line, '\n'))
}

func closeConn(c net.Conn, reset bool) {
	if tc, ok := c.(*net.TCPConn); ok && reset {
		_ = tc.SetLinger(0)
	}
	c.Close()
}

func (p *Proxy) track(c net.Conn, client bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return false
	}
	p.live[c] = struct{}{}
	if client {
		p.clients++
	}
	return true
}

func (p *Proxy) untrack(c net.Conn, client bool) {
	p.mu.Lock()
	delete(p.live, c)
	if client {
		p.clients--
	}
	p.mu.Unlock()
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			select {
			case <-p.stop:
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		p.accepted.Add(1)
		mode := p.Current().Mode
		p.mu.Lock()
		full := p.clients >= p.cfg.MaxConns
		p.mu.Unlock()
		if full || mode == Cut || mode == Drop {
			p.refused.Add(1)
			closeConn(c, mode == Drop)
			continue
		}
		if !p.track(c, true) {
			c.Close()
			continue
		}
		p.wg.Add(1)
		go p.serve(c, mode)
	}
}

func (p *Proxy) serve(client net.Conn, mode Mode) {
	defer p.wg.Done()
	defer p.untrack(client, true)
	defer client.Close()
	var first []byte
	if mode == Blackhole {
		var ok bool
		if first, ok = p.discard(client); !ok {
			return
		}
	}
	up, err := net.DialTimeout("tcp", p.cfg.Upstream, dialTimeout)
	if err != nil {
		return
	}
	if !p.track(up, false) {
		up.Close()
		return
	}
	defer p.untrack(up, false)
	defer up.Close()
	if len(first) > 0 {
		if !p.forward(up, first, p.Current()) {
			return
		}
		p.bytesUp.Add(int64(len(first)))
	}
	done := make(chan struct{}, 2)
	go func() { p.pipe(up, client, &p.bytesUp); done <- struct{}{} }()
	go func() { p.pipe(client, up, &p.bytesDown); done <- struct{}{} }()
	<-done
	client.Close()
	up.Close()
	<-done
}

// discard reads and drops what a blackholed connection sends. When the mode
// leaves blackhole it returns the first chunk read after that, to be forwarded;
// it returns false when the connection ends or the mode is cut or drop.
func (p *Proxy) discard(c net.Conn) ([]byte, bool) {
	buf := make([]byte, chunkSize)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			switch p.Current().Mode {
			case Blackhole:
			case Cut, Drop:
				return nil, false
			default:
				return append([]byte(nil), buf[:n]...), true
			}
		}
		if err != nil {
			return nil, false
		}
	}
}

// pipe copies src to dst chunk by chunk, reading the current setting for each
// chunk so a mode change reaches live connections.
func (p *Proxy) pipe(dst, src net.Conn, counter *atomic.Int64) {
	buf := make([]byte, chunkSize)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			s := p.Current()
			switch s.Mode {
			case Blackhole:
			case Cut, Drop:
				return
			default:
				if !p.forward(dst, buf[:n], s) {
					return
				}
				counter.Add(int64(n))
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *Proxy) forward(dst net.Conn, b []byte, s Setting) bool {
	switch s.Mode {
	case Delay:
		d := s.Latency
		if s.Jitter > 0 {
			d += time.Duration(rand.Int64N(int64(s.Jitter) + 1))
		}
		if !p.sleep(d) {
			return false
		}
	case Slow:
		for len(b) > 0 {
			piece := min(len(b), slowPiece)
			if !p.sleep(time.Duration(float64(piece) / float64(s.BytesPerSec) * float64(time.Second))) {
				return false
			}
			if _, err := dst.Write(b[:piece]); err != nil {
				return false
			}
			b = b[piece:]
		}
		return true
	}
	_, err := dst.Write(b)
	return err == nil
}

func (p *Proxy) sleep(d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-p.stop:
		return false
	}
}
