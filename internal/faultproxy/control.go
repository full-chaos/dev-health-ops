package faultproxy

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"
)

// ControlHandler serves GET /mode, POST /mode?name=..&latency=..&jitter=..&bps=..
// and GET /stats.
func (p *Proxy) ControlHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/mode", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			if r.URL.RawQuery != "" {
				http.Error(w, "use POST to change the mode", http.StatusMethodNotAllowed)
				return
			}
			s := p.Current()
			writeJSON(w, map[string]any{"mode": s.Mode, "latency_ms": s.Latency.Milliseconds(),
				"jitter_ms": s.Jitter.Milliseconds(), "bytes_per_sec": s.BytesPerSec})
		case http.MethodPost:
			s, err := settingFromQuery(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			p.Set(s)
			writeJSON(w, map[string]any{"mode": s.Mode})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, p.Stats())
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func settingFromQuery(r *http.Request) (Setting, error) {
	q := r.URL.Query()
	s := Setting{Mode: Mode(q.Get("name"))}
	var err error
	if v := q.Get("latency"); v != "" {
		if s.Latency, err = time.ParseDuration(v); err != nil {
			return Setting{}, errors.New("bad latency")
		}
	}
	if v := q.Get("jitter"); v != "" {
		if s.Jitter, err = time.ParseDuration(v); err != nil {
			return Setting{}, errors.New("bad jitter")
		}
	}
	if v := q.Get("bps"); v != "" {
		if s.BytesPerSec, err = strconv.Atoi(v); err != nil {
			return Setting{}, errors.New("bad bps")
		}
	}
	return s, s.Validate()
}

// CheckControlAddr refuses a control address that is not loopback.
func CheckControlAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host != "localhost" {
		if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
			return errors.New("control address must be loopback")
		}
	}
	return nil
}

// StartControl serves h on a loopback address only. The returned function
// stops the server and waits for it.
func StartControl(addr string, h http.Handler) (net.Addr, func(), error) {
	if err := CheckControlAddr(addr); err != nil {
		return nil, nil, err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	done := make(chan struct{})
	go func() { defer close(done); _ = srv.Serve(ln) }()
	return ln.Addr(), func() { _ = srv.Close(); <-done }, nil
}
