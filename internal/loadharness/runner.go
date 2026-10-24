package loadharness

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

// Options configure a run.
type Options struct {
	Profile    Profile
	AllowHosts []string
	Getenv     func(string) string
	Client     *http.Client
}

type job struct {
	target    int
	seq       int
	scheduled time.Time
}

type acc struct {
	lat     []time.Duration
	errs    int
	dropped int
}

// Run drives the profile against the config. Requests are scheduled at a
// fixed rate (open loop) and latency is measured from the scheduled send
// time, so a saturated server shows queueing delay instead of hiding it. A
// request that finds the queue full is dropped and counts as a failure.
func Run(ctx context.Context, cfg Config, opt Options) (Report, error) {
	if err := CheckHost(cfg.BaseURL, opt.AllowHosts); err != nil {
		return Report{}, err
	}
	p := opt.Profile
	if p.Rate <= 0 || p.Duration <= 0 || p.Concurrency <= 0 {
		return Report{}, errors.New("rate, duration and concurrency must be positive")
	}
	getenv := opt.Getenv
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	client := newClient(opt.Client)

	var wheel []int
	for i, t := range cfg.Targets {
		for w := 0; w < t.Weight; w++ {
			wheel = append(wheel, i)
		}
	}
	base := strings.TrimRight(cfg.BaseURL, "/")
	total := int(p.Rate * p.Duration.Seconds())
	if total < 1 {
		return Report{}, errors.New("rate x duration schedules no request")
	}
	interval := time.Duration(float64(time.Second) / p.Rate)
	jobs := make(chan job, max(p.Concurrency*4, 64))
	results := make([]acc, len(cfg.Targets))
	var mu sync.Mutex

	var wg sync.WaitGroup
	for w := 0; w < p.Concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				failed := send(ctx, client, base, cfg.Targets[j.target], j.seq, getenv)
				lat := time.Since(j.scheduled)
				mu.Lock()
				a := &results[j.target]
				a.lat = append(a.lat, lat)
				if failed {
					a.errs++
				}
				mu.Unlock()
			}
		}()
	}

	start := time.Now()
	for i := 0; i < total && ctx.Err() == nil; i++ {
		at := start.Add(time.Duration(i) * interval)
		if d := time.Until(at); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
			}
		}
		j := job{target: wheel[i%len(wheel)], seq: i, scheduled: at}
		select {
		case jobs <- j:
		default:
			mu.Lock()
			results[j.target].dropped++
			mu.Unlock()
		}
	}
	close(jobs)
	wg.Wait()

	in := ReportInput{
		Profile: p.Name, StartedAt: start, Duration: time.Since(start),
		Rate: p.Rate, Concurrency: p.Concurrency,
	}
	for i, t := range cfg.Targets {
		in.Paths = append(in.Paths, PathResult{Name: t.Name, Latencies: results[i].lat, Errors: results[i].errs, Dropped: results[i].dropped})
	}
	return BuildReport(in), nil
}

// send performs one request and reports whether it failed. The error text is
// never kept: it can carry the request URL.
func send(ctx context.Context, c *http.Client, base string, t Target, seq int, getenv func(string) string) bool {
	path, err := expand(t.Path, seq, getenv)
	if err != nil {
		return true
	}
	body, err := expand(t.Body, seq, getenv)
	if err != nil {
		return true
	}
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, t.Method, base+path, rd)
	if err != nil {
		return true
	}
	for k, v := range t.Headers {
		req.Header.Set(k, v)
	}
	for k, he := range t.HeaderEnv {
		req.Header.Set(k, he.Prefix+getenv(he.Env))
	}
	resp, err := c.Do(req)
	if err != nil {
		return true
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20)); err != nil {
		return true
	}
	for _, s := range t.ExpectStatus {
		if resp.StatusCode == s {
			return false
		}
	}
	return true
}

// newClient copies a supplied client (or builds one), gives it a transport that
// ignores the process proxy variables when it has none, and stops redirects.
func newClient(supplied *http.Client) *http.Client {
	client := &http.Client{Timeout: 30 * time.Second, Transport: httpguard.Transport()}
	if supplied != nil {
		c := *supplied
		if c.Transport == nil {
			c.Transport = httpguard.Transport()
		}
		client = &c
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return client
}
