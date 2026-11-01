package session

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// sendQueueSlots bounds the link e-mails in flight at once.
const sendQueueSlots = 64

// mailSendsMetric counts the register route's mails by purpose and outcome.
const mailSendsMetric = "devhealth_api_session_mail_sends_total"

// Mail purposes and outcomes: the closed label sets of mailSendsMetric.
const (
	mailVerification  = "email_verification"
	mailPasswordReset = "password_reset"
	mailAccountExists = "account_exists"

	outcomeSent    = "sent"
	outcomeFailed  = "failed"
	outcomeDropped = "dropped"
)

var (
	mailPurposes = []string{mailVerification, mailPasswordReset, mailAccountExists}
	mailOutcomes = []string{outcomeSent, outcomeFailed, outcomeDropped}
)

// sendQueue runs best-effort mail sends off the request. It never blocks the
// caller: when every slot is busy the send is dropped and counted.
type sendQueue struct {
	slots    chan struct{}
	wg       sync.WaitGroup
	mu       sync.Mutex
	counts   map[[2]string]*atomic.Uint64
	inflight map[*queuedJob]struct{}
	// fullDrops counts, by purpose, the real jobs a full queue refused; the one reporter reads it. A request
	// only adds to it: it makes no log call and takes no lock.
	fullDrops map[string]*atomic.Uint64
	// reporterOnce makes a second Start a no-op; reporterDone ends with the reporter's final report.
	reporterOnce sync.Once
	reporterDone chan struct{}
	// ctx ends when a drain gives up: every job bound to it is cancelled.
	ctx    context.Context
	cancel context.CancelFunc
}

// queuedJob is one accepted real job; drained is set once, by whichever side
// settles it first (the worker's own outcome or the drain's dropped count).
type queuedJob struct {
	purpose string
	settled bool
}

func newSendQueue(slots int) *sendQueue {
	counts := make(map[[2]string]*atomic.Uint64, len(mailPurposes)*len(mailOutcomes))
	for _, purpose := range mailPurposes {
		for _, outcome := range mailOutcomes {
			counts[[2]string{purpose, outcome}] = new(atomic.Uint64)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	fullDrops := make(map[string]*atomic.Uint64, len(mailPurposes))
	for _, purpose := range mailPurposes {
		fullDrops[purpose] = new(atomic.Uint64)
	}
	return &sendQueue{slots: make(chan struct{}, slots), counts: counts, inflight: map[*queuedJob]struct{}{}, ctx: ctx, cancel: cancel, fullDrops: fullDrops}
}

// linkMailQueue carries the register route's mail; register it on the api's
// operator registry (apiservice.RegisterOperatorMetrics).
var linkMailQueue = newSendQueue(sendQueueSlots)

// MailMetricsSource is the process-wide mail counters (health.MetricsSource).
func MailMetricsSource() *sendQueue { return linkMailQueue }

// count counts one outcome without a lock. A label outside the closed sets counts nothing.
func (q *sendQueue) count(purpose, outcome string) {
	if counter, known := q.counts[[2]string{purpose, outcome}]; known {
		counter.Add(1)
	}
}

// refuse counts a real job a full queue turned away. Nothing else happens on the request path.
func (q *sendQueue) refuse(purpose string) {
	q.count(purpose, outcomeDropped)
	if counter, known := q.fullDrops[purpose]; known {
		counter.Add(1)
	}
}

// mailDropReportInterval is how often the one reporter writes the queue-full warning.
var mailDropReportInterval = 30 * time.Second

// startReporter runs the one background reporter: at most once per interval it logs, per purpose, how many real
// jobs the full queue refused since its last report, and only when that number is above zero. It stops with the
// queue (the drain cancels the queue context) after a last report. It logs the purpose and a count, never an address.
func (q *sendQueue) startReporter(logger *slog.Logger, every time.Duration) {
	if logger == nil {
		return
	}
	q.reporterOnce.Do(func() {
		done := make(chan struct{})
		q.mu.Lock()
		q.reporterDone = done
		q.mu.Unlock()
		last := map[string]uint64{}
		report := func() {
			for _, purpose := range mailPurposes {
				now := q.fullDrops[purpose].Load()
				if n := now - last[purpose]; n > 0 {
					last[purpose] = now
					logger.Warn("api session mail dropped: queue full", "count", n, "purpose", purpose, "since", "last report")
				}
			}
		}
		go func() {
			defer close(done)
			ticker := time.NewTicker(every)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					report()
				case <-q.ctx.Done():
					report()
					return
				}
			}
		}()
	})
}

// finalReportGrace is how long the drain still waits for the reporter's final report after its own context has ended.
const finalReportGrace = 250 * time.Millisecond

// waitReporter waits for the reporter to finish its final report until ctx ends, then for at most finalReportGrace more.
func (q *sendQueue) waitReporter(ctx context.Context) {
	q.mu.Lock()
	done := q.reporterDone
	q.mu.Unlock()
	if done == nil {
		return
	}
	select {
	case <-done:
		return
	case <-ctx.Done():
	}
	grace := time.NewTimer(finalReportGrace)
	defer grace.Stop()
	select {
	case <-done:
	case <-grace.C:
	}
}

// bind returns ctx with a timeout that is also cancelled when a drain gives up.
func (q *sendQueue) bind(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	bound, cancel := context.WithTimeout(ctx, timeout)
	stop := context.AfterFunc(q.ctx, cancel)
	return bound, func() { stop(); cancel() }
}

// settle records the outcome of job unless the drain already counted it dropped.
func (q *sendQueue) settle(job *queuedJob, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inflight, job)
	if job.settled {
		return
	}
	job.settled = true
	outcome := outcomeSent
	if err != nil {
		outcome = outcomeFailed
	}
	q.count(job.purpose, outcome)
}

// submit runs send in the background and reports whether it was accepted; the
// send's error counts it as failed.
func (q *sendQueue) submit(purpose string, send func() error) bool {
	select {
	case q.slots <- struct{}{}:
	default:
		q.refuse(purpose)
		return false
	}
	job := &queuedJob{purpose: purpose}
	q.mu.Lock()
	q.inflight[job] = struct{}{}
	q.mu.Unlock()
	q.wg.Add(1)
	go func() {
		defer func() {
			<-q.slots
			q.wg.Done()
		}()
		q.settle(job, send())
	}()
	return true
}

// submitDecoy runs the same work shape for an address that has no account:
// it takes a slot like a real job and counts nothing, whatever happens.
func (q *sendQueue) submitDecoy(run func()) bool {
	select {
	case q.slots <- struct{}{}:
	default:
		return false
	}
	q.wg.Add(1)
	go func() {
		defer func() {
			<-q.slots
			q.wg.Done()
		}()
		run()
	}()
	return true
}

// drain waits for the accepted jobs until ctx ends. Jobs still running then are
// cancelled and counted as dropped by purpose, once; it returns how many.
func (q *sendQueue) drain(ctx context.Context) int {
	done := make(chan struct{})
	go func() { q.wg.Wait(); close(done) }()
	left := 0
	select {
	case <-done:
	case <-ctx.Done():
		left = q.settleLeft()
	}
	q.cancel()
	q.waitReporter(ctx)
	return left
}

// settleLeft counts every job still running as dropped, once.
func (q *sendQueue) settleLeft() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	left := 0
	for job := range q.inflight {
		if job.settled {
			continue
		}
		job.settled = true
		q.count(job.purpose, outcomeDropped)
		left++
	}
	return left
}

// MailDrain is the runtime component that lets the queued sign-in mail jobs
// finish, within the shutdown deadline, when the api stops.
type MailDrain struct{ Logger *slog.Logger }

func (MailDrain) Name() string { return "session-mail-queue" }
func (d MailDrain) Start(context.Context) error {
	linkMailQueue.startReporter(d.Logger, mailDropReportInterval)
	return nil
}
func (d MailDrain) Shutdown(ctx context.Context) error {
	if left := linkMailQueue.drain(ctx); left > 0 && d.Logger != nil {
		d.Logger.WarnContext(ctx, "api session mail queue stopped with jobs unfinished", "count", left)
	}
	return nil
}

// wait returns when every accepted send has finished.
func (q *sendQueue) wait() { q.wg.Wait() }

// WritePrometheus writes the family, every series present at 0 from the start.
func (q *sendQueue) WritePrometheus(w io.Writer) error {
	counts := make(map[[2]string]uint64, len(q.counts))
	for key, counter := range q.counts {
		counts[key] = counter.Load()
	}
	if _, err := io.WriteString(w, "# HELP "+mailSendsMetric+" Sign-up mails by purpose and outcome (sent, failed, dropped).\n"+
		"# TYPE "+mailSendsMetric+" counter\n"); err != nil {
		return err
	}
	for _, purpose := range mailPurposes {
		for _, outcome := range mailOutcomes {
			if _, err := io.WriteString(w, mailSendsMetric+"{purpose="+strconv.Quote(purpose)+",outcome="+strconv.Quote(outcome)+"} "+
				strconv.FormatUint(counts[[2]string{purpose, outcome}], 10)+"\n"); err != nil {
				return err
			}
		}
	}
	return nil
}
