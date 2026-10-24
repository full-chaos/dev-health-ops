package session

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"sync"
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
	counts   map[[2]string]uint64
	inflight map[*queuedJob]struct{}
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
	counts := make(map[[2]string]uint64, len(mailPurposes)*len(mailOutcomes))
	for _, purpose := range mailPurposes {
		for _, outcome := range mailOutcomes {
			counts[[2]string{purpose, outcome}] = 0
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &sendQueue{slots: make(chan struct{}, slots), counts: counts, inflight: map[*queuedJob]struct{}{}, ctx: ctx, cancel: cancel}
}

// linkMailQueue carries the register route's mail; register it on the api's
// operator registry (apiservice.RegisterOperatorMetrics).
var linkMailQueue = newSendQueue(sendQueueSlots)

// MailMetricsSource is the process-wide mail counters (health.MetricsSource).
func MailMetricsSource() *sendQueue { return linkMailQueue }

func (q *sendQueue) count(purpose, outcome string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.bump(purpose, outcome)
}

// bump counts one outcome; q.mu is held. A label outside the closed sets counts nothing.
func (q *sendQueue) bump(purpose, outcome string) {
	key := [2]string{purpose, outcome}
	if _, known := q.counts[key]; known {
		q.counts[key]++
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
	q.bump(job.purpose, outcome)
}

// submit runs send in the background and reports whether it was accepted; the
// send's error counts it as failed.
func (q *sendQueue) submit(purpose string, send func() error) bool {
	select {
	case q.slots <- struct{}{}:
	default:
		q.count(purpose, outcomeDropped)
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
	select {
	case <-done:
		return 0
	case <-ctx.Done():
	}
	q.cancel()
	q.mu.Lock()
	defer q.mu.Unlock()
	left := 0
	for job := range q.inflight {
		if job.settled {
			continue
		}
		job.settled = true
		if key := [2]string{job.purpose, outcomeDropped}; true {
			if _, known := q.counts[key]; known {
				q.counts[key]++
			}
		}
		left++
	}
	return left
}

// MailDrain is the runtime component that lets the queued sign-in mail jobs
// finish, within the shutdown deadline, when the api stops.
type MailDrain struct{ Logger *slog.Logger }

func (MailDrain) Name() string                { return "session-mail-queue" }
func (MailDrain) Start(context.Context) error { return nil }
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
	q.mu.Lock()
	counts := make(map[[2]string]uint64, len(q.counts))
	for key, value := range q.counts {
		counts[key] = value
	}
	q.mu.Unlock()
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
