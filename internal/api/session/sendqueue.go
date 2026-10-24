package session

import (
	"io"
	"strconv"
	"sync"
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
	slots  chan struct{}
	wg     sync.WaitGroup
	mu     sync.Mutex
	counts map[[2]string]uint64
}

func newSendQueue(slots int) *sendQueue {
	counts := make(map[[2]string]uint64, len(mailPurposes)*len(mailOutcomes))
	for _, purpose := range mailPurposes {
		for _, outcome := range mailOutcomes {
			counts[[2]string{purpose, outcome}] = 0
		}
	}
	return &sendQueue{slots: make(chan struct{}, slots), counts: counts}
}

// linkMailQueue carries the register route's mail; register it on the api's
// operator registry (apiservice.RegisterOperatorMetrics).
var linkMailQueue = newSendQueue(sendQueueSlots)

// MailMetricsSource is the process-wide mail counters (health.MetricsSource).
func MailMetricsSource() *sendQueue { return linkMailQueue }

func (q *sendQueue) count(purpose, outcome string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	key := [2]string{purpose, outcome}
	if _, known := q.counts[key]; known {
		q.counts[key]++
	}
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
	q.wg.Add(1)
	go func() {
		defer func() {
			<-q.slots
			q.wg.Done()
		}()
		if err := send(); err != nil {
			q.count(purpose, outcomeFailed)
			return
		}
		q.count(purpose, outcomeSent)
	}()
	return true
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
