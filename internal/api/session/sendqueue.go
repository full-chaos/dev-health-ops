package session

import (
	"sync"
	"sync/atomic"
)

// sendQueueSlots bounds the link e-mails in flight at once.
const sendQueueSlots = 64

// sendQueue runs best-effort mail sends off the request. It never blocks the
// caller: when every slot is busy the send is dropped and counted.
type sendQueue struct {
	slots   chan struct{}
	wg      sync.WaitGroup
	queued  atomic.Int64
	dropped atomic.Int64
}

func newSendQueue(slots int) *sendQueue {
	return &sendQueue{slots: make(chan struct{}, slots)}
}

// linkMailQueue carries the register route's mail.
var linkMailQueue = newSendQueue(sendQueueSlots)

// submit runs send in the background and reports whether it was accepted.
func (q *sendQueue) submit(send func()) bool {
	select {
	case q.slots <- struct{}{}:
	default:
		q.dropped.Add(1)
		return false
	}
	q.queued.Add(1)
	q.wg.Add(1)
	go func() {
		defer func() {
			<-q.slots
			q.wg.Done()
		}()
		send()
	}()
	return true
}

// outcomes is the counts by outcome: sends accepted and sends dropped.
func (q *sendQueue) outcomes() (queued, dropped int64) {
	return q.queued.Load(), q.dropped.Load()
}

// wait returns when every accepted send has finished.
func (q *sendQueue) wait() { q.wg.Wait() }
