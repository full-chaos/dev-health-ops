// Package reconcilejob runs the External Push projection reconcile (CHAOS-10897):
// the read-only walk over the domain store and the credential projection, the
// two-pass confirmation, and the worker drift probe. It writes nothing. The
// pure comparison lives in pushreconcile; this package holds the context, the
// clock and the database handle that package must not hold.
package reconcilejob

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/customerpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	// PageTimeout bounds one page pair (two reads in one snapshot).
	PageTimeout = 5 * time.Second
	// DefaultBatch is the page of the verb.
	DefaultBatch = 500
	// DefaultConfirmDelay is the wait between the two passes of the verb.
	DefaultConfirmDelay = 60 * time.Second
	// RunBudget bounds one reconcile run without its confirm delay.
	RunBudget = 5 * time.Minute
)

// Pass is the result of one sweep of both sides.
type Pass struct {
	Tokens    int
	Projected int
	Findings  []pushreconcile.Finding
}

// PassFunc runs one pass.
type PassFunc func(ctx context.Context) (Pass, error)

// Report is the result of one run: the findings both passes agree on.
type Report struct {
	Passes    int
	Tokens    int
	Projected int
	// Drift holds the confirmed findings, sorted by id then class.
	Drift []pushreconcile.Finding
	// Transient counts the findings of the first pass that the second pass did not repeat.
	Transient int
}

// Counts returns the confirmed findings per class; every class is present.
func (r Report) Counts() map[pushreconcile.Class]int {
	return CountByClass(r.Drift)
}

// CountByClass counts findings per class; every class is present, even at 0.
func CountByClass(findings []pushreconcile.Finding) map[pushreconcile.Class]int {
	out := make(map[pushreconcile.Class]int, len(pushreconcile.Classes))
	for _, class := range pushreconcile.Classes {
		out[class] = 0
	}
	for _, f := range findings {
		out[f.Class]++
	}
	return out
}

// Run runs the first pass, waits delay, runs the second pass and keeps the
// findings both report (pushreconcile.Confirm). A delay of zero or less runs
// one pass and reports its findings as they are.
func Run(ctx context.Context, pass PassFunc, delay time.Duration, sleep func(context.Context, time.Duration) error) (Report, error) {
	first, err := pass(ctx)
	if err != nil {
		return Report{}, err
	}
	if delay <= 0 {
		return Report{Passes: 1, Tokens: first.Tokens, Projected: first.Projected, Drift: first.Findings}, nil
	}
	if err := sleep(ctx, delay); err != nil {
		return Report{}, err
	}
	second, err := pass(ctx)
	if err != nil {
		return Report{}, err
	}
	drift := pushreconcile.Confirm(first.Findings, second.Findings)
	return Report{Passes: 2, Tokens: second.Tokens, Projected: second.Projected, Drift: drift, Transient: len(first.Findings) - len(drift)}, nil
}

// Sleep waits for d or until ctx ends.
func Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// NewPass returns the read-only walk of one pass over db. Domain sweep: tokens
// by id with their projection rows, every finding except extra. Projection
// sweep: projection rows by reference with their tokens, extra findings only.
// Each page pair is one REPEATABLE READ, READ ONLY snapshot.
func NewPass(db customerpush.TxBeginner, schema authschema.ValidatedIdentifier, batch int, now func() time.Time) PassFunc {
	if now == nil {
		now = time.Now
	}
	return func(ctx context.Context) (Pass, error) {
		var out Pass
		after := uuid.Nil
		for {
			pair, err := page(ctx, func(c context.Context) (customerpush.PagePair, error) {
				return customerpush.ReadDomainPage(c, db, schema, after, batch)
			})
			if err != nil {
				return Pass{}, err
			}
			findings, err := pushreconcile.Diff(pair.Tokens, pair.Projected, now())
			if err != nil {
				return Pass{}, fmt.Errorf("reconcilejob: compare a domain page: %w", err)
			}
			for _, f := range findings {
				if f.Class != pushreconcile.ClassExtra {
					out.Findings = append(out.Findings, f)
				}
			}
			out.Tokens += len(pair.Tokens)
			if pair.Last {
				break
			}
			if after, err = uuid.Parse(pair.Next); err != nil {
				return Pass{}, fmt.Errorf("reconcilejob: domain page position: %w", err)
			}
		}
		position := ""
		for {
			pair, err := page(ctx, func(c context.Context) (customerpush.PagePair, error) {
				return customerpush.ReadProjectionPage(c, db, schema, position, batch)
			})
			if err != nil {
				return Pass{}, err
			}
			findings, err := pushreconcile.Diff(pair.Tokens, pair.Projected, now())
			if err != nil {
				return Pass{}, fmt.Errorf("reconcilejob: compare a projection page: %w", err)
			}
			for _, f := range findings {
				if f.Class == pushreconcile.ClassExtra {
					out.Findings = append(out.Findings, f)
				}
			}
			out.Projected += len(pair.Projected)
			if pair.Last {
				break
			}
			position = pair.Next
		}
		slices.SortFunc(out.Findings, func(a, b pushreconcile.Finding) int {
			if c := strings.Compare(a.ID, b.ID); c != 0 {
				return c
			}
			return strings.Compare(string(a.Class), string(b.Class))
		})
		return out, nil
	}
}

func page(ctx context.Context, read func(context.Context) (customerpush.PagePair, error)) (customerpush.PagePair, error) {
	pageCtx, cancel := context.WithTimeout(ctx, PageTimeout)
	defer cancel()
	return read(pageCtx)
}
