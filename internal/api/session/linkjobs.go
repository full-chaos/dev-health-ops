package session

import (
	"context"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/authmail"
	"github.com/full-chaos/dev-health-ops/internal/mail"
)

// queueLinkJob runs the work behind a forgot or resend answer off the request.
// A real job counts sent, failed or dropped by purpose; a decoy (an address with
// no account to act on) takes a slot and runs the same shape of work, writes and
// sends nothing, and counts nothing. Nothing logged or labelled names an address
// or a user.
func (h handlers) queueLinkJob(ctx context.Context, r *http.Request, purpose string, real bool, job func(context.Context, *http.Request) error) {
	detached := context.WithoutCancel(ctx)
	jobRequest := r.Clone(detached)
	if !real {
		linkMailQueue.submitDecoy(func() { h.runDecoy(detached, purpose) })
		return
	}
	run := func() error {
		jobCtx, cancel := context.WithTimeout(detached, authmail.SendTimeout)
		defer cancel()
		err := job(jobCtx, jobRequest)
		if err != nil {
			h.Logger.ErrorContext(detached, "api: sign-in link job failed", "purpose", purpose)
		}
		return err
	}
	if !linkMailQueue.submit(purpose, run) {
		h.Logger.WarnContext(ctx, "api session mail dropped: queue full", "purpose", purpose)
	}
}

// runDecoy renders the mail of the purpose and stops there.
func (h handlers) runDecoy(_ context.Context, purpose string) {
	template, urlField, path := "verification", "verification_url", "/auth/verify"
	if purpose == mailPasswordReset {
		template, urlField, path = "password_reset", "reset_url", "/auth/reset-password"
	}
	_, _ = mail.RenderTemplate(template, map[string]string{"full_name": "-", urlField: h.Mail.Link(path, "-")})
}
