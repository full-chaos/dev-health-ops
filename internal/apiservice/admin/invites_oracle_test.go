//go:build integration

package admin_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/mail"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	mailpkg "github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/smtpcapture"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

var inviteTokenPattern = regexp.MustCompile(`token=([0-9a-f]{32}\.[0-9a-f]{64})`)

// decodedInviteMail is one captured message as its receiver reads it: the
// envelope and headers exactly, the HTML part decoded (the transfer encoding
// is proven byte for byte by the mail package's own oracle), and the invite
// token pulled out so it can be compared as a shape, not a random value.
type decodedInviteMail struct {
	MailFrom    string
	RcptTo      string
	From        string
	To          string
	Subject     string
	ContentType string
	PartType    string
	PartCTE     string
	HTML        string // token replaced by TOKEN
	Token       string
}

func decodeInviteMail(t *testing.T, captured smtpcapture.Mail) decodedInviteMail {
	t.Helper()
	message, err := mail.ReadMessage(strings.NewReader(captured.Data))
	if err != nil {
		t.Fatalf("captured message does not parse: %v\n%s", err, captured.Data)
	}
	out := decodedInviteMail{
		MailFrom: captured.MailFrom, RcptTo: strings.Join(captured.RcptTo, ","),
		From: message.Header.Get("From"), To: message.Header.Get("To"), Subject: message.Header.Get("Subject"),
	}
	mediaType, params, err := mime.ParseMediaType(message.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/alternative" {
		t.Fatalf("captured message is not multipart/alternative: %q (%v)", message.Header.Get("Content-Type"), err)
	}
	out.ContentType = mediaType
	reader := multipart.NewReader(message.Body, params["boundary"])
	part, err := reader.NextRawPart()
	if err != nil {
		t.Fatalf("captured message has no part: %v", err)
	}
	out.PartType = part.Header.Get("Content-Type")
	out.PartCTE = part.Header.Get("Content-Transfer-Encoding")
	raw, err := io.ReadAll(part)
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	if strings.EqualFold(out.PartCTE, "base64") {
		decoded, decodeErr := base64.StdEncoding.DecodeString(strings.NewReplacer("\r", "", "\n", "").Replace(body))
		if decodeErr != nil {
			t.Fatalf("part is not base64: %v", decodeErr)
		}
		body = string(decoded)
	}
	match := inviteTokenPattern.FindStringSubmatch(body)
	if match == nil {
		t.Fatalf("no invite token in the captured HTML:\n%s", body)
	}
	out.Token = match[1]
	out.HTML = strings.ReplaceAll(body, out.Token, "TOKEN")
	return out
}

// checkInviteToken proves a token is the one invites.py's _build_token makes
// for id under secret (the plane that produced it is named by label), and that
// the stored token_hash is the sha256 of it.
func checkInviteToken(t *testing.T, label, token, id, secret, storedHash string) {
	t.Helper()
	idHex := strings.ReplaceAll(id, "-", "")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(idHex))
	if want := idHex + "." + hex.EncodeToString(mac.Sum(nil)); token != want {
		t.Errorf("%s: token in the email is %q, want %q", label, token, want)
	}
	sum := sha256.Sum256([]byte(token))
	if want := hex.EncodeToString(sum[:]); storedHash != want {
		t.Errorf("%s: stored token_hash %q, want sha256(token) %q", label, storedHash, want)
	}
}

// TestCreateOrgInviteVenueOracle is the venue-oracle proof for
// POST /orgs/{org_id}/invites: the route's status and body, the org_invites
// and audit_logs rows it leaves, the invite EMAIL each plane sends (captured
// off the wire by one SMTP server both planes send to), the invite token's
// construction, and the route's 10/hour keyed rate limit.
func TestCreateOrgInviteVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-org-invites-flow-32-bytes!"

	orgID, plainOrgID, otherOrgID := uuid.New(), uuid.New(), uuid.New()
	ownerID, adminID, secondAdminID, memberID, outsiderID, superID, plainOwnerID, otherAdminID :=
		uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()

	sink := smtpcapture.Start(t)
	smtpHost, smtpPort := sink.HostPort(t)
	const fromAddress = "Dev Health <invites@example.test>"
	const appBaseURL = "https://app.example.test/"

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		PythonEnv: []string{
			"EMAIL_PROVIDER=smtp", "EMAIL_FROM_ADDRESS=" + fromAddress,
			"SMTP_HOST=" + smtpHost, "SMTP_PORT=" + strconv.Itoa(smtpPort),
			"APP_BASE_URL=" + appBaseURL,
		},
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			// A non-ASCII org name: the subject and body take the encoded
			// paths. The plain org takes the ASCII ones.
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-invite-org', $2, 'community', 'stripe', true, now(), now())`, orgID, "Café Org & Söhne")
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-invite-plain', 'Plain Invite Org', 'community', 'stripe', true, now(), now())`, plainOrgID)
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-invite-other', 'Other Invite Org', 'community', 'stripe', true, now(), now())`, otherOrgID)
			for _, row := range []struct {
				id       uuid.UUID
				email    string
				fullName *string
				super    bool
			}{
				{ownerID, "venue-invite-owner@example.com", strPtr("Olivia Öwner"), false},
				{adminID, "venue-invite-admin@example.com", nil, false}, // no full_name: the email is the inviter name
				{secondAdminID, "venue-invite-admin2@example.com", strPtr(""), false},
				{memberID, "venue-invite-member@example.com", strPtr("Mia Member"), false},
				{outsiderID, "venue-invite-outsider@example.com", strPtr("Otto Outsider"), false},
				{superID, "venue-invite-super@example.com", nil, true},
				{plainOwnerID, "venue-invite-plainowner@example.com", strPtr("Paula Plain"), false},
				{otherAdminID, "venue-invite-otheradmin@example.com", strPtr("Omar Other"), false},
			} {
				exec(`INSERT INTO users (id, email, full_name, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, true, true, $4, 0, now(), now())`, row.id, row.email, row.fullName, row.super)
			}
			for _, row := range []struct {
				org, user uuid.UUID
				role      string
			}{
				{orgID, ownerID, "owner"}, {orgID, adminID, "admin"}, {orgID, secondAdminID, "admin"},
				{orgID, memberID, "member"}, {plainOrgID, plainOwnerID, "owner"}, {otherOrgID, otherAdminID, "admin"},
			} {
				exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, now(), now(), now())`, uuid.New(), row.org, row.user, row.role)
			}
			token := func(id uuid.UUID, email string, org uuid.UUID, role string) map[string]any {
				return map[string]any{"user_id": id.String(), "email": email, "org_id": org.String(), "role": role}
			}
			return map[string]map[string]any{
				"owner":      token(ownerID, "venue-invite-owner@example.com", orgID, "owner"),
				"admin":      token(adminID, "venue-invite-admin@example.com", orgID, "admin"),
				"admin2":     token(secondAdminID, "venue-invite-admin2@example.com", orgID, "admin"),
				"member":     token(memberID, "venue-invite-member@example.com", orgID, "member"),
				"otheradmin": token(otherAdminID, "venue-invite-otheradmin@example.com", otherOrgID, "admin"),
				"outsider":   token(outsiderID, "venue-invite-outsider@example.com", otherOrgID, "member"),
				"plainowner": token(plainOwnerID, "venue-invite-plainowner@example.com", plainOrgID, "owner"),
				"super":      {"user_id": superID.String(), "email": "venue-invite-super@example.com", "is_superuser": true},
			}
		},
	})

	jsonHeaders := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name], "Content-Type": "application/json"}
	}
	invitesPath := func(org uuid.UUID) string { return "/api/v1/admin/orgs/" + org.String() + "/invites" }
	post := func(name, who string, org uuid.UUID, body string) venueoracle.Request {
		return venueoracle.Request{Name: name, Method: "POST", Path: invitesPath(org), Headers: jsonHeaders(who), Body: venueoracle.B64(body)}
	}

	requests := []venueoracle.Request{
		// Normalization: lower() then strip(), the default role, the 72h
		// expiry, the inviter's full_name in the email.
		post("invite normalizes case and whitespace", "owner", orgID, `{"email":"  New.Person@Example.COM "}`),
		post("invite duplicate pending is refused", "owner", orgID, `{"email":"new.person@example.com"}`),
		post("invite duplicate differing only in case is refused", "admin", orgID, `{"email":"NEW.PERSON@example.com"}`),
		post("invite explicit role", "owner", orgID, `{"email":"second@example.com","role":"admin"}`),
		post("invite empty role becomes member", "owner", orgID, `{"email":"third@example.com","role":""}`),
		post("invite non-ascii address", "owner", orgID, `{"email":"jörg@example.com"}`),
		// The inviter with no full_name is named by their email; the one with
		// an empty full_name likewise.
		post("invite by admin without a full name", "admin", orgID, `{"email":"fourth@example.com"}`),
		post("invite by admin with an empty full name", "admin2", orgID, `{"email":"fifth@example.com"}`),
		post("invite in an ascii-named org", "plainowner", plainOrgID, `{"email":"sixth@example.com"}`),
		post("invite as superuser", "super", orgID, `{"email":"seventh@example.com"}`),
		// Validation and authorization.
		post("invite short email", "owner", orgID, `{"email":"ab"}`),
		post("invite missing email", "owner", orgID, `{"role":"admin"}`),
		post("invite null role", "owner", orgID, `{"email":"nullrole@example.com","role":null}`),
		post("invite non-string email", "owner", orgID, `{"email":123}`),
		post("invite body not an object", "owner", orgID, `[]`),
		// require_admin (a token-role dependency) runs before body
		// validation: a non-admin token is refused 403 whatever it sends.
		post("invalid body by a non-admin token is refused before validation", "member", orgID, `{"email":"ab"}`),
		// The endpoint's org-access check runs AFTER validation: an admin of
		// another org gets 422 for a malformed body and 403 for a good one.
		post("invalid body by another org's admin is a 422", "otheradmin", orgID, `{"email":"ab"}`),
		post("valid body by another org's admin is a 403", "otheradmin", orgID, `{"email":"cross@example.com"}`),
		post("invite by a plain member is refused", "member", orgID, `{"email":"eighth@example.com"}`),
		post("invite into another org is refused", "outsider", orgID, `{"email":"ninth@example.com"}`),
		post("invite into a missing org", "super", uuid.New(), `{"email":"tenth@example.com"}`),
		{Name: "invite unauthenticated", Method: "POST", Path: invitesPath(orgID),
			Headers: map[string]string{"Content-Type": "application/json"}, Body: venueoracle.B64(`{"email":"anon@example.com"}`)},
	}
	// The route's own limit: 10 requests per admin per path per hour. The
	// second admin has sent one above; nine more reach ten, so the eleventh
	// call is the first refused. The last two calls, by other admins after
	// that bucket is exhausted, must still be admitted: the limit is per
	// (admin, path), never shared.
	for i := range 9 {
		requests = append(requests, post(fmt.Sprintf("admin2 bucket fill %d/9", i+1), "admin2", orgID,
			fmt.Sprintf(`{"email":"fill%d@example.com"}`, i)))
	}
	requests = append(requests,
		post("admin2 eleventh invite in the window is limited", "admin2", orgID, `{"email":"over@example.com"}`),
		post("admin still has its own bucket", "admin", orgID, `{"email":"admin-own-bucket@example.com"}`),
		post("owner still has its own bucket", "owner", orgID, `{"email":"owner-own-bucket@example.com"}`),
	)

	python := venue.ServePython(t, requests)

	// Every plane's mail, in the order sent. What each plane sent is compared
	// below by RECIPIENT, never assumed to be one per 201.
	pythonCaptured := sink.Drain(t)
	pythonMails := make([]decodedInviteMail, 0, len(pythonCaptured))
	for _, captured := range pythonCaptured {
		pythonMails = append(pythonMails, decodeInviteMail(t, smtpcapture.Normalize(captured)))
	}

	t.Setenv("EMAIL_PROVIDER", "smtp")
	t.Setenv("EMAIL_FROM_ADDRESS", fromAddress)
	t.Setenv("SMTP_HOST", smtpHost)
	t.Setenv("SMTP_PORT", strconv.Itoa(smtpPort))
	unsetMailEnv(t)
	goSender, err := mailpkg.NewSenderFromEnv(nil)
	if err != nil {
		t.Fatalf("go mail sender: %v", err)
	}
	goBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Invites = admin.InviteConfig{Mail: goSender, TokenSecret: jwtKey, AppBaseURL: appBaseURL, AppBaseURLSet: true}
	})

	var goResponses []venueoracle.Response
	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Inspect: func(_ venueoracle.Request, response venueoracle.Response) {
			goResponses = append(goResponses, response)
		},
		Normalize: func(request venueoracle.Request, body string) string {
			for _, field := range []string{"id", "created_at", "updated_at", "expires_at"} {
				body = redactField(t, body, field)
			}
			return body
		},
	})
	t.Log(receipt)

	// The mail each plane sent, message for message.
	goCaptured := sink.Drain(t)
	goMails := make([]decodedInviteMail, 0, len(goCaptured))
	for _, captured := range goCaptured {
		goMails = append(goMails, decodeInviteMail(t, smtpcapture.Normalize(captured)))
	}
	recipients := func(mails []decodedInviteMail) []string {
		out := make([]string, len(mails))
		for i, m := range mails {
			out[i] = m.RcptTo
		}
		return out
	}
	if !reflect.DeepEqual(recipients(pythonMails), recipients(goMails)) {
		t.Fatalf("the planes mailed different recipients:\n python: %q\n go:     %q", recipients(pythonMails), recipients(goMails))
	}
	for i := range pythonMails {
		py, gv := pythonMails[i], goMails[i]
		py.Token, gv.Token = "", ""
		if py != gv {
			t.Errorf("invite email %d differs:\n python: %+v\n go:     %+v", i+1, py, gv)
		}
	}

	// The token: each plane's own emails carry a token that is exactly
	// _build_token(invite id) under the shared secret, and the stored hash is
	// the sha256 of it -- so a token minted by either plane is one the other
	// would accept.
	verify := func(label, database string, responses []venueoracle.Response, mails []decodedInviteMail) {
		for _, response := range responses {
			if response.Status != 201 {
				continue
			}
			var body struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal([]byte(response.Body), &body); err != nil {
				t.Fatalf("%s: %v", label, err)
			}
			stored := venueoracle.TableRows(t, ctx, database, fmt.Sprintf(`SELECT token_hash FROM org_invites WHERE id = '%s'`, body.ID))
			found := false
			for _, m := range mails {
				if strings.HasPrefix(m.Token, strings.ReplaceAll(body.ID, "-", "")+".") {
					checkInviteToken(t, label, m.Token, body.ID, jwtKey, stored)
					found = true
				}
			}
			if !found {
				t.Logf("%s: no email carried invite %s's token (its send failed or was skipped)", label, body.ID)
			}
		}
	}
	verify("python", venue.AdminURI(t, venue.SourceDB), python, pythonMails)
	verify("go", venue.AdminURI(t, venue.GoDB), goResponses, goMails)

	// The rows each route left, ignoring only what is random or clock-derived.
	rowsQuery := `SELECT coalesce(string_agg(concat_ws('|', org_id::text, email, role, status, invited_by_id::text,
  round(extract(epoch from (expires_at - created_at)) / 3600)::text, coalesce(accepted_at::text, '<null>')), E'\x1e' ORDER BY email, org_id), '')
FROM org_invites`
	pythonRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), rowsQuery)
	goRows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), rowsQuery)
	if pythonRows != goRows {
		t.Errorf("org_invites rows differ:\n python: %q\n go:     %q", pythonRows, goRows)
	}
	auditQuery := `SELECT coalesce(string_agg(concat_ws('|', org_id::text, user_id::text, action, resource_type, description, status,
  coalesce(error_message, '<null>')), E'\x1e' ORDER BY changes::text, org_id), '')
FROM audit_logs WHERE action = 'member_invited'`
	pythonAudit := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), auditQuery)
	goAudit := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), auditQuery)
	if pythonAudit != goAudit {
		t.Errorf("member_invited audit rows differ:\n python: %q\n go:     %q", pythonAudit, goAudit)
	}
	if pythonAudit == "" {
		t.Error("no member_invited audit rows on either plane: the comparison compared nothing")
	}
	compareAuditJSONWithSpacingGap(t, ctx, venue, `action = 'member_invited'`, "changes")

	// The rate-limit is per (admin, path): the eleventh call above was
	// limited on both planes (Diff compared status and body), and the
	// invite rows for the limited call must not exist on either plane.
	limited := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), `SELECT count(*) FROM org_invites WHERE email = 'over@example.com'`)
	if limited != "0" {
		t.Errorf("the rate-limited invite was created on the Go plane: %s rows", limited)
	}

	// Best-effort mail: a transport that fails never changes the answer.
	// Python's send_invite_email failure is caught and logged (orgs.py), so
	// the same request is a 201 there; here a second Go server's sender
	// points at a port nothing listens on, and the invite and its audit row
	// still commit. (Python's side of this contract is read from source, not
	// diffed: the venue's Python plane has one fixed mail configuration.)
	t.Setenv("SMTP_PORT", "1")
	brokenSender, err := mailpkg.NewSenderFromEnv(nil)
	if err != nil {
		t.Fatal(err)
	}
	if sendErr := brokenSender.Send(ctx, mailpkg.Message{To: "x@example.com", Subject: "s", HTML: "<p>x</p>"}); sendErr == nil {
		t.Fatal("the broken sender is supposed to fail against a closed port")
	}
	brokenBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Invites = admin.InviteConfig{Mail: brokenSender, TokenSecret: jwtKey}
	})
	stranded := venueoracle.Do(t, brokenBase, post("invite with a failing mail sender", "owner", orgID, `{"email":"stranded@example.com"}`))
	if stranded.Status != 201 {
		t.Fatalf("status %d %s, want 201: a mail failure must not change the answer", stranded.Status, stranded.Body)
	}
	rows := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), `SELECT count(*) FROM org_invites WHERE email = 'stranded@example.com' AND status = 'pending'`)
	audit := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), `SELECT count(*) FROM audit_logs WHERE action = 'member_invited' AND changes::text LIKE '%stranded@example.com%'`)
	if rows != "1" || audit != "1" {
		t.Errorf("invite rows %s, audit rows %s, want 1 and 1 after a failed send", rows, audit)
	}
}

func strPtr(value string) *string { return &value }

// unsetMailEnv removes every mail variable the Go sender reads, so the test's
// own values are the only ones in play. A variable that is SET to "" is an
// error to the sender (SMTP_TLS_SERVER_NAME), so each one is removed, not
// blanked; t.Setenv first so the original value is restored afterwards.
func unsetMailEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{"EMAIL_API_KEY", "RESEND_API_KEY", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_USE_TLS", "SMTP_TLS_CA_FILE", "SMTP_TLS_SERVER_NAME"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
}
