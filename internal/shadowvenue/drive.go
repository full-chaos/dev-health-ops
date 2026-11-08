package shadowvenue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// MinGap is the least time between one answer and the next request: above the shadow's default 2 s decision
// timeout, so a slot's late result line can never fall in the next slot's window.
const MinGap = 2300 * time.Millisecond

const loginPath = "/api/v1/auth/login"

// CheckVenueBase is the one check of every origin the driver sends to: a plain-http origin with no path, query or
// credentials whose host is loopback, or go-api's own address on the venue's internal network (a container on an
// internal-only network has no published port). Such a host must be the private IP the egress record read just before
// this drive names as goapi_ip; that record must be readable, show an internal network with no default route, and name
// this drive's run id (run_id), so a record of another drive is refused. Any other host is refused before any request.
func CheckVenueBase(raw string, record EgressRead, runID string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || u.Opaque != "" {
		return nil, fmt.Errorf("base %q: want http://127.0.0.1:<port> (loopback) or go-api's venue IP, no path, query or credentials", raw)
	}
	host := u.Hostname()
	ip := net.ParseIP(host)
	switch {
	case host == "localhost" || (ip != nil && ip.IsLoopback()):
	case ip == nil || !ip.IsPrivate():
		return nil, fmt.Errorf("base %q: the host is neither loopback nor a private IP", raw)
	case record.Err != nil || record.Fields == nil:
		return nil, fmt.Errorf("base %q: a non-loopback base needs the readable egress record of the read before the drive (-egress)", raw)
	case record.Fields["network_internal"] != "true" || record.Fields["default_route"] != "none":
		return nil, fmt.Errorf("base %q: egress record %s does not show an internal network with no default route", raw, record.Name)
	case !net.ParseIP(record.Fields["goapi_ip"]).Equal(ip):
		return nil, fmt.Errorf("base %q: the host is not go-api's venue address %q in egress record %s", raw, record.Fields["goapi_ip"], record.Name)
	case runID == noRun || record.Fields["run_id"] != runID:
		return nil, fmt.Errorf("base %q: egress record %s names run %q, not this drive's run %q (-run-id)", raw, record.Name, record.Fields["run_id"], runID)
	}
	u.Path = ""
	return u, nil
}

// ReadPassword reads one password from an owner-only regular file. The value never reaches an error.
func ReadPassword(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("password file %s: %w", path, errors.Unwrap(err))
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > 1024 {
		return "", fmt.Errorf("password file %s: want an owner-only (0600) regular file of at most 1 KiB", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("password file %s: unreadable", path)
	}
	value := strings.TrimRight(string(raw), "\r\n")
	if value == "" || strings.ContainsAny(value, "\r\n") {
		return "", fmt.Errorf("password file %s: want one non-empty line", path)
	}
	return value, nil
}

// Driver sends a plan's slots to the venue go-api, one at a time. Base is the raw origin; every request takes its origin
// from CheckVenueBase(Base, Egress, RunID) (origin), so no request path has a check of its own.
type Driver struct {
	Base string
	// AllowExternal are the outside services a slot's route may call (-allow-external); a slot on a route of another
	// service refuses the drive before any request, as MakePlan makes it a gap.
	AllowExternal []string
	Egress        EgressRead
	RunID         string
	Client        *http.Client
	// Gap is the wait after each answer; at least MinGap.
	Gap time.Duration
	// Frozen is the frozen throwaway ids: before any request, a slot that is destructive by its own flag or by its
	// route's pinned class, and whose path or body holds another id, refuses the whole drive; so does a slot on a route
	// with no class.
	Frozen ThrowawayIDs
	Sleep  func(context.Context, time.Duration) error
	Now    func() time.Time
	// BodyKey and KeysOut write the side file of per-leaf HMACs of each answer; both or neither.
	BodyKey []byte
	KeysOut io.Writer
}

// origin is the checked origin of a request.
func (d *Driver) origin() (*url.URL, error) { return CheckVenueBase(d.Base, d.Egress, d.RunID) }

func (d *Driver) now() time.Time {
	if d.Now != nil {
		return d.Now().UTC()
	}
	return time.Now().UTC()
}

func (d *Driver) sleep(ctx context.Context, gap time.Duration) error {
	if d.Sleep != nil {
		return d.Sleep(ctx, gap)
	}
	timer := time.NewTimer(gap)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Login signs p in on go-api's login route (orgID "" asks for no org) and returns the access token. The token and
// the answer's body never reach an error.
func (d *Driver) Login(ctx context.Context, p Principal, orgID string) (string, error) {
	password, err := ReadPassword(p.PasswordFile)
	if err != nil {
		return "", fmt.Errorf("login %s: %w", p.Name, err)
	}
	in := map[string]string{"email": p.Email, "password": password}
	if orgID != "" {
		in["org_id"] = orgID
	}
	body, _ := json.Marshal(in)
	origin, err := d.origin()
	if err != nil {
		return "", fmt.Errorf("login %s: %w", p.Name, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, origin.String()+loginPath, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("login %s: build request", p.Name)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.Client.Do(req)
	if err != nil {
		return "", fmt.Errorf("login %s: transport failure", p.Name)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("login %s: http %d", p.Name, resp.StatusCode)
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if json.Unmarshal(raw, &out) != nil || out.AccessToken == "" {
		return "", fmt.Errorf("login %s: http 200 with no access_token (unverified account?)", p.Name)
	}
	return out.AccessToken, nil
}

// Drive sends every slot of plan in order with its principal's token, waits Gap after each answer, and writes the
// slots file to w. A gap slot is written, not sent. Each slot records the claims its token carries (read locally, never
// sent) and the digest of the answer; a token with less than MinTokenLife left stops the drive before its slot. Answer
// bodies are digested and dropped.
func (d *Driver) Drive(ctx context.Context, plan Plan, tokens map[string]string, w io.Writer) error {
	if d.Gap < MinGap {
		return fmt.Errorf("drive: gap %s is below %s (the shadow decision timeout plus a margin)", d.Gap, MinGap)
	}
	claims := map[string]TokenClaims{}
	for name, token := range tokens {
		if c, err := DecodeClaims(token); err == nil {
			claims[name] = c
		}
	}
	for _, slot := range plan.Slots {
		if !slot.Gap() && tokens[slot.Principal] == "" {
			return fmt.Errorf("drive: principal %s of row %s has no token", slot.Principal, slot.Row)
		}
		if slot.Gap() {
			continue
		}
		if c, ok := claims[slot.Principal]; ok && c.Exp.Sub(d.now()) < MinTokenLife {
			return fmt.Errorf("drive: the token of principal %s expires in %s (less than %s): refused before any request", slot.Principal, c.Exp.Sub(d.now()).Round(time.Second), MinTokenLife)
		}
		c, ok := classOf(slot.Key())
		switch {
		case !ok:
			return fmt.Errorf("drive: slot %d row %s refused before any request: route %s has no class in the pinned table", slot.Seq, slot.Row, slot.Key())
		case (strings.HasPrefix(slot.Row, "C") || slot.Expect == expectMismatch) && (c.ext != none || c.destructive):
			return fmt.Errorf("drive: slot %d row %s refused before any request: a control on %s, an external or destructive route", slot.Seq, slot.Row, slot.Key())
		case c.ext != none && !slices.Contains(d.AllowExternal, c.ext):
			return fmt.Errorf("drive: slot %d row %s refused before any request: route %s calls %s, not allowed (-allow-external %s)", slot.Seq, slot.Row, slot.Key(), c.ext, c.ext)
		}
		if slot.Destructive || c.destructive {
			if err := d.Frozen.Holds(slot.Path, slot.Body); err != nil {
				return fmt.Errorf("drive: destructive slot %d row %s refused before any request: %w", slot.Seq, slot.Row, err)
			}
		}
	}
	if (d.KeysOut != nil) != (d.BodyKey != nil) {
		return errors.New("drive: the side file and its key go together")
	}
	if d.KeysOut != nil {
		if _, err := io.WriteString(d.KeysOut, keysHeader+"\n"); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(w, slotsHeader+"\n"); err != nil {
		return err
	}
	for _, slot := range plan.Slots {
		if !slot.Gap() {
			if c, ok := claims[slot.Principal]; ok {
				if left := c.Exp.Sub(d.now()); left < MinTokenLife {
					return fmt.Errorf("drive: slot %d row %s stopped before its request: the token of principal %s expires in %s (less than %s)", slot.Seq, slot.Row, slot.Principal, left.Round(time.Second), MinTokenLife)
				}
				slot.ClaimSub, slot.ClaimOrg, slot.ClaimExp = c.Sub, c.Org, c.Exp
			}
			if err := d.send(ctx, &slot, tokens[slot.Principal]); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(w, slot.line()+"\n"); err != nil {
			return err
		}
		if !slot.Gap() {
			if err := d.sleep(ctx, d.Gap); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Driver) send(ctx context.Context, slot *Slot, token string) error {
	var body io.Reader
	if slot.Body != none {
		body = strings.NewReader(slot.Body)
	}
	origin, err := d.origin()
	if err != nil {
		return fmt.Errorf("drive: slot %d: %w", slot.Seq, err)
	}
	req, err := http.NewRequestWithContext(ctx, slot.Method, origin.String()+slot.Path, body)
	if err != nil {
		return fmt.Errorf("drive: slot %d: build request", slot.Seq)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	slot.Start = d.now()
	resp, err := d.Client.Do(req)
	if err != nil {
		slot.End, slot.Status, slot.Note = d.now(), 0, "transport failure"
		return ctx.Err()
	}
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, bodyLimit+1))
	_ = resp.Body.Close()
	slot.End, slot.Status = d.now(), resp.StatusCode
	if readErr != nil {
		return fmt.Errorf("drive: slot %d: the answer body ended before its end (%d bytes read): no digest, no slot line", slot.Seq, len(raw))
	}
	if len(raw) > bodyLimit {
		return fmt.Errorf("drive: slot %d: the answer is over 4 MiB: it would be digested cut", slot.Seq)
	}
	digest := DigestBody(slot.Key(), raw, d.Frozen, PinnedVolatile)
	slot.BodyLen, slot.BodySHA, slot.BodyNormSHA = digest.Len, digest.SHA, digest.Norm
	if d.KeysOut != nil {
		for _, leaf := range BodyLeaves(d.BodyKey, raw) {
			if _, err := io.WriteString(d.KeysOut, strconv.Itoa(slot.Seq)+"\t"+leaf.Path+"\t"+leaf.HMAC12+"\n"); err != nil {
				return err
			}
		}
	}
	return nil
}

const slotsHeaderV1 = "seq\trow\tcaller\tprincipal\tcross_org\texpect\tk\tmethod\troute\tpath\tt_start\tt_end\tstatus\tnote"

// slotsHeader is the v2 header: v1 plus the claims the plan wants, the claims the token carried, and the answer digests.
const slotsHeader = slotsHeaderV1 + "\twant_sub\twant_org\tclaim_sub\tclaim_org\tclaim_exp\tbody_len\tbody_sha\tbody_norm_sha"

func (s Slot) line() string {
	stamp := func(t time.Time) string {
		if t.IsZero() {
			return none
		}
		return t.UTC().Format(time.RFC3339Nano)
	}
	note := s.Note
	if note == "" {
		note = none
	}
	text := func(v string) string {
		if v == "" {
			return none
		}
		return v
	}
	bodyLen := none
	if s.BodySHA != "" {
		bodyLen = strconv.Itoa(s.BodyLen)
	}
	return strings.Join([]string{strconv.Itoa(s.Seq), s.Row, s.Caller, s.Principal, strconv.FormatBool(s.CrossOrg), s.Expect,
		strconv.Itoa(s.K), s.Method, s.Route, s.Path, stamp(s.Start), stamp(s.End), strconv.Itoa(s.Status), note,
		text(s.WantSub), text(s.WantOrg), text(s.ClaimSub), text(s.ClaimOrg), stamp(s.ClaimExp), bodyLen, text(s.BodySHA), text(s.BodyNormSHA)}, "\t")
}

// ParseSlots reads a slots file Drive wrote.
func ParseSlots(raw []byte) ([]Slot, error) {
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) > 0 && lines[0] == slotsHeaderV1 {
		return nil, errors.New("slots: header v1 refused: the file has no claim read-back and no body digests (rerun the drive)")
	}
	if len(lines) == 0 || lines[0] != slotsHeader {
		return nil, errors.New("slots: the first line is not the slots header")
	}
	var slots []Slot
	for n, line := range lines[1:] {
		f := strings.Split(line, "\t")
		if len(f) != 22 {
			return nil, fmt.Errorf("slots: line %d has %d columns, want 22", n+2, len(f))
		}
		s := Slot{Row: f[1], Caller: f[2], Principal: f[3], Expect: f[5], Method: f[7], Route: f[8], Path: f[9]}
		var err1, err2, err3, err4, err5 error
		s.Seq, err1 = strconv.Atoi(f[0])
		s.CrossOrg, err2 = strconv.ParseBool(f[4])
		s.K, err3 = strconv.Atoi(f[6])
		s.Status, err4 = strconv.Atoi(f[12])
		if f[13] != none {
			s.Note = f[13]
		}
		orNone := func(v string) string {
			if v == none {
				return ""
			}
			return v
		}
		s.WantSub, s.WantOrg, s.ClaimSub, s.ClaimOrg = orNone(f[14]), orNone(f[15]), orNone(f[16]), orNone(f[17])
		s.BodySHA, s.BodyNormSHA = orNone(f[20]), orNone(f[21])
		var err6, err7 error
		if f[18] != none {
			s.ClaimExp, err6 = time.Parse(time.RFC3339Nano, f[18])
		}
		if f[19] != none {
			s.BodyLen, err7 = strconv.Atoi(f[19])
		}
		if !s.Gap() {
			if s.Start, err5 = time.Parse(time.RFC3339Nano, f[10]); err5 == nil {
				s.End, err5 = time.Parse(time.RFC3339Nano, f[11])
			}
		}
		if err := errors.Join(err1, err2, err3, err4, err5, err6, err7); err != nil {
			return nil, fmt.Errorf("slots: line %d: %w", n+2, err)
		}
		slots = append(slots, s)
	}
	return slots, nil
}
