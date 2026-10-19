package smtpcapture

import "testing"

func TestListenAddrDefaultsToLoopback(t *testing.T) {
	t.Setenv(EnvListen, "")
	if got := ListenAddr(); got != "127.0.0.1:0" {
		t.Errorf("ListenAddr with %s unset = %q, want loopback 127.0.0.1:0", EnvListen, got)
	}
	t.Setenv(EnvListen, "0.0.0.0:2525")
	if got := ListenAddr(); got != "0.0.0.0:2525" {
		t.Errorf("ListenAddr with %s set = %q, want the value", EnvListen, got)
	}
}

func TestStartAtListensAtTheGivenAddress(t *testing.T) {
	probe := Start(t)
	_, port := probe.HostPort(t)
	probe.listener.Close()
	server := StartAt(t, "127.0.0.1:"+itoa(port))
	if _, got := server.HostPort(t); got != port {
		t.Errorf("StartAt(127.0.0.1:%d) listens on port %d", port, got)
	}
}

func itoa(n int) string {
	digits := ""
	for ; n > 0; n /= 10 {
		digits = string(rune('0'+n%10)) + digits
	}
	return digits
}
