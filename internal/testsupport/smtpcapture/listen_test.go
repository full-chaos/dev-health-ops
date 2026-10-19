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

func TestValidateListen(t *testing.T) {
	for _, good := range []string{"127.0.0.1:0", "0.0.0.0:2525", "[::1]:2525", ":2525"} {
		if err := ValidateListen(good); err != nil {
			t.Errorf("%q refused: %v", good, err)
		}
	}
	for _, bad := range []string{"not-an-address", "127.0.0.1", "127.0.0.1:port", "127.0.0.1:70000", ""} {
		if ValidateListen(bad) == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestValidateExternalListen(t *testing.T) {
	for _, good := range []string{"0.0.0.0:2525", "127.0.0.1:2525", ":2525"} {
		if err := ValidateExternalListen(good); err != nil {
			t.Errorf("%q refused: %v", good, err)
		}
	}
	for _, bad := range []string{"127.0.0.1:0", "0.0.0.0:0", ":0", "not-an-address", ""} {
		if ValidateExternalListen(bad) == nil {
			t.Errorf("%q accepted for an external target, which cannot know a random port", bad)
		}
	}
}
