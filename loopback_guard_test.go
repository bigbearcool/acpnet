package main

import (
	"net"
	"strings"
	"testing"
)

func TestLoopbackOnlyRejectsUnsafeListeners(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"default wildcard", []string{"--loopback-only"}},
		{"tcp wildcard", []string{"--loopback-only", "--listen", ":4601"}},
		{"tcp dns", []string{"--loopback-only", "--listen", "localhost:4601"}},
		{"tcp lan", []string{"--loopback-only", "--listen", "192.0.2.1:4601"}},
		{"http wildcard", []string{"--loopback-only", "--listen", "127.0.0.1:0", "--http-listen", "0.0.0.0:4602"}},
		{"malformed", []string{"--loopback-only", "--listen", "127.0.0.1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ACPX_HOST_BRIDGE_LISTEN", "0.0.0.0:4601")
			t.Setenv("ACPX_HOST_BRIDGE_HTTP_LISTEN", "")
			err := runServe(append([]string{"--token", "test"}, tc.args...))
			if err == nil || !strings.Contains(err.Error(), "requires literal loopback listeners") {
				t.Fatalf("expected loopback rejection, got %v", err)
			}
		})
	}
}

func TestLoopbackOnlyAcceptsLiteralAddressesAndLegacyRemainsUnrestricted(t *testing.T) {
	for _, tc := range []struct {
		name, host string
		guard      bool
	}{
		{"ipv4 guarded", "127.0.0.1", true},
		{"ipv6 guarded", "::1", true},
		{"wildcard legacy", "0.0.0.0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", net.JoinHostPort(tc.host, "0"))
			if err != nil {
				t.Skipf("listener unavailable: %v", err)
			}
			defer ln.Close()
			t.Setenv("ACPX_HOST_BRIDGE_HTTP_LISTEN", "")
			args := []string{"--token", "test", "--listen", ln.Addr().String()}
			if tc.guard {
				args = append(args, "--loopback-only")
			}
			err = runServe(args)
			if err == nil || !strings.Contains(err.Error(), "address already in use") {
				t.Fatalf("expected bind failure after validation, got %v", err)
			}
		})
	}
}
