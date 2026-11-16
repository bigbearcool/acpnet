package main

import "testing"

func TestClientLocalMCPTarget(t *testing.T) {
	cases := []struct {
		input string
		valid bool
	}{
		{"http://127.0.0.1:8080/mcp", true},
		{"http://[::1]:8080/mcp", true},
		{"https://127.0.0.1:8080/mcp", true},
		{"http://localhost:8080/mcp", false},
		{"http://127.0.0.2:8080/mcp", false},
		{"http://127.0.0.1.evil.test:8080/mcp", false},
		{"http://127.0.0.1:8080@evil.test/mcp", false},
		{"http://127.0.0.1/mcp", false},
		{"http://127.0.0.1:8080/mcp#fragment", false},
		{"http://user:secret@127.0.0.1:8080/mcp", false},
		{"file:///etc/passwd", false},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			if err := validateClientLocalMCPTarget(tc.input); (err == nil) != tc.valid {
				t.Errorf("valid=%v, got error %v", tc.valid, err)
			}
		})
	}
}

// Custom hostnames are deliberately not looked up: the remote agent resolves
// them after pass-through, even if a client hosts file maps one to loopback.
func TestClientLocalMCPURLClassification(t *testing.T) {
	cases := []struct {
		url   string
		local bool
	}{
		{"http://localhost:8080/mcp", true},
		{"http://api.localhost.:8080/mcp", true},
		{"http://127.0.0.2:8080/mcp", true},
		{"http://[::]:8080/mcp", true},
		{"http://127.1:8080/mcp", true},
		{"http://custom-local.test:8080/mcp", false},
		{"http://example.com:8080/mcp", false},
	}
	for _, tc := range cases {
		t.Run(tc.url, func(t *testing.T) {
			if got := isClientLocalMCPURL(tc.url); got != tc.local {
				t.Errorf("got %v, want %v", got, tc.local)
			}
		})
	}
	line := []byte(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"mcpServers":[{"url":"http://custom-local.test:8080/mcp"},{"url":"http://localhost:8080/mcp"}]}}`)
	bad := unforwardableLocalTargets(line)
	if len(bad) != 1 || bad[0] != "http://localhost:8080/mcp" {
		t.Fatalf("unexpected rejected URLs: %v", bad)
	}
}
