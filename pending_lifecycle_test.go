package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPendingAuthorizationIsolation(t *testing.T) {
	s := newReverseClientSessions()
	s.begin("a", []string{"http://127.0.0.1:1234/a"})
	s.begin("b", []string{"http://127.0.0.1:1234/b"})
	if !s.authorizePending("a", "http://127.0.0.1:1234/a") || s.authorizePending("b", "http://127.0.0.1:1234/a") || s.authorizePending("missing", "http://127.0.0.1:1234/a") {
		t.Fatal("pending authorization not isolated")
	}
	s.complete("a", "s", false)
	if !s.authorizePending("a", "http://127.0.0.1:1234/a") {
		t.Fatal("in-flight pending request lost after binding")
	}
	s.revoke("s")
	if s.authorizePending("a", "http://127.0.0.1:1234/a") {
		t.Fatal("pending alias survived close")
	}
}

func TestPendingEndpointExpires(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newReverseHub(ctx, &reverseWriter{w: io.Discard})
	defer h.closeAll()
	h.pendingLifetime = 30 * time.Millisecond
	mapped := h.mapTarget("1", "http://127.0.0.1:1234/mcp")
	deadline := time.Now().Add(time.Second)
	for {
		h.mu.Lock()
		n := len(h.byTarget)
		h.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pending listener did not expire")
		}
		time.Sleep(time.Millisecond)
	}
	c := http.Client{Timeout: 100 * time.Millisecond}
	if r, e := c.Get(mapped); e == nil {
		r.Body.Close()
		t.Fatal("expired endpoint reachable")
	}
}

func TestPreSessionForwardEndToEnd(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }))
	defer target.Close()
	h := startReverseHarness(t)
	writeLine(t, h.clientInW, `{"jsonrpc":"2.0","id":77,"method":"session/new","params":{"mcpServers":[{"url":"`+target.URL+`/mcp"}]}}`+"\n")
	mapped := extractMCPServerURL(t, readLineWithin(t, h.hostIn, 2*time.Second))
	c := http.Client{Timeout: time.Second}
	r, e := c.Get(mapped)
	if e != nil {
		t.Fatal(e)
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	if r.StatusCode != 200 || string(b) != "ok" {
		t.Fatalf("status=%d body=%s", r.StatusCode, b)
	}
}

func TestCloseCancelsInFlightUpstream(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done(); close(stopped) }))
	defer target.Close()
	h := startReverseHarness(t)
	mapped := openSession(t, h, "close-me", target.URL+"/mcp")
	go func() {
		c := http.Client{Timeout: 2 * time.Second}
		r, e := c.Get(mapped)
		if e == nil {
			r.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request not started")
	}
	writeLine(t, h.clientInW, `{"jsonrpc":"2.0","id":2,"method":"session/close","params":{"sessionId":"close-me"}}`+"\n")
	readLineWithin(t, h.hostIn, time.Second)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("upstream not canceled")
	}
}
