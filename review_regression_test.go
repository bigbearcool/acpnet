package main

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestReviewRejectLocalAliases(t *testing.T) {
	for _, u := range []string{"http://localhost.:1234/mcp", "http://127.1:1234/mcp", "http://0x7f000001:1234/mcp"} {
		if !isClientLocalMCPURL(u) {
			t.Errorf("local alias passed through: %s", u)
		}
	}
}
func TestReviewQueryAuthorization(t *testing.T) {
	s := newReverseClientSessions()
	s.begin("1", []string{"http://127.0.0.1:1234/mcp?token=secret"})
	s.complete("1", "s", false)
	if s.authorize("s", "http://127.0.0.1:1234/mcp?token=other") {
		t.Fatal("changed query authorized")
	}
}
func TestReviewPendingEndpointBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newReverseHub(ctx, &reverseWriter{w: io.Discard})
	defer h.closeAll()
	for i := 0; i < 100; i++ {
		h.mapTarget(string(rune(1000+i)), "http://127.0.0.1:12345/mcp")
	}
	if len(h.byTarget) > 64 {
		t.Fatalf("unbounded listeners: %d", len(h.byTarget))
	}
}
func TestReviewPreSessionForward(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frames := make(chan reverseFrame, 1)
	reader, writer := io.Pipe()
	h := newReverseHub(ctx, &reverseWriter{w: writer})
	defer h.closeAll()
	defer reader.Close()
	endpoint := h.mapTarget("1", "http://127.0.0.1:12345/mcp")
	go func() {
		f, e := readReverseFrame(bufio.NewReader(reader))
		if e == nil {
			frames <- f
			h.deliver(reverseFrame{Type: "response", ID: f.ID, Status: 200, Data: []byte("ok")})
		}
	}()
	c := http.Client{Timeout: time.Second}
	resp, err := c.Get(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("pre-session status %d", resp.StatusCode)
	}
	select {
	case <-frames:
	default:
		t.Fatal("no request forwarded")
	}
}
