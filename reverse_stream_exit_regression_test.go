package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestReverseStreamUpstreamFailureIsNotCleanEOF(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, b, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = fmt.Fprint(b, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n")
		_ = b.Flush() // No terminating chunk: the upstream failed mid-stream.
	}))
	defer target.Close()
	h := startReverseHarness(t)
	mapped := openSession(t, h, "failed-stream", target.URL+"/sse")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(mapped)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || string(body) != "hello" || err == nil {
		t.Fatalf("status=%d body=%q error=%v; expected upstream failure", resp.StatusCode, body, err)
	}
}

func TestReverseEOFDrainsAgentOutput(t *testing.T) {
	h := startReverseHarness(t)
	_ = h.clientInW.Close()
	done := make(chan struct{})
	go func() { _, _ = io.WriteString(h.hostOutW, "{\"jsonrpc\":\"2.0\",\"method\":\"final\"}\n"); close(done) }()
	line := readLineWithin(t, h.clientOut, time.Second)
	if line != "{\"jsonrpc\":\"2.0\",\"method\":\"final\"}\n" {
		t.Fatalf("unexpected output: %q", line)
	}
	<-done
	_ = h.hostOutW.Close()
	select {
	case err := <-h.clientDone:
		if err != nil {
			t.Fatalf("client: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not exit")
	}
}
