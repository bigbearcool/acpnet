package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// reverseHarness wires a reverse client and server over an in-memory pipe.
type reverseHarness struct {
	clientDone chan error
	clientInW  *io.PipeWriter
	clientOutR *io.PipeReader
	hostInR    *io.PipeReader
	hostOutW   *io.PipeWriter
	hostIn     *bufio.Reader
	clientOut  *bufio.Reader
	connA      net.Conn
	connB      net.Conn
	cancel     context.CancelFunc
}

func startReverseHarness(t *testing.T) *reverseHarness {
	t.Helper()
	a, b := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	clientInR, clientInW := io.Pipe()
	clientOutR, clientOutW := io.Pipe()
	hostInR, hostInW := io.Pipe()
	hostOutR, hostOutW := io.Pipe()
	clientDone := make(chan error, 1)
	go func() { clientDone <- runReverseClient(ctx, a, clientInR, clientOutW) }()
	go runReverseServer(ctx, b, hostOutR, hostInW, nil)
	h := &reverseHarness{
		clientDone: clientDone,
		clientInW:  clientInW,
		clientOutR: clientOutR,
		hostInR:    hostInR,
		hostOutW:   hostOutW,
		hostIn:     bufio.NewReader(hostInR),
		clientOut:  bufio.NewReader(clientOutR),
		connA:      a,
		connB:      b,
		cancel:     cancel,
	}
	t.Cleanup(func() {
		cancel()
		_ = a.Close()
		_ = b.Close()
		_ = clientInW.Close()
		_ = clientOutR.Close()
		_ = hostInR.Close()
		_ = hostOutW.Close()
	})
	return h
}

func writeLine(t *testing.T, w io.Writer, s string) {
	t.Helper()
	go func() { _, _ = io.WriteString(w, s) }()
}

func readLineWithin(t *testing.T, r *bufio.Reader, d time.Duration) string {
	t.Helper()
	type result struct {
		line []byte
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		line, err := r.ReadBytes('\n')
		ch <- result{line, err}
	}()
	select {
	case got := <-ch:
		if got.err != nil {
			t.Fatalf("read line: %v (%q)", got.err, got.line)
		}
		return string(got.line)
	case <-time.After(d):
		t.Fatalf("timed out waiting for line")
		return ""
	}
}

// openSession performs a session/new round trip and returns the mapped MCP URL.
func openSession(t *testing.T, h *reverseHarness, sessionID string, targetURL string) string {
	t.Helper()
	req := `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"mcpServers":[{"type":"http","name":"local","url":"` + targetURL + `"}]}}` + "\n"
	writeLine(t, h.clientInW, req)
	line := readLineWithin(t, h.hostIn, 2*time.Second)
	mapped := extractMCPServerURL(t, line)
	var request rpcMessage
	if err := json.Unmarshal([]byte(line), &request); err != nil {
		t.Fatal(err)
	}
	resp := `{"jsonrpc":"2.0","id":` + string(request.ID) + `,"result":{"sessionId":"` + sessionID + `"}}` + "\n"
	writeLine(t, h.hostOutW, resp)
	readLineWithin(t, h.clientOut, 2*time.Second)
	return mapped
}

func extractMCPServerURL(t *testing.T, line string) string {
	t.Helper()
	var v struct {
		Params struct {
			MCPServers []struct {
				URL string `json:"url"`
			} `json:"mcpServers"`
		} `json:"params"`
	}
	if err := json.Unmarshal([]byte(line), &v); err != nil || len(v.Params.MCPServers) == 0 {
		t.Fatalf("bad session/new line: %q (%v)", line, err)
	}
	return v.Params.MCPServers[0].URL
}

func TestReverseSSEStreamsBeforeUpstreamCompletes(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseUpstream := func() { releaseOnce.Do(func() { close(release) }) }
	finished := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, "data: second\n\n")
		w.(http.Flusher).Flush()
	}))
	defer target.Close()
	defer releaseUpstream()

	h := startReverseHarness(t)
	mapped := openSession(t, h, "s1", target.URL+"/sse")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(mapped)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	t.Logf("status=%d headers=%v", resp.StatusCode, resp.Header)
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content type %q", ct)
	}
	br := bufio.NewReader(resp.Body)
	select {
	case <-finished:
		// upstream legitimately finished; the first event must still arrive.
	case <-time.After(500 * time.Millisecond):
	}
	firstLine, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("first SSE event not streamed: %v", err)
	}
	if firstLine != "data: first\n" {
		t.Fatalf("first event %q", firstLine)
	}
	releaseUpstream()
	rest, err := io.ReadAll(br)
	if err != nil {
		t.Fatal(err)
	}
	if got := firstLine + string(rest); got != "data: first\n\ndata: second\n\n" {
		t.Fatalf("stream body %q", got)
	}
}

func TestReverseAuthorizationIsScopedToACPSession(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer target.Close()
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "other")
	}))
	defer other.Close()

	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	defer inR.Close()
	defer inW.Close()
	defer outR.Close()
	defer outW.Close()
	go io.Copy(io.Discard, outR)
	go runReverseClient(ctx, a, inR, outW)
	writer := reverseWriter{w: b}
	reader := bufio.NewReader(b)

	newLine := `{"jsonrpc":"2.0","id":"new-1","method":"session/new","params":{"mcpServers":[{"type":"http","url":"` + target.URL + `/mcp"}]}}` + "\n"
	writeLine(t, inW, newLine)
	_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
	f, err := readReverseFrame(reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessionNewTargets(f.Data); err != nil {
		t.Fatalf("session/new not forwarded: %v", err)
	}
	var first rpcMessage
	_ = json.Unmarshal(f.Data, &first)
	if err := writer.send(reverseFrame{Type: "acp", Data: []byte(`{"jsonrpc":"2.0","id":` + string(first.ID) + `,"result":{"sessionId":"s1"}}` + "\n")}); err != nil {
		t.Fatal(err)
	}
	// Give the client a chance to bind the session before probing.
	time.Sleep(50 * time.Millisecond)

	writeLine(t, inW, `{"jsonrpc":"2.0","id":"new-2","method":"session/new","params":{"mcpServers":[{"type":"http","url":"`+other.URL+`/mcp"}]}}`+"\n")
	_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
	f, err = readReverseFrame(reader)
	if err != nil {
		t.Fatal(err)
	}
	var second rpcMessage
	_ = json.Unmarshal(f.Data, &second)
	if err := writer.send(reverseFrame{Type: "acp", Data: []byte(`{"jsonrpc":"2.0","id":` + string(second.ID) + `,"result":{"sessionId":"s2"}}` + "\n")}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	probe := func(id uint64, session, targetURL, want string) {
		t.Helper()
		if err := writer.send(reverseFrame{Type: "request", ID: id, Session: session, Target: targetURL + "/mcp", Method: "GET"}); err != nil {
			t.Fatal(err)
		}
		f, err := readReverseFrame(reader)
		if err != nil {
			t.Fatal(err)
		}
		if f.ID != id || string(f.Data) != want || (want == "" && f.Error == "") {
			t.Fatalf("session %s target %s: got %#v (want data %q)", session, targetURL, f, want)
		}
	}

	// Unknown session cannot use a declared target.
	probe(7, "s3", target.URL, "")
	// A session cannot reach another session's declared target.
	probe(9, "s2", target.URL, "")
	probe(11, "s1", other.URL, "")
	// Each session can reach its own target.
	probe(8, "s1", target.URL, "ok")
	probe(12, "s2", other.URL, "other")

	writeLine(t, inW, `{"jsonrpc":"2.0","id":13,"method":"session/close","params":{"sessionId":"s1"}}`+"\n")
	_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := readReverseFrame(reader); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	// Closing one session revokes only that session.
	probe(14, "s1", target.URL, "")
	probe(15, "s2", other.URL, "other")
}

func TestReverseClosedSessionRevokesListener(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer target.Close()

	h := startReverseHarness(t)
	mapped := openSession(t, h, "s1", target.URL+"/mcp")
	resp, err := http.Get(mapped)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	writeLine(t, h.clientInW, `{"jsonrpc":"2.0","id":2,"method":"session/close","params":{"sessionId":"s1"}}`+"\n")
	readLineWithin(t, h.hostIn, 2*time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := http.Get(mapped)
		if err != nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("listener for closed session still reachable")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReversePreservesAppendedQuery(t *testing.T) {
	var gotURI string
	seen := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURI = r.URL.RequestURI()
		seen <- struct{}{}
		_, _ = io.WriteString(w, "ok")
	}))
	defer target.Close()

	h := startReverseHarness(t)
	mapped := openSession(t, h, "s1", target.URL+"/mcp?base=1")
	resp, err := http.Post(mapped+"?added=2", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	select {
	case <-seen:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream request missing")
	}
	if gotURI != "/mcp?base=1&added=2" {
		t.Fatalf("upstream URI %q", gotURI)
	}
}

func TestReverseRejectsUnforwardableLoopbackLoudly(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	defer inR.Close()
	defer inW.Close()
	defer outR.Close()
	defer outW.Close()
	go runReverseClient(ctx, a, inR, outW)

	writeLine(t, inW, `{"jsonrpc":"2.0","id":42,"method":"session/new","params":{"mcpServers":[{"type":"http","url":"http://localhost:8080/mcp"}]}}`+"\n")
	line := readLineWithin(t, bufio.NewReader(outR), 2*time.Second)
	var resp struct {
		ID    int             `json:"id"`
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal([]byte(line), &resp); err != nil || resp.ID != 42 || len(resp.Error) == 0 {
		t.Fatalf("expected local JSON-RPC error, got %q", line)
	}
	_ = b.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, err := readReverseFrame(bufio.NewReader(b)); err == nil {
		t.Fatal("invalid loopback session/new leaked to the host")
	}
}

func TestReverseRejectsOversizedLocalRequestBody(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = io.WriteString(w, "ok")
	}))
	defer target.Close()

	h := startReverseHarness(t)
	mapped := openSession(t, h, "s1", target.URL+"/mcp")
	body := bytes.Repeat([]byte("x"), reverseBodyLimit+1)
	resp, err := http.Post(mapped, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestReversePendingRequestTimesOut(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer target.Close()

	prev := reversePendingTimeout
	reversePendingTimeout = 150 * time.Millisecond
	defer func() { reversePendingTimeout = prev }()

	h := startReverseHarness(t)
	mapped := openSession(t, h, "s1", target.URL+"/mcp")
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(mapped)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestReverseMaximumACPLineFitsInFrame(t *testing.T) {
	if got := len(makeACPLine(acpLineLimit)) * 4 / 3; got >= reverseFrameLimit {
		t.Fatalf("frame limit %d cannot carry a base64-encoded maximum ACP line (%d)", reverseFrameLimit, got)
	}
	line := makeACPLine(acpLineLimit)
	var buf bytes.Buffer
	w := reverseWriter{w: &buf}
	if err := w.send(reverseFrame{Type: "acp", Data: line}); err != nil {
		t.Fatal(err)
	}
	if buf.Len() >= reverseFrameLimit {
		t.Fatalf("encoded frame %d exceeds limit %d", buf.Len(), reverseFrameLimit)
	}
	f, err := readReverseFrame(bufio.NewReader(&buf))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.Data, line) {
		t.Fatalf("round trip mismatch: %d vs %d bytes", len(f.Data), len(line))
	}
}

func TestReverseOversizedACPLineIsRejected(t *testing.T) {
	line := append(bytes.Repeat([]byte("a"), 64), '\n')
	if _, err := readBoundedLine(bufio.NewReader(bytes.NewReader(line)), 32); err == nil {
		t.Fatal("oversized ACP line accepted")
	}
	ok := []byte("short\n")
	got, err := readBoundedLine(bufio.NewReader(bytes.NewReader(ok)), 32)
	if err != nil || string(got) != string(ok) {
		t.Fatalf("bounded read failed: %q %v", got, err)
	}
}

func makeACPLine(size int) []byte {
	line := bytes.Repeat([]byte("a"), size-1)
	return append(line, '\n')
}

func TestReverseFrameLimitRejectsOversizedFrame(t *testing.T) {
	raw := []byte(`{"type":"acp","data":"`)
	raw = append(raw, bytes.Repeat([]byte("A"), reverseFrameLimit)...)
	raw = append(raw, '"', '}', '\n')
	if _, err := readReverseFrame(bufio.NewReader(bytes.NewReader(raw))); err == nil {
		t.Fatal("oversized frame accepted")
	}
}

func TestRelayStreamRejectsOversizedACPLine(t *testing.T) {
	input := makeACPLine(acpLineLimit + 1)
	var out bytes.Buffer
	mappings := pathMappings{{from: "/workspace", to: "/tmp"}}
	if _, err := relayStream(&out, bytes.NewReader(input), mappings, rewriteClientToHost); err == nil {
		t.Fatal("relayStream buffered an oversized ACP line")
	}
	if out.Len() != 0 {
		t.Fatalf("oversized line was forwarded: %d bytes", out.Len())
	}
}

func TestReverseClientRefusesOversizedACPInput(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	defer inR.Close()
	defer inW.Close()
	defer outR.Close()
	defer outW.Close()
	go io.Copy(io.Discard, outR)
	go runReverseClient(ctx, a, inR, outW)

	go func() { _, _ = inW.Write(makeACPLine(acpLineLimit + 1024)) }()
	_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
	f, err := readReverseFrame(bufio.NewReader(b))
	if err != nil {
		return // closing the bridge is an acceptable loud failure
	}
	if f.Type != "eof" || len(f.Data) != 0 {
		t.Fatalf("oversized ACP input was forwarded as %q (%d bytes)", f.Type, len(f.Data))
	}
}
