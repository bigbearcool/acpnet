package main

// Independent boundary tests for the ACPNet reverse bridge, copied from the
// independent review reproduction package. Every test drives the real bridge over a real loopback TCP
// socket against real HTTP targets (httptest), so backpressure and cancellation
// travel through real kernels instead of net.Pipe.
//
// Boundaries:
//   IB01 slow-consumer overflow must not surface as a truncated-but-clean 200,
//        and the endpoint must stay usable.
//   IB02 an overflowing stream in one ACP session must not disturb a live
//        stream in another session.
//   IB03 prompt consumer cancels must return all 32 in-flight request slots.
//   IB03b ACP $/cancelRequest for a tool id is forwarded verbatim and does not
//        disturb an unrelated stream; session/close stops its stream, cancels
//        upstream, retires its endpoint, and leaves the other session alive.
//   IB04 the shared 32-slot limiter rejects a second session quickly and
//        cleanly under saturation, and that session recovers afterwards.
//   IB05 stalled consumer that abandons the stream: fully consuming the queue
//        and then leaving must cancel the upstream request and release its
//        slot. This reproduced a leak on the reviewed candidate.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type ibBridge struct {
	clientInW  *io.PipeWriter
	hostOutW   *io.PipeWriter
	hostLines  chan string
	outLines   chan string
	clientDone chan error
	serverDone chan error
	cancel     context.CancelFunc
	conns      []net.Conn
	closeOnce  sync.Once
	mu         sync.Mutex
	hostSeen   []string
	outSeen    []string
}

func ibStart(t *testing.T) *ibBridge {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	serverConn, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	clientInR, clientInW := io.Pipe()
	clientOutR, clientOutW := io.Pipe()
	hostInR, hostInW := io.Pipe()
	hostOutR, hostOutW := io.Pipe()
	b := &ibBridge{
		clientInW: clientInW, hostOutW: hostOutW,
		hostLines: make(chan string, 8192), outLines: make(chan string, 8192),
		clientDone: make(chan error, 1), serverDone: make(chan error, 1),
		cancel: cancel, conns: []net.Conn{clientConn, serverConn},
	}
	go func() { b.clientDone <- runReverseClient(ctx, clientConn, clientInR, clientOutW) }()
	go func() { b.serverDone <- runReverseServer(ctx, serverConn, hostOutR, hostInW, nil) }()
	go ibPump(bufio.NewReader(clientOutR), b.outLines, &b.mu, &b.outSeen)
	go ibPump(bufio.NewReader(hostInR), b.hostLines, &b.mu, &b.hostSeen)
	t.Cleanup(func() {
		b.closeOnce.Do(func() {
			cancel()
			for _, c := range b.conns {
				_ = c.Close()
			}
			_ = clientInW.Close()
			_ = clientOutR.Close()
			_ = hostInR.Close()
			_ = hostOutW.Close()
		})
	})
	return b
}

func ibPump(r *bufio.Reader, ch chan string, mu *sync.Mutex, seen *[]string) {
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			s := string(line)
			mu.Lock()
			*seen = append(*seen, s)
			mu.Unlock()
			ch <- s
		}
		if err != nil {
			close(ch)
			return
		}
	}
}

func (b *ibBridge) writeIn(t *testing.T, s string) {
	t.Helper()
	done := make(chan error, 1)
	go func() { _, err := io.WriteString(b.clientInW, s); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ACP client stdin write: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("ACP client stdin write blocked for 3s")
	}
}

func (b *ibBridge) writeHost(t *testing.T, s string) {
	t.Helper()
	done := make(chan error, 1)
	go func() { _, err := io.WriteString(b.hostOutW, s); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("host stdin write: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("host stdin write blocked for 3s")
	}
}

func ibWait(t *testing.T, what string, ch chan string, d time.Duration, match func(string) bool) string {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case line, ok := <-ch:
			if !ok {
				t.Fatalf("%s: stream closed while waiting", what)
			}
			if match == nil || match(line) {
				return line
			}
		case <-deadline:
			t.Fatalf("%s: timed out after %v", what, d)
		}
	}
}

// openSession performs session/new with a distinct ACP client ID and returns
// the mapped endpoint URLs in declaration order.
func (b *ibBridge) openSession(t *testing.T, clientID int, sessionID string, targets ...string) []string {
	t.Helper()
	servers := make([]string, 0, len(targets))
	for _, target := range targets {
		servers = append(servers, fmt.Sprintf(`{"type":"http","name":"local","url":%q}`, target))
	}
	req := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"session/new","params":{"mcpServers":[%s]}}`+"\n",
		clientID, strings.Join(servers, ","))
	b.writeIn(t, req)
	line := ibWait(t, "client->host session/new", b.hostLines, 3*time.Second, func(s string) bool {
		return strings.Contains(s, `"session/new"`)
	})
	var msg struct {
		ID     json.RawMessage `json:"id"`
		Params struct {
			MCPServers []struct {
				URL string `json:"url"`
			} `json:"mcpServers"`
		} `json:"params"`
	}
	if err := json.Unmarshal([]byte(line), &msg); err != nil || len(msg.Params.MCPServers) == 0 {
		t.Fatalf("unexpected host session/new line: %q (%v)", line, err)
	}
	mapped := make([]string, 0, len(msg.Params.MCPServers))
	for _, s := range msg.Params.MCPServers {
		mapped = append(mapped, s.URL)
	}
	b.writeHost(t, fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":{"sessionId":%q}}`+"\n", string(msg.ID), sessionID))
	ibWait(t, "host->client session/new response", b.outLines, 3*time.Second, func(s string) bool {
		var resp struct {
			ID json.RawMessage `json:"id"`
		}
		if json.Unmarshal([]byte(s), &resp) != nil {
			return false
		}
		return string(resp.ID) == fmt.Sprintf("%d", clientID)
	})
	return mapped
}

func ibReadN(t *testing.T, r io.Reader, n int, d time.Duration, what string) (int, error) {
	t.Helper()
	type result struct {
		n   int
		err error
	}
	ch := make(chan result, 1)
	go func() {
		buf := make([]byte, n)
		got, err := io.ReadFull(r, buf)
		ch <- result{got, err}
	}()
	select {
	case got := <-ch:
		return got.n, got.err
	case <-time.After(d):
		t.Fatalf("%s: read blocked for %v", what, d)
		return 0, nil
	}
}

func ibCountEvents(body []byte) int {
	return bytes.Count(body, []byte("data: "))
}

// ibWatchCancel counts upstream cancellations that happen while the handler is
// still streaming (a normal handler return also cancels r.Context()).
func ibWatchCancel(r *http.Request, completed *atomic.Bool, counter *atomic.Int64, stop <-chan struct{}) {
	go func() {
		select {
		case <-r.Context().Done():
			if !completed.Load() {
				counter.Add(1)
			}
		case <-stop:
		}
	}()
}

func ibWaitFor(t *testing.T, what string, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// IB01: a consumer that stops reading must not receive a truncated body that
// looks like a complete, clean stream.
func TestIB01SlowConsumerOverflowIsNotCleanSuccess(t *testing.T) {
	const total = 600
	const chunk = 32 << 10
	var wrote atomic.Int64
	var completed atomic.Bool
	var canceled atomic.Int64
	stop := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if r.URL.Query().Get("small") != "" {
			for i := 0; i < 5; i++ {
				_, _ = fmt.Fprint(w, "data: small\n\n")
				w.(http.Flusher).Flush()
			}
			return
		}
		ibWatchCancel(r, &completed, &canceled, stop)
		payload := bytes.Repeat([]byte("x"), chunk)
		fl := w.(http.Flusher)
		for i := 0; i < total; i++ {
			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
				return
			}
			wrote.Add(1)
			fl.Flush()
		}
		completed.Store(true)
	}))
	defer func() { close(stop); target.Close() }()

	b := ibStart(t)
	mapped := b.openSession(t, 1, "sess-slow", target.URL+"/sse")[0]

	resp, err := (&http.Client{}).Get(mapped)
	if err != nil {
		t.Fatalf("connect to endpoint: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	start := time.Now()
	time.Sleep(2500 * time.Millisecond) // stall: never read the body
	body, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	elapsed := time.Since(start)
	events := ibCountEvents(body)
	time.Sleep(300 * time.Millisecond)
	t.Logf("EVIDENCE IB01 status=200 events=%d/%d bytes=%d readErr=%v upstreamWrote=%d upstreamCanceledMidStream=%d elapsed=%v",
		events, total, len(body), readErr, wrote.Load(), canceled.Load(), elapsed)

	if events == total {
		t.Fatalf("boundary not exercised: consumer received every event; increase producer volume")
	}
	if readErr == nil {
		t.Fatalf("silent truncation: %d/%d events delivered then a clean EOF", events, total)
	}
	if elapsed > 12*time.Second {
		t.Fatalf("overflow abort took %v; expected a prompt failure", elapsed)
	}

	small, err := (&http.Client{Timeout: 10 * time.Second}).Get(mapped + "?small=1")
	if err != nil {
		t.Fatalf("endpoint unusable after overflow: %v", err)
	}
	smallBody, err := io.ReadAll(small.Body)
	_ = small.Body.Close()
	if err != nil || small.StatusCode != 200 || ibCountEvents(smallBody) != 5 {
		t.Fatalf("post-overflow request: status=%d events=%d err=%v", small.StatusCode, ibCountEvents(smallBody), err)
	}
	t.Logf("EVIDENCE IB01 post-overflow request ok: events=%d", ibCountEvents(smallBody))
}

// IB02: one session's overflow must not kill another session's live stream.
func TestIB02OverflowDoesNotDisturbOtherSession(t *testing.T) {
	const bigTotal = 600
	const chunk = 32 << 10
	const steadyEvents = 60
	var bigWrote, bigCanceled, steadyCanceled atomic.Int64
	var bigDone, steadyDone atomic.Bool
	stop := make(chan struct{})

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		switch r.URL.Path {
		case "/big":
			ibWatchCancel(r, &bigDone, &bigCanceled, stop)
			payload := bytes.Repeat([]byte("x"), chunk)
			for i := 0; i < bigTotal; i++ {
				if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
					return
				}
				bigWrote.Add(1)
				fl.Flush()
			}
			bigDone.Store(true)
		case "/steady":
			ibWatchCancel(r, &steadyDone, &steadyCanceled, stop)
			for i := 0; i < steadyEvents; i++ {
				select {
				case <-r.Context().Done():
					return
				case <-stop:
					return
				case <-time.After(40 * time.Millisecond):
				}
				if _, err := fmt.Fprintf(w, "data: event-%02d\n\n", i); err != nil {
					return
				}
				fl.Flush()
			}
			steadyDone.Store(true)
		default:
			_, _ = fmt.Fprint(w, "ok")
		}
	}))
	defer func() { close(stop); target.Close() }()

	b := ibStart(t)
	bigURL := b.openSession(t, 1, "sess-A", target.URL+"/big")[0]
	steadyURL := b.openSession(t, 2, "sess-B", target.URL+"/steady")[0]
	if bigURL == steadyURL {
		t.Fatalf("sessions share one endpoint: %s", bigURL)
	}

	type steadyResult struct {
		status int
		events int
		err    error
		dur    time.Duration
	}
	steadyCh := make(chan steadyResult, 1)
	go func() {
		start := time.Now()
		resp, err := (&http.Client{Timeout: 20 * time.Second}).Get(steadyURL)
		if err != nil {
			steadyCh <- steadyResult{err: err}
			return
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		steadyCh <- steadyResult{resp.StatusCode, ibCountEvents(body), readErr, time.Since(start)}
	}()
	time.Sleep(250 * time.Millisecond) // B mid-stream before A stalls

	bigResp, err := (&http.Client{}).Get(bigURL)
	if err != nil {
		t.Fatalf("A: connect: %v", err)
	}
	bigStart := time.Now()
	time.Sleep(2500 * time.Millisecond)
	bigBody, bigErr := io.ReadAll(bigResp.Body)
	_ = bigResp.Body.Close()
	bigEvents := ibCountEvents(bigBody)
	bigDur := time.Since(bigStart)

	var steady steadyResult
	select {
	case steady = <-steadyCh:
	case <-time.After(20 * time.Second):
		t.Fatalf("session B stream did not finish within 20s")
	}
	t.Logf("EVIDENCE IB02 A: events=%d/%d err=%v upstreamWrote=%d upstreamCanceledMidStream=%d dur=%v",
		bigEvents, bigTotal, bigErr, bigWrote.Load(), bigCanceled.Load(), bigDur)
	t.Logf("EVIDENCE IB02 B: status=%d events=%d/%d err=%v upstreamCanceledMidStream=%d dur=%v",
		steady.status, steady.events, steadyEvents, steady.err, steadyCanceled.Load(), steady.dur)

	if bigEvents == bigTotal {
		t.Fatalf("A: overflow not exercised (received all %d events)", bigTotal)
	}
	if bigErr == nil {
		t.Fatalf("A: truncated stream ended in a clean EOF")
	}
	if steady.err != nil || steady.status != 200 || steady.events != steadyEvents {
		t.Fatalf("B stream damaged by A overflow: status=%d events=%d/%d err=%v",
			steady.status, steady.events, steadyEvents, steady.err)
	}
	if steadyCanceled.Load() != 0 {
		t.Fatalf("B upstream was cancelled by A's overflow")
	}
	if steady.dur > 10*time.Second {
		t.Fatalf("B stream stretched to %v", steady.dur)
	}

	after, err := (&http.Client{Timeout: 10 * time.Second}).Get(steadyURL)
	if err != nil {
		t.Fatalf("B after A overflow: %v", err)
	}
	afterBody, err := io.ReadAll(after.Body)
	_ = after.Body.Close()
	if err != nil || ibCountEvents(afterBody) != steadyEvents {
		t.Fatalf("B repeat after A overflow: events=%d err=%v", ibCountEvents(afterBody), err)
	}
	t.Logf("EVIDENCE IB02 B repeat after A overflow ok: events=%d", ibCountEvents(afterBody))
}

// IB03: prompt consumer cancels must return every request slot (32 in flight).
func TestIB03PromptCancelsReleaseCapacity(t *testing.T) {
	var active, canceled, probeActive, probeMax atomic.Int64
	stop := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("mode") == "ok" {
			n := probeActive.Add(1)
			for {
				cur := probeMax.Load()
				if n <= cur || probeMax.CompareAndSwap(cur, n) {
					break
				}
			}
			defer probeActive.Add(-1)
			time.Sleep(300 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"ok":true}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		active.Add(1)
		defer active.Add(-1)
		var done atomic.Bool
		ibWatchCancel(r, &done, &canceled, stop)
		fl := w.(http.Flusher)
		payload := bytes.Repeat([]byte("y"), 4096)
		for {
			select {
			case <-r.Context().Done():
				return
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
				return
			}
			fl.Flush()
		}
	}))
	defer func() { close(stop); target.Close() }()

	b := ibStart(t)
	mapped := b.openSession(t, 1, "sess-cancel", target.URL+"/endless")[0]
	client := &http.Client{}

	const cycles = 20
	leaks := 0
	for i := 0; i < cycles; i++ {
		resp, err := client.Get(mapped)
		if err != nil {
			t.Fatalf("cycle %d: connect: %v", i, err)
		}
		if _, rerr := ibReadN(t, resp.Body, 128, 5*time.Second, fmt.Sprintf("cycle %d stream", i)); rerr != nil {
			t.Fatalf("cycle %d: no stream data: %v", i, rerr)
		}
		_ = resp.Body.Close()
		if !ibWaitFor(t, fmt.Sprintf("cycle %d cancel", i), 5*time.Second, func() bool { return active.Load() == 0 }) {
			leaks++
			t.Logf("EVIDENCE IB03 cycle %d: upstream still active after 5s (active=%d canceled=%d)", i, active.Load(), canceled.Load())
			if !ibWaitFor(t, "settle", 5*time.Second, func() bool { return active.Load() == 0 }) {
				break
			}
		}
	}
	t.Logf("EVIDENCE IB03 prompt-cancel cycles=%d leaks=%d upstreamCanceled=%d", cycles, leaks, canceled.Load())
	if leaks != 0 {
		t.Fatalf("%d/%d prompt cancels failed to stop the upstream request", leaks, cycles)
	}

	const parallel = 32 // the bridge's in-flight limit
	var failures atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := (&http.Client{Timeout: 15 * time.Second}).Get(mapped + "?mode=ok")
			if err != nil {
				failures.Add(1)
				return
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != 200 || string(body) != `{"ok":true}` {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()
	t.Logf("EVIDENCE IB03 parallel=%d failures=%d maxConcurrent=%d", parallel, failures.Load(), probeMax.Load())
	if failures.Load() != 0 {
		t.Fatalf("%d/%d concurrent requests failed after cancel cycles (capacity leaked)", failures.Load(), parallel)
	}
	if probeMax.Load() != parallel {
		t.Fatalf("max concurrency %d, want %d: slots not fully released", probeMax.Load(), parallel)
	}
}

// IB03b: tool-level ACP cancel is forwarded verbatim and does not disturb an
// unrelated stream; session/close stops its stream and retires its endpoint.
func TestIB03bToolCancelForwardedAndSessionCloseIsolated(t *testing.T) {
	var cancelA, cancelB atomic.Int64
	var doneA, doneB atomic.Bool
	stop := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		switch r.URL.Path {
		case "/endless":
			ibWatchCancel(r, &doneA, &cancelA, stop)
			payload := bytes.Repeat([]byte("z"), 2048)
			for {
				select {
				case <-r.Context().Done():
					return
				case <-stop:
					return
				case <-time.After(5 * time.Millisecond):
				}
				if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
					return
				}
				fl.Flush()
			}
		case "/tiny":
			ibWatchCancel(r, &doneB, &cancelB, stop)
			for i := 0; i < 5; i++ {
				_, _ = fmt.Fprint(w, "data: tiny\n\n")
				fl.Flush()
			}
			doneB.Store(true)
		default:
			_, _ = fmt.Fprint(w, "ok")
		}
	}))
	defer func() { close(stop); target.Close() }()

	b := ibStart(t)
	urlA := b.openSession(t, 1, "sess-A", target.URL+"/endless")[0]
	urlB := b.openSession(t, 2, "sess-B", target.URL+"/tiny")[0]

	respA, err := (&http.Client{}).Get(urlA)
	if err != nil {
		t.Fatalf("A stream: %v", err)
	}
	defer func() { _ = respA.Body.Close() }()
	if _, err := ibReadN(t, respA.Body, 1024, 5*time.Second, "A first bytes"); err != nil {
		t.Fatalf("A: no data: %v", err)
	}

	// Tool-level cancel: the ACP client sends it toward the host.
	cancelLine := `{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":4242}}`
	b.writeIn(t, cancelLine+"\n")
	fwd := ibWait(t, "forwarded tool cancel", b.hostLines, 3*time.Second, func(s string) bool {
		return strings.Contains(s, "$/cancelRequest")
	})
	if strings.TrimSpace(fwd) != cancelLine {
		t.Fatalf("tool cancel rewritten in transit: %q", fwd)
	}
	if _, err := ibReadN(t, respA.Body, 2048, 5*time.Second, "A stream after tool cancel"); err != nil {
		t.Fatalf("tool cancel killed an unrelated stream: %v", err)
	}
	if cancelA.Load() != 0 {
		t.Fatalf("tool cancel killed the upstream stream")
	}
	t.Logf("EVIDENCE IB03b tool cancel forwarded verbatim; stream A still live")

	// Close session A (client-authoritative) while its stream is in flight.
	b.writeIn(t, `{"jsonrpc":"2.0","method":"session/close","params":{"sessionId":"sess-A"}}`+"\n")
	ibWait(t, "host observed session/close", b.hostLines, 3*time.Second, func(s string) bool {
		return strings.Contains(s, "session/close")
	})
	if !ibWaitFor(t, "session-close upstream cancel", 5*time.Second, func() bool { return cancelA.Load() > 0 }) {
		t.Fatalf("session/close did not cancel the upstream stream (canceled=%d, upstreamActive=%v)", cancelA.Load(), cancelA.Load() == 0)
	}
	bodyA, readErrA := io.ReadAll(respA.Body)
	t.Logf("EVIDENCE IB03b after session/close: events=%d readErr=%v upstreamCanceledMidStream=%d", ibCountEvents(bodyA), readErrA, cancelA.Load())
	if readErrA == nil {
		t.Fatalf("session/close stream end looked clean (expected an aborted/truncated body)")
	}
	if _, err := (&http.Client{Timeout: 3 * time.Second}).Get(urlA); err == nil {
		t.Fatalf("endpoint for closed session still accepts requests")
	}
	t.Logf("EVIDENCE IB03b closed-session endpoint refuses new requests")

	respB, err := (&http.Client{Timeout: 10 * time.Second}).Get(urlB)
	if err != nil {
		t.Fatalf("B after A session close: %v", err)
	}
	bodyB, err := io.ReadAll(respB.Body)
	_ = respB.Body.Close()
	if err != nil || respB.StatusCode != 200 || ibCountEvents(bodyB) != 5 {
		t.Fatalf("B damaged by A session close: status=%d events=%d err=%v", respB.StatusCode, ibCountEvents(bodyB), err)
	}
	if cancelB.Load() != 0 {
		t.Fatalf("B upstream was cancelled by A session close")
	}
	t.Logf("EVIDENCE IB03b session B unaffected: events=%d", ibCountEvents(bodyB))
}

// IB04: shared capacity. Saturating session A must reject session B quickly and
// cleanly, and B must recover once A releases.
func TestIB04SaturatedSessionIsolatesCleanRejection(t *testing.T) {
	var hangActive atomic.Int64
	stop := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("mode") == "ok" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"ok":true}`)
			return
		}
		hangActive.Add(1)
		defer hangActive.Add(-1)
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}))
	defer func() { close(stop); target.Close() }()

	b := ibStart(t)
	urlA := b.openSession(t, 1, "sess-hang", target.URL+"/hang")[0]
	urlB := b.openSession(t, 2, "sess-ok", target.URL+"/ok")[0]

	const stalled = 32
	var cancels []context.CancelFunc
	var holderWg sync.WaitGroup
	for i := 0; i < stalled; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlA, nil)
		if err != nil {
			t.Fatalf("build request %d: %v", i, err)
		}
		holderWg.Add(1)
		go func() {
			defer holderWg.Done()
			resp, err := (&http.Client{}).Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}()
	}
	if !ibWaitFor(t, "stalled requests", 10*time.Second, func() bool { return hangActive.Load() >= stalled }) {
		t.Fatalf("only %d/%d stalled requests reached the upstream target", hangActive.Load(), stalled)
	}

	startB := time.Now()
	respB, errB := (&http.Client{Timeout: 5 * time.Second}).Get(urlB + "?mode=ok")
	rejectDuration := time.Since(startB)
	status := 0
	var bodyB []byte
	if errB == nil {
		status = respB.StatusCode
		bodyB, _ = io.ReadAll(respB.Body)
		_ = respB.Body.Close()
	}
	t.Logf("EVIDENCE IB04 saturation: stalled=%d B status=%d body=%q err=%v dur=%v",
		hangActive.Load(), status, string(bodyB), errB, rejectDuration)
	if errB == nil && status == 200 {
		t.Fatalf("session B succeeded while A held every request slot")
	}
	if rejectDuration > 3*time.Second {
		t.Fatalf("session B rejection took %v; expected a prompt bounded failure", rejectDuration)
	}

	for _, cancel := range cancels {
		cancel()
	}
	holderWg.Wait()
	if !ibWaitFor(t, "stalled upstreams", 5*time.Second, func() bool { return hangActive.Load() == 0 }) {
		t.Fatalf("%d stalled upstream requests survived cancellation", hangActive.Load())
	}

	var recStatus int
	var recBody []byte
	var recErr error
	recovered := false
	for attempt := 0; attempt < 20; attempt++ {
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(urlB + "?mode=ok")
		if err != nil {
			recErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		recStatus = resp.StatusCode
		recBody, recErr = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if recErr == nil && recStatus == 200 {
			recovered = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("EVIDENCE IB04 recovery: status=%d body=%q err=%v", recStatus, string(recBody), recErr)
	if !recovered {
		t.Fatalf("session B did not recover after A released slots: status=%d body=%q err=%v", recStatus, string(recBody), recErr)
	}
}

// IB05: a stalled consumer that exhausts the queue and then abandons the stream
// must stop the upstream request and release its slot. This is the boundary the
// reviewed candidate does not close: handleEndpoint exits through the chunk
// write error without sending a cancel frame, so the upstream keeps streaming.
func TestIB05AbandonedStalledStreamCancelsUpstream(t *testing.T) {
	const chunk = 32 << 10
	var wrote, upstreamActive, upstreamCanceled atomic.Int64
	var probeActive, probeMax atomic.Int64
	stop := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("mode") == "ok" {
			n := probeActive.Add(1)
			for {
				cur := probeMax.Load()
				if n <= cur || probeMax.CompareAndSwap(cur, n) {
					break
				}
			}
			defer probeActive.Add(-1)
			time.Sleep(400 * time.Millisecond)
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"ok":true}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		upstreamActive.Add(1)
		defer upstreamActive.Add(-1)
		var done atomic.Bool
		ibWatchCancel(r, &done, &upstreamCanceled, stop)
		fl := w.(http.Flusher)
		payload := bytes.Repeat([]byte("x"), chunk)
		for i := 0; i < 4000; i++ {
			select {
			case <-stop:
				return
			case <-time.After(30 * time.Millisecond):
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", payload); err != nil {
				return
			}
			wrote.Add(1)
			fl.Flush()
		}
		done.Store(true)
	}))
	defer func() { close(stop); target.Close() }()

	b := ibStart(t)
	mapped := b.openSession(t, 1, "sess-abandon", target.URL+"/sse")[0]

	resp, err := (&http.Client{}).Get(mapped)
	if err != nil {
		t.Fatalf("connect to endpoint: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	// Stall long enough that the downstream write blocks and the pending queue
	// saturates, then abandon the stream the way a disconnected host would.
	time.Sleep(8 * time.Second)
	wroteAtAbandon := wrote.Load()
	_ = resp.Body.Close()

	canceled := ibWaitFor(t, "upstream cancel after abandon", 5*time.Second, func() bool {
		return upstreamCanceled.Load() > 0
	})
	time.Sleep(time.Second)
	t.Logf("EVIDENCE IB05 after abandon: upstreamCanceled=%v canceledCount=%d upstreamActive=%d wroteAtAbandon=%d wroteNow=%d",
		canceled, upstreamCanceled.Load(), upstreamActive.Load(), wroteAtAbandon, wrote.Load())
	if !canceled {
		t.Errorf("upstream request still running after the stalled consumer abandoned the stream (wrote %d -> %d events); no cancel frame reached the client",
			wroteAtAbandon, wrote.Load())
	}

	// A leaked upstream keeps one of the 32 in-flight slots.
	const parallel = 32
	var wg sync.WaitGroup
	var failures atomic.Int64
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := (&http.Client{Timeout: 15 * time.Second}).Get(mapped + "?mode=ok")
			if err != nil {
				failures.Add(1)
				return
			}
			body, _ := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if r.StatusCode != 200 || string(body) != `{"ok":true}` {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()
	t.Logf("EVIDENCE IB05 capacity: parallel=%d failures=%d maxConcurrent=%d (want %d/0/%d)",
		parallel, failures.Load(), probeMax.Load(), parallel, parallel)
	if failures.Load() != 0 || probeMax.Load() != parallel {
		t.Errorf("capacity after abandoned stream: %d/%d probes failed, maxConcurrent=%d (want 0 failures and %d)",
			failures.Load(), parallel, probeMax.Load(), parallel)
	}
}
