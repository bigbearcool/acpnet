package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Reverse protocol is explicitly negotiated; legacy peers continue to exchange raw ACP.
//
// ACP is newline-delimited JSON and is carried inside reverse frames as base64.
// Base64 inflates payloads by 4/3, so the frame limit must stay comfortably above
// the encoded size of the largest ACP line we accept.
const (
	acpLineLimit         = 7 << 20
	reverseFrameLimit    = 16 << 20
	reverseBodyLimit     = 4 << 20
	reverseChunkSize     = 32 << 10
	reversePendingSize   = 64
	reverseEndpointLimit = 64
)

// Timeouts are variables so tests can shorten them.
var (
	reverseRequestTimeout = 2 * time.Minute
	reversePendingTimeout = 2 * time.Minute
	reverseStreamTimeout  = 30 * time.Minute
	reverseWriteTimeout   = 30 * time.Second
)

var errACPLineTooLong = errors.New("ACP line too large")

type reverseFrame struct {
	Type    string      `json:"type"`
	Data    []byte      `json:"data,omitempty"`
	ID      uint64      `json:"id,omitempty"`
	Target  string      `json:"target,omitempty"`
	Session string      `json:"session,omitempty"`
	NewKey  string      `json:"newKey,omitempty"`
	Method  string      `json:"method,omitempty"`
	Header  http.Header `json:"header,omitempty"`
	Status  int         `json:"status,omitempty"`
	Stream  bool        `json:"stream,omitempty"`
	Done    bool        `json:"done,omitempty"`
	Error   string      `json:"error,omitempty"`
}

type reverseWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (w *reverseWriter) send(f reverseFrame) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return writeJSONLine(w.w, f)
}

func readReverseFrame(r *bufio.Reader) (reverseFrame, error) {
	var f reverseFrame
	line, err := readBoundedLine(r, reverseFrameLimit)
	if err != nil {
		return f, err
	}
	if len(line) == 0 {
		return f, io.ErrUnexpectedEOF
	}
	err = json.Unmarshal(line, &f)
	return f, err
}

// readBoundedLine reads a newline-terminated line without ever buffering more
// than limit bytes (a bare ReadBytes can exhaust memory on a hostile peer).
func readBoundedLine(r *bufio.Reader, limit int) ([]byte, error) {
	var line []byte
	for {
		fragment, err := r.ReadSlice('\n')
		if len(line)+len(fragment) > limit {
			return nil, errACPLineTooLong
		}
		line = append(line, fragment...)
		if err == nil {
			return line, nil
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			if len(line) == 0 {
				return nil, err
			}
			return line, err
		}
	}
}

func boundedBody(r io.Reader) ([]byte, error) {
	b, e := io.ReadAll(io.LimitReader(r, reverseBodyLimit+1))
	if e == nil && len(b) > reverseBodyLimit {
		e = errors.New("body too large")
	}
	return b, e
}

func cleanHeaders(h http.Header) http.Header {
	out := h.Clone()
	for _, v := range out.Values("Connection") {
		for _, key := range strings.Split(v, ",") {
			out.Del(strings.TrimSpace(key))
		}
	}
	for _, k := range []string{"Connection", "Proxy-Connection", "Keep-Alive", "Transfer-Encoding", "Upgrade", "Trailer", "Proxy-Authorization", "Proxy-Authenticate", "Host", "Content-Length"} {
		out.Del(k)
	}
	return out
}

func isEventStream(h http.Header) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(h.Get("Content-Type"))), "text/event-stream")
}

// rpcMessage is the subset of JSON-RPC that session bookkeeping needs.
type rpcMessage struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

func rpcIDKey(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var num json.Number
	if json.Unmarshal(raw, &num) == nil {
		return num.String()
	}
	var str string
	if json.Unmarshal(raw, &str) == nil {
		return "s:" + str
	}
	return string(raw)
}

type sessionNewPlan struct {
	ID      string
	Targets []string
}

// sessionNewTargets extracts the forwardable client-local MCP URLs declared by
// a session/new request, keyed by JSON-RPC id.
func sessionNewTargets(line []byte) (*sessionNewPlan, error) {
	var msg rpcMessage
	if json.Unmarshal(line, &msg) != nil {
		return nil, nil
	}
	if msg.Method != "session/new" {
		return nil, nil
	}
	var params struct {
		MCPServers []struct {
			URL string `json:"url"`
		} `json:"mcpServers"`
	}
	if json.Unmarshal(msg.Params, &params) != nil {
		return nil, nil
	}
	plan := &sessionNewPlan{ID: rpcIDKey(msg.ID)}
	for _, server := range params.MCPServers {
		if validateClientLocalMCPTarget(server.URL) == nil {
			plan.Targets = append(plan.Targets, server.URL)
		}
	}
	return plan, nil
}

// unforwardableLocalTargets lists recognized local-looking MCP URLs that
// cannot be forwarded. Arbitrary DNS names are not resolved or classified.
func unforwardableLocalTargets(line []byte) []string {
	var msg rpcMessage
	if json.Unmarshal(line, &msg) != nil || msg.Method != "session/new" {
		return nil
	}
	var params struct {
		MCPServers []struct {
			URL string `json:"url"`
		} `json:"mcpServers"`
	}
	if json.Unmarshal(msg.Params, &params) != nil {
		return nil
	}
	var bad []string
	for _, server := range params.MCPServers {
		if isClientLocalMCPURL(server.URL) && validateClientLocalMCPTarget(server.URL) != nil {
			bad = append(bad, server.URL)
		}
	}
	return bad
}

// rewriteSessionNewURLs rewrites only explicit session/new MCP URL fields.
func rewriteSessionNewURLs(line []byte, fn func(string) string) []byte {
	var obj map[string]json.RawMessage
	if json.Unmarshal(line, &obj) != nil {
		return line
	}
	var method string
	if json.Unmarshal(obj["method"], &method) != nil || method != "session/new" {
		return line
	}
	var params map[string]json.RawMessage
	if json.Unmarshal(obj["params"], &params) != nil {
		return line
	}
	var servers []map[string]json.RawMessage
	if json.Unmarshal(params["mcpServers"], &servers) != nil {
		return line
	}
	changed := false
	for _, server := range servers {
		var raw string
		if json.Unmarshal(server["url"], &raw) != nil || validateClientLocalMCPTarget(raw) != nil {
			continue
		}
		next := fn(raw)
		if next == "" || next == raw {
			continue
		}
		server["url"], _ = json.Marshal(next)
		changed = true
	}
	if !changed {
		return line
	}
	params["mcpServers"], _ = json.Marshal(servers)
	obj["params"], _ = json.Marshal(params)
	out, err := json.Marshal(obj)
	if err != nil {
		return line
	}
	return append(out, '\n')
}

func rpcErrorLine(id json.RawMessage, message string) []byte {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	out, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   map[string]any{"code": -32602, "message": message},
	})
	return append(out, '\n')
}

func sessionCloseID(line []byte) string {
	var msg rpcMessage
	if json.Unmarshal(line, &msg) != nil || msg.Method != "session/close" {
		return ""
	}
	var params struct {
		SessionID string `json:"sessionId"`
	}
	if json.Unmarshal(msg.Params, &params) != nil {
		return ""
	}
	return params.SessionID
}

// reverseClientSessions tracks which client-local MCP targets each ACP session
// declared. Authorization starts only when the agent confirms the session id
// and is dropped when the session closes.
type reverseClientSessions struct {
	mu       sync.RWMutex
	open     map[string]map[string]struct{}
	pending  map[string][]string
	expires  map[string]time.Time
	aliases  map[string]string
	tokens   map[string]string          // JSON-RPC ID -> unique forwarding generation
	original map[string]json.RawMessage // wire ID -> original client ID
	retired  map[string]struct{}        // generated IDs remain reserved for this connection
	closed   bool
	timers   map[string]*time.Timer
}

func newReverseClientSessions() *reverseClientSessions {
	return &reverseClientSessions{open: map[string]map[string]struct{}{}, pending: map[string][]string{}, expires: map[string]time.Time{}, aliases: map[string]string{}, tokens: map[string]string{}, original: map[string]json.RawMessage{}, retired: map[string]struct{}{}, timers: map[string]*time.Timer{}}
}

func (s *reverseClientSessions) begin(key string, targets []string) bool {
	return s.beginWithToken(key, key, targets)
}

func (s *reverseClientSessions) beginWithToken(key, token string, targets []string) bool {
	if key == "" || len(targets) == 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	for id, expiry := range s.expires {
		if !time.Now().Before(expiry) && s.timers[id] == nil {
			delete(s.pending, id)
			delete(s.expires, id)
			delete(s.tokens, id)
			delete(s.original, id)
		}
	}
	if _, ok := s.pending[key]; ok {
		return false
	}
	if len(s.pending) >= reverseEndpointLimit {
		return false
	}
	s.pending[key] = append([]string(nil), targets...)
	s.tokens[key] = token
	s.expires[key] = time.Now().Add(reversePendingTimeout)
	return true
}

func (s *reverseClientSessions) complete(key, sessionID string, failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeLocked(key, sessionID, failed)
}

func (s *reverseClientSessions) completeLocked(key, sessionID string, failed bool) {
	targets := s.pending[key]
	token := s.tokens[key]
	delete(s.tokens, key)
	if time.Now().After(s.expires[key]) {
		targets = nil
	}
	delete(s.pending, key)
	delete(s.expires, key)
	if failed || sessionID == "" || len(targets) == 0 {
		return
	}
	set := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		set[target] = struct{}{}
	}
	s.open[sessionID] = set
	s.aliases[token] = sessionID
}

func (s *reverseClientSessions) revoke(sessionID string) {
	if sessionID == "" {
		return
	}
	s.mu.Lock()
	delete(s.open, sessionID)
	for key, id := range s.aliases {
		if id == sessionID {
			delete(s.aliases, key)
		}
	}
	s.mu.Unlock()
}

func (s *reverseClientSessions) authorize(sessionID, target string) bool {
	if sessionID == "" {
		return false
	}
	s.mu.RLock()
	set := s.open[sessionID]
	s.mu.RUnlock()
	if set == nil {
		return false
	}
	for declared := range set {
		if sameEndpointPath(declared, target) {
			return true
		}
	}
	return false
}

// Pending keys are scoped to one JSON-RPC request, never to an arbitrary session.
func (s *reverseClientSessions) authorizePending(key, target string) bool {
	return s.authorizePendingSession(key, "", target)
}

func (s *reverseClientSessions) authorizePendingSession(key, session, target string) bool {
	if key == "" {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for requestID, targets := range s.pending {
		if session != "" || s.tokens[requestID] != key || !time.Now().Before(s.expires[requestID]) {
			continue
		}
		for _, declared := range targets {
			if sameEndpointPath(declared, target) {
				return true
			}
		}
	}
	if id := s.aliases[key]; id != "" && (session == "" || session == id) {
		for declared := range s.open[id] {
			if sameEndpointPath(declared, target) {
				return true
			}
		}
	}
	return false
}

// sameEndpointPath binds the declared query and permits distinct extra keys.
func sameEndpointPath(a, b string) bool {
	ua, err := url.Parse(a)
	if err != nil {
		return false
	}
	ub, err := url.Parse(b)
	if err != nil {
		return false
	}
	if ua.Scheme != ub.Scheme || ua.Host != ub.Host || ua.EscapedPath() != ub.EscapedPath() {
		return false
	}
	if ua.RawQuery == ub.RawQuery && ua.ForceQuery == ub.ForceQuery {
		return true
	}
	// Additional parameters must not modify or duplicate any declared key.
	if ua.RawQuery != "" && !strings.HasPrefix(ub.RawQuery, ua.RawQuery+"&") {
		return false
	}
	base, err := url.ParseQuery(ua.RawQuery)
	if err != nil {
		return false
	}
	extra := ub.RawQuery
	if ua.RawQuery != "" {
		extra = strings.TrimPrefix(extra, ua.RawQuery+"&")
	}
	added, err := url.ParseQuery(extra)
	if err != nil {
		return false
	}
	for key := range added {
		if _, exists := base[key]; exists {
			return false
		}
	}
	return true
}

func (s *reverseClientSessions) observe(line []byte) {
	var msg rpcMessage
	if json.Unmarshal(line, &msg) != nil || msg.Method != "" {
		return
	}
	key := rpcIDKey(msg.ID)
	s.mu.RLock()
	_, pending := s.pending[key]
	s.mu.RUnlock()
	if !pending {
		return
	}
	if len(msg.Error) > 0 {
		s.complete(key, "", true)
		return
	}
	var result struct {
		SessionID string `json:"sessionId"`
	}
	_ = json.Unmarshal(msg.Result, &result)
	s.complete(key, result.SessionID, result.SessionID == "")
}

func runReverseClient(parent context.Context, conn io.ReadWriteCloser, input io.Reader, output io.Writer) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer conn.Close()
	go func() { <-ctx.Done(); _ = conn.Close() }()
	writer := &reverseWriter{w: conn}
	sessions := newReverseClientSessions()
	var outputMu sync.Mutex
	writeOutput := func(p []byte) error {
		outputMu.Lock()
		defer outputMu.Unlock()
		_, err := output.Write(p)
		return err
	}
	finish := func(key, reason string) {
		sessions.mu.Lock()
		if id, ok := sessions.original[key]; ok {
			token := sessions.tokens[key]
			if t := sessions.timers[key]; t != nil {
				t.Stop()
			}
			delete(sessions.timers, key)
			delete(sessions.original, key)
			delete(sessions.pending, key)
			delete(sessions.expires, key)
			delete(sessions.tokens, key)
			_ = writeOutput(rpcErrorLine(id, reason))
			_ = writer.send(reverseFrame{Type: "reject", NewKey: token})
		}
		sessions.mu.Unlock()
	}
	defer func() {
		sessions.mu.Lock()
		sessions.closed = true
		keys := make([]string, 0, len(sessions.original))
		for k := range sessions.original {
			keys = append(keys, k)
		}
		sessions.mu.Unlock()
		for _, k := range keys {
			finish(k, "forwarding connection closed")
		}
	}()
	type activeRequest struct {
		session string
		cancel  context.CancelFunc
	}
	active := map[uint64]activeRequest{}
	var activeMu sync.Mutex
	sem := make(chan struct{}, 32)
	transport := &http.Transport{Proxy: nil, MaxIdleConnsPerHost: 4, IdleConnTimeout: 30 * time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	go func() {
		defer writer.send(reverseFrame{Type: "eof"})
		r := bufio.NewReader(input)
		for {
			line, err := readBoundedLine(r, acpLineLimit)
			if len(line) > 0 {
				var incoming rpcMessage
				_ = json.Unmarshal(line, &incoming)
				if incoming.Method == "$/cancelRequest" {
					var params struct {
						ID json.RawMessage `json:"id"`
					}
					_ = json.Unmarshal(incoming.Params, &params)
					sessions.mu.RLock()
					key := ""
					for k, id := range sessions.original {
						if rpcIDKey(id) == rpcIDKey(params.ID) {
							key = k
							break
						}
					}
					sessions.mu.RUnlock()
					if key != "" {
						finish(key, "session request cancelled")
						continue
					}
				}
				if incoming.Method != "" && len(incoming.ID) > 0 {
					sessions.mu.RLock()
					_, reserved := sessions.retired[rpcIDKey(incoming.ID)]
					duplicate := false
					for _, id := range sessions.original {
						if rpcIDKey(id) == rpcIDKey(incoming.ID) {
							duplicate = true
							break
						}
					}
					sessions.mu.RUnlock()
					if reserved || duplicate {
						_ = writeOutput(rpcErrorLine(incoming.ID, "request ID already in use or reserved"))
						continue
					}
				}
				if bad := unforwardableLocalTargets(line); len(bad) > 0 {
					var msg rpcMessage
					_ = json.Unmarshal(line, &msg)
					message := "client-local MCP URL cannot be forwarded (use a literal 127.0.0.1 or [::1] address with an explicit port)"
					if werr := writeOutput(rpcErrorLine(msg.ID, message)); werr != nil {
						return
					}
					continue
				}
				newKey := ""
				if plan, _ := sessionNewTargets(line); plan != nil && len(plan.Targets) > 0 {
					var token [24]byte
					if _, e := rand.Read(token[:]); e != nil {
						return
					}
					newKey = hex.EncodeToString(token[:])
					wireID, _ := json.Marshal("acpnet-forward-" + newKey)
					wireKey := rpcIDKey(wireID)
					if !sessions.beginWithToken(wireKey, newKey, plan.Targets) {
						var msg rpcMessage
						_ = json.Unmarshal(line, &msg)
						_ = writeOutput(rpcErrorLine(msg.ID, "invalid or excessive pending session"))
						continue
					}
					var obj map[string]json.RawMessage
					_ = json.Unmarshal(line, &obj)
					sessions.mu.Lock()
					if sessions.closed {
						sessions.mu.Unlock()
						sessions.complete(wireKey, "", true)
						return
					}
					if len(sessions.retired) >= 65536 {
						sessions.mu.Unlock()
						sessions.complete(wireKey, "", true)
						_ = writeOutput(rpcErrorLine(obj["id"], "forwarding ID capacity exhausted"))
						continue
					}
					sessions.retired[wireKey] = struct{}{}
					sessions.original[wireKey] = obj["id"]
					sessions.timers[wireKey] = time.AfterFunc(reversePendingTimeout, func() { finish(wireKey, "session request timed out") })
					sessions.mu.Unlock()
					obj["id"] = wireID
					line, _ = json.Marshal(obj)
					line = append(line, '\n')
				}
				if id := sessionCloseID(line); id != "" {
					activeMu.Lock()
					sessions.revoke(id)
					for _, req := range active {
						if req.session == id {
							req.cancel()
						}
					}
					activeMu.Unlock()
				}
				if writer.send(reverseFrame{Type: "acp", Data: line, NewKey: newKey}) != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	r := bufio.NewReader(conn)
	for {
		f, err := readReverseFrame(r)
		if err != nil {
			return err
		}
		switch f.Type {
		case "acp":
			var msg rpcMessage
			if json.Unmarshal(f.Data, &msg) == nil && msg.Method == "" {
				key := rpcIDKey(msg.ID)
				sessions.mu.Lock()
				original, ok := sessions.original[key]
				_, reserved := sessions.retired[key]
				expired := ok && !time.Now().Before(sessions.expires[key])
				if expired {
					sessions.mu.Unlock()
					finish(key, "session request timed out")
					continue
				}
				if reserved && !ok {
					sessions.mu.Unlock()
					continue
				}
				if ok {
					if t := sessions.timers[key]; t != nil {
						t.Stop()
					}
					delete(sessions.timers, key)
					var result struct {
						SessionID string `json:"sessionId"`
					}
					_ = json.Unmarshal(msg.Result, &result)
					sessions.completeLocked(key, result.SessionID, len(msg.Error) > 0 || result.SessionID == "")
					var obj map[string]json.RawMessage
					_ = json.Unmarshal(f.Data, &obj)
					obj["id"] = original
					f.Data, _ = json.Marshal(obj)
					f.Data = append(f.Data, '\n')
					err := writeOutput(f.Data)
					delete(sessions.original, key)
					sessions.mu.Unlock()
					if err != nil {
						return err
					}
					continue
				}
				sessions.mu.Unlock()
			}
			if err := writeOutput(f.Data); err != nil {
				return err
			}

		case "request":
			activeMu.Lock()
			if !((f.NewKey == "" && sessions.authorize(f.Session, f.Target)) || sessions.authorizePendingSession(f.NewKey, f.Session, f.Target)) || validateClientLocalMCPTarget(f.Target) != nil {
				activeMu.Unlock()
				_ = writer.send(reverseFrame{Type: "response", ID: f.ID, Error: "target not authorized"})
				continue
			}
			select {
			case sem <- struct{}{}:
			default:
				activeMu.Unlock()
				_ = writer.send(reverseFrame{Type: "response", ID: f.ID, Error: "too many requests"})
				continue
			}
			requestCtx, requestCancel := context.WithCancel(ctx)
			if _, exists := active[f.ID]; exists {
				activeMu.Unlock()
				requestCancel()
				<-sem
				return errors.New("duplicate request ID")
			}
			active[f.ID] = activeRequest{session: f.Session, cancel: requestCancel}
			activeMu.Unlock()
			go func(f reverseFrame, requestCtx context.Context, requestCancel context.CancelFunc) {
				defer func() { requestCancel(); activeMu.Lock(); delete(active, f.ID); activeMu.Unlock(); <-sem }()
				proxyRequest(writer, client, requestCtx, requestCancel, f)
			}(f, requestCtx, requestCancel)
		case "cancel":
			activeMu.Lock()
			cancelRequest := active[f.ID].cancel
			activeMu.Unlock()
			if cancelRequest != nil {
				cancelRequest()
			}
		case "eof":
			return nil
		default:
			return errors.New("invalid reverse frame")
		}
	}
}

// proxyRequest forwards one MCP request. SSE responses are relayed chunk by
// chunk so server-sent events reach the host without waiting for EOF.
func proxyRequest(writer *reverseWriter, client *http.Client, requestCtx context.Context, requestCancel context.CancelFunc, f reverseFrame) {
	req, err := http.NewRequestWithContext(requestCtx, f.Method, f.Target, bytes.NewReader(f.Data))
	if err != nil {
		_ = writer.send(reverseFrame{Type: "response", ID: f.ID, Error: "invalid request"})
		return
	}
	req.Header = cleanHeaders(f.Header)
	headerTimer := time.AfterFunc(reverseRequestTimeout, requestCancel)
	resp, err := client.Do(req)
	headerTimer.Stop()
	if err != nil {
		_ = writer.send(reverseFrame{Type: "response", ID: f.ID, Error: "local MCP request failed"})
		return
	}
	defer resp.Body.Close()
	if isEventStream(resp.Header) {
		if writer.send(reverseFrame{Type: "response", ID: f.ID, Status: resp.StatusCode, Header: cleanHeaders(resp.Header), Stream: true}) != nil {
			return
		}
		streamTimer := time.AfterFunc(reverseStreamTimeout, requestCancel)
		defer streamTimer.Stop()
		buf := make([]byte, reverseChunkSize)
		for {
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				chunk := append([]byte(nil), buf[:n]...)
				if writer.send(reverseFrame{Type: "chunk", ID: f.ID, Data: chunk}) != nil {
					return
				}
			}
			if readErr != nil {
				done := reverseFrame{Type: "response", ID: f.ID, Stream: true, Done: true}
				if !errors.Is(readErr, io.EOF) {
					done.Error = "local MCP stream failed"
				}
				_ = writer.send(done)
				return
			}
		}
	}
	bodyTimer := time.AfterFunc(reverseRequestTimeout, requestCancel)
	defer bodyTimer.Stop()
	body, err := boundedBody(resp.Body)
	if err != nil {
		_ = writer.send(reverseFrame{Type: "response", ID: f.ID, Error: "local MCP response too large"})
		return
	}
	_ = writer.send(reverseFrame{Type: "response", ID: f.ID, Status: resp.StatusCode, Header: cleanHeaders(resp.Header), Data: body})
}

type reverseEndpoint struct {
	listener net.Listener
	server   *http.Server
	path     string
	target   string
	newKey   string
	mapKey   string
	session  string
	timer    *time.Timer
}

type reversePending struct {
	ch       chan reverseFrame
	cancel   context.CancelFunc
	session  string
	newKey   string
	overflow atomic.Bool
}

// reverseHub owns listeners for one ACP connection, grouped by the ACP session
// that declared them.
type reverseHub struct {
	ctx    context.Context
	writer *reverseWriter
	seq    atomic.Uint64

	mu              sync.Mutex
	pending         map[uint64]*reversePending
	byTarget        map[string]*reverseEndpoint
	byNewKey        map[string][]*reverseEndpoint
	bySession       map[string][]*reverseEndpoint
	pendingLifetime time.Duration
	closed          bool
}

func newReverseHub(ctx context.Context, writer *reverseWriter) *reverseHub {
	return &reverseHub{
		ctx:             ctx,
		writer:          writer,
		pending:         map[uint64]*reversePending{},
		byTarget:        map[string]*reverseEndpoint{},
		byNewKey:        map[string][]*reverseEndpoint{},
		bySession:       map[string][]*reverseEndpoint{},
		pendingLifetime: reversePendingTimeout,
	}
}

func (h *reverseHub) mapTarget(newKey, target string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	mapKey := newKey + "\x00" + target
	if ep, ok := h.byTarget[mapKey]; ok {
		return "http://" + ep.listener.Addr().String() + ep.path
	}
	if newKey == "" || len(h.byTarget) >= reverseEndpointLimit || h.closed || h.ctx.Err() != nil {
		return "http://127.0.0.1:0/unavailable"
	}
	var secret [24]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return "http://127.0.0.1:0/unavailable"
	}
	path := "/" + hex.EncodeToString(secret[:])
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "http://127.0.0.1:0/unavailable"
	}
	ep := &reverseEndpoint{listener: ln, path: path, target: target, newKey: newKey, mapKey: mapKey}
	srv := &http.Server{
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.handleEndpoint(ep, w, r) }),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	ep.server = srv
	h.byTarget[mapKey] = ep
	h.byNewKey[newKey] = append(h.byNewKey[newKey], ep)
	go func() { _ = srv.Serve(ln) }()
	ep.timer = time.AfterFunc(h.pendingLifetime, func() { h.expireEndpoint(ep) })
	return "http://" + ln.Addr().String() + path
}

func (h *reverseHub) expireEndpoint(ep *reverseEndpoint) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.byTarget[ep.mapKey] != ep || ep.session != "" {
		return
	}
	h.closeEndpointLocked(ep)
	eps := h.byNewKey[ep.newKey]
	for i, e := range eps {
		if e == ep {
			eps = append(eps[:i], eps[i+1:]...)
			break
		}
	}
	if len(eps) == 0 {
		delete(h.byNewKey, ep.newKey)
	} else {
		h.byNewKey[ep.newKey] = eps
	}
	for id, pr := range h.pending {
		if pr.newKey == ep.newKey && pr.session == "" {
			pr.cancel()
			delete(h.pending, id)
		}
	}
}

func (h *reverseHub) bindNew(newKey, sessionID string) {
	if newKey == "" || sessionID == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.bindNewLocked(newKey, sessionID)
}

func (h *reverseHub) bindNewLocked(newKey, sessionID string) {
	eps := h.byNewKey[newKey]
	delete(h.byNewKey, newKey)
	for _, ep := range eps {
		ep.session = sessionID
		if ep.timer != nil {
			ep.timer.Stop()
		}
	}
	for _, pr := range h.pending {
		if pr.newKey == newKey && pr.session == "" {
			pr.session = sessionID
		}
	}
	if len(eps) > 0 {
		h.bySession[sessionID] = append(h.bySession[sessionID], eps...)
	}
}

func (h *reverseHub) closeEndpointLocked(ep *reverseEndpoint) {
	if ep.timer != nil {
		ep.timer.Stop()
	}
	delete(h.byTarget, ep.mapKey)
	_ = ep.listener.Close()
	if ep.server != nil {
		_ = ep.server.Close()
	}
}

func (h *reverseHub) failNew(newKey string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ep := range h.byNewKey[newKey] {
		h.closeEndpointLocked(ep)
	}
	delete(h.byNewKey, newKey)
	for id, pr := range h.pending {
		if pr.newKey == newKey && pr.session == "" {
			pr.cancel()
			delete(h.pending, id)
		}
	}
}

// rejectNew revokes one forwarding generation, even if it already bound.
func (h *reverseHub) rejectNew(key string) {
	if key == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, ep := range h.byTarget {
		if ep.newKey == key {
			h.closeEndpointLocked(ep)
		}
	}
	delete(h.byNewKey, key)
	for session, eps := range h.bySession {
		kept := eps[:0]
		for _, ep := range eps {
			if ep.newKey != key {
				kept = append(kept, ep)
			}
		}
		if len(kept) == 0 {
			delete(h.bySession, session)
		} else {
			h.bySession[session] = kept
		}
	}
	for id, pr := range h.pending {
		if pr.newKey == key {
			pr.cancel()
			delete(h.pending, id)
		}
	}
}

func (h *reverseHub) closeSession(sessionID string) {
	if sessionID == "" {
		return
	}
	h.mu.Lock()
	eps := h.bySession[sessionID]
	delete(h.bySession, sessionID)
	for _, ep := range eps {
		h.closeEndpointLocked(ep)
	}
	for id, pr := range h.pending {
		if pr.session == sessionID {
			pr.cancel()
			delete(h.pending, id)
		}
	}
	h.mu.Unlock()
}

func (h *reverseHub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for _, ep := range h.byTarget {
		h.closeEndpointLocked(ep)
	}
	h.byTarget = map[string]*reverseEndpoint{}
	h.byNewKey = map[string][]*reverseEndpoint{}
	h.bySession = map[string][]*reverseEndpoint{}
	for id, pr := range h.pending {
		pr.cancel()
		delete(h.pending, id)
	}
}

// observeAgentLine binds or discards listeners once the agent answers
// session/new with a session id.
func (h *reverseHub) observeAgentLine(line []byte) {
	var msg rpcMessage
	if json.Unmarshal(line, &msg) != nil {
		return
	}
	h.observeAgentLineKey(line, rpcIDKey(msg.ID))
}

func (h *reverseHub) observeAgentLineKey(line []byte, key string) {
	var msg rpcMessage
	if json.Unmarshal(line, &msg) != nil || msg.Method != "" {
		return
	}
	if key == "" {
		return
	}
	h.mu.Lock()
	_, pending := h.byNewKey[key]
	h.mu.Unlock()
	if !pending {
		return
	}
	if len(msg.Error) > 0 {
		h.failNew(key)
		return
	}
	var result struct {
		SessionID string `json:"sessionId"`
	}
	_ = json.Unmarshal(msg.Result, &result)
	if result.SessionID == "" {
		h.failNew(key)
		return
	}
	h.bindNew(key, result.SessionID)
}

func (h *reverseHub) deliver(f reverseFrame) {
	h.mu.Lock()
	pr := h.pending[f.ID]
	h.mu.Unlock()
	if pr == nil {
		return
	}
	select {
	case pr.ch <- f:
	default:
		// A stalled HTTP client must not wedge the control connection.
		pr.overflow.Store(true)
		pr.cancel()
	}
}

func mergeQuery(target, rawQuery string) string {
	if rawQuery == "" {
		return target
	}
	u, err := url.Parse(target)
	if err != nil {
		return target
	}
	if u.RawQuery == "" {
		u.RawQuery = rawQuery
	} else {
		u.RawQuery = u.RawQuery + "&" + rawQuery
	}
	return u.String()
}

func (h *reverseHub) handleEndpoint(ep *reverseEndpoint, w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != ep.path {
		http.NotFound(w, r)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, reverseBodyLimit)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	target := mergeQuery(ep.target, r.URL.RawQuery)
	id := h.seq.Add(1)
	reqCtx, reqCancel := context.WithCancel(r.Context())
	pr := &reversePending{ch: make(chan reverseFrame, reversePendingSize), cancel: reqCancel, newKey: ep.newKey}
	h.mu.Lock()
	if h.byTarget[ep.mapKey] != ep || h.ctx.Err() != nil {
		h.mu.Unlock()
		reqCancel()
		http.Error(w, "MCP endpoint closed", http.StatusBadGateway)
		return
	}
	if len(h.pending) >= reverseEndpointLimit {
		h.mu.Unlock()
		reqCancel()
		http.Error(w, "too many requests", http.StatusServiceUnavailable)
		return
	}
	sessionID := ep.session
	pr.session = sessionID
	h.pending[id] = pr
	h.mu.Unlock()
	defer func() {
		reqCancel()
		h.mu.Lock()
		delete(h.pending, id)
		h.mu.Unlock()
	}()
	if h.writer.send(reverseFrame{Type: "request", ID: id, Session: sessionID, NewKey: ep.newKey, Target: target, Method: r.Method, Header: cleanHeaders(r.Header), Data: body}) != nil {
		http.Error(w, "bridge unavailable", http.StatusBadGateway)
		return
	}
	// Cancellation must not depend on the handler returning from a blocked
	// HTTP write. Exactly one path claims cancellation or terminal completion.
	var state atomic.Uint32 // 0 active, 1 terminal received, 2 cancel sent
	cancelUpstream := func() {
		if state.CompareAndSwap(0, 2) {
			// Never block the HTTP handler or frame reader on the shared writer.
			go func() { _ = h.writer.send(reverseFrame{Type: "cancel", ID: id}) }()
		}
	}
	watchDone := make(chan struct{})
	go func() {
		select {
		case <-reqCtx.Done():
			cancelUpstream()
		case <-watchDone:
		}
	}()
	defer func() {
		cancelUpstream()
		close(watchDone)
	}()
	timer := time.NewTimer(reversePendingTimeout)
	defer timer.Stop()
	started := false
	for {
		select {
		case f := <-pr.ch:
			switch f.Type {
			case "response":
				if f.Done || !f.Stream {
					state.CompareAndSwap(0, 1)
				}
				if !started {
					if f.Error != "" || f.Status < 100 || f.Status > 599 {
						http.Error(w, "MCP bridge failed", http.StatusBadGateway)
						return
					}
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					for k, vs := range cleanHeaders(f.Header) {
						for _, v := range vs {
							w.Header().Add(k, v)
						}
					}
					started = true
					w.WriteHeader(f.Status)
					if f.Stream {
						if err := http.NewResponseController(w).Flush(); err != nil {
							return
						}
						continue
					}
					_, _ = w.Write(f.Data)
					return
				}
				if f.Done {
					if f.Error != "" || pr.overflow.Load() {
						panic(http.ErrAbortHandler)
					}
					return
				}
			case "chunk":
				if !started {
					http.Error(w, "MCP bridge failed", http.StatusBadGateway)
					return
				}
				rc := http.NewResponseController(w)
				_ = rc.SetWriteDeadline(time.Now().Add(reverseWriteTimeout))
				if _, err := w.Write(f.Data); err != nil {
					return
				}
				if err := rc.Flush(); err != nil {
					return
				}
			}
		case <-reqCtx.Done():
			// Host disconnected or the ACP session closed: stop the upstream
			// MCP call even if the response writer is blocked.
			cancelUpstream()
			if started {
				// A clean HTTP EOF would falsely report a truncated stream as
				// successful. Abort the response (RST_STREAM for HTTP/2).
				panic(http.ErrAbortHandler)
			}
			if r.Context().Err() == nil {
				http.Error(w, "MCP bridge interrupted", http.StatusBadGateway)
			}
			return
		case <-timer.C:
			cancelUpstream()
			http.Error(w, "MCP request timed out", http.StatusGatewayTimeout)
			return
		}
	}
}

func runReverseServer(parent context.Context, conn io.ReadWriteCloser, agentOutput io.Reader, agentInput io.Writer, mappings pathMappings) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	defer conn.Close()
	go func() { <-ctx.Done(); _ = conn.Close() }()
	writer := &reverseWriter{w: conn}
	hub := newReverseHub(ctx, writer)
	defer hub.closeAll()
	var drainTimer *time.Timer
	defer func() {
		if drainTimer != nil {
			drainTimer.Stop()
		}
	}()

	go func() {
		r := bufio.NewReader(agentOutput)
		for {
			line, err := readBoundedLine(r, acpLineLimit)
			if len(line) > 0 {
				var response rpcMessage
				var token string
				if json.Unmarshal(line, &response) == nil && response.Method == "" {
					var wire string
					if json.Unmarshal(response.ID, &wire) == nil && strings.HasPrefix(wire, "acpnet-forward-") {
						token = strings.TrimPrefix(wire, "acpnet-forward-")
					}
				}
				line = rewriteLine(line, mappings, rewriteHostToClient)
				// Serialize publishing the response and binding against endpoint
				// requests and closeSession. A bound request cannot overtake it.
				hub.mu.Lock()
				if _, ok := hub.byNewKey[token]; !ok {
					token = ""
				}
				err := writer.send(reverseFrame{Type: "acp", Data: line})
				var result struct {
					SessionID string `json:"sessionId"`
				}
				_ = json.Unmarshal(response.Result, &result)
				if err == nil && token != "" && len(response.Error) == 0 && result.SessionID != "" {
					hub.bindNewLocked(token, result.SessionID)
				}
				hub.mu.Unlock()
				if err != nil {
					return
				}
				if token != "" && (len(response.Error) > 0 || result.SessionID == "") {
					hub.failNew(token)
				}
			}
			if err != nil {
				_ = writer.send(reverseFrame{Type: "eof"})
				_ = conn.Close()
				return
			}
		}
	}()

	r := bufio.NewReader(conn)
	for {
		f, err := readReverseFrame(r)
		if err != nil {
			return err
		}
		switch f.Type {
		case "acp":
			if id := sessionCloseID(f.Data); id != "" {
				hub.closeSession(id)
			}
			line := f.Data
			if plan, _ := sessionNewTargets(f.Data); plan != nil && len(plan.Targets) > 0 {
				failed := false
				var request rpcMessage
				var wire string
				if f.NewKey == "" || json.Unmarshal(f.Data, &request) != nil || json.Unmarshal(request.ID, &wire) != nil || wire != "acpnet-forward-"+f.NewKey {
					return errors.New("invalid forwarding generation")
				}
				line = rewriteSessionNewURLs(line, func(raw string) string {
					mapped := hub.mapTarget(f.NewKey, raw)
					if mapped == "http://127.0.0.1:0/unavailable" {
						failed = true
					}
					return mapped
				})
				if failed {
					hub.failNew(f.NewKey)
					var msg rpcMessage
					_ = json.Unmarshal(f.Data, &msg)
					if err := writer.send(reverseFrame{Type: "acp", Data: rpcErrorLine(msg.ID, "cannot allocate MCP forwarding endpoint")}); err != nil {
						return err
					}
					continue
				}
			}
			line = rewriteLine(line, mappings, rewriteClientToHost)
			if _, err = agentInput.Write(line); err != nil {
				return err
			}
		case "reject":
			hub.rejectNew(f.NewKey)
		case "response", "chunk":
			hub.deliver(f)
		case "eof":
			if c, ok := agentInput.(io.Closer); ok {
				_ = c.Close()
			}
			if drainTimer == nil {
				// Match legacy mode: allow the agent to flush its last output,
				// but do not wait forever if it ignores stdin EOF.
				drainTimer = time.AfterFunc(inputDrainGrace, func() { _ = conn.Close() })
			}
		default:
			return fmt.Errorf("invalid reverse frame type")
		}
	}
}
