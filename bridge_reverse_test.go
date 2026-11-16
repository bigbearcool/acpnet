package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReverseThroughAuthenticatedBridge(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "mcp-ok") }))
	defer target.Close()
	client, server := net.Pipe()
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		agent := `sh -c 'read line; echo "$line"; echo "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"sessionId\":\"s1\"}}"; cat'`
		handleBridgeConn(ctx, server, "test", serverConfig{token: "test-token", codexCmd: agent})
		close(done)
	}()
	if err := writeJSONLine(client, handshakeRequest{Token: "test-token", Agent: "codex", Cwd: t.TempDir(), ReverseMCP: true}); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(client)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var resp handshakeResponse
	if json.Unmarshal(line, &resp) != nil || !resp.OK || !resp.ReverseMCP {
		t.Fatalf("bad handshake: %s", line)
	}
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	defer inR.Close()
	defer inW.Close()
	defer outR.Close()
	defer outW.Close()
	go runReverseClient(ctx, &prefixedConn{Conn: client, reader: reader}, inR, outW)
	go io.WriteString(inW, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"mcpServers":[{"type":"http","url":"`+target.URL+`/mcp"}]}}`+"\n")
	line, err = bufio.NewReader(outR).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Params struct {
			MCPServers []struct {
				URL string `json:"url"`
			} `json:"mcpServers"`
		} `json:"params"`
	}
	if err = json.Unmarshal(line, &v); err != nil {
		t.Fatal(err)
	}
	// The fake agent answers session/new with a session id; the client binds
	// the mapped endpoint to that session before it can be used.
	if _, err = bufio.NewReader(outR).ReadBytes('\n'); err != nil {
		t.Fatalf("session/new response not relayed: %v", err)
	}
	endpoint := v.Params.MCPServers[0].URL
	hc := http.Client{Timeout: 3 * time.Second}
	r, err := hc.Get(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if string(body) != "mcp-ok" {
		t.Fatalf("unexpected response %q", body)
	}
	r, err = hc.Get(strings.TrimSuffix(endpoint, endpoint[strings.LastIndex(endpoint, "/"):]) + "/wrong")
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("unprotected endpoint: %d", r.StatusCode)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("bridge failed to stop")
	}
}
