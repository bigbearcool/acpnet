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
	"testing"
	"time"
)

func TestReverseSessionMCP(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(append([]byte("reply:"), b...))
	}))
	defer target.Close()
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientInR, clientInW := io.Pipe()
	clientOutR, clientOutW := io.Pipe()
	hostInR, hostInW := io.Pipe()
	hostOutR, hostOutW := io.Pipe()
	defer clientInR.Close()
	defer clientInW.Close()
	defer clientOutR.Close()
	defer clientOutW.Close()
	defer hostInR.Close()
	defer hostInW.Close()
	defer hostOutR.Close()
	defer hostOutW.Close()
	go runReverseClient(ctx, a, clientInR, clientOutW)
	go runReverseServer(ctx, b, hostOutR, hostInW, nil)
	input := `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"mcpServers":[{"name":"local","type":"http","url":"` + target.URL + `/mcp"}]}}` + "\n"
	go clientInW.Write([]byte(input))
	line, err := bufio.NewReader(hostInR).ReadBytes('\n')
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
	got := v.Params.MCPServers[0].URL
	if got == target.URL+"/mcp" || !strings.HasPrefix(got, "http://127.0.0.1:") {
		t.Fatalf("not mapped: %s", got)
	}
	hostOut := bufio.NewWriter(hostOutW)
	if _, err := hostOut.WriteString(`{"jsonrpc":"2.0","id":1,"result":{"sessionId":"s1"}}` + "\n"); err != nil {
		t.Fatal(err)
	}
	if err := hostOut.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(clientOutR).ReadBytes('\n'); err != nil {
		t.Fatalf("session/new response not relayed: %v", err)
	}
	hc := http.Client{Timeout: 3 * time.Second}
	resp, err := hc.Post(got, "application/json", bytes.NewBufferString("hello"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "reply:hello" {
		t.Fatalf("response: %s", body)
	}
	go hostOutW.Write([]byte("{\"ok\":true}\n"))
	line, err = bufio.NewReader(clientOutR).ReadBytes('\n')
	if err != nil || string(line) != "{\"ok\":true}\n" {
		t.Fatalf("ACP return %s %v", line, err)
	}
	cancel()
	time.Sleep(100 * time.Millisecond)
	_, err = hc.Post(got, "application/json", bytes.NewBufferString("x"))
	if err == nil {
		t.Fatal("listener survived session cancellation")
	}
}
