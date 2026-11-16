package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestResponseBeforeBoundRequest(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") }))
	defer target.Close()
	h := startReverseHarness(t)
	writeLine(t, h.clientInW, `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"mcpServers":[{"url":"`+target.URL+`/mcp"}]}}`+"\n")
	line := readLineWithin(t, h.hostIn, time.Second)
	mapped := extractMCPServerURL(t, line)
	var request rpcMessage
	if err := json.Unmarshal([]byte(line), &request); err != nil {
		t.Fatal(err)
	}
	writeLine(t, h.hostOutW, `{"jsonrpc":"2.0","id":`+string(request.ID)+`,"result":{"sessionId":"bound"}}`+"\n")
	result := make(chan error, 1)
	go func() {
		c := http.Client{Timeout: 2 * time.Second}
		r, err := c.Get(mapped)
		if err != nil {
			result <- err
			return
		}
		defer r.Body.Close()
		if r.StatusCode != 200 {
			result <- fmt.Errorf("status %d", r.StatusCode)
			return
		}
		result <- nil
	}()
	// The ACP output is an unbuffered pipe: hold publication while the HTTP
	// request races with binding, then allow the response through.
	time.Sleep(20 * time.Millisecond)
	readLineWithin(t, h.clientOut, time.Second)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestLateResponseAfterClientIDReuse(t *testing.T) {
	old := reversePendingTimeout
	reversePendingTimeout = 80 * time.Millisecond
	defer func() { reversePendingTimeout = old }()
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") }))
	defer target.Close()
	h := startReverseHarness(t)
	req := `{"jsonrpc":"2.0","id":7,"method":"session/new","params":{"mcpServers":[{"url":"` + target.URL + `/mcp"}]}}` + "\n"
	writeLine(t, h.clientInW, req)
	var first, second rpcMessage
	if err := json.Unmarshal([]byte(readLineWithin(t, h.hostIn, time.Second)), &first); err != nil {
		t.Fatal(err)
	}
	timeoutLine := readLineWithin(t, h.clientOut, time.Second)
	var timeoutResponse rpcMessage
	if err := json.Unmarshal([]byte(timeoutLine), &timeoutResponse); err != nil || string(timeoutResponse.ID) != "7" || len(timeoutResponse.Error) == 0 {
		t.Fatalf("missing timeout terminal response: %s (%v)", timeoutLine, err)
	}
	writeLine(t, h.clientInW, req)
	if err := json.Unmarshal([]byte(readLineWithin(t, h.hostIn, time.Second)), &second); err != nil {
		t.Fatal(err)
	}
	if string(first.ID) == string(second.ID) {
		t.Fatal("wire ID reused")
	}
	writeLine(t, h.hostOutW, `{"jsonrpc":"2.0","id":`+string(first.ID)+`,"result":{"sessionId":"old"}}`+"\n")
	writeLine(t, h.hostOutW, `{"jsonrpc":"2.0","id":`+string(second.ID)+`,"result":{"sessionId":"new"}}`+"\n")
	line := readLineWithin(t, h.clientOut, time.Second)
	var response rpcMessage
	if err := json.Unmarshal([]byte(line), &response); err != nil {
		t.Fatal(err)
	}
	if string(response.ID) != "7" || string(response.Result) != `{"sessionId":"new"}` {
		t.Fatalf("late response leaked or ID not restored: %s", line)
	}
}

func TestOrdinaryPrefixedIDPassesThrough(t *testing.T) {
	h := startReverseHarness(t)
	req := `{"jsonrpc":"2.0","id":"acpnet-forward-ordinary","method":"other"}` + "\n"
	writeLine(t, h.clientInW, req)
	_ = readLineWithin(t, h.hostIn, time.Second)
	resp := `{"jsonrpc":"2.0","id":"acpnet-forward-ordinary","result":42}` + "\n"
	writeLine(t, h.hostOutW, resp)
	if got := readLineWithin(t, h.clientOut, time.Second); got != resp {
		t.Fatalf("ordinary response altered: %s", got)
	}
}

func TestCancelPendingSessionTerminates(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer target.Close()
	h := startReverseHarness(t)
	writeLine(t, h.clientInW, `{"jsonrpc":"2.0","id":19,"method":"session/new","params":{"mcpServers":[{"url":"`+target.URL+`/mcp"}]}}`+"\n")
	_ = readLineWithin(t, h.hostIn, time.Second)
	writeLine(t, h.clientInW, `{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":19}}`+"\n")
	var response rpcMessage
	line := readLineWithin(t, h.clientOut, time.Second)
	if err := json.Unmarshal([]byte(line), &response); err != nil || string(response.ID) != "19" || len(response.Error) == 0 {
		t.Fatalf("cancel response: %s", line)
	}
}
