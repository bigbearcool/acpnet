package main

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

func TestReverseClientRejectsUnregisteredTarget(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inR, inW := net.Pipe()
	defer inR.Close()
	defer inW.Close()
	go runReverseClient(ctx, a, inR, inW)
	writer := reverseWriter{w: b}
	if err := writer.send(reverseFrame{Type: "request", ID: 1, Target: "http://127.0.0.1:1234/mcp", Method: "GET"}); err != nil {
		t.Fatal(err)
	}
	_ = b.SetReadDeadline(time.Now().Add(time.Second))
	f, err := readReverseFrame(bufio.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	if f.Type != "response" || f.ID != 1 || f.Error == "" {
		t.Fatalf("unexpected response %#v", f)
	}
}
