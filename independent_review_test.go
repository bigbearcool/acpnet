package main

import (
	"fmt"
	"testing"
	"time"
)

func TestIndependentBindBeforeClientResponse(t *testing.T) {
	s := newReverseClientSessions()
	target := "http://127.0.0.1:1234/mcp"
	s.beginWithToken("1", "token", []string{target})
	// A pending token must never authorize a claimed session: the server
	// serializes publishing the response before binding the endpoint instead.
	if s.authorizePendingSession("token", "session-1", target) {
		t.Fatal("pending token authorized an unconfirmed session")
	}
}
func TestIndependentExpiredPendingExhaustion(t *testing.T) {
	s := newReverseClientSessions()
	target := "http://127.0.0.1:1234/mcp"
	for i := 0; i < reverseEndpointLimit; i++ {
		k := fmt.Sprint(i)
		if !s.beginWithToken(k, k, []string{target}) {
			t.Fatal(i)
		}
		s.expires[k] = time.Now().Add(-time.Hour)
	}
	if !s.beginWithToken("fresh", "fresh", []string{target}) {
		t.Fatal("expired unresolved pending requests permanently exhaust capacity")
	}
}
