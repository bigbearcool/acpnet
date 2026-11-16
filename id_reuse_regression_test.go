package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestReviewIDReuseAndPendingSessionIsolation(t *testing.T) {
	s := newReverseClientSessions()
	target := "http://127.0.0.1:1234/mcp"
	if !s.begin("id", []string{target}) {
		t.Fatal("initial begin")
	}
	if s.authorizePendingSession("id", "unrelated-session", target) {
		t.Error("pending key authorized unrelated session")
	}
	s.complete("id", "first-session", false)
	if !s.begin("id", []string{target}) {
		t.Fatal("completed JSON-RPC ID cannot be reused")
	}
}

func TestGenerationIsolationDuringIDReuse(t *testing.T) {
	s := newReverseClientSessions()
	const target = "http://127.0.0.1:1234/mcp"
	if !s.beginWithToken("id", "generation-one", []string{target}) {
		t.Fatal("first begin")
	}
	s.complete("id", "first", false)
	if !s.beginWithToken("id", "generation-two", []string{target}) {
		t.Fatal("reuse")
	}
	if !s.authorizePendingSession("generation-one", "", target) {
		t.Error("delayed first-generation request lost")
	}
	if s.authorizePendingSession("generation-two", "first", target) {
		t.Error("pending generation accepted first session")
	}
	s.complete("id", "second", false)
	if s.authorizePendingSession("generation-one", "second", target) || s.authorizePendingSession("generation-two", "first", target) {
		t.Error("generation crossed sessions")
	}
	s.revoke("first")
	if s.authorizePendingSession("generation-one", "", target) || !s.authorizePendingSession("generation-two", "second", target) {
		t.Error("revoke crossed generations")
	}
}

func TestExpiredIDCannotBeReusedBeforeResponse(t *testing.T) {
	s := newReverseClientSessions()
	target := "http://127.0.0.1:1234/mcp"
	s.begin("id", []string{target})
	s.mu.Lock()
	s.expires["id"] = time.Now().Add(-time.Second)
	s.mu.Unlock()
	if s.authorizePending("id", target) {
		t.Fatal("expired authorization")
	}
	if !s.begin("new-generation", []string{target}) {
		t.Fatal("expired slot was not released")
	}
	s.complete("id", "late", false)
	if s.authorize("late", target) || !s.authorizePending("new-generation", target) {
		t.Fatal("late response affected new generation")
	}
}

func TestIDReuseEndToEndConcurrentSessions(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, r.URL.Path) }))
	defer target.Close()
	h := startReverseHarness(t)
	first := openSession(t, h, "first", target.URL+"/first")
	second := openSession(t, h, "second", target.URL+"/second")
	var wg sync.WaitGroup
	for _, item := range []struct{ url, want string }{{first, "/first"}, {second, "/second"}} {
		item := item
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c := http.Client{Timeout: 2 * time.Second}
				r, err := c.Get(item.url)
				if err != nil {
					t.Error(err)
					return
				}
				defer r.Body.Close()
				b, err := io.ReadAll(r.Body)
				if err != nil || r.StatusCode != 200 || string(b) != item.want {
					t.Errorf("got status %d body %q error %v", r.StatusCode, b, err)
				}
			}()
		}
	}
	wg.Wait()
}
