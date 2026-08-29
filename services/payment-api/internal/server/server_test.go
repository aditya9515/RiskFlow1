package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServeCompletesInflightRequestDuringGracefulShutdown(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusNoContent)
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := New(listener.Addr().String(), handler, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- srv.Serve(ctx, listener)
	}()

	responseDone := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 2 * time.Second}
		response, requestErr := client.Get(fmt.Sprintf("http://%s", listener.Addr()))
		if requestErr != nil {
			responseDone <- requestErr
			return
		}
		defer response.Body.Close()

		if response.StatusCode != http.StatusNoContent {
			responseDone <- fmt.Errorf("status = %d, want 204", response.StatusCode)
			return
		}
		responseDone <- nil
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("request did not reach handler")
	}

	cancel()
	select {
	case serveErr := <-serveDone:
		close(release)
		t.Fatalf("server stopped before in-flight request completed: %v", serveErr)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if requestErr := <-responseDone; requestErr != nil {
		t.Fatalf("in-flight request: %v", requestErr)
	}

	select {
	case serveErr := <-serveDone:
		if serveErr != nil {
			t.Fatalf("serve returned error: %v", serveErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not shut down within timeout")
	}

	connection, dialErr := net.DialTimeout("tcp", listener.Addr().String(), 100*time.Millisecond)
	if dialErr == nil {
		connection.Close()
		t.Fatal("listener still accepts connections after shutdown")
	}
}
