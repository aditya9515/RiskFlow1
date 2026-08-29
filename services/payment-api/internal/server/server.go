package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Server owns the HTTP listener lifecycle and graceful shutdown behavior.
type Server struct {
	httpServer      *http.Server
	shutdownTimeout time.Duration
	logger          *slog.Logger
}

// New creates a production HTTP server with bounded request timeouts.
func New(addr string, handler http.Handler, shutdownTimeout time.Duration, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	return &Server{
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
		shutdownTimeout: shutdownTimeout,
		logger:          logger,
	}
}

// Run listens on the configured address until the context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return err
	}

	return s.Serve(ctx, listener)
}

// Serve runs on an existing listener, which also makes lifecycle behavior testable.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- normalizeServeError(s.httpServer.Serve(listener))
	}()

	select {
	case err := <-serveErrors:
		return err
	case <-ctx.Done():
		s.logger.Info("shutting down HTTP server")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
		defer cancel()

		shutdownErr := s.httpServer.Shutdown(shutdownCtx)
		if shutdownErr != nil {
			_ = s.httpServer.Close()
		}

		serveErr := <-serveErrors
		return errors.Join(shutdownErr, serveErr)
	}
}

func normalizeServeError(err error) error {
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}

	return err
}
