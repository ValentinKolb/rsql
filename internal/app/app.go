package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ValentinKolb/rsql/internal/config"
	"github.com/ValentinKolb/rsql/internal/httpapi"
	"github.com/ValentinKolb/rsql/internal/observability"
	"github.com/ValentinKolb/rsql/internal/service"
)

// RunServe starts the rsql HTTP server and blocks until shutdown.
func RunServe(ctx context.Context, cfg config.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: toSlogLevel(cfg.LogLevel)}))
	metrics := observability.NewMetrics()
	svc, err := service.New(cfg.DataDir, cfg.NamespaceIdleTimeout)
	if err != nil {
		return fmt.Errorf("initialize service: %w", err)
	}
	defer svc.Close()

	h := httpapi.NewHandler(httpapi.Dependencies{
		Token:   cfg.APIToken,
		Metrics: metrics,
		Logger:  logger,
		Service: svc,
	})

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           h,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ReadHeaderTimeout: 5 * time.Second,
	}

	var pprofSrv *http.Server
	var pprofErrCh <-chan error
	if cfg.PprofEnabled {
		pprofMux := http.NewServeMux()
		pprofMux.HandleFunc("/debug/pprof/", pprof.Index)
		pprofMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		pprofMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		pprofMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		pprofMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		pprofMux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
		pprofMux.Handle("/debug/pprof/block", pprof.Handler("block"))
		pprofMux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
		pprofMux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
		pprofMux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
		pprofMux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

		pprofSrv = &http.Server{
			Addr:              cfg.PprofListen,
			Handler:           pprofMux,
			ReadHeaderTimeout: 5 * time.Second,
		}

		ch := make(chan error, 1)
		pprofErrCh = ch
		go func() {
			logger.Info("pprof_server_starting", slog.String("listen", cfg.PprofListen))
			if err := pprofSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				ch <- fmt.Errorf("pprof listen and serve: %w", err)
			}
		}()
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("server_starting", slog.String("listen", cfg.Listen))
		errCh <- srv.ListenAndServe()
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("listen and serve: %w", err)
	case err := <-pprofErrCh:
		return err
	case <-ctx.Done():
		logger.Info("shutdown_requested", slog.String("source", "context"))
	case <-sigCtx.Done():
		logger.Info("shutdown_requested", slog.String("source", "signal"))
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown server: %w", err)
	}
	if pprofSrv != nil {
		if err := pprofSrv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown pprof server: %w", err)
		}
	}

	return nil
}

func toSlogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
