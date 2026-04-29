package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ValentinKolb/rsql/internal/config"
)

func TestRunServeValidationError(t *testing.T) {
	err := RunServe(context.Background(), config.Config{})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestRunServeContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.Config{
		Listen:               "127.0.0.1:0",
		DataDir:              t.TempDir(),
		APIToken:             "secret",
		LogLevel:             "info",
		QueryTimeout:         2 * time.Second,
		NamespaceIdleTimeout: 10 * time.Millisecond,
		MaxOpenNamespaces:    8,
		ReadTimeout:          2 * time.Second,
		WriteTimeout:         2 * time.Second,
		IdleTimeout:          2 * time.Second,
		ShutdownTimeout:      2 * time.Second,
	}

	go func() {
		time.Sleep(120 * time.Millisecond)
		cancel()
	}()

	if err := RunServe(ctx, cfg); err != nil {
		t.Fatalf("run serve with cancel: %v", err)
	}
}

func TestRunServeDataDirCreateError(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	cfg := config.Config{
		Listen:               "127.0.0.1:0",
		DataDir:              file,
		APIToken:             "secret",
		LogLevel:             "info",
		QueryTimeout:         time.Second,
		NamespaceIdleTimeout: 10 * time.Millisecond,
		MaxOpenNamespaces:    1,
		ReadTimeout:          time.Second,
		WriteTimeout:         time.Second,
		IdleTimeout:          time.Second,
		ShutdownTimeout:      time.Second,
	}

	err := RunServe(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "create data directory") {
		t.Fatalf("expected data dir error, got %v", err)
	}
}

func TestRunServeListenError(t *testing.T) {
	cfg := config.Config{
		Listen:               "127.0.0.1:bad",
		DataDir:              t.TempDir(),
		APIToken:             "secret",
		LogLevel:             "info",
		QueryTimeout:         time.Second,
		NamespaceIdleTimeout: 10 * time.Millisecond,
		MaxOpenNamespaces:    1,
		ReadTimeout:          time.Second,
		WriteTimeout:         time.Second,
		IdleTimeout:          time.Second,
		ShutdownTimeout:      time.Second,
	}

	err := RunServe(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "listen and serve") {
		t.Fatalf("expected listen error, got %v", err)
	}
}
