package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ValentinKolb/rsql/internal/config"
)

func TestVersionCommand(t *testing.T) {
	cmd := newRootCmd(func(context.Context, config.Config) error { return nil })
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"version"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute version: %v", err)
	}
	if !strings.Contains(buf.String(), "rsql v") {
		t.Fatalf("unexpected output: %s", buf.String())
	}
}

func TestNewRootCmd(t *testing.T) {
	if NewRootCmd() == nil {
		t.Fatal("expected root command")
	}
}

func TestConfigPrintRedactsToken(t *testing.T) {
	t.Setenv("RSQL_API_TOKEN", "")

	cmd := newRootCmd(func(context.Context, config.Config) error { return nil })
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"config", "print", "--api-token", "super-secret", "--listen", "127.0.0.1:7777"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute config print: %v", err)
	}
	if strings.Contains(buf.String(), "super-secret") {
		t.Fatalf("token should be redacted: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "***redacted***") {
		t.Fatalf("expected redacted token output: %s", buf.String())
	}
}

func TestServeCommandPassesConfig(t *testing.T) {
	called := false
	cmd := newRootCmd(func(_ context.Context, cfg config.Config) error {
		called = true
		if cfg.Listen != "127.0.0.1:9000" {
			t.Fatalf("unexpected listen: %s", cfg.Listen)
		}
		if !cfg.PprofEnabled {
			t.Fatal("expected pprof to be enabled")
		}
		if cfg.PprofListen != "127.0.0.1:6061" {
			t.Fatalf("unexpected pprof listen: %s", cfg.PprofListen)
		}
		return nil
	})

	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"serve",
		"--api-token", "secret",
		"--listen", "127.0.0.1:9000",
		"--data-dir", t.TempDir(),
		"--pprof-enabled",
		"--pprof-listen", "127.0.0.1:6061",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute serve: %v", err)
	}
	if !called {
		t.Fatal("expected serve runner to be called")
	}
}

func TestServeCommandRunnerError(t *testing.T) {
	expected := "runner failed"
	cmd := newRootCmd(func(context.Context, config.Config) error {
		return errors.New(expected)
	})

	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"serve", "--api-token", "secret", "--listen", "127.0.0.1:9000", "--data-dir", t.TempDir()})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("expected runner error %q, got %v", expected, err)
	}
}

func TestConfigPrintUnsupportedFormat(t *testing.T) {
	cmd := newRootCmd(func(context.Context, config.Config) error { return nil })
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"config", "print", "--api-token", "super-secret", "--listen", "127.0.0.1:7777", "--format", "yaml"})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), `unsupported format "yaml"`) {
		t.Fatalf("expected unsupported format error, got %v", err)
	}
}

func TestServeCommandValidationError(t *testing.T) {
	cmd := newRootCmd(func(context.Context, config.Config) error { return nil })
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"serve", "--listen", "127.0.0.1:9000", "--data-dir", t.TempDir()})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestServeCommandPprofValidationError(t *testing.T) {
	cmd := newRootCmd(func(context.Context, config.Config) error { return nil })
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{
		"serve",
		"--listen", "127.0.0.1:9000",
		"--data-dir", t.TempDir(),
		"--api-token", "secret",
		"--pprof-enabled",
		"--pprof-listen", "",
	})

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected pprof validation error")
	}
}
