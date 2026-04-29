package config

import (
	"testing"
	"time"
)

func TestLoadAndValidate(t *testing.T) {
	v := NewViper()
	v.Set("listen", "127.0.0.1:8080")
	v.Set("pprof-enabled", true)
	v.Set("pprof-listen", "127.0.0.1:6061")
	v.Set("data-dir", "./data")
	v.Set("api-token", "secret")
	v.Set("log-level", "INFO")
	v.Set("query-timeout-ms", 2500)
	v.Set("namespace-idle-timeout-ms", 60000)
	v.Set("max-open-namespaces", 42)
	v.Set("read-timeout-ms", 11000)
	v.Set("write-timeout-ms", 12000)
	v.Set("idle-timeout-ms", 13000)
	v.Set("shutdown-timeout-ms", 14000)

	cfg, err := Load(v)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate config: %v", err)
	}

	if cfg.LogLevel != "info" {
		t.Fatalf("expected lowercase log level, got %q", cfg.LogLevel)
	}
	if cfg.QueryTimeout != 2500*time.Millisecond {
		t.Fatalf("unexpected query timeout: %v", cfg.QueryTimeout)
	}
	if !cfg.PprofEnabled {
		t.Fatal("expected pprof to be enabled")
	}
	if cfg.PprofListen != "127.0.0.1:6061" {
		t.Fatalf("unexpected pprof listen: %s", cfg.PprofListen)
	}
	if cfg.ShutdownTimeout != 14*time.Second {
		t.Fatalf("unexpected shutdown timeout: %v", cfg.ShutdownTimeout)
	}
}

func TestLoadRejectsNegativeDurations(t *testing.T) {
	v := NewViper()
	v.Set("query-timeout-ms", -1)

	if _, err := Load(v); err == nil {
		t.Fatal("expected error for negative duration")
	}
}

func TestValidateRejectsMissingToken(t *testing.T) {
	cfg := Config{
		Listen:            ":8080",
		DataDir:           "data",
		MaxOpenNamespaces: 1,
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for missing API token")
	}
}

func TestValidateRejectsOtherInvalidFields(t *testing.T) {
	tests := []Config{
		{DataDir: "data", APIToken: "x", MaxOpenNamespaces: 1},
		{Listen: ":8080", APIToken: "x", MaxOpenNamespaces: 1},
		{Listen: ":8080", DataDir: "data", APIToken: "x", MaxOpenNamespaces: 0},
		{Listen: ":8080", PprofEnabled: true, DataDir: "data", APIToken: "x", MaxOpenNamespaces: 1},
	}
	for i, cfg := range tests {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("expected validation error for case %d", i)
		}
	}
}

func TestLoadNegativeOtherDurations(t *testing.T) {
	keys := []string{
		"namespace-idle-timeout-ms",
		"read-timeout-ms",
		"write-timeout-ms",
		"idle-timeout-ms",
		"shutdown-timeout-ms",
	}
	for _, key := range keys {
		v := NewViper()
		v.Set("listen", ":8080")
		v.Set("data-dir", "data")
		v.Set("api-token", "secret")
		v.Set("max-open-namespaces", 1)
		v.Set(key, -1)
		if _, err := Load(v); err == nil {
			t.Fatalf("expected negative duration error for key %s", key)
		}
	}
}

func TestRedactedHidesToken(t *testing.T) {
	cfg := Config{APIToken: "super-secret", PprofEnabled: true, PprofListen: "127.0.0.1:6060"}
	redacted := cfg.Redacted()

	if redacted["api_token"] != "***redacted***" {
		t.Fatalf("expected redacted token, got %v", redacted["api_token"])
	}
	if redacted["pprof_enabled"] != true {
		t.Fatalf("expected pprof enabled in redacted output, got %v", redacted["pprof_enabled"])
	}
}
