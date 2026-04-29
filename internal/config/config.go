package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config describes the runtime configuration for the rsql server.
type Config struct {
	Listen               string
	PprofEnabled         bool
	PprofListen          string
	DataDir              string
	APIToken             string
	LogLevel             string
	QueryTimeout         time.Duration
	NamespaceIdleTimeout time.Duration
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	IdleTimeout          time.Duration
	ShutdownTimeout      time.Duration
}

// NewViper creates a Viper instance configured for rsql flags and env variables.
func NewViper() *viper.Viper {
	v := viper.New()
	v.SetEnvPrefix("rsql")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	return v
}

// InitEnv loads dotenv files and enables environment variable lookup.
func InitEnv(v *viper.Viper) {
	_ = godotenv.Load(".env")
	_ = godotenv.Load(".env.local")
	v.AutomaticEnv()
}

// BindServeFlags binds runtime configuration flags to a flag set.
func BindServeFlags(flags *pflag.FlagSet) {
	flags.String("listen", ":8080", "HTTP listen address")
	flags.Bool("pprof-enabled", false, "Enable pprof endpoints on a dedicated listener")
	flags.String("pprof-listen", "127.0.0.1:6060", "pprof listen address")
	flags.String("data-dir", "data", "Path to the data directory")
	flags.String("api-token", "", "API bearer token")
	flags.String("log-level", "info", "Log level (debug, info, warn, error)")
	flags.Int("query-timeout-ms", 10000, "Default query timeout in milliseconds")
	flags.Int("namespace-idle-timeout-ms", 300000, "Namespace idle close timeout in milliseconds")
	flags.Int("read-timeout-ms", 15000, "HTTP read timeout in milliseconds")
	flags.Int("write-timeout-ms", 15000, "HTTP write timeout in milliseconds")
	flags.Int("idle-timeout-ms", 120000, "HTTP idle timeout in milliseconds")
	flags.Int("shutdown-timeout-ms", 10000, "Graceful shutdown timeout in milliseconds")
}

// Load creates a Config from Viper values.
func Load(v *viper.Viper) (Config, error) {
	cfg := Config{
		Listen:            v.GetString("listen"),
		PprofEnabled:      v.GetBool("pprof-enabled"),
		PprofListen:       v.GetString("pprof-listen"),
		DataDir:           v.GetString("data-dir"),
		APIToken:          v.GetString("api-token"),
		LogLevel:          strings.ToLower(v.GetString("log-level")),
	}

	var err error
	if cfg.QueryTimeout, err = fromMillis(v.GetInt("query-timeout-ms"), "query-timeout-ms"); err != nil {
		return Config{}, err
	}
	if cfg.NamespaceIdleTimeout, err = fromMillis(v.GetInt("namespace-idle-timeout-ms"), "namespace-idle-timeout-ms"); err != nil {
		return Config{}, err
	}
	if cfg.ReadTimeout, err = fromMillis(v.GetInt("read-timeout-ms"), "read-timeout-ms"); err != nil {
		return Config{}, err
	}
	if cfg.WriteTimeout, err = fromMillis(v.GetInt("write-timeout-ms"), "write-timeout-ms"); err != nil {
		return Config{}, err
	}
	if cfg.IdleTimeout, err = fromMillis(v.GetInt("idle-timeout-ms"), "idle-timeout-ms"); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = fromMillis(v.GetInt("shutdown-timeout-ms"), "shutdown-timeout-ms"); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func fromMillis(ms int, key string) (time.Duration, error) {
	if ms < 0 {
		return 0, fmt.Errorf("%s must be >= 0", key)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// Validate validates runtime constraints that must hold for server startup.
func (c Config) Validate() error {
	if c.Listen == "" {
		return fmt.Errorf("listen must not be empty")
	}
	if c.PprofEnabled && c.PprofListen == "" {
		return fmt.Errorf("pprof-listen must not be empty when pprof-enabled=true")
	}
	if c.DataDir == "" {
		return fmt.Errorf("data-dir must not be empty")
	}
	if c.APIToken == "" {
		return fmt.Errorf("api-token must not be empty")
	}
	return nil
}

// Redacted returns a safe map representation for config output.
func (c Config) Redacted() map[string]any {
	token := ""
	if c.APIToken != "" {
		token = "***redacted***"
	}

	return map[string]any{
		"listen":                 c.Listen,
		"pprof_enabled":          c.PprofEnabled,
		"pprof_listen":           c.PprofListen,
		"data_dir":               c.DataDir,
		"api_token":              token,
		"log_level":              c.LogLevel,
		"query_timeout":          c.QueryTimeout.String(),
		"namespace_idle_timeout": c.NamespaceIdleTimeout.String(),
		"read_timeout":           c.ReadTimeout.String(),
		"write_timeout":          c.WriteTimeout.String(),
		"idle_timeout":           c.IdleTimeout.String(),
		"shutdown_timeout":       c.ShutdownTimeout.String(),
	}
}
