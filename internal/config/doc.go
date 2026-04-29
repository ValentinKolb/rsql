// Package config provides runtime configuration loading and validation for rsql.
// It centralizes flag defaults, environment parsing, and typed configuration
// values used by the application bootstrap.
//
// The package focuses on:
//   - Cobra/Viper compatible configuration wiring
//   - Environment variable and dotenv loading
//   - Typed runtime settings with validation and redaction helpers
package config
