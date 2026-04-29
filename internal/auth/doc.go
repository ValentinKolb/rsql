// Package auth provides bearer-token authentication helpers for rsql HTTP APIs.
// It keeps authorization checks centralized and consistent across all endpoints.
//
// The package focuses on:
//   - Authorization header parsing
//   - Constant-time token comparison
//   - JSON error responses for unauthorized requests
package auth
