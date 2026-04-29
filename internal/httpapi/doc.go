// Package httpapi defines HTTP routing, middleware composition, and API response
// behavior for the rsql service.
//
// The package focuses on:
//   - Stable JSON response and error envelopes
//   - Cross-cutting middleware wiring (auth, recovery, logging, request-id)
//   - Endpoint registration for the public rsql API surface
package httpapi
