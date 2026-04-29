package app

import "testing"

func TestToSlogLevel(t *testing.T) {
	if got := toSlogLevel("debug"); got.String() != "DEBUG" {
		t.Fatalf("unexpected level: %s", got)
	}
	if got := toSlogLevel("warn"); got.String() != "WARN" {
		t.Fatalf("unexpected level: %s", got)
	}
	if got := toSlogLevel("error"); got.String() != "ERROR" {
		t.Fatalf("unexpected level: %s", got)
	}
	if got := toSlogLevel("anything"); got.String() != "INFO" {
		t.Fatalf("unexpected fallback level: %s", got)
	}
}
