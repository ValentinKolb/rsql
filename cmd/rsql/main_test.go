package main

import (
	"errors"
	"os"
	"os/exec"
	"testing"
)

func TestRunSuccessAndError(t *testing.T) {
	old := executeRoot
	defer func() { executeRoot = old }()

	executeRoot = func() error { return nil }
	if code := run(); code != 0 {
		t.Fatalf("expected exit code 0, got %d", code)
	}

	executeRoot = func() error { return assertErr{} }
	if code := run(); code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
}

type assertErr struct{}

func (assertErr) Error() string { return "boom" }

func TestMainExitCodeSuccess(t *testing.T) {
	if os.Getenv("RSQL_TEST_MAIN_SUCCESS") == "1" {
		executeRoot = func() error { return nil }
		main()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestMainExitCodeSuccess")
	cmd.Env = append(os.Environ(), "RSQL_TEST_MAIN_SUCCESS=1")
	err := cmd.Run()
	if err != nil {
		t.Fatalf("expected exit code 0, got %v", err)
	}
}

func TestMainExitCodeError(t *testing.T) {
	if os.Getenv("RSQL_TEST_MAIN_ERROR") == "1" {
		executeRoot = func() error { return errors.New("boom") }
		main()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestMainExitCodeError")
	cmd.Env = append(os.Environ(), "RSQL_TEST_MAIN_ERROR=1")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected non-zero exit")
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("expected ExitError, got %T", err)
	}
	if exitErr.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %d", exitErr.ExitCode())
	}
}
