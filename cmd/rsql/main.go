package main

import (
	"os"

	"github.com/ValentinKolb/rsql/internal/cli"
)

var executeRoot = func() error {
	return cli.NewRootCmd().Execute()
}

func run() int {
	if err := executeRoot(); err != nil {
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
