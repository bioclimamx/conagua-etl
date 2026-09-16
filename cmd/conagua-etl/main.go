// Package main is the conagua-etl entry point. All logic lives in
// internal/cmd; main only prints the command tree's error (once) and
// translates it into a process exit code.
package main

import (
	"fmt"
	"os"

	"github.com/bioclimamx/conagua-etl/internal/cmd"
)

func main() {
	err := cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
	}
	os.Exit(cmd.ExitCode(err))
}
